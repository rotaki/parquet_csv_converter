//! Optimized parallel CSV writer that maintains Direct I/O performance without data corruption
//! 
//! This implementation uses a two-tier approach:
//! 1. Each thread accumulates data in large buffers (4-8MB)
//! 2. When buffers are full, they're written sequentially to avoid alignment issues

use crate::{
    aligned_buffer::AlignedBuffer,
    constants::{align_up, open_file_with_direct_io, DIRECT_IO_ALIGNMENT},
    file_manager::pwrite_fd,
    IoStatsTracker,
};
use std::collections::BTreeMap;
use std::os::unix::io::{IntoRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::time::Duration;

/// Optimized parallel CSV writer
pub struct OptimizedParallelCsvWriter {
    fd: RawFd,
    next_write_offset: Arc<AtomicU64>,
    pending_writes: Arc<Mutex<BTreeMap<u64, Vec<u8>>>>,
    flusher_handle: Option<thread::JoinHandle<()>>,
    shutdown: Arc<AtomicU64>, // 0 = running, 1 = shutdown
    write_cv: Arc<Condvar>,
    io_tracker: Option<IoStatsTracker>,
}

impl OptimizedParallelCsvWriter {
    /// Create a new optimized writer
    pub fn new(
        path: impl AsRef<Path>, 
        initial_offset: u64,
        io_tracker: Option<IoStatsTracker>
    ) -> Result<Self, String> {
        let file = open_file_with_direct_io(path.as_ref())
            .map_err(|e| format!("Failed to open file: {}", e))?;
        let fd = file.into_raw_fd();
        
        let next_write_offset = Arc::new(AtomicU64::new(initial_offset));
        let pending_writes = Arc::new(Mutex::new(BTreeMap::new()));
        let shutdown = Arc::new(AtomicU64::new(0));
        let write_cv = Arc::new(Condvar::new());
        
        // Spawn background flusher thread
        let flusher_handle = {
            let fd = fd;
            let next_offset = next_write_offset.clone();
            let pending = pending_writes.clone();
            let shutdown = shutdown.clone();
            let cv = write_cv.clone();
            let tracker = io_tracker.clone();
            
            thread::spawn(move || {
                let mut local_next_offset = initial_offset;
                
                loop {
                    // Check for shutdown
                    if shutdown.load(Ordering::Relaxed) == 1 {
                        break;
                    }
                    
                    // Try to write pending blocks in order
                    let mut made_progress = false;
                    
                    {
                        let mut pending_map = pending.lock().unwrap();
                        
                        // Process all consecutive blocks starting from next_offset
                        while let Some(data) = pending_map.remove(&local_next_offset) {
                            drop(pending_map); // Release lock during I/O
                            
                            // Write the data
                            if let Err(e) = Self::write_aligned_data(fd, &data, local_next_offset) {
                                eprintln!("Write error: {}", e);
                                return;
                            }
                            
                            // Update stats
                            if let Some(ref tracker) = tracker {
                                tracker.add_write(1, data.len() as u64);
                            }
                            
                            local_next_offset += data.len() as u64;
                            next_offset.store(local_next_offset, Ordering::Release);
                            made_progress = true;
                            
                            pending_map = pending.lock().unwrap();
                        }
                    }
                    
                    if !made_progress {
                        // Wait for new data
                        let mut pending_map = pending.lock().unwrap();
                        let _ = cv.wait_timeout(pending_map, Duration::from_millis(10)).unwrap();
                    }
                }
                
                // Final flush on shutdown
                let pending_map = pending.lock().unwrap();
                let mut blocks: Vec<_> = pending_map.iter().collect();
                blocks.sort_by_key(|(k, _)| *k);
                drop(pending_map);
                
                for (offset, data) in blocks {
                    if let Err(e) = Self::write_aligned_data(fd, data, *offset) {
                        eprintln!("Final flush error: {}", e);
                    }
                }
            })
        };
        
        Ok(Self {
            fd,
            next_write_offset,
            pending_writes,
            flusher_handle: Some(flusher_handle),
            shutdown,
            write_cv,
            io_tracker,
        })
    }
    
    /// Write aligned data using Direct I/O
    fn write_aligned_data(fd: RawFd, data: &[u8], offset: u64) -> Result<(), String> {
        let aligned_size = align_up(data.len(), DIRECT_IO_ALIGNMENT);
        let mut buffer = AlignedBuffer::new(aligned_size, DIRECT_IO_ALIGNMENT)
            .map_err(|e| format!("Failed to create buffer: {}", e))?;
        
        // Copy data
        buffer.as_mut_slice()[..data.len()].copy_from_slice(data);
        
        // Fill rest with zeros (not spaces!) - zeros are safe in this context
        // because we're writing past the actual file content
        for i in data.len()..aligned_size {
            buffer.as_mut_slice()[i] = 0;
        }
        
        // Write
        let written = pwrite_fd(fd, buffer.as_slice(), offset)
            .map_err(|e| format!("Write failed: {}", e))?;
            
        if written != aligned_size {
            return Err(format!("Incomplete write: {} vs {}", written, aligned_size));
        }
        
        Ok(())
    }
    
    /// Get a thread writer
    pub fn get_thread_writer(&self) -> OptimizedThreadWriter {
        OptimizedThreadWriter {
            writer: self,
            buffer: Vec::with_capacity(4 * 1024 * 1024), // 4MB local buffer
            current_offset: 0,
        }
    }
    
    /// Shutdown the writer
    pub fn shutdown(mut self) -> Result<(), String> {
        self.shutdown.store(1, Ordering::Relaxed);
        self.write_cv.notify_all();
        
        if let Some(handle) = self.flusher_handle.take() {
            handle.join().map_err(|_| "Flusher thread panicked".to_string())?;
        }
        
        // Close file
        unsafe {
            libc::close(self.fd);
        }
        
        Ok(())
    }
}

impl Drop for OptimizedParallelCsvWriter {
    fn drop(&mut self) {
        self.shutdown.store(1, Ordering::Relaxed);
        self.write_cv.notify_all();
    }
}

/// Thread writer that batches data
pub struct OptimizedThreadWriter<'a> {
    writer: &'a OptimizedParallelCsvWriter,
    buffer: Vec<u8>,
    current_offset: u64,
}

impl<'a> OptimizedThreadWriter<'a> {
    /// Write lines with internal batching
    pub fn write_lines_buffered(
        &mut self,
        lines: impl Iterator<Item = String>,
        flush_size: usize,
    ) -> Result<(), String> {
        for line in lines {
            // Add line to buffer
            self.buffer.extend_from_slice(line.as_bytes());
            self.buffer.push(b'\n');
            
            // Flush if buffer is large enough
            if self.buffer.len() >= flush_size {
                self.flush()?;
            }
        }
        
        // Final flush
        if !self.buffer.is_empty() {
            self.flush()?;
        }
        
        Ok(())
    }
    
    /// Flush the current buffer
    fn flush(&mut self) -> Result<(), String> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        
        // Get offset for this chunk
        let offset = self.writer.next_write_offset
            .fetch_add(self.buffer.len() as u64, Ordering::AcqRel);
        
        // Clone data for async write
        let data = std::mem::take(&mut self.buffer);
        self.buffer.clear();
        
        // Add to pending writes
        {
            let mut pending = self.writer.pending_writes.lock().unwrap();
            pending.insert(offset, data);
        }
        
        // Notify flusher
        self.writer.write_cv.notify_one();
        
        Ok(())
    }
}