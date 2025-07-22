//! Simple parallel CSV writer that bypasses Direct I/O for the output file
//! 
//! While the input Parquet file uses Direct I/O for reading, the output CSV file
//! uses regular buffered I/O to avoid alignment issues and data corruption.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::IoStatsTracker;

/// Simple parallel CSV writer using buffered I/O
pub struct SimpleParallelCsvWriter {
    /// File handle
    file: Arc<Mutex<BufWriter<File>>>,
    /// Current write position (for stats only)
    bytes_written: Arc<AtomicU64>,
    /// I/O tracker
    io_tracker: Option<IoStatsTracker>,
}

impl SimpleParallelCsvWriter {
    /// Create a new writer
    pub fn new(path: impl AsRef<Path>, io_tracker: Option<IoStatsTracker>) -> Result<Self, String> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())
            .map_err(|e| format!("Failed to open file: {}", e))?;
        
        // Use a large buffer for better performance
        let buf_writer = BufWriter::with_capacity(8 * 1024 * 1024, file); // 8MB buffer
        
        Ok(Self {
            file: Arc::new(Mutex::new(buf_writer)),
            bytes_written: Arc::new(AtomicU64::new(0)),
            io_tracker,
        })
    }
    
    /// Write the header
    pub fn write_header(&self, header: &str) -> Result<(), String> {
        let mut file = self.file.lock().unwrap();
        writeln!(file, "{}", header)
            .map_err(|e| format!("Failed to write header: {}", e))?;
        
        let bytes = header.len() + 1;
        self.bytes_written.fetch_add(bytes as u64, Ordering::Relaxed);
        
        if let Some(ref tracker) = self.io_tracker {
            tracker.add_write(1, bytes as u64);
        }
        
        Ok(())
    }
    
    /// Get a thread writer
    pub fn get_thread_writer(&self) -> SimpleThreadWriter {
        SimpleThreadWriter {
            file: self.file.clone(),
            bytes_written: self.bytes_written.clone(),
            io_tracker: self.io_tracker.clone(),
        }
    }
    
    /// Flush all pending writes
    pub fn flush(&self) -> Result<(), String> {
        let mut file = self.file.lock().unwrap();
        file.flush()
            .map_err(|e| format!("Failed to flush: {}", e))
    }
    
    /// Get total bytes written
    pub fn get_bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }
}

/// Thread-specific writer
pub struct SimpleThreadWriter {
    file: Arc<Mutex<BufWriter<File>>>,
    bytes_written: Arc<AtomicU64>,
    io_tracker: Option<IoStatsTracker>,
}

impl SimpleThreadWriter {
    /// Write a batch of lines
    pub fn write_lines(&self, lines: &[String]) -> Result<(), String> {
        if lines.is_empty() {
            return Ok(());
        }
        
        // Prepare all data in memory first
        let mut buffer = String::with_capacity(lines.iter().map(|l| l.len() + 1).sum());
        for line in lines {
            buffer.push_str(line);
            buffer.push('\n');
        }
        
        let bytes = buffer.len();
        
        // Write under lock
        {
            let mut file = self.file.lock().unwrap();
            file.write_all(buffer.as_bytes())
                .map_err(|e| format!("Failed to write: {}", e))?;
        }
        
        // Update stats
        self.bytes_written.fetch_add(bytes as u64, Ordering::Relaxed);
        
        if let Some(ref tracker) = self.io_tracker {
            tracker.add_write(1, bytes as u64);
        }
        
        Ok(())
    }
    
    /// Write lines with buffering
    pub fn write_lines_buffered(
        &self,
        lines: impl Iterator<Item = String>,
        buffer_size: usize,
    ) -> Result<(), String> {
        let mut batch = Vec::with_capacity(100);
        let mut batch_size = 0;
        
        for line in lines {
            let line_size = line.len() + 1;
            
            if batch_size + line_size > buffer_size && !batch.is_empty() {
                self.write_lines(&batch)?;
                batch.clear();
                batch_size = 0;
            }
            
            batch_size += line_size;
            batch.push(line);
            
            if batch.len() >= 1000 {
                self.write_lines(&batch)?;
                batch.clear();
                batch_size = 0;
            }
        }
        
        if !batch.is_empty() {
            self.write_lines(&batch)?;
        }
        
        Ok(())
    }
}

/// Alternative: Use memory-mapped file for true parallel writes
pub struct MmapParallelCsvWriter {
    // Implementation using memory-mapped files
    // This allows true parallel writes without alignment issues
    // but requires pre-calculating file size
}