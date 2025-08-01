//! Memory-mapped CSV writer for parallel output without alignment concerns
//!
//! This implementation uses lock-free parallel writes by:
//! 1. Each thread atomically reserves a section of the mmap buffer using fetch_add
//! 2. Threads write directly to their reserved sections using unsafe pointer operations
//! 3. No mutex is needed because each thread has exclusive access to its section
//!
//! This approach maximizes performance by eliminating all synchronization overhead
//! during the write phase. The only synchronization is the atomic offset allocation.

use libc::{
    c_void, ftruncate, mmap, msync, munmap, MAP_FAILED, MAP_SHARED, MS_SYNC, PROT_READ, PROT_WRITE,
};
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Memory-mapped CSV writer that allows true parallel writes
pub struct MmapCsvWriter {
    /// Raw pointer to the memory-mapped region
    mmap_ptr: *mut u8,
    /// Length of the memory-mapped region
    mmap_len: usize,
    /// Current write position (atomic)
    write_position: Arc<AtomicUsize>,
    /// The actual file handle (kept for truncation)
    file: File,
}

impl std::fmt::Debug for MmapCsvWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapCsvWriter")
            .field("mmap_ptr", &(self.mmap_ptr as usize))
            .field("mmap_len", &self.mmap_len)
            .field(
                "write_position",
                &self.write_position.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl MmapCsvWriter {
    /// Create a new memory-mapped CSV writer
    ///
    /// # Arguments
    /// * `path` - Output file path
    /// * `estimated_size` - Estimated file size (will be expanded if needed)
    /// * `io_tracker` - Optional I/O statistics tracker
    pub fn new(path: impl AsRef<Path>, estimated_size: usize) -> Result<Self, String> {
        // Create file with estimated size
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())
            .map_err(|e| format!("Failed to create file: {}", e))?;

        let fd = file.as_raw_fd();

        // Set file size using ftruncate
        unsafe {
            if ftruncate(fd, estimated_size as i64) != 0 {
                return Err(format!(
                    "Failed to set file size: {}",
                    std::io::Error::last_os_error()
                ));
            }
        }

        // Memory map the file
        let mmap_ptr = unsafe {
            let ptr = mmap(
                ptr::null_mut(),
                estimated_size,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd,
                0,
            );

            if std::ptr::eq(ptr, MAP_FAILED) {
                return Err(format!(
                    "Failed to memory map file: {}",
                    std::io::Error::last_os_error()
                ));
            }

            ptr as *mut u8
        };

        Ok(Self {
            mmap_ptr,
            mmap_len: estimated_size,
            write_position: Arc::new(AtomicUsize::new(0)),
            file,
        })
    }

    /// Write header and return the number of bytes written
    pub fn write_header(&self, header: &str) -> Result<usize, String> {
        let header_bytes = format!("{}\n", header);
        let bytes_len = header_bytes.len();

        if bytes_len > self.mmap_len {
            return Err(format!("Header too large: {} bytes", bytes_len));
        }

        // Write header at the beginning
        unsafe {
            ptr::copy_nonoverlapping(header_bytes.as_ptr(), self.mmap_ptr, bytes_len);
        }

        // Update position
        self.write_position.store(bytes_len, Ordering::SeqCst);

        Ok(bytes_len)
    }

    /// Get a writer handle for a thread
    pub fn get_thread_writer(&self) -> MmapThreadWriter {
        MmapThreadWriter {
            mmap_ptr: self.mmap_ptr,
            mmap_len: self.mmap_len,
            write_position: self.write_position.clone(),
        }
    }

    /// Finalize the file by truncating to actual size and syncing
    ///
    /// # Safety
    /// This method consumes self and unmaps the memory. Any MmapThreadWriter instances
    /// created from this writer will have invalid pointers after this call.
    /// Ensure all thread writers have completed their work before calling finalize.
    pub fn finalize(mut self) -> Result<(), String> {
        // Get final position
        let final_size = self.write_position.load(Ordering::SeqCst);

        // Sync the memory map
        unsafe {
            if msync(self.mmap_ptr as *mut c_void, self.mmap_len, MS_SYNC) != 0 {
                return Err(format!(
                    "Failed to sync mmap: {}",
                    std::io::Error::last_os_error()
                ));
            }
        }

        // Unmap the memory
        unsafe {
            if munmap(self.mmap_ptr as *mut c_void, self.mmap_len) != 0 {
                return Err(format!(
                    "Failed to unmap memory: {}",
                    std::io::Error::last_os_error()
                ));
            }
        }

        // Mark the pointer as null to prevent use-after-unmap in Drop
        self.mmap_ptr = ptr::null_mut();

        // Truncate file to actual size
        self.file
            .set_len(final_size as u64)
            .map_err(|e| format!("Failed to truncate file: {}", e))?;

        self.file
            .sync_all()
            .map_err(|e| format!("Failed to sync file: {}", e))?;

        Ok(())
    }
}

impl Drop for MmapCsvWriter {
    fn drop(&mut self) {
        // Only unmap if the pointer is still valid (not already unmapped in finalize)
        if !self.mmap_ptr.is_null() {
            unsafe {
                let _ = munmap(self.mmap_ptr as *mut c_void, self.mmap_len);
            }
        }
    }
}

// SAFETY: MmapCsvWriter is safe to send between threads because:
// - The mmap_ptr points to a memory-mapped region that is valid for the lifetime of the struct
// - All access is coordinated through atomic operations
// - The file handle is only used in finalize() which consumes self
unsafe impl Send for MmapCsvWriter {}
unsafe impl Sync for MmapCsvWriter {}

/// Thread-specific writer for memory-mapped file
pub struct MmapThreadWriter {
    mmap_ptr: *mut u8,
    mmap_len: usize,
    write_position: Arc<AtomicUsize>,
}

// SAFETY: MmapThreadWriter is safe to send between threads because:
// - The mmap_ptr points to a memory-mapped region that outlives the writer
// - All access is coordinated through atomic offset allocation
unsafe impl Send for MmapThreadWriter {}
unsafe impl Sync for MmapThreadWriter {}

impl MmapThreadWriter {
    /// Write a batch of CSV lines
    pub fn write_lines(&self, lines: &[String]) -> Result<(), String> {
        if lines.is_empty() {
            return Ok(());
        }

        // Calculate total size
        let total_size: usize = lines.iter().map(|line| line.len() + 1).sum();

        // Reserve space atomically using Acquire-Release ordering for better performance
        let start_pos = self.write_position.fetch_add(total_size, Ordering::AcqRel);
        let end_pos = start_pos + total_size;

        // Check bounds
        if end_pos > self.mmap_len {
            // Rollback the reservation to avoid wasting space
            self.write_position.fetch_sub(total_size, Ordering::AcqRel);
            return Err(format!(
                "File size exceeded: need {} bytes but only {} available. Consider increasing estimated size.",
                end_pos, self.mmap_len
            ));
        }

        // Write all lines directly to mmap using unsafe pointer operations
        unsafe {
            let mut current_pos = start_pos;

            for line in lines {
                let line_bytes = line.as_bytes();

                // Copy line data
                ptr::copy_nonoverlapping(
                    line_bytes.as_ptr(),
                    self.mmap_ptr.add(current_pos),
                    line_bytes.len(),
                );
                current_pos += line_bytes.len();

                // Add newline
                *self.mmap_ptr.add(current_pos) = b'\n';
                current_pos += 1;
            }
        }

        Ok(())
    }

    /// Write lines with buffering for better performance
    pub fn write_lines_buffered(
        &self,
        lines: impl Iterator<Item = String>,
        batch_size: usize,
    ) -> Result<(), String> {
        let mut batch = Vec::with_capacity(100);
        let mut current_batch_size = 0;

        for line in lines {
            let line_size = line.len() + 1;

            // Check if we should flush
            if current_batch_size + line_size > batch_size && !batch.is_empty() {
                self.write_lines(&batch)?;
                batch.clear();
                current_batch_size = 0;
            }

            current_batch_size += line_size;
            batch.push(line);

            // Also flush if we have too many lines
            if batch.len() >= 1000 {
                self.write_lines(&batch)?;
                batch.clear();
                current_batch_size = 0;
            }
        }

        // Write remaining lines
        if !batch.is_empty() {
            self.write_lines(&batch)?;
        }

        Ok(())
    }
}

/// Estimate CSV file size from Parquet metadata
pub fn estimate_csv_size(num_rows: usize, num_columns: usize, avg_field_size: usize) -> usize {
    // Estimate: rows * (columns * avg_field_size + commas + newline)
    // Add 20% buffer for safety
    let row_size = num_columns * avg_field_size + num_columns - 1 + 1; // commas + newline
    let base_size = num_rows * row_size;
    let buffer = base_size / 5; // 20% buffer

    base_size + buffer + 1024 // Extra 1KB for header
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use tempfile::tempdir;

    #[test]
    fn test_parallel_mmap_writes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_mmap.csv");

        // Create writer with 10MB estimated size
        let writer = MmapCsvWriter::new(&path, 10 * 1024 * 1024).unwrap();

        // Write header
        writer.write_header("id,name,value").unwrap();

        // Spawn multiple threads
        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let thread_writer = writer.get_thread_writer();

                thread::spawn(move || {
                    let lines: Vec<String> = (0..1000)
                        .map(|i| {
                            format!(
                                "{},{},thread_{}_row_{}",
                                thread_id * 1000 + i,
                                format!("name_{}", i),
                                thread_id,
                                i
                            )
                        })
                        .collect();

                    thread_writer.write_lines(&lines).unwrap();
                })
            })
            .collect();

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Finalize file
        writer.finalize().unwrap();

        // Verify content
        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // Should have header + 4000 data lines
        assert_eq!(lines.len(), 4001);
        assert_eq!(lines[0], "id,name,value");

        // Verify some data is present from each thread
        assert!(content.contains("thread_0_row_"));
        assert!(content.contains("thread_1_row_"));
        assert!(content.contains("thread_2_row_"));
        assert!(content.contains("thread_3_row_"));
    }

    #[test]
    fn test_size_estimation() {
        // 1000 rows, 3 columns, avg 10 chars per field
        let estimated = estimate_csv_size(1000, 3, 10);

        // Each row: ~30 chars + 2 commas + 1 newline = 33 chars
        // 1000 * 33 = 33,000
        // Plus 20% buffer = 39,600
        // Plus 1KB header = 40,624

        assert!(estimated >= 40_000);
        assert!(estimated <= 50_000);
    }
}
