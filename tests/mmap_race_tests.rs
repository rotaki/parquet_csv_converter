//! Race condition tests for the lock-free mmap writer
//! 
//! Run these tests with ThreadSanitizer:
//! RUSTFLAGS="-Z sanitizer=thread" cargo test --target x86_64-apple-darwin test_race_ -- --nocapture

#![cfg(test)]

use parquet_csv_converter::mmap_csv_writer::MmapCsvWriter;
use std::sync::{Arc, Barrier, atomic::{AtomicBool, Ordering}};
use std::thread;
use tempfile::tempdir;

#[test]
fn test_race_concurrent_offset_allocation() {
    // This test verifies that atomic offset allocation is race-free
    let dir = tempdir().unwrap();
    let path = dir.path().join("race_offset.csv");
    
    let writer = Arc::new(MmapCsvWriter::new(&path, 100 * 1024 * 1024, None).unwrap());
    writer.write_header("id").unwrap();
    
    let num_threads = 32;
    let writes_per_thread = 10000;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let writer = writer.clone();
            let barrier = barrier.clone();
            
            thread::spawn(move || {
                let thread_writer = writer.get_thread_writer();
                
                // Synchronize to maximize contention
                barrier.wait();
                
                for i in 0..writes_per_thread {
                    let line = vec![format!("{}", thread_id * writes_per_thread + i)];
                    thread_writer.write_lines(&line).unwrap();
                }
            })
        })
        .collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    Arc::try_unwrap(writer).unwrap().finalize().unwrap();
    
    // Verify no data corruption
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 1 + num_threads * writes_per_thread);
}

#[test]
fn test_race_boundary_writes() {
    // Test writes at exact boundaries to detect any off-by-one races
    let dir = tempdir().unwrap();
    let path = dir.path().join("race_boundary.csv");
    
    let writer = Arc::new(MmapCsvWriter::new(&path, 50 * 1024 * 1024, None).unwrap());
    writer.write_header("data").unwrap();
    
    let num_threads = 16;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    // Each thread writes strings of different lengths to create varied boundaries
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let writer = writer.clone();
            let barrier = barrier.clone();
            
            thread::spawn(move || {
                let thread_writer = writer.get_thread_writer();
                barrier.wait();
                
                for i in 0..1000 {
                    // Vary string length based on thread_id to create different patterns
                    let data = "x".repeat((thread_id + 1) * (i % 10 + 1));
                    thread_writer.write_lines(&vec![data]).unwrap();
                }
            })
        })
        .collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    Arc::try_unwrap(writer).unwrap().finalize().unwrap();
    
    // Verify file integrity
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 1 + num_threads * 1000);
}

#[test]
fn test_race_rapid_small_writes() {
    // Many threads doing tiny writes to maximize contention
    let dir = tempdir().unwrap();
    let path = dir.path().join("race_rapid.csv");
    
    let writer = Arc::new(MmapCsvWriter::new(&path, 20 * 1024 * 1024, None).unwrap());
    writer.write_header("x").unwrap();
    
    let num_threads = 64;
    let stop_flag = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let writer = writer.clone();
            let stop_flag = stop_flag.clone();
            let barrier = barrier.clone();
            
            thread::spawn(move || {
                let thread_writer = writer.get_thread_writer();
                let mut count = 0;
                
                barrier.wait();
                
                while !stop_flag.load(Ordering::Relaxed) {
                    thread_writer.write_lines(&vec![thread_id.to_string()]).unwrap();
                    count += 1;
                    
                    // Occasionally yield to create more interleaving
                    if count % 100 == 0 {
                        thread::yield_now();
                    }
                }
                
                count
            })
        })
        .collect();
    
    // Let threads run for a bit
    thread::sleep(std::time::Duration::from_millis(100));
    stop_flag.store(true, Ordering::Relaxed);
    
    let total_writes: usize = handles.into_iter()
        .map(|h| h.join().unwrap())
        .sum();
    
    Arc::try_unwrap(writer).unwrap().finalize().unwrap();
    
    // Verify we have the expected number of lines
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 1 + total_writes);
}

#[test]
fn test_race_pattern_verification() {
    // Write specific patterns and verify no corruption
    let dir = tempdir().unwrap();
    let path = dir.path().join("race_pattern.csv");
    
    let writer = Arc::new(MmapCsvWriter::new(&path, 50 * 1024 * 1024, None).unwrap());
    writer.write_header("thread,seq,checksum").unwrap();
    
    let num_threads = 8;
    let sequences_per_thread = 5000;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let writer = writer.clone();
            let barrier = barrier.clone();
            
            thread::spawn(move || {
                let thread_writer = writer.get_thread_writer();
                barrier.wait();
                
                for seq in 0..sequences_per_thread {
                    // Create a line with a checksum
                    let checksum = thread_id * 1000000 + seq;
                    let line = format!("{},{},{}", thread_id, seq, checksum);
                    thread_writer.write_lines(&vec![line]).unwrap();
                    
                    // Add some chaos
                    if seq % 137 == 0 {
                        thread::yield_now();
                    }
                }
            })
        })
        .collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    Arc::try_unwrap(writer).unwrap().finalize().unwrap();
    
    // Verify all patterns are intact
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().skip(1).collect(); // Skip header
    
    assert_eq!(lines.len(), num_threads * sequences_per_thread);
    
    // Verify each line's checksum
    for line in lines {
        let parts: Vec<&str> = line.split(',').collect();
        assert_eq!(parts.len(), 3);
        
        let thread_id: usize = parts[0].parse().unwrap();
        let seq: usize = parts[1].parse().unwrap();
        let checksum: usize = parts[2].parse().unwrap();
        
        assert_eq!(checksum, thread_id * 1000000 + seq);
    }
}

#[test]
fn test_race_mixed_operations() {
    // Mix different types of writes to stress test
    let dir = tempdir().unwrap();
    let path = dir.path().join("race_mixed.csv");
    
    let writer = Arc::new(MmapCsvWriter::new(&path, 100 * 1024 * 1024, None).unwrap());
    writer.write_header("type,data").unwrap();
    
    let barrier = Arc::new(Barrier::new(3));
    
    // Thread 1: Small frequent writes
    let h1 = {
        let writer = writer.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            let thread_writer = writer.get_thread_writer();
            barrier.wait();
            
            for i in 0..10000 {
                thread_writer.write_lines(&vec![format!("small,{}", i)]).unwrap();
            }
        })
    };
    
    // Thread 2: Large batch writes
    let h2 = {
        let writer = writer.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            let thread_writer = writer.get_thread_writer();
            barrier.wait();
            
            for batch in 0..100 {
                let lines: Vec<String> = (0..100)
                    .map(|i| format!("batch,{}_{}", batch, i))
                    .collect();
                thread_writer.write_lines(&lines).unwrap();
            }
        })
    };
    
    // Thread 3: Variable size writes
    let h3 = {
        let writer = writer.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            let thread_writer = writer.get_thread_writer();
            barrier.wait();
            
            for i in 0..1000 {
                let size = (i % 10) + 1;
                let lines: Vec<String> = (0..size)
                    .map(|j| format!("variable,{}_{}_{}", i, j, "x".repeat(j)))
                    .collect();
                thread_writer.write_lines(&lines).unwrap();
            }
        })
    };
    
    h1.join().unwrap();
    h2.join().unwrap();
    h3.join().unwrap();
    
    Arc::try_unwrap(writer).unwrap().finalize().unwrap();
    
    // Verify total line count
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    
    let expected_lines = 1 + 10000 + 10000 + (0..1000).map(|i| (i % 10) + 1).sum::<usize>();
    assert_eq!(lines.len(), expected_lines);
}

#[test]
fn test_race_extreme_concurrency() {
    // Push the limits with many threads
    let dir = tempdir().unwrap();
    let path = dir.path().join("race_extreme.csv");
    
    let writer = Arc::new(MmapCsvWriter::new(&path, 200 * 1024 * 1024, None).unwrap());
    writer.write_header("tid,data").unwrap();
    
    let num_threads = 128;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let writer = writer.clone();
            let barrier = barrier.clone();
            
            thread::spawn(move || {
                let thread_writer = writer.get_thread_writer();
                barrier.wait();
                
                // Each thread writes a burst
                for i in 0..100 {
                    let line = format!("{},{}", thread_id, i);
                    thread_writer.write_lines(&vec![line]).unwrap();
                }
            })
        })
        .collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    Arc::try_unwrap(writer).unwrap().finalize().unwrap();
    
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 1 + num_threads * 100);
}