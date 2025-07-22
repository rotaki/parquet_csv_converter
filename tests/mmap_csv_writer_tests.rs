//! Comprehensive tests for the lock-free memory-mapped CSV writer

use parquet_csv_converter::mmap_csv_writer::{MmapCsvWriter, estimate_csv_size};
use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;

#[test]
fn test_basic_write() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("basic.csv");
    
    let writer = MmapCsvWriter::new(&path, 1024 * 1024, None).unwrap();
    
    // Write header
    let header_len = writer.write_header("id,name,value").unwrap();
    assert_eq!(header_len, 14); // "id,name,value\n"
    
    // Get thread writer and write some data
    let thread_writer = writer.get_thread_writer();
    let lines = vec![
        "1,Alice,100".to_string(),
        "2,Bob,200".to_string(),
        "3,Charlie,300".to_string(),
    ];
    
    thread_writer.write_lines(&lines).unwrap();
    
    // Finalize
    writer.finalize().unwrap();
    
    // Verify content
    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content, "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n");
}

#[test]
fn test_concurrent_writes_no_overlap() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("concurrent.csv");
    
    let writer = Arc::new(MmapCsvWriter::new(&path, 10 * 1024 * 1024, None).unwrap());
    writer.write_header("thread_id,row_id,data").unwrap();
    
    let num_threads = 8;
    let rows_per_thread = 1000;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let writer = writer.clone();
            let barrier = barrier.clone();
            
            thread::spawn(move || {
                let thread_writer = writer.get_thread_writer();
                
                // Synchronize start
                barrier.wait();
                
                // Write in batches
                for batch in 0..10 {
                    let lines: Vec<String> = (0..100)
                        .map(|i| {
                            let row_id = batch * 100 + i;
                            format!("{},{},data_{}_{}",thread_id, row_id, thread_id, row_id)
                        })
                        .collect();
                    
                    thread_writer.write_lines(&lines).unwrap();
                }
            })
        })
        .collect();
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Finalize
    Arc::try_unwrap(writer).unwrap().finalize().unwrap();
    
    // Verify all data is present and no corruption
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    
    // Should have header + all data lines
    assert_eq!(lines.len(), 1 + num_threads * rows_per_thread);
    
    // Verify no duplicate lines
    let unique_lines: HashSet<&str> = lines.iter().copied().collect();
    assert_eq!(unique_lines.len(), lines.len());
    
    // Verify each thread's data is present
    for thread_id in 0..num_threads {
        let thread_lines: Vec<_> = lines.iter()
            .filter(|line| line.starts_with(&format!("{},", thread_id)))
            .collect();
        assert_eq!(thread_lines.len(), rows_per_thread);
    }
}

#[test]
fn test_stress_many_small_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("stress_small.csv");
    
    let writer = Arc::new(MmapCsvWriter::new(&path, 50 * 1024 * 1024, None).unwrap());
    writer.write_header("id,data").unwrap();
    
    let num_threads = 16;
    let writes_per_thread = 10000;
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let writer = writer.clone();
            
            thread::spawn(move || {
                let thread_writer = writer.get_thread_writer();
                
                for i in 0..writes_per_thread {
                    let line = vec![format!("{},{}", thread_id * writes_per_thread + i, "x".repeat(20))];
                    thread_writer.write_lines(&line).unwrap();
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
    assert_eq!(lines.len(), 1 + num_threads * writes_per_thread);
}

#[test]
fn test_large_batch_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("large_batch.csv");
    
    let writer = MmapCsvWriter::new(&path, 100 * 1024 * 1024, None).unwrap();
    writer.write_header("id,value").unwrap();
    
    let thread_writer = writer.get_thread_writer();
    
    // Write a very large batch
    let large_batch: Vec<String> = (0..100000)
        .map(|i| format!("{},{}", i, i * 2))
        .collect();
    
    thread_writer.write_lines(&large_batch).unwrap();
    
    writer.finalize().unwrap();
    
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 100001);
}

#[test]
fn test_buffered_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("buffered.csv");
    
    let writer = MmapCsvWriter::new(&path, 10 * 1024 * 1024, None).unwrap();
    writer.write_header("id,name").unwrap();
    
    let thread_writer = writer.get_thread_writer();
    
    // Generate iterator of lines
    let lines = (0..10000).map(|i| format!("{},name_{}", i, i));
    
    // Write with buffering
    thread_writer.write_lines_buffered(lines, 1024 * 1024).unwrap();
    
    writer.finalize().unwrap();
    
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 10001);
    assert_eq!(lines[0], "id,name");
    assert_eq!(lines[1], "0,name_0");
    assert_eq!(lines[10000], "9999,name_9999");
}

#[test]
fn test_file_size_exceeded() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("exceeded.csv");
    
    // Create writer with small size
    let writer = MmapCsvWriter::new(&path, 1024, None).unwrap(); // Only 1KB
    writer.write_header("id,data").unwrap();
    
    let thread_writer = writer.get_thread_writer();
    
    // Try to write more than 1KB
    let large_data = vec!["x".repeat(1000)];
    let result = thread_writer.write_lines(&large_data);
    
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("File size exceeded"));
}

#[test]
fn test_empty_lines() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.csv");
    
    let writer = MmapCsvWriter::new(&path, 1024, None).unwrap();
    writer.write_header("col1,col2").unwrap();
    
    let thread_writer = writer.get_thread_writer();
    
    // Write empty lines array
    thread_writer.write_lines(&vec![]).unwrap();
    
    // Write some data
    thread_writer.write_lines(&vec!["a,b".to_string()]).unwrap();
    
    // Write empty again
    thread_writer.write_lines(&vec![]).unwrap();
    
    writer.finalize().unwrap();
    
    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content, "col1,col2\na,b\n");
}

#[test]
fn test_special_characters_in_csv() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("special.csv");
    
    let writer = MmapCsvWriter::new(&path, 1024 * 1024, None).unwrap();
    writer.write_header("id,text").unwrap();
    
    let thread_writer = writer.get_thread_writer();
    
    let lines = vec![
        r#"1,"quoted text""#.to_string(),
        r#"2,"text with, comma""#.to_string(),
        r#"3,"text with ""quotes"" inside""#.to_string(),
        r#"4,"multiline
text""#.to_string(),
    ];
    
    thread_writer.write_lines(&lines).unwrap();
    writer.finalize().unwrap();
    
    let content = std::fs::read_to_string(&path).unwrap();
    assert!(content.contains(r#""quoted text""#));
    assert!(content.contains(r#""text with, comma""#));
}

#[test]
fn test_unicode_content() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("unicode.csv");
    
    let writer = MmapCsvWriter::new(&path, 1024 * 1024, None).unwrap();
    writer.write_header("id,text").unwrap();
    
    let thread_writer = writer.get_thread_writer();
    
    let lines = vec![
        "1,Hello 世界".to_string(),
        "2,Здравствуй мир".to_string(),
        "3,🦀 Rust 🚀".to_string(),
        "4,日本語テキスト".to_string(),
    ];
    
    thread_writer.write_lines(&lines).unwrap();
    writer.finalize().unwrap();
    
    let content = std::fs::read_to_string(&path).unwrap();
    assert!(content.contains("世界"));
    assert!(content.contains("Здравствуй"));
    assert!(content.contains("🦀"));
    assert!(content.contains("日本語"));
}

#[test]
fn test_exact_file_size_boundary() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("boundary.csv");
    
    // Calculate exact size needed
    let header = "id,data";
    let line = "1,test";
    let total_size = header.len() + 1 + line.len() + 1; // +1 for newlines
    
    let writer = MmapCsvWriter::new(&path, total_size, None).unwrap();
    writer.write_header(header).unwrap();
    
    let thread_writer = writer.get_thread_writer();
    thread_writer.write_lines(&vec![line.to_string()]).unwrap();
    
    // Should succeed - exactly at boundary
    writer.finalize().unwrap();
    
    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content, "id,data\n1,test\n");
}

#[test]
fn test_concurrent_readers_during_write() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("concurrent_read.csv");
    
    let writer = Arc::new(MmapCsvWriter::new(&path, 10 * 1024 * 1024, None).unwrap());
    writer.write_header("id,value").unwrap();
    
    let barrier = Arc::new(Barrier::new(3)); // 2 writers + 1 reader
    
    // Spawn writers
    let writer1 = {
        let writer = writer.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            let thread_writer = writer.get_thread_writer();
            barrier.wait();
            for i in 0..1000 {
                thread_writer.write_lines(&vec![format!("w1_{},value_{}", i, i)]).unwrap();
            }
        })
    };
    
    let writer2 = {
        let writer = writer.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            let thread_writer = writer.get_thread_writer();
            barrier.wait();
            for i in 0..1000 {
                thread_writer.write_lines(&vec![format!("w2_{},value_{}", i, i)]).unwrap();
            }
        })
    };
    
    // Spawn reader (note: reading while writing is undefined behavior in general,
    // but we're testing that our implementation doesn't crash)
    let reader = {
        let path = path.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            barrier.wait();
            thread::sleep(std::time::Duration::from_millis(10));
            // Try to read file - may see partial data
            let _ = std::fs::read(&path);
        })
    };
    
    writer1.join().unwrap();
    writer2.join().unwrap();
    reader.join().unwrap();
    
    Arc::try_unwrap(writer).unwrap().finalize().unwrap();
    
    // Verify final state
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 2001); // header + 2000 data lines
}

#[test]
fn test_size_estimation() {
    // Test basic estimation
    let size = estimate_csv_size(1000, 3, 10);
    // Each row: 3 * 10 + 2 commas + 1 newline = 33 bytes
    // 1000 * 33 = 33,000
    // With 20% buffer = 39,600
    // Plus 1KB = 40,624
    assert!(size >= 40_000);
    assert!(size <= 50_000);
    
    // Test with larger fields
    let size = estimate_csv_size(100, 10, 50);
    // Each row: 10 * 50 + 9 commas + 1 newline = 510 bytes
    // 100 * 510 = 51,000
    // With 20% buffer = 61,200
    // Plus 1KB = 62,224
    assert!(size >= 60_000);
    assert!(size <= 70_000);
}

#[test]
fn test_rapid_alternating_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("alternating.csv");
    
    let writer = Arc::new(MmapCsvWriter::new(&path, 10 * 1024 * 1024, None).unwrap());
    writer.write_header("thread,sequence,data").unwrap();
    
    let num_threads = 4;
    let iterations = 1000;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let writer = writer.clone();
            let barrier = barrier.clone();
            
            thread::spawn(move || {
                let thread_writer = writer.get_thread_writer();
                barrier.wait();
                
                for i in 0..iterations {
                    let line = vec![format!("{},{},data", thread_id, i)];
                    thread_writer.write_lines(&line).unwrap();
                    
                    // Tiny sleep to increase interleaving
                    if i % 10 == 0 {
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
    
    // Verify all sequences are present
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().skip(1).collect(); // Skip header
    
    assert_eq!(lines.len(), num_threads * iterations);
    
    // Verify each thread wrote all its sequences
    for thread_id in 0..num_threads {
        for seq in 0..iterations {
            let expected = format!("{},{},data", thread_id, seq);
            assert!(lines.iter().any(|&line| line == expected));
        }
    }
}

#[test]
fn test_io_stats_tracking() {
    use parquet_csv_converter::IoStatsTracker;
    
    let dir = tempdir().unwrap();
    let path = dir.path().join("stats.csv");
    
    let io_tracker = IoStatsTracker::new();
    let writer = MmapCsvWriter::new(&path, 1024 * 1024, Some(io_tracker.clone())).unwrap();
    
    writer.write_header("id,value").unwrap();
    
    let thread_writer = writer.get_thread_writer();
    let lines = vec!["1,100".to_string(), "2,200".to_string()];
    thread_writer.write_lines(&lines).unwrap();
    
    writer.finalize().unwrap();
    
    let (write_ops, write_bytes) = io_tracker.get_write_stats();
    assert_eq!(write_ops, 2); // header + data batch
    assert!(write_bytes > 0);
}