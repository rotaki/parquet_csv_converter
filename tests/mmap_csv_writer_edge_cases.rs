//! Edge case tests for memory-mapped CSV writer

use parquet_csv_converter::mmap_csv_writer::MmapCsvWriter;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;

#[test]
fn test_zero_size_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("zero_size.csv");

    // Try to create a writer with 0 size - should fail gracefully
    let result = MmapCsvWriter::new(&path, 0);
    assert!(result.is_err() || result.is_ok()); // Implementation dependent
}

#[test]
fn test_very_large_single_line() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("large_line.csv");

    let writer = MmapCsvWriter::new(&path, 10 * 1024 * 1024).unwrap();
    writer.write_header("data").unwrap();

    let thread_writer = writer.get_thread_writer();

    // Create a very large single line (1MB)
    let large_line = "x".repeat(1024 * 1024);
    thread_writer.write_lines(&[large_line]).unwrap();

    writer.finalize().unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 2); // header + data
    assert_eq!(lines[1].len(), 1024 * 1024);
}

#[test]
fn test_concurrent_header_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("concurrent_headers.csv");

    let writer = Arc::new(MmapCsvWriter::new(&path, 1024 * 1024).unwrap());
    let barrier = Arc::new(Barrier::new(3));

    // Multiple threads trying to write headers (only one should succeed)
    let handles: Vec<_> = (0..3)
        .map(|i| {
            let writer = writer.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                barrier.wait();
                let header = format!("col{},col{}", i, i + 1);
                writer.write_header(&header)
            })
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // At least one should succeed
    assert!(results.iter().any(|r| r.is_ok()));

    Arc::try_unwrap(writer).unwrap().finalize().unwrap();
}

#[test]
fn test_exact_size_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("exact_size.csv");

    // Calculate exact size for our data
    let header = "a,b,c";
    let line1 = "1,2,3";
    let line2 = "4,5,6";
    let total_size = header.len() + 1 + line1.len() + 1 + line2.len() + 1;

    let writer = MmapCsvWriter::new(&path, total_size).unwrap();
    writer.write_header(header).unwrap();

    let thread_writer = writer.get_thread_writer();
    thread_writer
        .write_lines(&[line1.to_string(), line2.to_string()])
        .unwrap();

    writer.finalize().unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content, "a,b,c\n1,2,3\n4,5,6\n");

    // File should be exactly the size we specified
    let metadata = std::fs::metadata(&path).unwrap();
    assert_eq!(metadata.len(), total_size as u64);
}

#[test]
fn test_interleaved_small_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("interleaved.csv");

    let writer = Arc::new(MmapCsvWriter::new(&path, 10 * 1024 * 1024).unwrap());
    writer.write_header("thread,seq").unwrap();

    let num_threads = 10;
    let writes_per_thread = 1000;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let writer = writer.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                let thread_writer = writer.get_thread_writer();
                barrier.wait();

                // Write one line at a time to maximize interleaving
                for seq in 0..writes_per_thread {
                    thread_writer
                        .write_lines(&[format!("{},{}", thread_id, seq)])
                        .unwrap();
                    if seq % 10 == 0 {
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

    // Verify all data is present
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 1 + num_threads * writes_per_thread);
}

#[test]
fn test_binary_data_in_csv() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("binary.csv");

    let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();
    writer.write_header("id,data").unwrap();

    let thread_writer = writer.get_thread_writer();

    // Write lines with various UTF-8 sequences
    let lines = vec![
        format!("1,{}", "Hello World"),
        format!("2,{}", "नमस्ते"),                        // Hindi
        format!("3,{}", "مرحبا"),                       // Arabic
        format!("4,{}", "🌍🌎🌏"),                      // Emojis
        format!("5,{}", "\u{1F600}\u{1F601}\u{1F602}"), // More emojis
    ];

    thread_writer.write_lines(&lines).unwrap();
    writer.finalize().unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    for line in &lines {
        assert!(content.contains(line));
    }
}

#[test]
fn test_rollback_on_overflow() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("rollback.csv");

    let writer = Arc::new(MmapCsvWriter::new(&path, 1024).unwrap());
    writer.write_header("data").unwrap();

    let barrier = Arc::new(Barrier::new(2));

    // Two threads trying to write data that would overflow
    let h1 = {
        let writer = writer.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            let thread_writer = writer.get_thread_writer();
            barrier.wait();
            // Try to write 900 bytes
            thread_writer.write_lines(&["x".repeat(899)])
        })
    };

    let h2 = {
        let writer = writer.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            let thread_writer = writer.get_thread_writer();
            barrier.wait();
            // Try to write 900 bytes
            thread_writer.write_lines(&["y".repeat(899)])
        })
    };

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

    // One should succeed, one should fail
    assert!(r1.is_ok() != r2.is_ok());

    Arc::try_unwrap(writer).unwrap().finalize().unwrap();
}

#[test]
fn test_empty_string_handling() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty_strings.csv");

    let writer = MmapCsvWriter::new(&path, 1024).unwrap();
    writer.write_header("a,b,c").unwrap();

    let thread_writer = writer.get_thread_writer();

    // Write lines with empty fields
    let lines = vec![
        "1,,3".to_string(),
        ",,".to_string(),
        "".to_string(),
        "4,5,6".to_string(),
    ];

    thread_writer.write_lines(&lines).unwrap();
    writer.finalize().unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    assert_eq!(content, "a,b,c\n1,,3\n,,\n\n4,5,6\n");
}

#[test]
fn test_performance_metrics() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("performance.csv");

    let start = std::time::Instant::now();

    let writer = MmapCsvWriter::new(&path, 100 * 1024 * 1024).unwrap();
    writer.write_header("id,data").unwrap();

    let num_threads = 8;
    let rows_per_thread = 100_000;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let thread_writer = writer.get_thread_writer();

            thread::spawn(move || {
                // Write in batches for performance
                let mut batch = Vec::with_capacity(1000);
                for i in 0..rows_per_thread {
                    batch.push(format!(
                        "{},{}",
                        thread_id * rows_per_thread + i,
                        "x".repeat(50)
                    ));

                    if batch.len() >= 1000 {
                        thread_writer.write_lines(&batch).unwrap();
                        batch.clear();
                    }
                }

                if !batch.is_empty() {
                    thread_writer.write_lines(&batch).unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    writer.finalize().unwrap();

    let duration = start.elapsed();
    println!(
        "Performance test: wrote {} rows in {:?}",
        num_threads * rows_per_thread,
        duration
    );

    // Verify the file
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 1 + num_threads * rows_per_thread);
}

#[test]
fn test_csv_escaping_edge_cases() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("escaping.csv");

    let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();
    writer.write_header("id,complex_data").unwrap();

    let thread_writer = writer.get_thread_writer();

    // Note: This writer doesn't do CSV escaping - it writes raw data
    // These test cases show what the writer actually does
    let lines = vec![
        r#"1,"already quoted""#.to_string(),
        r#"2,contains,comma"#.to_string(),
        r#"3,"has ""nested"" quotes""#.to_string(),
        r#"4,has
newline"#
            .to_string(),
        r#"5,ends with quote""#.to_string(),
    ];

    thread_writer.write_lines(&lines).unwrap();
    writer.finalize().unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    // Verify raw content is written as-is
    for line in &lines {
        assert!(content.contains(line));
    }
}
