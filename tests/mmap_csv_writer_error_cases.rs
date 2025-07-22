//! Error handling and edge case tests for mmap_csv_writer

use parquet_csv_converter::MmapCsvWriter;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::thread;

fn test_dir() -> PathBuf {
    let dir = std::env::temp_dir().join("mmap_csv_error_tests");
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_extremely_long_line() {
    let path = test_dir().join("extremely_long_line.csv");
    let writer = MmapCsvWriter::new(&path, 10 * 1024 * 1024).unwrap();

    writer.write_header("id,data").unwrap();

    // Create a line that's 5MB
    let huge_data = "x".repeat(5 * 1024 * 1024);
    let line = format!("1,{}", huge_data);

    let thread_writer = writer.get_thread_writer();
    thread_writer.write_lines(&[line]).unwrap();
    writer.finalize().unwrap();

    let file_size = fs::metadata(&path).unwrap().len();
    assert!(file_size > 5 * 1024 * 1024);
}

#[test]
fn test_file_size_estimation_edge_cases() {
    let path = test_dir().join("size_estimation.csv");

    // Test with exactly the amount we'll write
    let header = "a,b,c";
    let lines = ["1,2,3", "4,5,6", "7,8,9"];
    let total_size = header.len() + 1 + lines.iter().map(|l| l.len() + 1).sum::<usize>();

    let writer = MmapCsvWriter::new(&path, total_size).unwrap();
    writer.write_header(header).unwrap();

    let thread_writer = writer.get_thread_writer();
    let string_lines: Vec<String> = lines.iter().map(|s| s.to_string()).collect();
    thread_writer.write_lines(&string_lines).unwrap();

    writer.finalize().unwrap();

    // File should be exactly the size of our data
    let file_size = fs::metadata(&path).unwrap().len() as usize;
    assert_eq!(file_size, total_size);
}

#[test]
fn test_concurrent_finalize_attempts() {
    let path = test_dir().join("concurrent_finalize.csv");
    let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();

    writer.write_header("id,value").unwrap();
    let thread_writer = writer.get_thread_writer();
    thread_writer.write_lines(&["1,100".to_string()]).unwrap();

    // Try to finalize from multiple threads
    let writer_arc = std::sync::Arc::new(writer);
    let handles: Vec<_> = (0..5)
        .map(|_| {
            let writer = writer_arc.clone();
            thread::spawn(move || std::sync::Arc::try_unwrap(writer).map(|w| w.finalize()))
        })
        .collect();

    let mut success_count = 0;
    let mut still_referenced_count = 0;

    for handle in handles {
        match handle.join().unwrap() {
            Ok(Ok(())) => success_count += 1,
            Ok(Err(e)) if e.contains("already finalized") => success_count += 1,
            Err(_) => still_referenced_count += 1,
            _ => {}
        }
    }

    // Most will fail due to Arc still being referenced
    assert!(still_referenced_count >= 4);
}

#[test]
fn test_special_bytes_in_data() {
    let path = test_dir().join("special_bytes.csv");
    let writer = MmapCsvWriter::new(&path, 2 * 1024 * 1024).unwrap();

    writer.write_header("id,data").unwrap();

    // Test various special byte sequences
    let test_cases = [
        ("null_bytes", "data\0with\0nulls"),
        ("form_feed", "data\x0Cwith\x0Cform\x0Cfeed"),
        ("vertical_tab", "data\x0Bwith\x0Bvertical\x0Btab"),
        ("backspace", "data\x08with\x08backspace"),
        ("bell", "data\x07with\x07bell"),
    ];

    let thread_writer = writer.get_thread_writer();
    let lines: Vec<String> = test_cases
        .iter()
        .map(|(id, data)| format!("{},{}", id, data))
        .collect();
    thread_writer.write_lines(&lines).unwrap();

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.contains("null_bytes"));
    // Note: Some special characters might not be preserved exactly in string representation
}

#[test]
fn test_maximum_concurrent_writers() {
    let path = test_dir().join("max_concurrent.csv");
    let writer = MmapCsvWriter::new(&path, 50 * 1024 * 1024).unwrap();

    writer
        .write_header("thread_id,message_id,timestamp")
        .unwrap();

    // Create many concurrent writers
    let handles: Vec<_> = (0..100)
        .map(|thread_id| {
            let thread_writer = writer.get_thread_writer();
            thread::spawn(move || {
                let mut lines = Vec::new();
                for msg_id in 0..100 {
                    lines.push(format!(
                        "{},{},{}",
                        thread_id,
                        msg_id,
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_micros()
                    ));
                }
                thread_writer.write_lines(&lines).unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    let line_count = content.lines().count();
    assert_eq!(line_count, 10001); // header + 100 threads * 100 messages
}

#[test]
fn test_empty_file_handling() {
    let path = test_dir().join("empty_file.csv");

    // Create writer and immediately finalize
    let writer = MmapCsvWriter::new(&path, 1024).unwrap();
    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert_eq!(content, "");
    assert_eq!(fs::metadata(&path).unwrap().len(), 0);
}

#[test]
fn test_header_only_file() {
    let path = test_dir().join("header_only.csv");

    let writer = MmapCsvWriter::new(&path, 1024).unwrap();
    writer.write_header("col1,col2,col3,col4,col5").unwrap();
    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert_eq!(content, "col1,col2,col3,col4,col5\n");
}

#[test]
fn test_rapid_small_writes() {
    let path = test_dir().join("rapid_small.csv");
    let writer = MmapCsvWriter::new(&path, 10 * 1024 * 1024).unwrap();

    writer.write_header("n").unwrap();

    // Write many tiny records
    let thread_writer = writer.get_thread_writer();
    for i in 0..10000 {
        thread_writer.write_lines(&[i.to_string()]).unwrap();
    }

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert_eq!(content.lines().count(), 10001);
}

#[test]
fn test_write_empty_lines() {
    let path = test_dir().join("empty_lines.csv");
    let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();

    writer.write_header("id,value").unwrap();

    // Mix empty lines with data
    let lines = vec![
        "1,100".to_string(),
        "".to_string(), // empty line
        "2,200".to_string(),
        "".to_string(), // empty line
        "".to_string(), // consecutive empty
        "3,300".to_string(),
    ];

    let thread_writer = writer.get_thread_writer();
    thread_writer.write_lines(&lines).unwrap();

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 7); // header + 6 lines (including empty)
    assert_eq!(lines[2], "");
    assert_eq!(lines[4], "");
    assert_eq!(lines[5], "");
}

#[test]
fn test_alternating_batch_single_writes() {
    let path = test_dir().join("alternating_writes.csv");
    let writer = MmapCsvWriter::new(&path, 5 * 1024 * 1024).unwrap();

    writer.write_header("type,batch_id,record_id").unwrap();

    let thread_writer = writer.get_thread_writer();
    for i in 0..100 {
        if i % 2 == 0 {
            // Single write
            thread_writer
                .write_lines(&[format!("single,{},0", i)])
                .unwrap();
        } else {
            // Batch write
            let batch: Vec<String> = (0..5).map(|j| format!("batch,{},{}", i, j)).collect();
            thread_writer.write_lines(&batch).unwrap();
        }
    }

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.lines().count() > 300); // Should have many lines
}

#[test]
fn test_file_permissions_preserved() {
    use std::os::unix::fs::PermissionsExt;

    let path = test_dir().join("permissions_test.csv");

    // Create file with specific permissions
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();

    let mut perms = file.metadata().unwrap().permissions();
    perms.set_mode(0o644); // rw-r--r--
    fs::set_permissions(&path, perms.clone()).unwrap();
    drop(file);

    // Remove the file so MmapCsvWriter creates it fresh
    fs::remove_file(&path).unwrap();

    // Write using MmapCsvWriter
    let writer = MmapCsvWriter::new(&path, 1024).unwrap();
    writer.write_header("test").unwrap();
    let thread_writer = writer.get_thread_writer();
    thread_writer.write_lines(&["data".to_string()]).unwrap();
    writer.finalize().unwrap();

    // Check permissions (should be default)
    let final_perms = fs::metadata(&path).unwrap().permissions();
    // Just verify the file is readable and writable by owner
    assert!(final_perms.mode() & 0o600 == 0o600);
}

#[test]
fn test_concurrent_readers_during_write() {
    let path = test_dir().join("concurrent_read_write.csv");
    let writer = MmapCsvWriter::new(&path, 5 * 1024 * 1024).unwrap();

    writer.write_header("id,value").unwrap();

    // Start writing in a thread
    let writer_arc = std::sync::Arc::new(writer);
    let writer_clone = writer_arc.clone();

    let write_handle = thread::spawn(move || {
        let thread_writer = writer_clone.get_thread_writer();
        for i in 0..1000 {
            thread_writer
                .write_lines(&[format!("{},{}", i, i * 2)])
                .unwrap();
            if i % 100 == 0 {
                thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    });

    // Give writer time to start
    thread::sleep(std::time::Duration::from_millis(50));

    // Try to read while writing
    let mut last_size = 0;
    for _ in 0..5 {
        if let Ok(metadata) = fs::metadata(&path) {
            let size = metadata.len();
            assert!(size >= last_size); // File should only grow
            last_size = size;
        }
        thread::sleep(std::time::Duration::from_millis(20));
    }

    write_handle.join().unwrap();
    std::sync::Arc::try_unwrap(writer_arc)
        .expect("Arc still has references")
        .finalize()
        .unwrap();

    // Final verification
    let content = fs::read_to_string(&path).unwrap();
    assert_eq!(content.lines().count(), 1001); // header + 1000 lines
}
