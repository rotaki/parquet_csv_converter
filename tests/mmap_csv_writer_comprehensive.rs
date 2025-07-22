//! Comprehensive tests for mmap_csv_writer covering various CSV formats and edge cases

use parquet_csv_converter::MmapCsvWriter;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

fn test_dir() -> PathBuf {
    let dir = std::env::temp_dir().join("mmap_csv_comprehensive_tests");
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_simple_numeric_csv() {
    let path = test_dir().join("simple_numeric.csv");
    let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();

    // Write header
    writer.write_header("id,value,score").unwrap();

    // Write numeric data
    let data = vec![
        "1,100,95.5",
        "2,200,87.3",
        "3,300,92.1",
        "4,400,88.9",
        "5,500,91.7",
    ];

    let thread_writer = writer.get_thread_writer();
    for line in data {
        thread_writer.write_lines(&[line.to_string()]).unwrap();
    }

    writer.finalize().unwrap();

    // Verify
    let content = fs::read_to_string(&path).unwrap();
    assert!(content.starts_with("id,value,score\n"));
    assert!(content.contains("3,300,92.1"));
    assert_eq!(content.lines().count(), 6); // header + 5 data lines
}

#[test]
fn test_mixed_types_csv() {
    let path = test_dir().join("mixed_types.csv");
    let writer = MmapCsvWriter::new(&path, 2 * 1024 * 1024).unwrap();

    writer
        .write_header("id,name,age,salary,active,joined_date")
        .unwrap();

    let data = [
        "1,John Doe,30,75000.50,true,2020-01-15",
        "2,Jane Smith,28,82000.00,true,2019-06-20",
        "3,Bob Johnson,45,95000.75,false,2015-03-10",
        "4,Alice Brown,33,78500.25,true,2018-11-30",
        "5,Charlie Wilson,52,120000.00,true,2010-07-22",
    ];

    let thread_writer = writer.get_thread_writer();
    let string_data: Vec<String> = data.iter().map(|s| s.to_string()).collect();
    thread_writer.write_lines(&string_data).unwrap();

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.contains("Bob Johnson,45,95000.75,false"));
    assert!(content.contains("2020-01-15"));
}

#[test]
fn test_csv_with_quoted_fields() {
    let path = test_dir().join("quoted_fields.csv");
    let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();

    writer
        .write_header("id,company,description,location")
        .unwrap();

    let data = [
        r#"1,"Acme, Inc.","We make everything","New York, NY""#,
        r#"2,"Smith & Sons","Family business since 1950","Los Angeles, CA""#,
        r#"3,"Tech \"Innovation\" Ltd","Cutting-edge solutions","San Francisco, CA""#,
        r#"4,"Global Trading Co.","Import/Export specialists","Chicago, IL""#,
    ];

    let thread_writer = writer.get_thread_writer();
    let string_data: Vec<String> = data.iter().map(|s| s.to_string()).collect();
    thread_writer.write_lines(&string_data).unwrap();

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.contains(r#""Acme, Inc.""#));
    assert!(content.contains(r#""Tech \"Innovation\" Ltd""#));
}

#[test]
fn test_csv_with_newlines_in_fields() {
    let path = test_dir().join("multiline_fields.csv");
    let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();

    writer.write_header("id,address,notes").unwrap();

    let data = [
        r#"1,"123 Main St
Apt 4B
New York, NY 10001","Customer since 2019""#,
        r#"2,"456 Oak Ave","Prefers morning deliveries
Leave packages at door""#,
        r#"3,"789 Pine Rd
Suite 200","Business account
Net 30 terms""#,
    ];

    let thread_writer = writer.get_thread_writer();
    let string_data: Vec<String> = data.iter().map(|s| s.to_string()).collect();
    thread_writer.write_lines(&string_data).unwrap();

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.contains("123 Main St\nApt 4B"));
    assert!(content.contains("Prefers morning deliveries\nLeave packages at door"));
}

#[test]
fn test_large_text_fields() {
    let path = test_dir().join("large_text_fields.csv");
    let writer = MmapCsvWriter::new(&path, 5 * 1024 * 1024).unwrap();

    writer.write_header("id,title,content,tags").unwrap();

    // Generate large text content
    let lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(50);
    let description = "This is a detailed description that contains many words and extends to multiple sentences. ".repeat(20);

    let data = vec![
        format!("1,\"Article One\",\"{}\",\"tech,ai,ml\"", lorem),
        format!(
            "2,\"Research Paper\",\"{}\",\"science,biology,genetics\"",
            description
        ),
        format!("3,\"Blog Post\",\"{}\",\"travel,europe,food\"", lorem),
    ];

    let thread_writer = writer.get_thread_writer();
    thread_writer.write_lines(&data).unwrap();

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    // Each lorem has 50 * ~50 chars = 2500 chars, times 2 entries = 5000+ chars minimum
    assert!(content.len() > 5000); // Should be quite large
    assert!(content.contains("Lorem ipsum"));
    assert!(content.contains("tech,ai,ml"));
}

#[test]
fn test_unicode_and_special_chars() {
    let path = test_dir().join("unicode_special.csv");
    let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();

    writer
        .write_header("id,name,emoji,symbol,language")
        .unwrap();

    let data = [
        "1,José García,😀,€,Español",
        "2,李明,🎯,¥,中文",
        "3,Müller,🚀,£,Deutsch",
        "4,Владимир,❤️,₽,Русский",
        "5,محمد,🌟,﷼,العربية",
        "6,Σωκράτης,🏛️,₯,Ελληνικά",
    ];

    let thread_writer = writer.get_thread_writer();
    let string_data: Vec<String> = data.iter().map(|s| s.to_string()).collect();
    thread_writer.write_lines(&string_data).unwrap();

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.contains("José García"));
    assert!(content.contains("李明"));
    assert!(content.contains("😀"));
    assert!(content.contains("€"));
    assert!(content.contains("العربية"));
}

#[test]
fn test_empty_fields_and_nulls() {
    let path = test_dir().join("empty_fields.csv");
    let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();

    writer
        .write_header("id,first_name,middle_name,last_name,phone,email")
        .unwrap();

    let data = [
        "1,John,,Doe,555-1234,john@example.com",
        "2,Jane,Marie,Smith,,jane@example.com",
        "3,Bob,,,555-5678,",
        "4,,,,555-9999,unknown@example.com",
        "5,Alice,,Brown,,",
    ];

    let thread_writer = writer.get_thread_writer();
    let string_data: Vec<String> = data.iter().map(|s| s.to_string()).collect();
    thread_writer.write_lines(&string_data).unwrap();

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.contains("John,,Doe"));
    assert!(content.contains(",,,,555-9999"));
    assert!(content.contains("Alice,,Brown,,"));
}

#[test]
fn test_scientific_notation_and_decimals() {
    let path = test_dir().join("scientific_data.csv");
    let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();

    writer
        .write_header("experiment_id,measurement,error_margin,coefficient,normalized")
        .unwrap();

    let data = [
        "1,1.23456789e-10,±0.00001,3.14159265359,0.99999999",
        "2,6.02214076e23,±0.00000001,2.71828182846,1.00000001",
        "3,-9.87654321e-15,±0.0001,1.41421356237,0.50000000",
        "4,3.33333333e+05,±0.01,0.57721566490,-0.99999999",
    ];

    let thread_writer = writer.get_thread_writer();
    let string_data: Vec<String> = data.iter().map(|s| s.to_string()).collect();
    thread_writer.write_lines(&string_data).unwrap();

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.contains("1.23456789e-10"));
    assert!(content.contains("6.02214076e23"));
    assert!(content.contains("3.14159265359"));
}

#[test]
fn test_financial_data_csv() {
    let path = test_dir().join("financial_data.csv");
    let writer = MmapCsvWriter::new(&path, 2 * 1024 * 1024).unwrap();

    writer
        .write_header("date,symbol,open,high,low,close,volume,adjusted_close")
        .unwrap();

    let data = [
        "2024-01-15,AAPL,182.50,184.25,181.75,183.90,52834521,183.90",
        "2024-01-15,GOOGL,138.25,139.50,137.80,139.15,18562347,139.15",
        "2024-01-15,MSFT,378.90,381.25,377.50,380.00,23456789,380.00",
        "2024-01-15,AMZN,155.75,157.00,155.25,156.50,34567890,156.50",
        "2024-01-15,TSLA,218.50,222.75,216.25,220.90,98765432,220.90",
    ];

    // Write multiple days of data
    let thread_writer = writer.get_thread_writer();
    for day in 15..25 {
        let modified_data: Vec<String> = data
            .iter()
            .map(|line| line.replace("2024-01-15", &format!("2024-01-{}", day)))
            .collect();
        thread_writer.write_lines(&modified_data).unwrap();
    }

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.contains("AAPL"));
    assert!(content.contains("2024-01-20"));
    assert!(content.lines().count() > 50);
}

#[test]
fn test_logs_and_timestamps() {
    let path = test_dir().join("server_logs.csv");
    let writer = MmapCsvWriter::new(&path, 2 * 1024 * 1024).unwrap();

    writer
        .write_header("timestamp,level,service,message,request_id,duration_ms")
        .unwrap();

    let services = [
        "auth-service",
        "api-gateway",
        "user-service",
        "payment-service",
    ];
    let levels = ["INFO", "WARN", "ERROR", "DEBUG"];
    let messages = [
        "Request processed successfully",
        "High memory usage detected",
        "Connection timeout",
        "Cache miss for key",
    ];

    // Generate realistic log entries
    let thread_writer = writer.get_thread_writer();
    let mut log_lines = Vec::new();

    for i in 0..100 {
        let timestamp = format!("2024-01-15T10:{}:00.{}Z", 10 + (i / 60), i * 100 % 1000);
        let level = levels[i % levels.len()];
        let service = services[i % services.len()];
        let message = messages[i % messages.len()];
        let request_id = format!("req-{:08x}", i * 12345);
        let duration = 50 + (i * 7) % 450;

        let line = format!(
            "{},{},{},\"{}\",{},{}",
            timestamp, level, service, message, request_id, duration
        );
        log_lines.push(line);
    }

    thread_writer.write_lines(&log_lines).unwrap();
    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.contains("auth-service"));
    assert!(content.contains("ERROR"));
    assert!(content.contains("req-"));
    assert_eq!(content.lines().count(), 101); // header + 100 logs
}

#[test]
fn test_parallel_different_csv_types() {
    let base_dir = test_dir();
    let writers = vec![
        ("numeric", vec!["1,2,3", "4,5,6", "7,8,9"]),
        ("text", vec!["a,b,c", "d,e,f", "g,h,i"]),
        ("mixed", vec!["1,a,true", "2,b,false", "3,c,true"]),
        (
            "quoted",
            vec![r#""a,b","c,d","e,f""#, r#""1,2","3,4","5,6""#],
        ),
    ];

    let handles: Vec<_> = writers
        .into_iter()
        .map(|(name, data)| {
            let path = base_dir.join(format!("{}.csv", name));
            thread::spawn(move || {
                let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();
                writer.write_header("col1,col2,col3").unwrap();

                let thread_writer = writer.get_thread_writer();
                let string_data: Vec<String> = data.iter().map(|s| s.to_string()).collect();
                thread_writer.write_lines(&string_data).unwrap();

                writer.finalize().unwrap();

                // Verify
                let content = fs::read_to_string(&path).unwrap();
                assert!(content.starts_with("col1,col2,col3\n"));
                content.lines().count()
            })
        })
        .collect();

    for handle in handles {
        let line_count = handle.join().unwrap();
        assert!(line_count >= 3); // At least header + 2 data lines
    }
}

#[test]
fn test_incremental_csv_building() {
    let path = test_dir().join("incremental.csv");
    let writer = MmapCsvWriter::new(&path, 5 * 1024 * 1024).unwrap();

    // Start with header
    writer
        .write_header("iteration,value,squared,cubed,is_even")
        .unwrap();

    // Build CSV incrementally
    let thread_writer = writer.get_thread_writer();
    for i in 1..=1000 {
        let squared = i * i;
        let cubed = i * i * i;
        let is_even = if i % 2 == 0 { "true" } else { "false" };

        let line = format!("{},{},{},{},{}", i, i, squared, cubed, is_even);
        thread_writer.write_lines(&[line]).unwrap();

        // Simulate some processing time
        if i % 100 == 0 {
            thread::yield_now();
        }
    }

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert_eq!(content.lines().count(), 1001); // header + 1000 data lines
    assert!(content.contains("1000,1000,1000000,1000000000,true"));
}

#[test]
fn test_csv_with_iso8601_dates() {
    let path = test_dir().join("iso8601_dates.csv");
    let writer = MmapCsvWriter::new(&path, 1024 * 1024).unwrap();

    writer
        .write_header("id,created_at,updated_at,expires_at,duration")
        .unwrap();

    let data = [
        "1,2024-01-15T10:30:00Z,2024-01-15T14:25:00Z,2024-12-31T23:59:59Z,P30D",
        "2,2024-01-15T08:00:00-05:00,2024-01-15T16:30:00-05:00,2025-01-15T08:00:00-05:00,P1Y",
        "3,2024-01-15T15:45:30.123Z,2024-01-15T16:00:00.456Z,2024-01-16T15:45:30.123Z,PT24H",
        "4,2024-01-15T00:00:00+09:00,2024-01-15T12:00:00+09:00,2024-02-15T00:00:00+09:00,P1M",
    ];

    let thread_writer = writer.get_thread_writer();
    let string_data: Vec<String> = data.iter().map(|s| s.to_string()).collect();
    thread_writer.write_lines(&string_data).unwrap();

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.contains("2024-01-15T10:30:00Z"));
    assert!(content.contains("-05:00"));
    assert!(content.contains(".123Z"));
    assert!(content.contains("P30D"));
}

#[test]
fn test_batch_writes_different_sizes() {
    let path = test_dir().join("batch_writes.csv");
    let writer = MmapCsvWriter::new(&path, 10 * 1024 * 1024).unwrap();

    writer.write_header("batch_id,record_id,data").unwrap();

    // Different batch sizes
    let batch_sizes = [1, 10, 100, 1000];

    let thread_writer = writer.get_thread_writer();
    for (batch_idx, &batch_size) in batch_sizes.iter().enumerate() {
        let mut lines = Vec::new();
        for i in 0..batch_size {
            lines.push(format!("{},{},data-{}-{}", batch_idx, i, batch_idx, i));
        }

        // Write entire batch
        thread_writer.write_lines(&lines).unwrap();
    }

    writer.finalize().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    let total_lines = 1 + batch_sizes.iter().sum::<i32>(); // header + all data
    assert_eq!(content.lines().count(), total_lines as usize);
}

#[test]
fn test_concurrent_mixed_write_patterns() {
    let path = test_dir().join("concurrent_mixed.csv");
    let writer = Arc::new(MmapCsvWriter::new(&path, 10 * 1024 * 1024).unwrap());

    writer
        .write_header("thread_id,write_type,sequence,value")
        .unwrap();

    let handles: Vec<_> = (0..8)
        .map(|thread_id| {
            let writer = writer.clone();
            thread::spawn(move || {
                let thread_writer = writer.get_thread_writer();

                // Mix single writes and batch writes
                for i in 0..100 {
                    if i % 10 == 0 {
                        // Batch write every 10th iteration
                        let mut batch = Vec::new();
                        for j in 0..5 {
                            batch.push(format!(
                                "{},batch,{},{}",
                                thread_id,
                                i + j,
                                (i + j) * thread_id
                            ));
                        }
                        thread_writer.write_lines(&batch).unwrap();
                    } else {
                        // Single write
                        let line = format!("{},single,{},{}", thread_id, i, i * thread_id);
                        thread_writer.write_lines(&[line]).unwrap();
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    Arc::try_unwrap(writer)
        .expect("Arc still has multiple references")
        .finalize()
        .unwrap();

    let content = fs::read_to_string(&path).unwrap();

    // Verify each thread wrote its data
    for thread_id in 0..8 {
        assert!(content.contains(&format!("{},single", thread_id)));
        assert!(content.contains(&format!("{},batch", thread_id)));
    }
}
