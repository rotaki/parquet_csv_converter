//! Performance and stress tests for mmap_csv_writer

use parquet_csv_converter::MmapCsvWriter;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn test_dir() -> PathBuf {
    let dir = std::env::temp_dir().join("mmap_csv_performance_tests");
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_throughput_small_records() {
    let path = test_dir().join("throughput_small.csv");
    let writer = MmapCsvWriter::new(&path, 100 * 1024 * 1024).unwrap();

    writer.write_header("id,timestamp,value").unwrap();

    let start = Instant::now();
    let num_records = 1_000_000;

    for i in 0..num_records {
        let line = format!("{},{},{}", i, i * 1000, i % 100);
        writer.get_thread_writer().write_lines(&[line]).unwrap();
    }

    writer.finalize().unwrap();
    let duration = start.elapsed();

    let file_size = fs::metadata(&path).unwrap().len() as f64;
    let throughput_mbps = (file_size / 1_048_576.0) / duration.as_secs_f64();
    let records_per_sec = num_records as f64 / duration.as_secs_f64();

    println!("Small records performance:");
    println!("  Records: {}", num_records);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.2} MB/s", throughput_mbps);
    println!("  Records/sec: {:.0}", records_per_sec);

    assert!(throughput_mbps > 50.0); // Should achieve at least 50 MB/s
}

#[test]
fn test_throughput_large_records() {
    let path = test_dir().join("throughput_large.csv");
    let writer = MmapCsvWriter::new(&path, 500 * 1024 * 1024).unwrap();

    writer.write_header("id,data,checksum").unwrap();

    // Create a 1KB data field
    let large_data = "x".repeat(1024);

    let start = Instant::now();
    let num_records = 100_000;

    let thread_writer = writer.get_thread_writer();
    for i in 0..num_records {
        let line = format!("{},{},{}", i, large_data, (i as u32).wrapping_mul(12345));
        thread_writer.write_lines(&[line]).unwrap();
    }

    writer.finalize().unwrap();
    let duration = start.elapsed();

    let file_size = fs::metadata(&path).unwrap().len() as f64;
    let throughput_mbps = (file_size / 1_048_576.0) / duration.as_secs_f64();

    println!("Large records performance:");
    println!("  Records: {} (1KB each)", num_records);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.2} MB/s", throughput_mbps);

    assert!(throughput_mbps > 100.0); // Should achieve at least 100 MB/s for large records
}

#[test]
fn test_batch_write_performance() {
    let path = test_dir().join("batch_performance.csv");
    let writer = MmapCsvWriter::new(&path, 200 * 1024 * 1024).unwrap();

    writer.write_header("batch_id,record_id,value").unwrap();

    let batch_sizes = vec![1, 10, 100, 1000, 10000];
    let records_per_test = 100_000;

    for &batch_size in &batch_sizes {
        let start = Instant::now();
        let num_batches = records_per_test / batch_size;

        for batch_id in 0..num_batches {
            let mut lines = Vec::with_capacity(batch_size);
            for record_id in 0..batch_size {
                lines.push(format!(
                    "{},{},{}",
                    batch_id,
                    record_id,
                    batch_id * record_id
                ));
            }
            writer.get_thread_writer().write_lines(&lines).unwrap();
        }

        let duration = start.elapsed();
        let records_per_sec = records_per_test as f64 / duration.as_secs_f64();

        println!("Batch size {} performance:", batch_size);
        println!("  Duration: {:.3}s", duration.as_secs_f64());
        println!("  Records/sec: {:.0}", records_per_sec);
    }

    writer.finalize().unwrap();
}

#[test]
fn test_parallel_write_scaling() {
    let thread_counts = vec![1, 2, 4, 8, 16];
    let records_per_thread = 100_000;

    for &num_threads in &thread_counts {
        let path = test_dir().join(format!("parallel_scaling_{}.csv", num_threads));
        let writer = Arc::new(MmapCsvWriter::new(&path, 200 * 1024 * 1024).unwrap());

        writer.write_header("thread_id,record_id,value").unwrap();

        let start = Instant::now();

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let writer = writer.clone();
                thread::spawn(move || {
                    let thread_writer = writer.get_thread_writer();
                    for record_id in 0..records_per_thread {
                        let line = format!("{},{},{}", thread_id, record_id, thread_id * record_id);
                        thread_writer.write_lines(&[line]).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        Arc::try_unwrap(writer)
            .expect("Arc still has references")
            .finalize()
            .unwrap();
        let duration = start.elapsed();

        let total_records = num_threads * records_per_thread;
        let records_per_sec = total_records as f64 / duration.as_secs_f64();
        let file_size = fs::metadata(&path).unwrap().len() as f64;
        let throughput_mbps = (file_size / 1_048_576.0) / duration.as_secs_f64();

        println!("Parallel scaling with {} threads:", num_threads);
        println!("  Total records: {}", total_records);
        println!("  Duration: {:.2}s", duration.as_secs_f64());
        println!("  Records/sec: {:.0}", records_per_sec);
        println!("  Throughput: {:.2} MB/s", throughput_mbps);
    }
}

#[test]
fn test_memory_mapping_efficiency() {
    let path = test_dir().join("mmap_efficiency.csv");
    let file_size = 110 * 1024 * 1024; // 110 MB (with some buffer)
    let writer = MmapCsvWriter::new(&path, file_size).unwrap();

    writer.write_header("id,data").unwrap();

    // Track memory usage pattern by writing in bursts
    let burst_size = 10000;
    let data_line = "x".repeat(100); // 100 byte data field

    let start = Instant::now();
    let mut burst_times = Vec::new();

    for burst in 0..100 {
        let burst_start = Instant::now();

        for i in 0..burst_size {
            let line = format!("{},{}", burst * burst_size + i, data_line);
            writer.get_thread_writer().write_lines(&[line]).unwrap();
        }

        burst_times.push(burst_start.elapsed());

        // Small pause between bursts
        thread::sleep(Duration::from_millis(10));
    }

    writer.finalize().unwrap();
    let total_duration = start.elapsed();

    // Calculate variance in burst times
    let avg_burst_time =
        burst_times.iter().map(|d| d.as_secs_f64()).sum::<f64>() / burst_times.len() as f64;
    let variance = burst_times
        .iter()
        .map(|d| {
            let diff = d.as_secs_f64() - avg_burst_time;
            diff * diff
        })
        .sum::<f64>()
        / burst_times.len() as f64;
    let std_dev = variance.sqrt();

    println!("Memory mapping efficiency:");
    println!("  Total duration: {:.2}s", total_duration.as_secs_f64());
    println!("  Average burst time: {:.3}s", avg_burst_time);
    println!("  Std deviation: {:.3}s", std_dev);
    println!(
        "  Relative std dev: {:.1}%",
        (std_dev / avg_burst_time) * 100.0
    );

    // Burst times should be consistent (low variance)
    assert!((std_dev / avg_burst_time) < 0.2); // Less than 20% variation
}

#[test]
fn test_concurrent_batch_writes() {
    let path = test_dir().join("concurrent_batch.csv");
    let writer = Arc::new(MmapCsvWriter::new(&path, 300 * 1024 * 1024).unwrap());

    writer.write_header("thread,batch,record,value").unwrap();

    let num_threads = 16;
    let batches_per_thread = 100;
    let records_per_batch = 1000;

    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let writer = writer.clone();
            thread::spawn(move || {
                let thread_writer = writer.get_thread_writer();

                for batch_id in 0..batches_per_thread {
                    let mut lines = Vec::with_capacity(records_per_batch);

                    for record_id in 0..records_per_batch {
                        lines.push(format!(
                            "{},{},{},{}",
                            thread_id,
                            batch_id,
                            record_id,
                            thread_id * 1000 + batch_id * 100 + record_id
                        ));
                    }

                    thread_writer.write_lines(&lines).unwrap();

                    // Simulate some processing between batches
                    thread::yield_now();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    Arc::try_unwrap(writer)
        .expect("Arc still has references")
        .finalize()
        .unwrap();
    let duration = start.elapsed();

    let total_records = num_threads * batches_per_thread * records_per_batch;
    let records_per_sec = total_records as f64 / duration.as_secs_f64();
    let file_size = fs::metadata(&path).unwrap().len() as f64;
    let throughput_mbps = (file_size / 1_048_576.0) / duration.as_secs_f64();

    println!("Concurrent batch writes:");
    println!("  Threads: {}", num_threads);
    println!("  Total batches: {}", num_threads * batches_per_thread);
    println!("  Total records: {}", total_records);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Records/sec: {:.0}", records_per_sec);
    println!("  Throughput: {:.2} MB/s", throughput_mbps);

    // Should maintain high throughput even with many concurrent batch writers
    assert!(throughput_mbps > 80.0);
}

#[test]
#[ignore] // This test takes a long time, run with --ignored flag
fn test_very_large_file() {
    let path = test_dir().join("very_large.csv");
    let writer = MmapCsvWriter::new(&path, 2 * 1024 * 1024 * 1024).unwrap(); // 2GB

    writer.write_header("id,timestamp,data").unwrap();

    let data_field = "x".repeat(100);
    let start = Instant::now();
    let mut last_report = Instant::now();

    for i in 0..10_000_000 {
        let line = format!("{},{},{}", i, i * 1000, data_field);
        writer.get_thread_writer().write_lines(&[line]).unwrap();

        // Progress reporting
        if i > 0 && i % 1_000_000 == 0 && last_report.elapsed() > Duration::from_secs(1) {
            let elapsed = start.elapsed();
            let progress = (i as f64 / 10_000_000.0) * 100.0;
            println!(
                "Progress: {:.1}% ({} records in {:.1}s)",
                progress,
                i,
                elapsed.as_secs_f64()
            );
            last_report = Instant::now();
        }
    }

    writer.finalize().unwrap();
    let duration = start.elapsed();

    let file_size = fs::metadata(&path).unwrap().len() as f64;
    let throughput_mbps = (file_size / 1_048_576.0) / duration.as_secs_f64();

    println!("Very large file test:");
    println!("  File size: {:.2} GB", file_size / 1_073_741_824.0);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.2} MB/s", throughput_mbps);

    assert!(file_size > 1_000_000_000.0); // Should be over 1GB
}
