//! Benchmark to demonstrate the performance of lock-free mmap writes

use parquet_csv_converter::{IoStatsTracker, ParquetToCsvConverter};
use std::time::Instant;
use arrow::array::{Int32Array, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;
use tempfile::tempdir;

fn create_large_parquet(path: &std::path::Path, num_rows: usize) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]));
    
    // Create data in batches to avoid memory issues
    let batch_size = 100_000;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
    
    for batch_start in (0..num_rows).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(num_rows);
        let batch_rows = batch_end - batch_start;
        
        let ids: Vec<i32> = (batch_start as i32..batch_end as i32).collect();
        let values: Vec<f64> = (0..batch_rows).map(|i| (i as f64) * 3.14159).collect();
        let names: Vec<String> = (0..batch_rows).map(|i| format!("user_{}", batch_start + i)).collect();
        let categories: Vec<String> = (0..batch_rows).map(|i| ["A", "B", "C", "D"][i % 4].to_string()).collect();
        let descriptions: Vec<String> = (0..batch_rows)
            .map(|i| format!("This is a longer description for row {} with some additional text", batch_start + i))
            .collect();
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(categories)),
                Arc::new(StringArray::from(descriptions)),
            ],
        )?;
        
        writer.write(&batch)?;
    }
    
    writer.close()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let input_path = temp_dir.path().join("benchmark.parquet");
    let output_path = temp_dir.path().join("output.csv");
    
    // Test different sizes
    let test_sizes = vec![
        ("Small (100K rows)", 100_000),
        ("Medium (1M rows)", 1_000_000),
        ("Large (5M rows)", 5_000_000),
    ];
    
    for (name, num_rows) in test_sizes {
        println!("\n=== Benchmark: {} ===", name);
        
        // Create test data
        print!("Creating test data... ");
        std::io::Write::flush(&mut std::io::stdout())?;
        let start = Instant::now();
        create_large_parquet(&input_path, num_rows)?;
        println!("done in {:.2?}", start.elapsed());
        
        // Test with different thread counts
        for num_threads in [1, 2, 4, 8] {
            let io_tracker = IoStatsTracker::new();
            
            print!("Converting with {} threads... ", num_threads);
            std::io::Write::flush(&mut std::io::stdout())?;
            
            let start = Instant::now();
            let converter = ParquetToCsvConverter::new(&input_path)?;
            converter.convert_to_csv_mmap(&output_path, num_threads, Some(io_tracker.clone()))?;
            let elapsed = start.elapsed();
            
            let (read_ops, read_bytes) = io_tracker.get_read_stats();
            let (write_ops, write_bytes) = io_tracker.get_write_stats();
            let total_mb = (read_bytes + write_bytes) as f64 / 1_048_576.0;
            let throughput = total_mb / elapsed.as_secs_f64();
            
            println!("{:.2?}, {:.2} MB/s", elapsed, throughput);
            
            // Clean up output
            std::fs::remove_file(&output_path)?;
        }
        
        // Clean up input
        std::fs::remove_file(&input_path)?;
    }
    
    println!("\n=== Lock-free mmap Benefits ===");
    println!("1. No mutex contention between threads");
    println!("2. Each thread writes directly to its reserved section");
    println!("3. Only atomic operation is offset allocation");
    println!("4. Can saturate SSD bandwidth with enough threads");
    
    Ok(())
}