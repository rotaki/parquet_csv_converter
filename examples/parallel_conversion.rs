//! Example of using parallel CSV conversion for maximum performance

use parquet_csv_converter::{IoStatsTracker, ParquetToCsvConverter};
use std::env;
use std::path::Path;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() != 3 {
        eprintln!("Usage: {} <input.parquet> <output.csv>", args[0]);
        eprintln!("\nExample: {} data.parquet output.csv", args[0]);
        std::process::exit(1);
    }
    
    let input_path = Path::new(&args[1]);
    let output_path = Path::new(&args[2]);
    
    // Create I/O stats tracker
    let io_tracker = IoStatsTracker::new();
    
    println!("Converting {} to {}", input_path.display(), output_path.display());
    let start = Instant::now();
    
    // Open the Parquet file
    let converter = ParquetToCsvConverter::new(input_path)
        .map_err(|e| format!("Failed to open Parquet file: {}", e))?;
    
    println!("Parquet file opened successfully");
    println!("Total rows: {}", converter.len());
    println!("Schema: {}", converter.get_header());
    
    // Determine number of threads (use available CPUs)
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    
    println!("Using {} threads for parallel conversion", num_threads);
    
    // Convert using parallel writers
    // Buffer size: 4MB per thread for optimal performance
    let buffer_size = 4 * 1024 * 1024;
    
    converter.convert_to_csv_parallel(
        output_path,
        num_threads,
        buffer_size,
        Some(io_tracker.clone()),
    )?;
    
    let elapsed = start.elapsed();
    
    // Print statistics
    let (read_ops, read_bytes) = io_tracker.get_read_stats();
    let (write_ops, write_bytes) = io_tracker.get_write_stats();
    
    println!("\nConversion completed in {:.2?}", elapsed);
    println!("I/O Statistics:");
    println!("  Read:  {} operations, {:.2} MB", read_ops, read_bytes as f64 / 1_048_576.0);
    println!("  Write: {} operations, {:.2} MB", write_ops, write_bytes as f64 / 1_048_576.0);
    
    let total_mb = (read_bytes + write_bytes) as f64 / 1_048_576.0;
    let throughput = total_mb / elapsed.as_secs_f64();
    println!("  Total throughput: {:.2} MB/s", throughput);
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::tempdir;
    
    #[test]
    fn test_parallel_conversion() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("test_input.parquet");
        let output_path = temp_dir.path().join("test_output.csv");
        
        // Create test Parquet file
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        
        let num_rows = 10000;
        let ids: Vec<i32> = (0..num_rows).collect();
        let names: Vec<String> = (0..num_rows)
            .map(|i| format!("name_{}", i))
            .collect();
        let values: Vec<i32> = (0..num_rows)
            .map(|i| i * 2)
            .collect();
        
        let id_array = Int32Array::from(ids);
        let name_array = StringArray::from(names);
        let value_array = Int32Array::from(values);
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        ).unwrap();
        
        // Write Parquet file
        let file = File::create(&input_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        // Convert to CSV using parallel writer
        let converter = ParquetToCsvConverter::new(&input_path).unwrap();
        converter.convert_to_csv_parallel(
            &output_path,
            4, // 4 threads
            1024 * 1024, // 1MB buffer
            None,
        ).unwrap();
        
        // Verify output
        let csv_content = std::fs::read_to_string(&output_path).unwrap();
        let lines: Vec<&str> = csv_content.lines().collect();
        
        // Check header
        assert_eq!(lines[0], "id,name,value");
        
        // Check we have all rows (+1 for header)
        assert_eq!(lines.len(), num_rows as usize + 1);
        
        // Spot check some rows
        assert!(csv_content.contains("0,name_0,0"));
        assert!(csv_content.contains("4999,name_4999,9998"));
        assert!(csv_content.contains("9999,name_9999,19998"));
    }
}