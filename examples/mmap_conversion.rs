//! Example of using memory-mapped file conversion for maximum performance

use parquet_csv_converter::{IoStatsTracker, ParquetToCsvConverter};
use std::env;
use std::path::Path;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() != 3 {
        eprintln!("Usage: {} <input.parquet> <output.csv>", args[0]);
        eprintln!("\nExample: {} data.parquet output.csv", args[0]);
        eprintln!("\nThis example uses memory-mapped files for parallel CSV writing.");
        eprintln!("Benefits:");
        eprintln!("  - No Direct I/O alignment issues");
        eprintln!("  - True parallel writes without locks");
        eprintln!("  - Maximum SSD bandwidth utilization");
        eprintln!("  - Simple and efficient implementation");
        std::process::exit(1);
    }
    
    let input_path = Path::new(&args[1]);
    let output_path = Path::new(&args[2]);
    
    // Create I/O stats tracker
    let io_tracker = IoStatsTracker::new();
    
    println!("Converting {} to {} using memory-mapped I/O", 
        input_path.display(), 
        output_path.display()
    );
    let start = Instant::now();
    
    // Open the Parquet file
    let converter = ParquetToCsvConverter::new(input_path)?;
    
    println!("Parquet file opened successfully");
    println!("Total rows: {}", converter.len());
    println!("Schema: {}", converter.get_header());
    
    // Determine number of threads
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    
    println!("Using {} threads for parallel conversion", num_threads);
    
    // Convert using memory-mapped approach
    converter.convert_to_csv_mmap(
        output_path,
        num_threads,
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
    
    // Display file size
    let output_size = std::fs::metadata(output_path)?.len();
    println!("\nOutput file size: {:.2} MB", output_size as f64 / 1_048_576.0);
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::tempdir;
    
    #[test]
    fn test_mmap_conversion_performance() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("large_test.parquet");
        let output_path = temp_dir.path().join("output.csv");
        
        // Create a larger test file to see performance benefits
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        
        let num_rows = 100_000;
        let ids: Vec<i32> = (0..num_rows).collect();
        let names: Vec<String> = (0..num_rows)
            .map(|i| format!("customer_{}", i))
            .collect();
        let values: Vec<f64> = (0..num_rows)
            .map(|i| (i as f64) * 1.23)
            .collect();
        let categories: Vec<String> = (0..num_rows)
            .map(|i| match i % 4 {
                0 => "A",
                1 => "B", 
                2 => "C",
                _ => "D",
            }.to_string())
            .collect();
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(categories)),
            ],
        ).unwrap();
        
        // Write Parquet file
        let file = File::create(&input_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        
        // Convert using mmap
        let start = Instant::now();
        let converter = ParquetToCsvConverter::new(&input_path).unwrap();
        converter.convert_to_csv_mmap(&output_path, 4, None).unwrap();
        let elapsed = start.elapsed();
        
        println!("Converted {} rows in {:?}", num_rows, elapsed);
        
        // Verify output
        let csv_content = std::fs::read_to_string(&output_path).unwrap();
        let lines: Vec<&str> = csv_content.lines().collect();
        assert_eq!(lines.len(), num_rows as usize + 1); // +1 for header
        
        // Check header
        assert_eq!(lines[0], "id,name,value,category");
        
        // Spot check some data
        assert!(lines[1].starts_with("0,customer_0,0,A"));
        assert!(lines[50001].contains("customer_50000"));
    }
}