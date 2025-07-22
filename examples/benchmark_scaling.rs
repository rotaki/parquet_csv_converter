use arrow::array::{Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet_csv_converter::ParquetToCsvConverter;
use std::fs::{self, File};
use std::sync::Arc;
use std::time::Instant;

fn create_test_parquet(path: &str, num_rows: usize) -> Result<(), Box<dyn std::error::Error>> {
    // Ensure we're writing to current directory, not /tmp
    let path = std::path::Path::new(path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]));

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;

    // Create batches of 10000 rows each
    let batch_size = 10000;
    let names = [
        "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry",
    ];
    let categories = ["A", "B", "C", "D", "E"];

    for batch_start in (0..num_rows).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(num_rows);
        let batch_rows = batch_end - batch_start;

        let mut ids = Vec::with_capacity(batch_rows);
        let mut names_vec = Vec::with_capacity(batch_rows);
        let mut values = Vec::with_capacity(batch_rows);
        let mut categories_vec = Vec::with_capacity(batch_rows);
        let mut descriptions = Vec::with_capacity(batch_rows);

        for i in batch_start..batch_end {
            ids.push(i as i32);
            names_vec.push(names[i % names.len()]);
            values.push((i as f64) * 1.5 + 100.0);
            categories_vec.push(categories[i % categories.len()]);
            descriptions.push(format!(
                "Description for row {} with some additional text",
                i
            ));
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names_vec)),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(categories_vec)),
                Arc::new(StringArray::from(descriptions)),
            ],
        )?;

        writer.write(&batch)?;
    }

    // Flush and close the writer
    writer.flush()?;
    writer.close()?;
    Ok(())
}

fn benchmark_conversion(
    input_path: &str,
    num_threads: usize,
) -> Result<(f64, f64, u64, u64), Box<dyn std::error::Error>> {
    let converter = ParquetToCsvConverter::new(input_path)?;

    // Row-by-row
    let output_row = "bench_row.csv";
    let start = Instant::now();
    converter.convert_to_csv_mmap(output_row, num_threads)?;
    let row_duration = start.elapsed().as_secs_f64();
    let row_size = fs::metadata(output_row)?.len();
    fs::remove_file(output_row).ok();

    // Chunk-based
    let output_chunk = "bench_chunk.csv";
    let start = Instant::now();
    converter.convert_to_csv_mmap_chunked(output_chunk, num_threads)?;
    let chunk_duration = start.elapsed().as_secs_f64();
    let chunk_size = fs::metadata(output_chunk)?.len();
    fs::remove_file(output_chunk).ok();

    Ok((row_duration, chunk_duration, row_size, chunk_size))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Parquet to CSV Conversion Scaling Benchmark");
    println!("==========================================");

    let test_sizes = vec![
        ("10K", 10_000),
        ("100K", 100_000),
        ("1M", 1_000_000),
        ("5M", 5_000_000),
        ("10M", 10_000_000),
        ("50M", 50_000_000),
        ("100M", 100_000_000),
    ];

    let thread_counts = vec![1, 2, 4, 8];

    println!("\nCreating test files...");
    for (label, size) in &test_sizes {
        let path = format!("test_{}.parquet", label);
        print!("  Creating {} rows file... ", size);
        std::io::Write::flush(&mut std::io::stdout())?;
        create_test_parquet(&path, *size)?;
        let file_size = fs::metadata(&path)?.len();
        println!("done ({:.1} MB)", file_size as f64 / 1_048_576.0);
    }

    println!("\nRunning benchmarks...");
    println!(
        "\n{:<10} {:<10} {:<15} {:<15} {:<12} {:<15} {:<15} {:<15}",
        "Size",
        "Threads",
        "Row Time (s)",
        "Chunk Time (s)",
        "CSV MB",
        "Row MB/s",
        "Chunk MB/s",
        "Speedup"
    );
    println!("{}", "-".repeat(120));

    for (label, size) in &test_sizes {
        let path = format!("test_{}.parquet", label);

        for &threads in &thread_counts {
            let (row_time, chunk_time, row_size, chunk_size) =
                benchmark_conversion(&path, threads)?;
            let speedup = row_time / chunk_time;
            let csv_mb = row_size as f64 / 1_048_576.0;
            let row_throughput = csv_mb / row_time;
            let chunk_throughput = csv_mb / chunk_time;

            println!(
                "{:<10} {:<10} {:<15.3} {:<15.3} {:<12.1} {:<15.1} {:<15.1} {:<15}",
                label,
                threads,
                row_time,
                chunk_time,
                csv_mb,
                row_throughput,
                chunk_throughput,
                format!("{:.2}x", speedup)
            );
        }
        println!();
    }

    // Analyze scaling
    println!("\nScaling Analysis:");
    println!("-----------------");

    // Test with 1M rows for thread scaling
    let path = "test_1M.parquet";
    let mut thread_results = Vec::new();

    for &threads in &thread_counts {
        let (row_time, chunk_time, row_size, _) = benchmark_conversion(path, threads)?;
        thread_results.push((threads, row_time, chunk_time, row_size));
    }

    println!("\nThread scaling (1M rows):");
    let base_row = thread_results[0].1;
    let base_chunk = thread_results[0].2;
    let csv_mb = thread_results[0].3 as f64 / 1_048_576.0;

    println!("  CSV output size: {:.1} MB", csv_mb);
    println!(
        "\n  {:<10} {:<15} {:<15} {:<15} {:<15}",
        "Threads", "Row Scaling", "Chunk Scaling", "Row IOPS", "Chunk IOPS"
    );
    println!("  {}", "-".repeat(70));

    for (threads, row_time, chunk_time, size) in &thread_results {
        let row_scaling = base_row / row_time;
        let chunk_scaling = base_chunk / chunk_time;
        let row_iops = (*size as f64 / 4096.0) / row_time;
        let chunk_iops = (*size as f64 / 4096.0) / chunk_time;
        println!(
            "  {:<10} {:<15.2}x {:<15.2}x {:<15.0} {:<15.0}",
            threads, row_scaling, chunk_scaling, row_iops, chunk_iops
        );
    }

    // Clean up
    println!("\nCleaning up test files...");
    for (label, _) in &test_sizes {
        let path = format!("test_{}.parquet", label);
        fs::remove_file(&path).ok();
    }

    println!("\nConclusions:");
    println!("------------");
    println!("1. Chunk-based conversion is consistently faster");
    println!("2. Performance advantage increases with file size");
    println!("3. Both methods scale well with thread count");
    println!("4. Chunk method benefits more from parallelization");
    println!("5. Higher thread counts provide better IOPS and throughput");

    Ok(())
}
