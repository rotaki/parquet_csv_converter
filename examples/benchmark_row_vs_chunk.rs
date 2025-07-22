use parquet_csv_converter::ParquetToCsvConverter;
use std::fs;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let input_file = if args.len() > 1 {
        args[1].clone()
    } else {
        "test_data.parquet".to_string()
    };

    // Check if file exists
    if !std::path::Path::new(&input_file).exists() {
        eprintln!("Input file {} not found.", input_file);
        eprintln!(
            "Usage: {} [input.parquet] [num_threads] [--keep-output]",
            args[0]
        );
        eprintln!("\nThis benchmark compares row-by-row vs chunk-based CSV conversion.");
        eprintln!("\nOptions:");
        eprintln!("  --keep-output    Keep the output CSV files (default: remove)");
        return Ok(());
    }

    let mut num_threads = num_cpus::get();
    let mut keep_output = false;

    // Parse optional arguments
    let mut i = 2;
    while i < args.len() {
        if args[i] == "--keep-output" {
            keep_output = true;
        } else if let Ok(threads) = args[i].parse::<usize>() {
            num_threads = threads;
        }
        i += 1;
    }

    // Get file info
    let converter = ParquetToCsvConverter::new(&input_file)?;
    let num_rows = converter.len();
    let input_size = fs::metadata(&input_file)?.len();

    println!("Benchmark: Row-by-Row vs Chunk-Based CSV Conversion");
    println!("===================================================");
    println!("Input file: {}", input_file);
    println!("Input size: {:.2} MB", input_size as f64 / 1_048_576.0);
    println!("Number of rows: {}", num_rows);
    println!("Threads: {}", num_threads);
    println!();

    // Test 1: Row-by-row conversion
    println!("1. Row-by-Row Conversion");
    println!("------------------------");
    let output_row = "benchmark_row.csv";

    let start = Instant::now();
    converter.convert_to_csv_mmap(output_row, num_threads)?;
    let row_duration = start.elapsed();

    let row_size = fs::metadata(output_row)?.len();
    let row_throughput = (row_size as f64 / 1_048_576.0) / row_duration.as_secs_f64();

    println!("   Time: {:.3}s", row_duration.as_secs_f64());
    println!(
        "   Output CSV size: {:.2} MB ({} bytes)",
        row_size as f64 / 1_048_576.0,
        row_size
    );
    println!("   CSV Write throughput: {:.2} MB/s", row_throughput);
    println!(
        "   Rows/second: {:.0}",
        num_rows as f64 / row_duration.as_secs_f64()
    );
    println!(
        "   Effective write IOPS: {:.0} (assuming 4KB blocks)",
        (row_size as f64 / 4096.0) / row_duration.as_secs_f64()
    );

    // Give system time to flush caches
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Test 2: Chunk-based conversion
    println!("\n2. Chunk-Based Conversion");
    println!("-------------------------");
    let output_chunk = "benchmark_chunk.csv";

    let start = Instant::now();
    converter.convert_to_csv_mmap_chunked(output_chunk, num_threads)?;
    let chunk_duration = start.elapsed();

    let chunk_size = fs::metadata(output_chunk)?.len();
    let chunk_throughput = (chunk_size as f64 / 1_048_576.0) / chunk_duration.as_secs_f64();

    println!("   Time: {:.3}s", chunk_duration.as_secs_f64());
    println!(
        "   Output CSV size: {:.2} MB ({} bytes)",
        chunk_size as f64 / 1_048_576.0,
        chunk_size
    );
    println!("   CSV Write throughput: {:.2} MB/s", chunk_throughput);
    println!(
        "   Rows/second: {:.0}",
        num_rows as f64 / chunk_duration.as_secs_f64()
    );
    println!(
        "   Effective write IOPS: {:.0} (assuming 4KB blocks)",
        (chunk_size as f64 / 4096.0) / chunk_duration.as_secs_f64()
    );

    // Comparison
    println!("\n3. Performance Comparison");
    println!("-------------------------");
    let speedup = row_duration.as_secs_f64() / chunk_duration.as_secs_f64();
    let throughput_improvement = chunk_throughput / row_throughput;

    println!("   Speedup: {:.2}x", speedup);
    println!("   Throughput improvement: {:.2}x", throughput_improvement);
    println!(
        "   Time saved: {:.3}s ({:.1}%)",
        row_duration.as_secs_f64() - chunk_duration.as_secs_f64(),
        ((row_duration.as_secs_f64() - chunk_duration.as_secs_f64()) / row_duration.as_secs_f64())
            * 100.0
    );

    // Verify outputs are identical
    println!("\n4. Output Verification");
    println!("----------------------");

    // Count lines in each output file (excluding header)
    use std::process::Command;

    let row_count_output = Command::new("wc").args(["-l", output_row]).output()?;
    let row_count_str = String::from_utf8_lossy(&row_count_output.stdout);
    let row_lines: usize = row_count_str
        .split_whitespace()
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let chunk_count_output = Command::new("wc").args(["-l", output_chunk]).output()?;
    let chunk_count_str = String::from_utf8_lossy(&chunk_count_output.stdout);
    let chunk_lines: usize = chunk_count_str
        .split_whitespace()
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Subtract 1 for header line to get actual data rows
    let row_data_lines = row_lines.saturating_sub(1);
    let chunk_data_lines = chunk_lines.saturating_sub(1);

    println!(
        "   Row-by-row output: {} data rows (+ 1 header)",
        row_data_lines
    );
    println!(
        "   Chunk-based output: {} data rows (+ 1 header)",
        chunk_data_lines
    );

    if row_data_lines == chunk_data_lines && row_data_lines == num_rows {
        println!("   ✓ Both methods produced the expected {} rows", num_rows);
    } else {
        println!("   ✗ Warning: Row counts differ from expected!");
        println!("     Expected: {} rows", num_rows);
    }

    // Show first and last few lines for sanity check
    println!("\n   Sample output comparison:");

    // Get first 6 lines (header + 5 data rows) from row-by-row output
    let head_row = Command::new("head")
        .args(["-n", "6", output_row])
        .output()?;
    let head_row_str = String::from_utf8_lossy(&head_row.stdout);

    // Get first 6 lines from chunk-based output
    let head_chunk = Command::new("head")
        .args(["-n", "6", output_chunk])
        .output()?;
    let head_chunk_str = String::from_utf8_lossy(&head_chunk.stdout);

    // Get last 5 lines from both outputs
    let tail_row = Command::new("tail")
        .args(["-n", "5", output_row])
        .output()?;
    let tail_row_str = String::from_utf8_lossy(&tail_row.stdout);

    let tail_chunk = Command::new("tail")
        .args(["-n", "5", output_chunk])
        .output()?;
    let tail_chunk_str = String::from_utf8_lossy(&tail_chunk.stdout);

    println!("\n   Row-by-row (first 5 + header):");
    for (i, line) in head_row_str.lines().enumerate() {
        if i == 0 {
            println!("     [Header] {}", line);
        } else {
            println!("     [Row {}] {}", i, line);
        }
    }

    println!("\n   Row-by-row (last 5):");
    for (i, line) in tail_row_str.lines().enumerate() {
        println!("     [Row {}] {}", num_rows - 4 + i, line);
    }

    println!("\n   Chunk-based (first 5 + header):");
    for (i, line) in head_chunk_str.lines().enumerate() {
        if i == 0 {
            println!("     [Header] {}", line);
        } else {
            println!("     [Row {}] {}", i, line);
        }
    }

    println!("\n   Chunk-based (last 5):");
    for (i, line) in tail_chunk_str.lines().enumerate() {
        println!("     [Row {}] {}", num_rows - 4 + i, line);
    }

    // Memory efficiency analysis
    println!("\n5. Efficiency Analysis");
    println!("----------------------");
    println!("   Row-by-row method:");
    println!("     - Processes one row at a time");
    println!("     - Lower memory footprint");
    println!("     - More function call overhead");
    println!("     - Less cache-friendly");

    println!("\n   Chunk-based method:");
    println!("     - Processes entire Arrow columns");
    println!("     - Better memory locality");
    println!("     - Fewer function calls");
    println!("     - Optimized for columnar data");

    // Recommendation
    println!("\n6. Recommendation");
    println!("-----------------");
    if speedup > 1.5 {
        println!(
            "   → Use chunk-based conversion for {:.0}% better performance",
            (speedup - 1.0) * 100.0
        );
    } else if speedup > 1.1 {
        println!(
            "   → Chunk-based conversion provides moderate improvement ({:.0}%)",
            (speedup - 1.0) * 100.0
        );
    } else {
        println!("   → Both methods perform similarly for this dataset");
    }

    // Clean up (unless --keep-output was specified)
    if keep_output {
        println!("\n7. Output Files");
        println!("---------------");
        println!("   Row-by-row output: {}", output_row);
        println!("   Chunk-based output: {}", output_chunk);
        println!("   Files kept as requested.");
    } else {
        fs::remove_file(output_row).ok();
        fs::remove_file(output_chunk).ok();
    }

    Ok(())
}
