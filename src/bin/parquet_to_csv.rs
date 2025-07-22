use parquet_csv_converter::ParquetToCsvConverter;
use std::env;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <input.parquet> <output.csv> [options]", args[0]);
        eprintln!("\nOptions:");
        eprintln!("  --threads <N>     Number of parallel threads (default: number of CPU cores)");
        eprintln!("  --method <M>      Conversion method: chunk (default) or row");
        eprintln!("\nExamples:");
        eprintln!("  {} input.parquet output.csv", args[0]);
        eprintln!("  {} input.parquet output.csv --threads 8", args[0]);
        eprintln!("  {} input.parquet output.csv --method row", args[0]);
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    // Parse optional arguments
    let mut num_threads = num_cpus::get();
    let mut method = "chunk";

    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--threads" => {
                if i + 1 < args.len() {
                    num_threads = args[i + 1].parse::<usize>().unwrap_or_else(|_| {
                        eprintln!("Invalid number of threads, using default");
                        num_cpus::get()
                    });
                    i += 1;
                }
            }
            "--method" => {
                if i + 1 < args.len() {
                    method = &args[i + 1];
                    if !["chunk", "row"].contains(&method) {
                        eprintln!("Invalid method '{}', using 'chunk'", method);
                        method = "chunk";
                    }
                    i += 1;
                }
            }
            _ => {
                eprintln!("Unknown option: {}", args[i]);
            }
        }
        i += 1;
    }

    println!("Converting {} to {}", input_path, output_path);
    println!("Method: {}, Threads: {}", method, num_threads);

    // Start timing
    let start_time = Instant::now();

    // Create the converter
    let converter = ParquetToCsvConverter::new(input_path)?;
    println!("Input file has {} rows", converter.len());

    // Perform conversion based on selected method
    let result = match method {
        "chunk" => {
            println!("Using chunk-based conversion (processes entire columns at once)");
            converter.convert_to_csv_mmap_chunked(output_path, num_threads)
        }
        "row" => {
            println!("Using row-by-row conversion");
            converter.convert_to_csv_mmap(output_path, num_threads)
        }
        _ => unreachable!(),
    };

    match result {
        Ok(()) => {
            let elapsed = start_time.elapsed();

            // Get actual output file size
            match std::fs::metadata(output_path) {
                Ok(metadata) => {
                    let output_size = metadata.len();
                    let output_mb = output_size as f64 / 1_048_576.0;
                    let throughput_mbps = output_mb / elapsed.as_secs_f64();
                    let rows_per_sec = converter.len() as f64 / elapsed.as_secs_f64();
                    let iops = (output_size as f64 / 4096.0) / elapsed.as_secs_f64();

                    println!("\nConversion complete!");
                    println!("Time: {:.2}s", elapsed.as_secs_f64());
                    println!("Output size: {:.2} MB ({} bytes)", output_mb, output_size);
                    println!("Throughput: {:.2} MB/s", throughput_mbps);
                    println!("Rows/second: {:.0}", rows_per_sec);
                    println!("IOPS: {:.0} (assuming 4KB blocks)", iops);
                }
                Err(_) => {
                    println!("\nConversion complete!");
                    println!("Time: {:.2}s", elapsed.as_secs_f64());
                    println!("(Could not determine output file size)");
                }
            }
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
