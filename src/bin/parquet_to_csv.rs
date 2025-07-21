use parquet_csv_converter::ParquetToCsvConverter;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::thread;
use std::sync::mpsc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 3 {
        eprintln!("Usage: {} <input.parquet> <output.csv> [num_threads]", args[0]);
        eprintln!("\nOptional arguments:");
        eprintln!("  num_threads: Number of parallel threads (default: number of CPU cores)");
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];
    let num_threads = if args.len() > 3 {
        args[3].parse::<usize>().unwrap_or_else(|_| {
            eprintln!("Invalid number of threads, using default");
            num_cpus::get()
        })
    } else {
        num_cpus::get()
    };

    println!("Converting {} to {} using {} threads", input_path, output_path, num_threads);

    // Create the converter
    let converter = ParquetToCsvConverter::new(input_path)?;
    println!("Input file has {} rows", converter.len());

    // Get the CSV header
    let header = converter.get_header();

    // Create output file
    let output_file = File::create(output_path)?;
    let mut writer = BufWriter::new(output_file);
    
    // Write header
    writeln!(writer, "{}", header)?;

    // Create channel for collecting results
    let (tx, rx) = mpsc::channel();

    // Create parallel scanners
    let scanners = converter.create_parallel_csv_scanners(num_threads, None);
    let _num_scanners = scanners.len();

    // Spawn threads to process partitions
    let handles: Vec<_> = scanners
        .into_iter()
        .enumerate()
        .map(|(i, scanner)| {
            let tx = tx.clone();
            thread::spawn(move || {
                let mut lines = Vec::new();
                let mut count = 0;
                
                // Collect lines in batches for better performance
                for line in scanner {
                    lines.push(line);
                    count += 1;
                    
                    // Send batch when it reaches a certain size
                    if lines.len() >= 1000 {
                        tx.send((i, lines.clone())).unwrap();
                        lines.clear();
                    }
                }
                
                // Send remaining lines
                if !lines.is_empty() {
                    tx.send((i, lines)).unwrap();
                }
                
                println!("Thread {} processed {} rows", i, count);
            })
        })
        .collect();

    // Drop the original sender so the channel closes when all threads finish
    drop(tx);

    // Collect and write results
    let mut total_rows = 0;
    for (_thread_id, lines) in rx {
        for line in lines {
            writeln!(writer, "{}", line)?;
            total_rows += 1;
        }
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Flush the writer
    writer.flush()?;

    // Print statistics
    println!("\nConversion complete!");
    println!("Total rows converted: {}", total_rows);

    Ok(())
}