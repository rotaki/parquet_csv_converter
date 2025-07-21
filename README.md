# Parquet to CSV Converter

A high-performance parallel Parquet to CSV converter written in Rust, utilizing Direct I/O for efficient file operations.

## Features

- **Parallel Processing**: Converts Parquet files to CSV using multiple threads for improved performance
- **Direct I/O**: Uses aligned reads with Direct I/O to bypass OS page cache for better performance with large files
- **Type Support**: Handles various data types including:
  - Numeric types (Int8/16/32/64, UInt8/16/32/64, Float32/64)
  - String types (Utf8, LargeUtf8)
  - Binary types (Binary, LargeBinary)
  - Date/Time types (Date32, Date64, Timestamp)
  - Decimal128
  - Boolean
- **RFC 4180 Compliant**: Properly escapes CSV values according to RFC 4180 standard
- **Memory Efficient**: Processes data in streaming fashion without loading entire file into memory

## Building

```bash
cargo build --release
```

## Usage

```bash
parquet_to_csv <input.parquet> <output.csv> [num_threads]
```

Arguments:
- `input.parquet`: Path to the input Parquet file
- `output.csv`: Path to the output CSV file
- `num_threads`: (Optional) Number of parallel threads to use (default: number of CPU cores)

### Example

```bash
# Convert using default number of threads
./target/release/parquet_to_csv data.parquet data.csv

# Convert using 8 threads
./target/release/parquet_to_csv data.parquet data.csv 8
```

## Implementation Details

The converter uses:
- Arrow and Parquet crates for reading Parquet files
- Direct I/O with aligned buffers for optimal disk access
- Parallel processing by dividing row groups among threads
- Channel-based communication for collecting results from worker threads

## Performance

The converter is optimized for large files and can process multiple row groups in parallel. Performance scales with the number of threads up to the number of row groups in the Parquet file.

## Testing

Create a test Parquet file using the provided Python script:

```bash
python3 create_test_parquet.py
cargo run --bin parquet_to_csv test_data.parquet test_data.csv 4
```

## Dependencies

- `parquet`: Apache Parquet format support
- `arrow`: Apache Arrow columnar data format
- `libc`: Direct I/O system calls
- `chrono`: Date/time handling
- `hex`: Binary to hex conversion
- `num_cpus`: CPU core detection