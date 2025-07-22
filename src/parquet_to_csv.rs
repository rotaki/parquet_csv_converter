//! Parquet to CSV converter with parallel processing support

use crate::{
    aligned_reader::AlignedChunkReader,
    constants::open_file_with_direct_io,
    parallel_csv_writer::ParallelCsvWriterBuilder,
    IoStatsTracker,
};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::os::fd::{IntoRawFd, RawFd};
use std::path::Path;
use std::sync::Arc;
use std::thread;

/// Convert a value from an Arrow array to a CSV-compatible string
fn value_to_csv_string(array: &ArrayRef, row: usize) -> String {
    if array.is_null(row) {
        return String::new();
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Int8Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Int16Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<arrow::array::UInt8Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<arrow::array::UInt16Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<arrow::array::UInt32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Float32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            escape_csv_value(arr.value(row))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<arrow::array::LargeStringArray>().unwrap();
            escape_csv_value(arr.value(row))
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<arrow::array::BinaryArray>().unwrap();
            hex::encode(arr.value(row))
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<arrow::array::LargeBinaryArray>().unwrap();
            hex::encode(arr.value(row))
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Date32Array>().unwrap();
            let days = arr.value(row);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            date.format("%Y-%m-%d").to_string()
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Date64Array>().unwrap();
            let millis = arr.value(row);
            let datetime = chrono::DateTime::from_timestamp_millis(millis)
                .unwrap_or_else(|| chrono::DateTime::from_timestamp_millis(0).unwrap());
            datetime.format("%Y-%m-%d").to_string()
        }
        DataType::Timestamp(unit, _tz) => {
            match unit {
                TimeUnit::Second => {
                    let arr = array.as_any().downcast_ref::<arrow::array::TimestampSecondArray>().unwrap();
                    let ts = arr.value(row);
                    let datetime = chrono::DateTime::from_timestamp(ts, 0)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                }
                TimeUnit::Millisecond => {
                    let arr = array.as_any().downcast_ref::<arrow::array::TimestampMillisecondArray>().unwrap();
                    let ts = arr.value(row);
                    let datetime = chrono::DateTime::from_timestamp_millis(ts)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp_millis(0).unwrap());
                    datetime.format("%Y-%m-%d %H:%M:%S.%3f").to_string()
                }
                TimeUnit::Microsecond => {
                    let arr = array.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().unwrap();
                    let ts = arr.value(row);
                    let datetime = chrono::DateTime::from_timestamp_micros(ts)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp_micros(0).unwrap());
                    datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                }
                TimeUnit::Nanosecond => {
                    let arr = array.as_any().downcast_ref::<arrow::array::TimestampNanosecondArray>().unwrap();
                    let ts = arr.value(row);
                    let secs = ts / 1_000_000_000;
                    let nanos = (ts % 1_000_000_000) as u32;
                    let datetime = chrono::DateTime::from_timestamp(secs, nanos)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                    datetime.format("%Y-%m-%d %H:%M:%S.%9f").to_string()
                }
            }
        }
        DataType::Decimal128(_, scale) => {
            let arr = array.as_any().downcast_ref::<arrow::array::Decimal128Array>().unwrap();
            let value = arr.value(row);
            format_decimal128(value, *scale as i32)
        }
        _ => {
            // For unsupported types, return a string representation
            format!("<{:?}>", array.data_type())
        }
    }
}

/// Format a decimal128 value with the given scale
fn format_decimal128(value: i128, scale: i32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    
    let divisor = 10i128.pow(scale as u32);
    let int_part = value / divisor;
    let frac_part = (value % divisor).abs();
    
    if frac_part == 0 {
        int_part.to_string()
    } else {
        format!("{}.{:0width$}", int_part, frac_part, width = scale as usize)
            .trim_end_matches('0')
            .to_string()
    }
}

/// Escape a CSV value according to RFC 4180
fn escape_csv_value(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// Convert a RecordBatch row to a CSV line
fn record_batch_row_to_csv(batch: &RecordBatch, row: usize) -> String {
    let mut csv_values = Vec::new();
    
    for col_idx in 0..batch.num_columns() {
        let array = batch.column(col_idx);
        csv_values.push(value_to_csv_string(array, row));
    }
    
    csv_values.join(",")
}

/// ParquetToCsvConverter reads Parquet files and converts them to CSV
pub struct ParquetToCsvConverter {
    fd: RawFd,
    num_rows: usize,
    num_row_groups: usize,
    schema: Arc<arrow::datatypes::Schema>,
}

impl Drop for ParquetToCsvConverter {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

impl ParquetToCsvConverter {
    /// Create a new converter from a Parquet file
    pub fn new(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            return Err(format!("Parquet file does not exist: {:?}", path));
        }

        let file = open_file_with_direct_io(&path)
            .map_err(|e| format!("Failed to open Parquet file: {}", e))?;

        let fd = file.into_raw_fd();

        let chunk_reader = AlignedChunkReader::new(fd)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
            .map_err(|e| format!("Failed to create reader builder: {}", e))?;

        let metadata = builder.metadata();
        let num_rows = metadata.file_metadata().num_rows() as usize;
        let num_row_groups = metadata.num_row_groups();
        let schema = builder.schema();

        Ok(Self {
            fd,
            num_rows,
            num_row_groups,
            schema: schema.clone(),
        })
    }

    /// Get the number of rows
    pub fn len(&self) -> usize {
        self.num_rows
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// Get the CSV header
    pub fn get_header(&self) -> String {
        self.schema
            .fields()
            .iter()
            .map(|f| escape_csv_value(f.name()))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Create parallel scanners that output CSV lines
    pub fn create_parallel_csv_scanners(
        &self,
        num_scanners: usize,
        io_tracker: Option<IoStatsTracker>,
    ) -> Vec<Box<dyn Iterator<Item = String> + Send>> {
        if num_scanners == 0 || self.is_empty() {
            return vec![];
        }

        let mut scanners = Vec::new();
        let groups_per_scanner = self.num_row_groups.div_ceil(num_scanners);

        for i in 0..num_scanners {
            let start_group = i * groups_per_scanner;
            let end_group = ((i + 1) * groups_per_scanner).min(self.num_row_groups);

            if start_group >= self.num_row_groups {
                break;
            }

            let scanner = CsvPartitionScanner {
                fd: self.fd,
                start_row_group: start_group,
                end_row_group: end_group,
                current_batch: None,
                current_row: 0,
                reader: None,
                io_stats: io_tracker.clone(),
            };

            scanners.push(Box::new(scanner) as Box<dyn Iterator<Item = String> + Send>);
        }

        scanners
    }
    
    /// Convert Parquet to CSV using parallel writers for maximum throughput
    pub fn convert_to_csv_parallel(
        &self,
        output_path: impl AsRef<Path>,
        num_threads: usize,
        buffer_size: usize,
        io_tracker: Option<IoStatsTracker>,
    ) -> Result<(), String> {
        let output_path = output_path.as_ref();
        
        // Open output file with Direct I/O
        let output_file = open_file_with_direct_io(output_path)
            .map_err(|e| format!("Failed to open output file: {}", e))?;
        let output_fd = output_file.into_raw_fd();
        
        // Write header first
        let header = format!("{}\n", self.get_header());
        use crate::file_manager::pwrite_fd;
        pwrite_fd(output_fd, header.as_bytes(), 0)
            .map_err(|e| format!("Failed to write header: {}", e))?;
        
        // Create parallel writer starting after header
        let writer = ParallelCsvWriterBuilder::new()
            .buffer_size(buffer_size)
            .enable_io_stats(io_tracker.is_some())
            .build(output_fd, header.len() as u64);
        
        // Create scanners for each thread
        let scanners = self.create_parallel_csv_scanners(num_threads, io_tracker);
        
        if scanners.is_empty() {
            return Ok(());
        }
        
        // Spawn threads to process partitions in parallel
        let handles: Vec<_> = scanners
            .into_iter()
            .enumerate()
            .map(|(_thread_id, scanner)| {
                let thread_writer = writer.get_thread_writer();
                
                thread::spawn(move || -> Result<(), String> {
                    // Process scanner output with buffering
                    thread_writer.write_lines_buffered(scanner, buffer_size)?;
                    
                    Ok(())
                })
            })
            .collect();
        
        // Wait for all threads to complete
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(result) => result?,
                Err(e) => return Err(format!("Thread {} panicked: {:?}", i, e)),
            }
        }
        
        // Close the file descriptor
        unsafe {
            libc::close(output_fd);
        }
        
        Ok(())
    }
    
    /// Convert Parquet to CSV using memory-mapped file for maximum performance
    /// This approach avoids Direct I/O alignment issues while maintaining high throughput
    pub fn convert_to_csv_mmap(
        &self,
        output_path: impl AsRef<Path>,
        num_threads: usize,
        io_tracker: Option<IoStatsTracker>,
    ) -> Result<(), String> {
        use crate::mmap_csv_writer::{MmapCsvWriter, estimate_csv_size};
        
        let output_path = output_path.as_ref();
        
        // Estimate file size based on schema and row count
        let avg_field_size = match self.schema.fields().len() {
            0..=5 => 20,    // Small schemas: assume larger fields
            6..=15 => 15,   // Medium schemas
            _ => 10,        // Large schemas: assume smaller fields
        };
        
        let estimated_size = estimate_csv_size(
            self.num_rows,
            self.schema.fields().len(),
            avg_field_size,
        );
        
        // Create memory-mapped writer
        let writer = MmapCsvWriter::new(output_path, estimated_size, io_tracker.clone())?;
        
        // Write header
        writer.write_header(&self.get_header())?;
        
        // Create scanners for each thread
        let scanners = self.create_parallel_csv_scanners(num_threads, io_tracker);
        
        if scanners.is_empty() {
            writer.finalize()?;
            return Ok(());
        }
        
        // Spawn threads to process partitions
        let handles: Vec<_> = scanners
            .into_iter()
            .enumerate()
            .map(|(_thread_id, scanner)| {
                let thread_writer = writer.get_thread_writer();
                
                thread::spawn(move || -> Result<(), String> {
                    // Process with batching for efficiency
                    thread_writer.write_lines_buffered(scanner, 1024 * 1024)?; // 1MB batches
                    Ok(())
                })
            })
            .collect();
        
        // Wait for all threads
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(result) => result?,
                Err(e) => return Err(format!("Thread {} panicked: {:?}", i, e)),
            }
        }
        
        // Finalize the file (truncate to actual size)
        writer.finalize()?;
        
        Ok(())
    }
}

/// Iterator over a partition of row groups that outputs CSV lines
struct CsvPartitionScanner {
    fd: RawFd,
    start_row_group: usize,
    end_row_group: usize,
    current_batch: Option<RecordBatch>,
    current_row: usize,
    reader: Option<Box<dyn arrow::record_batch::RecordBatchReader + Send>>,
    io_stats: Option<IoStatsTracker>,
}

impl CsvPartitionScanner {
    fn init_reader(&mut self) -> Option<()> {
        let chunk_reader = AlignedChunkReader::new_with_tracker(self.fd, self.io_stats.clone()).ok()?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(chunk_reader).ok()?;
        let row_groups: Vec<usize> = (self.start_row_group..self.end_row_group).collect();
        let reader = builder.with_row_groups(row_groups).build().ok()?;
        self.reader = Some(Box::new(reader));
        Some(())
    }
}

impl Iterator for CsvPartitionScanner {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.is_none() {
            self.init_reader()?;
        }

        loop {
            if let Some(ref batch) = self.current_batch {
                if self.current_row < batch.num_rows() {
                    let csv_line = record_batch_row_to_csv(batch, self.current_row);
                    self.current_row += 1;
                    return Some(csv_line);
                }
            }

            match self.reader.as_mut()?.next() {
                Some(Ok(batch)) => {
                    self.current_batch = Some(batch);
                    self.current_row = 0;
                }
                _ => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn test_dir() -> PathBuf {
        let dir = std::env::temp_dir().join("parquet_to_csv_tests");
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_csv_conversion() {
        let path = test_dir().join("test.parquet");

        // Create test parquet file
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids = vec![1, 2, 3, 4, 5];
        let names = vec!["Alice", "Bob", "Charlie", "David", "Eve"];

        let id_array = Int32Array::from(ids);
        let name_array = StringArray::from(names);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Convert to CSV
        let converter = ParquetToCsvConverter::new(&path).unwrap();
        assert_eq!(converter.len(), 5);
        assert_eq!(converter.get_header(), "id,name");

        let scanners = converter.create_parallel_csv_scanners(1, None);
        let csv_lines: Vec<_> = scanners.into_iter().flatten().collect();

        assert_eq!(csv_lines.len(), 5);
        assert_eq!(csv_lines[0], "1,Alice");
        assert_eq!(csv_lines[1], "2,Bob");
        assert_eq!(csv_lines[2], "3,Charlie");
        assert_eq!(csv_lines[3], "4,David");
        assert_eq!(csv_lines[4], "5,Eve");

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_csv_escaping() {
        let path = test_dir().join("test_escaping.parquet");

        // Create test parquet file with values that need escaping
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
        ]));

        let ids = vec![1, 2, 3];
        let texts = vec!["Hello, World", "Quote: \"test\"", "Line\nbreak"];

        let id_array = Int32Array::from(ids);
        let text_array = StringArray::from(texts);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(text_array)],
        )
        .unwrap();

        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Convert to CSV
        let converter = ParquetToCsvConverter::new(&path).unwrap();
        let scanners = converter.create_parallel_csv_scanners(1, None);
        let csv_lines: Vec<_> = scanners.into_iter().flatten().collect();

        assert_eq!(csv_lines[0], "1,\"Hello, World\"");
        assert_eq!(csv_lines[1], "2,\"Quote: \"\"test\"\"\"");
        assert_eq!(csv_lines[2], "3,\"Line\nbreak\"");

        std::fs::remove_file(&path).unwrap();
    }
}