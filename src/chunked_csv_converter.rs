//! High-performance chunk-based CSV conversion
//!
//! This module provides optimized CSV conversion by processing entire columns
//! at once and then transposing them into CSV rows.

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

/// Convert an entire RecordBatch to CSV lines in chunks
/// This is significantly faster than row-by-row conversion
pub fn record_batch_to_csv_lines(batch: &RecordBatch) -> Vec<String> {
    if batch.num_rows() == 0 {
        return vec![];
    }

    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    // Pre-convert all columns to string arrays
    let mut column_strings: Vec<Vec<String>> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let array = batch.column(col_idx);
        let strings = convert_column_to_strings(array);
        column_strings.push(strings);
    }

    // Now transpose and build CSV lines
    let mut csv_lines = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut line = String::with_capacity(100); // Estimate capacity

        for col_idx in 0..num_cols {
            if col_idx > 0 {
                line.push(',');
            }
            line.push_str(&column_strings[col_idx][row_idx]);
        }

        csv_lines.push(line);
    }

    csv_lines
}

/// Convert an entire column to strings at once
/// This is optimized for each data type
fn convert_column_to_strings(array: &ArrayRef) -> Vec<String> {
    let len = array.len();
    let mut strings = Vec::with_capacity(len);

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    strings.push(String::new());
                } else {
                    strings.push(if arr.value(i) { "true" } else { "false" }.to_string());
                }
            }
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    strings.push(String::new());
                } else {
                    strings.push(arr.value(i).to_string());
                }
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    strings.push(String::new());
                } else {
                    strings.push(arr.value(i).to_string());
                }
            }
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    strings.push(String::new());
                } else {
                    strings.push(arr.value(i).to_string());
                }
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    strings.push(String::new());
                } else {
                    strings.push(arr.value(i).to_string());
                }
            }
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    strings.push(String::new());
                } else {
                    strings.push(escape_csv_value(arr.value(i)));
                }
            }
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    strings.push(String::new());
                } else {
                    strings.push(escape_csv_value(arr.value(i)));
                }
            }
        }
        DataType::Timestamp(unit, _tz) => {
            strings = convert_timestamp_column(array, unit);
        }
        _ => {
            // Fallback to row-by-row for unsupported types
            for i in 0..len {
                strings.push(value_to_csv_string_single(array, i));
            }
        }
    }

    strings
}

/// Optimized timestamp conversion for entire column
fn convert_timestamp_column(array: &ArrayRef, unit: &TimeUnit) -> Vec<String> {
    let len = array.len();
    let mut strings = Vec::with_capacity(len);

    match unit {
        TimeUnit::Second => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    strings.push(String::new());
                } else {
                    let ts = arr.value(i);
                    let datetime = chrono::DateTime::from_timestamp(ts, 0)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                    strings.push(datetime.format("%Y-%m-%d %H:%M:%S").to_string());
                }
            }
        }
        TimeUnit::Millisecond => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    strings.push(String::new());
                } else {
                    let ts = arr.value(i);
                    let datetime = chrono::DateTime::from_timestamp_millis(ts)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp_millis(0).unwrap());
                    strings.push(datetime.format("%Y-%m-%d %H:%M:%S.%3f").to_string());
                }
            }
        }
        TimeUnit::Microsecond => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    strings.push(String::new());
                } else {
                    let ts = arr.value(i);
                    let datetime = chrono::DateTime::from_timestamp_micros(ts)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp_micros(0).unwrap());
                    strings.push(datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string());
                }
            }
        }
        TimeUnit::Nanosecond => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    strings.push(String::new());
                } else {
                    let ts = arr.value(i);
                    let secs = ts / 1_000_000_000;
                    let nanos = (ts % 1_000_000_000) as u32;
                    let datetime = chrono::DateTime::from_timestamp(secs, nanos)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                    strings.push(datetime.format("%Y-%m-%d %H:%M:%S.%9f").to_string());
                }
            }
        }
    }

    strings
}

/// Fallback function for single value conversion
fn value_to_csv_string_single(array: &ArrayRef, row: usize) -> String {
    if array.is_null(row) {
        return String::new();
    }

    // For unsupported types, return a placeholder
    format!("<{:?}>", array.data_type())
}

/// Escape a CSV value according to RFC 4180
fn escape_csv_value(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// Ultra-fast version that pre-allocates a single buffer and builds all CSV lines at once
pub fn record_batch_to_csv_buffer(batch: &RecordBatch) -> String {
    if batch.num_rows() == 0 {
        return String::new();
    }

    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    // Estimate total size (rough estimate: 20 chars per field + delimiters)
    let estimated_size = num_rows * num_cols * 20 + num_rows;
    let mut buffer = String::with_capacity(estimated_size);

    // Pre-convert all columns
    let mut column_strings: Vec<Vec<String>> = Vec::with_capacity(num_cols);
    for col_idx in 0..num_cols {
        let array = batch.column(col_idx);
        let strings = convert_column_to_strings(array);
        column_strings.push(strings);
    }

    // Build the entire CSV buffer
    for row_idx in 0..num_rows {
        for col_idx in 0..num_cols {
            if col_idx > 0 {
                buffer.push(',');
            }
            buffer.push_str(&column_strings[col_idx][row_idx]);
        }
        buffer.push('\n');
    }

    buffer
}

/// Even faster version for numeric-heavy data using pre-sized buffers
pub fn record_batch_to_csv_optimized(batch: &RecordBatch, buffer: &mut Vec<u8>) {
    buffer.clear();

    if batch.num_rows() == 0 {
        return;
    }

    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    // Pre-convert columns to byte arrays for even faster concatenation
    let mut column_bytes: Vec<Vec<Vec<u8>>> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let array = batch.column(col_idx);
        let bytes = convert_column_to_bytes(array);
        column_bytes.push(bytes);
    }

    // Now build CSV lines directly in bytes
    for row_idx in 0..num_rows {
        for col_idx in 0..num_cols {
            if col_idx > 0 {
                buffer.push(b',');
            }
            buffer.extend_from_slice(&column_bytes[col_idx][row_idx]);
        }
        buffer.push(b'\n');
    }
}

/// Convert column to bytes for maximum performance
fn convert_column_to_bytes(array: &ArrayRef) -> Vec<Vec<u8>> {
    let len = array.len();
    let mut bytes_vec = Vec::with_capacity(len);

    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    bytes_vec.push(Vec::new());
                } else {
                    bytes_vec.push(arr.value(i).to_string().into_bytes());
                }
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            for i in 0..len {
                if arr.is_null(i) {
                    bytes_vec.push(Vec::new());
                } else {
                    bytes_vec.push(arr.value(i).to_string().into_bytes());
                }
            }
        }
        _ => {
            // Fallback to string conversion
            let strings = convert_column_to_strings(array);
            for s in strings {
                bytes_vec.push(s.into_bytes());
            }
        }
    }

    bytes_vec
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_chunk_conversion() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let value_array = Float64Array::from(vec![1.5, 2.5, 3.5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(value_array),
                Arc::new(name_array),
            ],
        )
        .unwrap();

        let lines = record_batch_to_csv_lines(&batch);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "1,1.5,Alice");
        assert_eq!(lines[1], "2,2.5,Bob");
        assert_eq!(lines[2], "3,3.5,Charlie");
    }
}
