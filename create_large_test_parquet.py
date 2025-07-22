#!/usr/bin/env python3
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import sys

if len(sys.argv) > 1:
    num_rows = int(sys.argv[1])
else:
    num_rows = 1_000_000

print(f"Creating test parquet file with {num_rows:,} rows...")

# Create large dataset
data = {
    'id': np.arange(num_rows, dtype=np.int32),
    'value1': np.random.rand(num_rows) * 1000,
    'value2': np.random.rand(num_rows) * 1000,
    'value3': np.random.rand(num_rows) * 1000,
    'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], num_rows),
    'timestamp': np.random.randint(1600000000, 1700000000, num_rows),
    'flag': np.random.choice([True, False], num_rows),
    'score': np.random.rand(num_rows) * 100,
}

# Create table
table = pa.table(data)

# Write to parquet
pq.write_table(table, 'large_test.parquet', compression='snappy')

import os
print(f"Created large_test.parquet with {num_rows:,} rows")
print(f"File size: {os.path.getsize('large_test.parquet') / 1024 / 1024:.2f} MB")