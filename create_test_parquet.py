#!/usr/bin/env python3
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta

# Create sample data
n_rows = 10000

data = {
    'id': np.arange(n_rows),
    'name': [f'Person_{i}' for i in range(n_rows)],
    'age': np.random.randint(18, 80, size=n_rows),
    'salary': np.round(np.random.uniform(30000, 150000, size=n_rows), 2),
    'department': np.random.choice(['Sales', 'Engineering', 'Marketing', 'HR', 'Finance'], size=n_rows),
    'hire_date': [datetime(2020, 1, 1) + timedelta(days=int(x)) for x in np.random.randint(0, 1000, size=n_rows)],
    'is_active': np.random.choice([True, False], size=n_rows, p=[0.9, 0.1]),
    'description': [f'Employee description with some text, including special chars: "quotes", commas, and\nnewlines' if i % 100 == 0 else f'Regular employee {i}' for i in range(n_rows)]
}

# Create DataFrame
df = pd.DataFrame(data)

# Convert to PyArrow Table
table = pa.Table.from_pandas(df)

# Write to Parquet file with multiple row groups
pq.write_table(table, 'test_data.parquet', row_group_size=1000)

print(f"Created test_data.parquet with {n_rows} rows")
print(f"Columns: {list(df.columns)}")
print(f"Data types:")
for col in df.columns:
    print(f"  {col}: {df[col].dtype}")