import pandas as pd
import numpy as np
import os

n_rows = int(os.environ.get('DATASET_SIZE', 1000000))

data = {
    'id_col': np.arange(n_rows),
    'category_col': np.random.choice(['Electronics', 'Books', 'Home', 'Toys', 'Health'], n_rows),
    'numeric_col': np.random.uniform(10.5, 1000.0, n_rows),
    'numeric_col2': np.random.randint(1, 100, n_rows),
    'timestamp': pd.date_range(start='2023-01-01', periods=n_rows, freq='min'),
    'is_active': np.random.choice([True, False], n_rows)
}

df = pd.DataFrame(data)
df.to_csv('./data/dataset.csv', index=False)
print(f"Dataset with {len(df)} rows saved to ./data/dataset.csv")