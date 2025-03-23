import pandas as pd
import numpy as np
import os

# Create a directory for sample data if it doesn't exist
if not os.path.exists('sample_data'):
    os.makedirs('sample_data')

# Set random seed for reproducibility
np.random.seed(42)

# Create sample data for File 1 (source file)
num_records = 100
data1 = {
    'customer_id': np.random.randint(1000, 9999, num_records),
    'transaction_date': pd.date_range(start='2024-01-01', periods=num_records),
    'amount': np.round(np.random.uniform(10, 1000, num_records), 2),
    'description': [f"Transaction {i}" for i in range(1, num_records + 1)],
    'status': np.random.choice(['Completed', 'Pending', 'Failed'], num_records),
    'category': np.random.choice(['Retail', 'Food', 'Travel', 'Entertainment'], num_records)
}

df1 = pd.DataFrame(data1)

# Create sample data for File 2 (target file) with some differences
# 1. Include most of the same records
# 2. Change some values to create mismatches
# 3. Add some new records
# 4. Omit some records
# 5. Use slightly different column names

# Start with 90% of records from df1 (randomly selected)
indices_to_keep = np.random.choice(num_records, int(num_records * 0.9), replace=False)
df2 = df1.iloc[indices_to_keep].copy()

# Rename columns slightly differently
df2 = df2.rename(columns={
    'customer_id': 'cust_id',
    'transaction_date': 'trans_date',
    'amount': 'transaction_amount',
    'description': 'transaction_description',
    'status': 'transaction_status',
    'category': 'transaction_category'
})

# Modify some values to create mismatches (alter about 20% of the rows)
rows_to_modify = np.random.choice(len(df2), int(len(df2) * 0.2), replace=False)
for i in rows_to_modify:
    # Change amount (use iloc to access by position instead of loc with label)
    df2.iloc[i, df2.columns.get_loc('transaction_amount')] = round(
        df2.iloc[i, df2.columns.get_loc('transaction_amount')] * np.random.uniform(0.9, 1.1), 2
    )
    
    # Change status sometimes
    if np.random.random() > 0.5:
        current_status = df2.iloc[i, df2.columns.get_loc('transaction_status')]
        other_statuses = [s for s in ['Completed', 'Pending', 'Failed'] if s != current_status]
        df2.iloc[i, df2.columns.get_loc('transaction_status')] = np.random.choice(other_statuses)

# Add 10 completely new records
new_records = {
    'cust_id': np.random.randint(9000, 9999, 10),
    'trans_date': pd.date_range(start='2024-01-01', periods=10),
    'transaction_amount': np.round(np.random.uniform(10, 1000, 10), 2),
    'transaction_description': [f"New Transaction {i}" for i in range(1, 11)],
    'transaction_status': np.random.choice(['Completed', 'Pending', 'Failed'], 10),
    'transaction_category': np.random.choice(['Retail', 'Food', 'Travel', 'Entertainment'], 10)
}

df2_new = pd.DataFrame(new_records)
df2 = pd.concat([df2, df2_new], ignore_index=True)

# Save the DataFrames to CSV files
df1.to_csv('sample_data/source_transactions.csv', index=False)
df2.to_csv('sample_data/target_transactions.csv', index=False)

print(f"Generated sample files in the 'sample_data' directory:")
print(f"1. source_transactions.csv - {len(df1)} records")
print(f"2. target_transactions.csv - {len(df2)} records")
print("\nSample Preview (source_transactions.csv):")
print(df1.head(3).to_string())
print("\nSample Preview (target_transactions.csv):")
print(df2.head(3).to_string())