import pandas as pd
import numpy as np
import os

# Create a directory for sample data if it doesn't exist
if not os.path.exists('sample_data'):
    os.makedirs('sample_data')

print("Creating sample data with deliberate mismatches for testing the reconciliation tool...")

# Set random seed for reproducibility
np.random.seed(42)

# Create base data - 50 records that will be consistent across both files
num_base_records = 50
base_data = {
    'customer_id': np.random.randint(1000, 2000, num_base_records),
    'transaction_date': pd.date_range(start='2025-01-01', periods=num_base_records),
    'amount': np.round(np.random.uniform(100, 500, num_base_records), 2),
    'description': [f"Common Transaction {i}" for i in range(1, num_base_records + 1)],
    'status': np.random.choice(['Completed', 'Pending', 'Failed'], num_base_records),
    'category': np.random.choice(['Retail', 'Food', 'Travel', 'Entertainment'], num_base_records)
}

# Create File 1 (source file) with the base data + records only in File 1
num_only_in_file1 = 20
only_in_file1_data = {
    'customer_id': np.random.randint(5000, 6000, num_only_in_file1),  # Distinct range
    'transaction_date': pd.date_range(start='2025-02-01', periods=num_only_in_file1),
    'amount': np.round(np.random.uniform(100, 500, num_only_in_file1), 2),
    'description': [f"Only in File 1 - Transaction {i}" for i in range(1, num_only_in_file1 + 1)],
    'status': np.random.choice(['Completed', 'Pending', 'Failed'], num_only_in_file1),
    'category': np.random.choice(['Retail', 'Food', 'Travel', 'Entertainment'], num_only_in_file1)
}

# Combine base data with records only in File 1
df1_data = {}
for key in base_data:
    df1_data[key] = np.concatenate([base_data[key], only_in_file1_data[key]])

df1 = pd.DataFrame(df1_data)

# Create File 2 (target file) with the base data + modified values + records only in File 2

# 1. Start with a copy of the base data (not including the "only in file 1" records)
df2 = pd.DataFrame(base_data).copy()

# 2. Rename columns to simulate different column names in File 2
df2 = df2.rename(columns={
    'customer_id': 'cust_id',
    'transaction_date': 'trans_date',
    'amount': 'transaction_amount',
    'description': 'transaction_description',
    'status': 'transaction_status',
    'category': 'transaction_category'
})

# 3. Deliberately modify values in some records to create mismatches (first 15 records)
num_to_modify = 15
for i in range(num_to_modify):
    # Modify amount by adding $100
    df2.loc[i, 'transaction_amount'] = df2.loc[i, 'transaction_amount'] + 100
    
    # Change description
    df2.loc[i, 'transaction_description'] = f"MODIFIED: {df2.loc[i, 'transaction_description']}"
    
    # Change status
    current_status = df2.loc[i, 'transaction_status']
    if current_status == 'Completed':
        df2.loc[i, 'transaction_status'] = 'Failed'
    else:
        df2.loc[i, 'transaction_status'] = 'Completed'

# 4. Add records that only exist in File 2
num_only_in_file2 = 25
only_in_file2_data = {
    'cust_id': np.random.randint(8000, 9000, num_only_in_file2),  # Distinct range
    'trans_date': pd.date_range(start='2025-03-01', periods=num_only_in_file2),
    'transaction_amount': np.round(np.random.uniform(100, 500, num_only_in_file2), 2),
    'transaction_description': [f"Only in File 2 - Transaction {i}" for i in range(1, num_only_in_file2 + 1)],
    'transaction_status': np.random.choice(['Completed', 'Pending', 'Failed'], num_only_in_file2),
    'transaction_category': np.random.choice(['Retail', 'Food', 'Travel', 'Entertainment'], num_only_in_file2)
}

df2_only = pd.DataFrame(only_in_file2_data)
df2 = pd.concat([df2, df2_only], ignore_index=True)

# Save the DataFrames to CSV files
df1.to_csv('sample_data/mismatched_source.csv', index=False)
df2.to_csv('sample_data/mismatched_target.csv', index=False)

# Print summary of what was created
print("\nMismatched Sample Data Summary:")
print(f"File 1 (mismatched_source.csv): {len(df1)} records")
print(f"  - {num_base_records} records that also appear in File 2")
print(f"  - {num_only_in_file1} records that only appear in File 1")

print(f"\nFile 2 (mismatched_target.csv): {len(df2)} records")
print(f"  - {num_base_records} records that also appear in File 1")
print(f"    - {num_to_modify} records with deliberately modified values (amounts, descriptions, statuses)")
print(f"    - {num_base_records - num_to_modify} records with identical values to File 1")
print(f"  - {num_only_in_file2} records that only appear in File 2")

print("\nColumn Mappings:")
print("  File 1 -> File 2")
print("  -----------------")
print("  customer_id -> cust_id")
print("  transaction_date -> trans_date")
print("  amount -> transaction_amount")
print("  description -> transaction_description")
print("  status -> transaction_status")
print("  category -> transaction_category")