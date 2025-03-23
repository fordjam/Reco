import pandas as pd
import os

# Create a directory for sample presets if it doesn't exist
if not os.path.exists('presets'):
    os.makedirs('presets')

# Create a sample preset for the transaction files
preset_data = [
    {"file1_column": "customer_id", "file2_column": "cust_id", "is_key_column": True},
    {"file1_column": "transaction_date", "file2_column": "trans_date", "is_key_column": False},
    {"file1_column": "amount", "file2_column": "transaction_amount", "is_key_column": False},
    {"file1_column": "description", "file2_column": "transaction_description", "is_key_column": False},
    {"file1_column": "status", "file2_column": "transaction_status", "is_key_column": False},
    {"file1_column": "category", "file2_column": "transaction_category", "is_key_column": False}
]

# Convert to DataFrame and save as CSV
preset_df = pd.DataFrame(preset_data)
preset_df.to_csv("presets/transaction_mapping_preset.csv", index=False)

print("Sample mapping preset created at: presets/transaction_mapping_preset.csv")
print("\nPreset contents:")
print(preset_df.to_string(index=False))