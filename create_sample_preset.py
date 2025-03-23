import pandas as pd
import os

def create_sample_preset():
    # Define the mappings with aggregation settings
    mappings = [
        {
            "file1_column": "customer_id",
            "file2_column": "cust_id",
            "is_key_column": True,
            "use_for_aggregation": True,
            "aggregation_function": ""
        },
        {
            "file1_column": "transaction_date",
            "file2_column": "trans_date",
            "is_key_column": False,
            "use_for_aggregation": True,
            "aggregation_function": ""
        },
        {
            "file1_column": "amount",
            "file2_column": "transaction_amount",
            "is_key_column": False,
            "use_for_aggregation": False,
            "aggregation_function": "sum"
        },
        {
            "file1_column": "description",
            "file2_column": "transaction_description",
            "is_key_column": False,
            "use_for_aggregation": False,
            "aggregation_function": ""
        },
        {
            "file1_column": "status",
            "file2_column": "transaction_status",
            "is_key_column": False,
            "use_for_aggregation": False,
            "aggregation_function": ""
        },
        {
            "file1_column": "category",
            "file2_column": "transaction_category",
            "is_key_column": False,
            "use_for_aggregation": True,
            "aggregation_function": ""
        }
    ]
    
    # Create a DataFrame from the mappings
    df = pd.DataFrame(mappings)
    
    # Create presets directory if it doesn't exist
    if not os.path.exists("presets"):
        os.makedirs("presets")
    
    # Save the preset
    preset_path = "presets/transaction_mapping_preset.csv"
    df.to_csv(preset_path, index=False)
    
    print(f"\nSample mapping preset created at: {preset_path}\n")
    print("Preset contents:")
    print(df.to_string())

if __name__ == "__main__":
    create_sample_preset()