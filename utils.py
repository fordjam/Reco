import pandas as pd
import numpy as np

def perform_reconciliation(df1, df2, column_mappings, key_columns, use_aggregation=False, agg_columns_file1=None, agg_columns_file2=None, agg_functions=None):
    """
    Perform reconciliation between two dataframes based on column mappings and key columns.
    
    Args:
        df1 (pd.DataFrame): Primary dataframe (File 1)
        df2 (pd.DataFrame): Secondary dataframe (File 2)
        column_mappings (dict): Mapping of columns from df1 to df2
        key_columns (dict): Key columns used for matching records
        use_aggregation (bool): Whether to aggregate data before reconciliation
        agg_columns_file1 (list): Columns to group by for df1
        agg_columns_file2 (list): Columns to group by for df2
        agg_functions (dict): Aggregation functions to apply for each column
    
    Returns:
        dict: Dictionary containing reconciliation results
    """
    # Create copies to avoid modifying original dataframes
    df1_copy = df1.copy()
    df2_copy = df2.copy()

    # If aggregation is enabled, aggregate the data first
    if use_aggregation and agg_columns_file1 and agg_columns_file2 and agg_functions:
        df1_copy, df2_copy, column_mappings, key_columns = aggregate_dataframes(
            df1_copy, df2_copy, column_mappings, key_columns, 
            agg_columns_file1, agg_columns_file2, agg_functions
        )
    
    # Create a subset of df2 with only mapped columns, renamed to match df1
    df2_mapped = pd.DataFrame()
    
    for col1, col2 in column_mappings.items():
        if col2 in df2_copy.columns:
            df2_mapped[col1] = df2_copy[col2]
    
    # Add unique identifiers to keep track of records
    df1_copy['_rec_id_1'] = range(len(df1_copy))
    df2_mapped['_rec_id_2'] = range(len(df2_copy))
    
    # Get the key columns from File 1 and their mapped equivalents in File 2
    key_cols_file1 = list(key_columns.keys())
    
    # Create composite keys for matching
    if key_cols_file1:
        df1_copy['_composite_key'] = create_composite_key(df1_copy, key_cols_file1)
        df2_mapped['_composite_key'] = create_composite_key(df2_mapped, key_cols_file1)
    else:
        raise ValueError("No key columns provided for matching")
    
    # Find matches, mismatches, and unmatched records using merge
    merged = pd.merge(
        df1_copy, df2_mapped, on='_composite_key', 
        how='outer', suffixes=('_file1', '_file2'), 
        indicator=True
    )
    
    # Split into matched vs. unmatched
    matched = merged[merged['_merge'] == 'both']
    only_in_file1 = merged[merged['_merge'] == 'left_only']
    only_in_file2 = merged[merged['_merge'] == 'right_only']
    
    # Find mismatches in matched records
    mismatched_data = identify_mismatches(matched, column_mappings.keys())
    
    # Get the original records for those only in File 1
    if not only_in_file1.empty:
        rec_ids_file1 = only_in_file1['_rec_id_1'].values
        only_in_file1_data = df1.iloc[rec_ids_file1].reset_index(drop=True)
    else:
        only_in_file1_data = pd.DataFrame()
    
    # Get the original records for those only in File 2
    if not only_in_file2.empty:
        rec_ids_file2 = only_in_file2['_rec_id_2'].values
        only_in_file2_data = df2.iloc[rec_ids_file2].reset_index(drop=True)
    else:
        only_in_file2_data = pd.DataFrame()
    
    # Prepare statistics
    stats = {
        "matched": len(matched) - len(mismatched_data),
        "mismatched": len(mismatched_data),
        "only_in_file1": len(only_in_file1),
        "only_in_file2": len(only_in_file2)
    }
    
    return {
        "stats": stats,
        "mismatched_data": mismatched_data,
        "only_in_file1_data": only_in_file1_data,
        "only_in_file2_data": only_in_file2_data
    }

def aggregate_dataframes(df1, df2, column_mappings, key_columns, agg_columns_file1, agg_columns_file2, agg_functions):
    """
    Aggregate dataframes based on specified columns and aggregation functions.
    
    Args:
        df1 (pd.DataFrame): Primary dataframe (File 1)
        df2 (pd.DataFrame): Secondary dataframe (File 2)
        column_mappings (dict): Mapping of columns from df1 to df2
        key_columns (dict): Key columns used for matching records
        agg_columns_file1 (list): Columns to group by for df1
        agg_columns_file2 (list): Columns to group by for df2
        agg_functions (dict): Aggregation functions to apply for each column
        
    Returns:
        tuple: (aggregated_df1, aggregated_df2, updated_column_mappings, updated_key_columns)
    """
    # Create aggregation dictionaries for each dataframe
    agg_dict_df1 = {}
    for col, func in agg_functions.items():
        if col in df1.columns and col not in agg_columns_file1:
            agg_dict_df1[col] = func
    
    agg_dict_df2 = {}
    for col1, func in agg_functions.items():
        if col1 in column_mappings:
            col2 = column_mappings[col1]
            if col2 in df2.columns and col2 not in agg_columns_file2:
                agg_dict_df2[col2] = func
    
    # Perform aggregation
    aggregated_df1 = df1.groupby(agg_columns_file1).agg(agg_dict_df1).reset_index()
    aggregated_df2 = df2.groupby(agg_columns_file2).agg(agg_dict_df2).reset_index()
    
    # After aggregation, the key columns are the groupby columns
    updated_key_columns = {}
    for col1 in agg_columns_file1:
        if col1 in column_mappings:
            col2 = column_mappings[col1]
            updated_key_columns[col1] = col2
    
    # Filter column_mappings to only include columns that exist in the aggregated dataframes
    updated_column_mappings = {}
    for col1, col2 in column_mappings.items():
        if col1 in aggregated_df1.columns and col2 in aggregated_df2.columns:
            updated_column_mappings[col1] = col2
    
    return aggregated_df1, aggregated_df2, updated_column_mappings, updated_key_columns

def create_composite_key(df, key_columns):
    """
    Create a composite key from multiple columns.
    
    Args:
        df (pd.DataFrame): Dataframe
        key_columns (list): List of column names to use for the key
        
    Returns:
        pd.Series: Composite key series
    """
    if not key_columns:
        raise ValueError("No key columns provided")
    
    # Convert all columns to string and concatenate
    key_parts = []
    for col in key_columns:
        if col in df.columns:
            # Handle NaN values
            key_parts.append(df[col].fillna('').astype(str))
        else:
            # If column doesn't exist, use empty string
            key_parts.append(pd.Series([''] * len(df)))
    
    return pd.Series(['|'.join(row) for row in zip(*key_parts)])

def identify_mismatches(matched_df, columns_to_compare):
    """
    Identify mismatches in values between matched records.
    
    Args:
        matched_df (pd.DataFrame): DataFrame with matched records
        columns_to_compare (list): List of columns to compare
        
    Returns:
        pd.DataFrame: DataFrame with mismatched records and highlighted differences
    """
    # Create a copy of matched_df to add difference columns
    result_df = matched_df.copy()
    
    # Track which rows have at least one mismatch
    has_mismatch = pd.Series(False, index=matched_df.index)
    
    # For each column, check for mismatches and add difference column
    for col in columns_to_compare:
        col_file1 = f"{col}_file1"
        col_file2 = f"{col}_file2"
        
        # Skip key columns and internal columns
        if col.startswith('_') or col_file1 not in matched_df.columns or col_file2 not in matched_df.columns:
            continue
        
        # Find rows where the values don't match
        # Convert to string for comparison to handle different types
        mismatch_mask = (matched_df[col_file1].fillna('').astype(str) != 
                         matched_df[col_file2].fillna('').astype(str))
        
        # Update the overall mismatch tracker
        has_mismatch = has_mismatch | mismatch_mask
        
        # Create a new column to show the difference for mismatched values
        result_df[f"{col}_diff"] = None
        if mismatch_mask.any():
            result_df.loc[mismatch_mask, f"{col}_diff"] = (
                "File1: " + matched_df.loc[mismatch_mask, col_file1].fillna('').astype(str) + 
                " | File2: " + matched_df.loc[mismatch_mask, col_file2].fillna('').astype(str)
            )
    
    # Filter to only rows with at least one mismatch
    if not has_mismatch.any():
        return pd.DataFrame()
    
    mismatched_result = result_df[has_mismatch].copy()
    
    # Keep only necessary columns
    cols_to_keep = ['_composite_key'] 
    for col in columns_to_compare:
        if col.startswith('_'):
            continue
        cols_to_keep.extend([f"{col}_file1", f"{col}_file2", f"{col}_diff"])
    
    # Filter to columns that exist (some diff columns might not exist if no mismatches)
    cols_to_keep = [col for col in cols_to_keep if col in mismatched_result.columns]
    
    return mismatched_result[cols_to_keep]