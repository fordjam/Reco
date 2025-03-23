import streamlit as st
import pandas as pd
import os
import json
import io
from utils import perform_reconciliation

st.set_page_config(page_title="CSV Reconciliation Tool", layout="wide")

def import_preset_from_csv(file):
    """
    Import column mappings, key columns, and aggregation settings from a CSV file.
    
    Args:
        file: File object from st.file_uploader or file handle
        
    Returns:
        dict: Dictionary with column_mappings, key_columns, and aggregation settings
    """
    # Read the CSV file
    df = pd.read_csv(file)
    
    # Extract mappings and settings
    column_mappings = {}
    key_columns = {}
    agg_columns = []
    agg_functions = {}
    
    for _, row in df.iterrows():
        file1_col = row["file1_column"]
        file2_col = row["file2_column"]
        is_key = row["is_key_column"]
        
        # Extract column mappings
        column_mappings[file1_col] = file2_col
        
        # Extract key columns
        if is_key:
            key_columns[file1_col] = file2_col
            
        # Extract aggregation settings if they exist
        if "use_for_aggregation" in row and "aggregation_function" in row:
            if row["use_for_aggregation"]:
                agg_columns.append(file1_col)
            if pd.notna(row["aggregation_function"]):
                agg_functions[file1_col] = row["aggregation_function"]
    
    return {
        "column_mappings": column_mappings,
        "key_columns": key_columns,
        "agg_columns": agg_columns,
        "agg_functions": agg_functions,
        "use_aggregation": bool(agg_functions)
    }

def load_default_preset():
    """Load the default transaction mapping preset if it exists."""
    preset_path = "presets/transaction_mapping_preset.csv"
    if os.path.exists(preset_path):
        with open(preset_path, 'r') as f:
            preset_data = import_preset_from_csv(f)
            st.session_state.mapping_presets["Default Transaction Mapping"] = preset_data
            st.session_state.column_mappings = preset_data["column_mappings"]
            st.session_state.key_columns = preset_data["key_columns"]
            st.session_state.current_preset_name = "Default Transaction Mapping"
            
            # Apply aggregation settings if they exist
            if "agg_columns" in preset_data:
                st.session_state.agg_columns_file1 = preset_data["agg_columns"]
            if "agg_functions" in preset_data:
                st.session_state.agg_functions = preset_data["agg_functions"]
            if "use_aggregation" in preset_data:
                st.session_state.use_aggregation = preset_data["use_aggregation"]

# Initialize session state variables if they don't exist
if 'file1_data' not in st.session_state:
    st.session_state.file1_data = None
if 'file2_data' not in st.session_state:
    st.session_state.file2_data = None
if 'column_mappings' not in st.session_state:
    st.session_state.column_mappings = {}
if 'key_columns' not in st.session_state:
    st.session_state.key_columns = {}
if 'reconciliation_results' not in st.session_state:
    st.session_state.reconciliation_results = None
if 'mapping_presets' not in st.session_state:
    st.session_state.mapping_presets = {}
    load_default_preset()  # Load default preset when initializing mapping_presets
if 'current_preset_name' not in st.session_state:
    st.session_state.current_preset_name = ""
# New session state variables for aggregation
if 'use_aggregation' not in st.session_state:
    st.session_state.use_aggregation = False
if 'agg_columns_file1' not in st.session_state:
    st.session_state.agg_columns_file1 = []
if 'agg_columns_file2' not in st.session_state:
    st.session_state.agg_columns_file2 = []
if 'agg_functions' not in st.session_state:
    st.session_state.agg_functions = {}

def main():
    st.title("CSV Reconciliation Tool")
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["File Upload", "Column Mapping", "Reconciliation Results"])
    
    with tab1:
        file_upload_tab()
    
    with tab2:
        column_mapping_tab()
    
    with tab3:
        reconciliation_results_tab()

def file_upload_tab():
    st.header("Upload CSV Files for Reconciliation")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("File 1")
        file1 = st.file_uploader("Upload primary CSV file", type="csv", key="file1_uploader")
        if file1 is not None:
            try:
                df1 = pd.read_csv(file1)
                st.session_state.file1_data = df1
                st.success(f"File 1 loaded successfully with {len(df1)} rows and {len(df1.columns)} columns")
                st.dataframe(df1.head(5))
            except Exception as e:
                st.error(f"Error loading File 1: {e}")
    
    with col2:
        st.subheader("File 2")
        file2 = st.file_uploader("Upload secondary CSV file", type="csv", key="file2_uploader")
        if file2 is not None:
            try:
                df2 = pd.read_csv(file2)
                st.session_state.file2_data = df2
                st.success(f"File 2 loaded successfully with {len(df2)} rows and {len(df2.columns)} columns")
                st.dataframe(df2.head(5))
            except Exception as e:
                st.error(f"Error loading File 2: {e}")

def column_mapping_tab():
    st.header("Configure Column Mappings")
    
    if st.session_state.file1_data is None or st.session_state.file2_data is None:
        st.warning("Please upload both CSV files first in the File Upload tab")
        return
    
    df1 = st.session_state.file1_data
    df2 = st.session_state.file2_data
    
    # Add a section for aggregation settings
    st.subheader("Data Aggregation Settings")
    st.write("Aggregate data before reconciliation to compare summary values instead of individual records")
    
    # Toggle for enabling/disabling aggregation
    st.session_state.use_aggregation = st.toggle(
        "Enable Data Aggregation", 
        value=st.session_state.use_aggregation
    )
    
    if st.session_state.use_aggregation:
        agg_col1, agg_col2 = st.columns(2)
        
        with agg_col1:
            st.write("File 1 Aggregation")
            # Select columns to group by for File 1
            st.session_state.agg_columns_file1 = st.multiselect(
                "Group by columns (File 1):",
                options=df1.columns,
                default=st.session_state.agg_columns_file1
            )
        
        with agg_col2:
            st.write("File 2 Aggregation")
            # Select columns to group by for File 2 based on mappings
            mapped_columns = {col2: col1 for col1, col2 in st.session_state.column_mappings.items()}
            
            agg_columns_file2_options = [
                col2 for col2 in df2.columns 
                if col2 in mapped_columns and mapped_columns[col2] in st.session_state.agg_columns_file1
            ]
            
            if agg_columns_file2_options:
                st.info("Matching columns from File 2 will be used based on your column mappings")
                st.session_state.agg_columns_file2 = [
                    st.session_state.column_mappings.get(col1)
                    for col1 in st.session_state.agg_columns_file1
                    if col1 in st.session_state.column_mappings
                ]
                st.write("Selected aggregate columns (File 2):", ", ".join(st.session_state.agg_columns_file2))
            else:
                st.warning("Map columns in File 1 to corresponding columns in File 2 first")
        
        # Configure aggregation functions for numeric columns
        st.write("---")
        st.write("Configure Aggregation Functions for Numeric Columns")
        
        # Get numeric columns from both files
        numeric_cols_file1 = df1.select_dtypes(include=['number']).columns.tolist()
        
        # Remove columns used for grouping
        numeric_cols_file1 = [col for col in numeric_cols_file1 if col not in st.session_state.agg_columns_file1]
        
        # If no numeric columns remain after removing grouping columns
        if not numeric_cols_file1:
            st.warning("No numeric columns available for aggregation in File 1")
        else:
            # For each numeric column, let the user select an aggregation function
            for col in numeric_cols_file1:
                if col in st.session_state.column_mappings:
                    agg_options = ["sum", "mean", "count", "min", "max"]
                    default_ix = 0
                    if col in st.session_state.agg_functions:
                        if st.session_state.agg_functions[col] in agg_options:
                            default_ix = agg_options.index(st.session_state.agg_functions[col])
                    
                    selected_agg = st.selectbox(
                        f"Aggregation function for '{col}':",
                        options=agg_options,
                        index=default_ix,
                        key=f"agg_func_{col}"
                    )
                    
                    st.session_state.agg_functions[col] = selected_agg
    
    # Display a note about aggregation
    if st.session_state.use_aggregation:
        st.info("Note: When aggregation is enabled, reconciliation will compare aggregated data instead of individual records. Key columns will be the grouping columns.")
    
    st.markdown("---")
    
    # Add a section for mapping presets
    st.subheader("Mapping Presets")
    
    preset_col1, preset_col2 = st.columns([3, 2])
    
    with preset_col1:
        # Preset management
        preset_tabs = st.tabs(["Load Preset", "Save Preset", "Load From CSV"])
        
        with preset_tabs[0]:  # Load Preset tab
            if not st.session_state.mapping_presets:
                st.info("No saved presets. Create one in the Save Preset tab.")
            else:
                selected_preset = st.selectbox(
                    "Select a saved preset:",
                    options=list(st.session_state.mapping_presets.keys()),
                    index=0 if st.session_state.current_preset_name not in st.session_state.mapping_presets else 
                           list(st.session_state.mapping_presets.keys()).index(st.session_state.current_preset_name)
                )
                
                if st.button("Apply Selected Preset"):
                    preset_data = st.session_state.mapping_presets[selected_preset]
                    # Apply the mappings
                    st.session_state.column_mappings = preset_data["column_mappings"]
                    st.session_state.key_columns = preset_data["key_columns"]
                    st.session_state.current_preset_name = selected_preset
                    st.success(f"Applied preset: {selected_preset}")
                    st.experimental_rerun()
        
        with preset_tabs[1]:  # Save Preset tab
            new_preset_name = st.text_input("Preset Name:", st.session_state.current_preset_name)
            
            if st.button("Save Current Mappings as Preset"):
                if not new_preset_name:
                    st.error("Please enter a name for the preset")
                elif not st.session_state.column_mappings:
                    st.error("No column mappings to save")
                else:
                    # Save the current mappings as a preset
                    st.session_state.mapping_presets[new_preset_name] = {
                        "column_mappings": st.session_state.column_mappings.copy(),
                        "key_columns": st.session_state.key_columns.copy()
                    }
                    st.session_state.current_preset_name = new_preset_name
                    st.success(f"Saved preset: {new_preset_name}")
            
            # Option to export preset to CSV
            if st.button("Export Current Preset to CSV"):
                if not st.session_state.column_mappings:
                    st.error("No column mappings to export")
                else:
                    export_preset_to_csv(new_preset_name or "unnamed_preset")
                    st.success(f"Exported preset to CSV file")
        
        with preset_tabs[2]:  # Load from CSV tab
            preset_file = st.file_uploader("Upload a mapping preset CSV file", type="csv", key="preset_uploader")
            
            if preset_file is not None:
                try:
                    imported_preset = import_preset_from_csv(preset_file)
                    preset_name = os.path.splitext(preset_file.name)[0]
                    
                    st.write("Preview of imported mappings:")
                    mapping_df = pd.DataFrame(
                        [{"File1 Column": k, "File2 Column": v} for k, v in imported_preset["column_mappings"].items()]
                    )
                    st.dataframe(mapping_df)
                    
                    if st.button("Apply Imported Preset"):
                        st.session_state.column_mappings = imported_preset["column_mappings"]
                        st.session_state.key_columns = imported_preset["key_columns"]
                        st.session_state.mapping_presets[preset_name] = imported_preset
                        st.session_state.current_preset_name = preset_name
                        st.success(f"Applied imported preset: {preset_name}")
                        st.experimental_rerun()
                        
                except Exception as e:
                    st.error(f"Error loading preset file: {e}")
    
    with preset_col2:
        # Display current mappings summary
        st.subheader("Current Mappings")
        if st.session_state.column_mappings:
            mapping_summary = []
            for col1, col2 in st.session_state.column_mappings.items():
                mapping_summary.append({"File1": col1, "File2": col2, "Key Column": col1 in st.session_state.key_columns})
            
            st.dataframe(pd.DataFrame(mapping_summary))
            
            if st.button("Clear All Mappings"):
                st.session_state.column_mappings = {}
                st.session_state.key_columns = {}
                st.session_state.current_preset_name = ""
                st.experimental_rerun()
        else:
            st.info("No column mappings configured")
    
    st.markdown("---")
    st.subheader("Column Mappings")
    st.write("Map columns from File 2 to corresponding columns in File 1")
    
    # Create column mappings
    for col1 in df1.columns:
        col_options = ["None"] + list(df2.columns)
        default_ix = 0
        if col1 in st.session_state.column_mappings:
            if st.session_state.column_mappings[col1] in col_options:
                default_ix = col_options.index(st.session_state.column_mappings[col1])
        
        selected_col = st.selectbox(
            f"Map '{col1}' from File 1 to:",
            options=col_options,
            index=default_ix,
            key=f"mapping_{col1}"
        )
        
        if selected_col != "None":
            st.session_state.column_mappings[col1] = selected_col
        elif col1 in st.session_state.column_mappings:
            del st.session_state.column_mappings[col1]
    
    st.subheader("Key Columns for Matching")
    st.write("Select columns that uniquely identify records for matching")
    
    # Select key columns from File 1
    key_cols = st.multiselect(
        "Select key columns from File 1 for matching:",
        options=df1.columns,
        default=list(st.session_state.key_columns.keys()) if st.session_state.key_columns else []
    )
    
    # Update key columns in session state
    st.session_state.key_columns = {}
    for key_col in key_cols:
        if key_col in st.session_state.column_mappings:
            st.session_state.key_columns[key_col] = st.session_state.column_mappings[key_col]
        else:
            st.warning(f"Column '{key_col}' is not mapped to any column in File 2")
    
    # Reconciliation button
    if st.button("Run Reconciliation"):
        if not st.session_state.column_mappings:
            st.error("Please define at least one column mapping")
            return
        
        if not st.session_state.key_columns:
            st.error("Please select at least one key column for matching")
            return
        
        # If aggregation is enabled, validate the aggregation settings
        if st.session_state.use_aggregation:
            if not st.session_state.agg_columns_file1:
                st.error("Please select at least one column to group by for aggregation")
                return
            
            if not st.session_state.agg_functions:
                st.error("Please configure aggregation functions for at least one numeric column")
                return
        
        # Perform reconciliation
        results = perform_reconciliation(
            df1, 
            df2, 
            st.session_state.column_mappings,
            st.session_state.key_columns,
            use_aggregation=st.session_state.use_aggregation,
            agg_columns_file1=st.session_state.agg_columns_file1,
            agg_columns_file2=st.session_state.agg_columns_file2,
            agg_functions=st.session_state.agg_functions
        )
        
        st.session_state.reconciliation_results = results
        st.success("Reconciliation completed! View results in the Reconciliation Results tab")

def reconciliation_results_tab():
    """Display reconciliation results and provide export options."""
    import io
    import zipfile
    
    st.header("Reconciliation Results")
    
    if st.session_state.reconciliation_results is None:
        st.info("Run reconciliation from the Column Mapping tab to see results here")
        return
    
    results = st.session_state.reconciliation_results
    
    # Display summary statistics
    st.subheader("Summary")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Matched Records", results["stats"]["matched"])
    with col2:
        st.metric("Mismatched Records", results["stats"]["mismatched"])
    with col3:
        st.metric("Only in File 1", results["stats"]["only_in_file1"])
    with col4:
        st.metric("Only in File 2", results["stats"]["only_in_file2"])
    
    # Display matched records with differences
    st.subheader("Matched Records with Differences")
    if not results["mismatched_data"].empty:
        st.dataframe(results["mismatched_data"])
    else:
        st.success("No mismatches found in matched records!")
    
    # Display records only in File 1
    st.subheader("Records only in File 1")
    if not results["only_in_file1_data"].empty:
        st.dataframe(results["only_in_file1_data"])
    else:
        st.success("No records found exclusively in File 1!")
    
    # Display records only in File 2
    st.subheader("Records only in File 2")
    if not results["only_in_file2_data"].empty:
        st.dataframe(results["only_in_file2_data"])
    else:
        st.success("No records found exclusively in File 2!")
    
    # Export options
    st.subheader("Export Results")
    
    # Add Excel download option
    if (not results["mismatched_data"].empty or 
        not results["only_in_file1_data"].empty or 
        not results["only_in_file2_data"].empty):
        
        # Create an Excel file in memory
        output = io.BytesIO()
        current_time = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        
        # Create Excel writer object
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Write summary sheet
            summary_data = pd.DataFrame([{
                'Metric': 'Matched Records',
                'Count': results["stats"]["matched"]
            }, {
                'Metric': 'Mismatched Records',
                'Count': results["stats"]["mismatched"]
            }, {
                'Metric': 'Records Only in File 1',
                'Count': results["stats"]["only_in_file1"]
            }, {
                'Metric': 'Records Only in File 2',
                'Count': results["stats"]["only_in_file2"]
            }])
            summary_data.to_excel(writer, sheet_name='Summary', index=False)
            
            # Write mismatched records
            if not results["mismatched_data"].empty:
                results["mismatched_data"].to_excel(writer, sheet_name='Mismatched Records', index=False)
            
            # Write records only in File 1
            if not results["only_in_file1_data"].empty:
                results["only_in_file1_data"].to_excel(writer, sheet_name='Only in File 1', index=False)
            
            # Write records only in File 2
            if not results["only_in_file2_data"].empty:
                results["only_in_file2_data"].to_excel(writer, sheet_name='Only in File 2', index=False)
        
        # Get the Excel file data
        excel_data = output.getvalue()
        
        # Add download button for Excel file
        st.download_button(
            label="📊 Download Complete Report (Excel)",
            data=excel_data,
            file_name=f"reconciliation_report_{current_time}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Download all reconciliation results in a single Excel file with multiple sheets"
        )
        
        st.markdown("---")
        st.write("Download individual results as CSV:")
        
        # Create download buttons for individual CSV files
        export_col1, export_col2, export_col3 = st.columns(3)
        
        with export_col1:
            # Export mismatched records
            if not results["mismatched_data"].empty:
                csv_mismatched = results["mismatched_data"].to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Mismatched Records",
                    data=csv_mismatched,
                    file_name=f"mismatched_records_{current_time}.csv",
                    mime="text/csv",
                    help="Download records that exist in both files but have differences"
                )
            else:
                st.info("No mismatched records to export")
        
        with export_col2:
            # Export records only in File 1
            if not results["only_in_file1_data"].empty:
                csv_only_file1 = results["only_in_file1_data"].to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Records Only in File 1",
                    data=csv_only_file1,
                    file_name=f"only_in_file1_{current_time}.csv",
                    mime="text/csv",
                    help="Download records that exist only in File 1"
                )
            else:
                st.info("No records exclusive to File 1 to export")
        
        with export_col3:
            # Export records only in File 2
            if not results["only_in_file2_data"].empty:
                csv_only_file2 = results["only_in_file2_data"].to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Records Only in File 2",
                    data=csv_only_file2,
                    file_name=f"only_in_file2_{current_time}.csv",
                    mime="text/csv",
                    help="Download records that exist only in File 2"
                )
            else:
                st.info("No records exclusive to File 2 to export")
    
    # Option to download all results in a single action
    if (not results["mismatched_data"].empty or 
        not results["only_in_file1_data"].empty or 
        not results["only_in_file2_data"].empty):
        
        st.markdown("---")
        
        # Create a comprehensive report with all results
        with st.expander("Download Complete Reconciliation Report"):
            st.write("This will download all reconciliation results as separate CSV files in a zip archive.")
            
            import io
            import zipfile
            
            # Create a BytesIO object to store the zip file
            zip_buffer = io.BytesIO()
            
            # Create a ZipFile object with the BytesIO object as the file
            with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
                # Add each dataframe as a CSV file to the zip archive
                if not results["mismatched_data"].empty:
                    zip_file.writestr('mismatched_records.csv', results["mismatched_data"].to_csv(index=False))
                
                if not results["only_in_file1_data"].empty:
                    zip_file.writestr('only_in_file1.csv', results["only_in_file1_data"].to_csv(index=False))
                
                if not results["only_in_file2_data"].empty:
                    zip_file.writestr('only_in_file2.csv', results["only_in_file2_data"].to_csv(index=False))
                
                # Add a summary report
                summary = f"""Reconciliation Summary
Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

Statistics:
- Matched Records: {results["stats"]["matched"]}
- Mismatched Records: {results["stats"]["mismatched"]}
- Only in File 1: {results["stats"]["only_in_file1"]}
- Only in File 2: {results["stats"]["only_in_file2"]}
"""
                zip_file.writestr('summary.txt', summary)
            
            # Reset the buffer's position to the beginning
            zip_buffer.seek(0)
            
            # Create the download button for the zip file
            st.download_button(
                label="Download Complete Report",
                data=zip_buffer.getvalue(),
                file_name="reconciliation_report.zip",
                mime="application/zip"
            )

def export_preset_to_csv(preset_name):
    """
    Export the current column mappings and key columns to a CSV file.
    
    Args:
        preset_name (str): Name of the preset to use for the filename
    """
    # Create a directory for presets if it doesn't exist
    if not os.path.exists("presets"):
        os.makedirs("presets")
    
    # Prepare data for export
    export_data = []
    
    # Add column mappings
    for col1, col2 in st.session_state.column_mappings.items():
        is_key = col1 in st.session_state.key_columns
        export_data.append({
            "file1_column": col1,
            "file2_column": col2,
            "is_key_column": is_key
        })
    
    # Convert to DataFrame and save as CSV
    export_df = pd.DataFrame(export_data)
    filepath = f"presets/{preset_name}.csv"
    export_df.to_csv(filepath, index=False)
    return filepath

if __name__ == "__main__":
    main()