# CSV Reconciliation Tool

A Streamlit-based tool for comparing and reconciling two CSV files with flexible column mapping and aggregation capabilities.

## Features

- Upload and compare two CSV files
- Define custom column mappings between files
- Save and load mapping presets
- Aggregate data before reconciliation (sum, mean, count, min, max)
- Identify matched records with differences
- Identify records unique to each file
- Export reconciliation results to CSV files

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd Reco

# Create a virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

1. Run the application:
   ```
   streamlit run app.py
   ```

2. Access the application in your web browser (typically at http://localhost:8501)

3. Upload your CSV files, configure column mappings, and run the reconciliation

## Project Structure

- `app.py`: Main Streamlit application
- `utils.py`: Reconciliation logic and utility functions
- `requirements.txt`: Project dependencies
- `generate_sample_data.py`: Script to generate sample data for testing
- `create_sample_preset.py`: Script to create a sample mapping preset

## Sample Data

The project includes scripts to generate sample data for testing:

```bash
python generate_sample_data.py  # Creates sample CSV files
python create_sample_preset.py  # Creates a sample mapping preset
```