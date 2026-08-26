import pandas as pd
import sys

file_path = r"e:\Internship\PocketFM\CT _ US _ Pipeline Master Sheet.xlsx"

print(f"Loading Excel file {file_path} using pandas ExcelFile...", flush=True)
try:
    xl = pd.ExcelFile(file_path)
    
    target_sheet = None
    for name in xl.sheet_names:
        if "self-pub prioritization" in name.lower() or "self pub prioritization" in name.lower():
            target_sheet = name
            break
            
    if not target_sheet:
        print("Could not find the self pub prioritization sheet.", flush=True)
        sys.exit(1)
        
    print(f"Reading sheet: {target_sheet} (first 20 rows)...", flush=True)
    df = pd.read_excel(xl, sheet_name=target_sheet, nrows=20, header=None) # No header to see raw data
    
    print("\nFirst 10 rows raw data:")
    for i, row in df.head(10).iterrows():
        print(f"Row {i}: {row.dropna().tolist()}")

except Exception as e:
    print(f"Error reading with pandas: {e}", flush=True)
