import pandas as pd
import sys

file_path = r"E:\Internship\PocketFM\CT _ US _ Pipeline Master Sheet.xlsx"
sheet_name = "Self-Pub Prioritization"

try:
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    # Print the first 20 rows of the first 10 columns
    print(df.iloc[:20, :10].to_string())
            
except Exception as e:
    print(f"Error: {e}")
