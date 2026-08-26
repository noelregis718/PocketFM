import pandas as pd
import openpyxl

file_path = r"e:\Internship\PocketFM\CT _ US _ Pipeline Master Sheet.xlsx"

# Find the exact sheet name quickly using openpyxl in read-only mode
wb = openpyxl.load_workbook(file_path, read_only=True)
target_sheet = None
for name in wb.sheetnames:
    if "self pub prioritization" in name.lower() or "self pub" in name.lower():
        target_sheet = name
        break
wb.close()

if not target_sheet:
    print("Could not find the self pub prioritization sheet.")
    exit(1)

print(f"Reading sheet: {target_sheet}...")
# Read just the header and a few rows to get the columns
try:
    df = pd.read_excel(file_path, sheet_name=target_sheet, nrows=20)
    print("Columns found in the sheet:")
    for i, col in enumerate(df.columns):
        print(f" - {col}")
        
    print("\nLooking for specific columns related to Goodreads, Book1, Book 2, Drive links...")
    relevant_cols = [c for c in df.columns if any(keyword in str(c).lower() for keyword in ["goodreads", "book1", "book 1", "book2", "book 2", "word", "drive", "link", "report"])]
    print("\nRelevant columns found:")
    for col in relevant_cols:
        print(f" - {col}")
        # Print a sample of non-null values
        sample = df[col].dropna().head(3).tolist()
        print(f"   Sample data: {sample}")
except Exception as e:
    print(f"Error reading with pandas: {e}")
