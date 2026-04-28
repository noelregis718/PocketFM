import pandas as pd
import os

file_path = r"E:\Internship\scraped_data_dark_romance.xlsx"
if os.path.exists(file_path):
    df = pd.read_excel(file_path)
    print(f"Total rows: {len(df)}")
    
    # Check for missing or INR pricing
    def needs_repair(val):
        if pd.isna(val): return True
        s = str(val).strip()
        if s == "" or s.lower() == "n/a" or s.lower() == "nan": return True
        if "INR" in s or "₹" in s or "\u20b9" in s: return True
        if "$" not in s and "USD" not in s: return True
        return False

    to_repair = df['Price_Tier'].apply(needs_repair)
    print(f"Rows needing repair: {to_repair.sum()}")
else:
    print(f"File not found: {file_path}")
