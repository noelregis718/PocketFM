import pandas as pd
import os

file_path = r"E:\Internship\scraped_data_forbidden_romance.xlsx"
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
    
    # Show first 5 rows needing repair to see index range
    repair_indices = df.index[to_repair].tolist()
    if repair_indices:
        print(f"Repair indices range: {min(repair_indices)} to {max(repair_indices)}")
        print(f"First 10 repair indices: {repair_indices[:10]}")
else:
    print(f"File not found: {file_path}")
