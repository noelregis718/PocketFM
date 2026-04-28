import pandas as pd
import os

source = r"E:\Internship\PocketFM\Romantasy _ Self Publication Master.xlsx"
target = r"E:\Internship\PocketFM\Romantasy _ Self Publication Master_Recovered.xlsx"

try:
    print(f"Attempting to rebuild {source}...")
    # Read all sheets
    all_sheets = pd.read_excel(source, sheet_name=None)
    
    with pd.ExcelWriter(target, engine='xlsxwriter') as writer:
        for sheet_name, df in all_sheets.items():
            print(f"  Rebuilding sheet: {sheet_name}")
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"Rebuild successful: {target}")
    # Replace original if successful
    # os.replace(target, source)
except Exception as e:
    print(f"Critical Failure during rebuild: {e}")
