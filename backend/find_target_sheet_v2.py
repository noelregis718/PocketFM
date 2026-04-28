import pandas as pd

file_path = r"E:\Internship\PocketFM\Romantasy _ Self Publication Master.xlsx"
xl = pd.ExcelFile(file_path)

targets = ["Author Email ID", "Author Contact Form - Website", "Agency Email ID"]

for sheet in xl.sheet_names:
    try:
        df = xl.parse(sheet, nrows=5) # check first few rows for header
        cols = [str(c) for c in df.columns]
        matches = [t for t in targets if t in cols]
        if matches:
            print(f"SHEET: {sheet}")
            print(f"MATCHES: {matches}")
            print(f"ALL COLS: {cols}")
    except:
        pass
