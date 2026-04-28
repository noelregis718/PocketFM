import pandas as pd

file_path = r"E:\Internship\PocketFM\Romantasy _ Self Publication Master.xlsx"
xl = pd.ExcelFile(file_path)

for sheet in xl.sheet_names:
    try:
        df = xl.parse(sheet, nrows=0)
        cols = [str(c) for c in df.columns]
        if any("Author Contact Form" in c for c in cols):
            print(f"FOUND IN SHEET: {sheet}")
            print(f"COLUMNS: {cols}")
    except:
        pass
