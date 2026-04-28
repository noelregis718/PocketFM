import pandas as pd
import os

file_path = r"E:\Internship\scraped_data_forbidden_romance.xlsx"
if os.path.exists(file_path):
    df = pd.read_excel(file_path)
    print(f"Total rows: {len(df)}")
    
    missing_sub = df['Sub_Genre'].isna() | (df['Sub_Genre'] == 'N/A')
    print(f"Rows missing Sub_Genre: {missing_sub.sum()}")
else:
    print(f"File not found: {file_path}")
