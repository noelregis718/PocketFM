import pandas as pd
import os

file_path = r"e:\Internship\PocketFM\Amazon Keyword - Fantasy Romance.xlsx"
if os.path.exists(file_path):
    df = pd.read_excel(file_path)
    print(f"Total rows: {len(df)}")
    if len(df) > 0:
        print("Last 5 Book Titles:")
        print(df.tail(5)['Book Title'].tolist())
else:
    print(f"File not found: {file_path}")
