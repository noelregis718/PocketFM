import pandas as pd
import json

file_path = r'E:\Internship\PocketFM\Combined List of Titles.xlsx'
df = pd.read_excel(file_path)

total_rows = len(df)
keywords = []
if 'Keyword' in df.columns:
    keywords = df['Keyword'].dropna().unique().tolist()
elif 'Sub-Genre' in df.columns:
    keywords = df['Sub-Genre'].dropna().unique().tolist()

print(f"TOTAL_ROWS: {total_rows}")
print(f"TOTAL_KEYWORDS: {len(keywords)}")
print(f"KEYWORD_LIST: {keywords}")
