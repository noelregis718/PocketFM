import pandas as pd
import numpy as np
import re

df = pd.read_excel(r'E:\Internship\PocketFM\Combined List of Titles.xlsx')

def extract_number(val):
    if pd.isna(val) or val is None: return 0
    try: return float(val)
    except:
        match = re.search(r'(\d+(\.\d+)?)', str(val))
        if match: return float(match.group(1))
    return 0

count_strict_gt_4 = 0
count_ge_4 = 0

for idx, row in df.iterrows():
    gr_books = extract_number(row.get('Goodreads Primary Books Number'))
    gr_pages = extract_number(row.get('Goodreads Primary Books Page Count'))
    
    if gr_books > 4 and gr_pages > 1600:
        count_strict_gt_4 += 1
        
    if gr_books >= 4 and gr_pages > 1600:
        count_ge_4 += 1

print(f"Number of books where Goodreads Primary Books > 4 AND Goodreads Pages > 1600: {count_strict_gt_4}")
print(f"Number of books where Goodreads Primary Books >= 4 AND Goodreads Pages > 1600: {count_ge_4}")
