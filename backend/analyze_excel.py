import pandas as pd
import json

excel_path = r"e:\Internship\PocketFM\noel_part2.xlsx"
df = pd.read_excel(excel_path)

total_rows = len(df)

# Check for Goodreads Details: 
# We'll use 'Romantasy Checker' as the indicator that Goodreads processing was completed for a row.
goodreads_filled = df['Romantasy Checker'].notna() & (df['Romantasy Checker'].astype(str).str.strip() != '')

# Check for Amazon Details:
# Based on the columns, 'Amazon URL' is present. Let's see if there are other Amazon columns.
# We'll just check if 'Amazon URL' is filled for now as it's the primary indicator of Amazon data.
if 'Amazon URL' in df.columns:
    amazon_filled = df['Amazon URL'].notna() & (df['Amazon URL'].astype(str).str.strip() != '')
else:
    amazon_filled = pd.Series([False]*len(df))

num_goodreads_filled = goodreads_filled.sum()
num_amazon_filled = amazon_filled.sum()

# Rows filled with BOTH
both_filled = goodreads_filled & amazon_filled
num_both_filled = both_filled.sum()

# Rows filled with NEITHER (empty rows in terms of these details)
neither_filled = (~goodreads_filled) & (~amazon_filled)
num_neither_filled = neither_filled.sum()

# Rows with missing Goodreads
missing_goodreads = (~goodreads_filled).sum()

# Rows with missing Amazon
missing_amazon = (~amazon_filled).sum()

stats = {
    "Total Books (Rows)": total_rows,
    "Filled with Goodreads Details": int(num_goodreads_filled),
    "Missing Goodreads Details": int(missing_goodreads),
    "Filled with Amazon Details (Amazon URL)": int(num_amazon_filled),
    "Missing Amazon Details": int(missing_amazon),
    "Filled with BOTH": int(num_both_filled),
    "Filled with NEITHER": int(num_neither_filled)
}

print(json.dumps(stats, indent=4))
