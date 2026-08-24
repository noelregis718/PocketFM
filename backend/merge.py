import pandas as pd
import numpy as np

file1 = r'E:\Internship\PocketFM\Combined List of Titles.xlsx'
file2 = r'E:\Internship\PocketFM\Combined_List_of_Titles (1).xlsx'
output_file = r'E:\Internship\PocketFM\Mega_Combined_List.xlsx'

print("Loading both Excel files... (this may take a minute)")
df1 = pd.read_excel(file1)
df2 = pd.read_excel(file2)

print("Cleaning up empty cells...")
# Replace empty strings or pure whitespace with true NaNs so they can be overwritten
df1.replace(r'^\s*$', np.nan, regex=True, inplace=True)
df2.replace(r'^\s*$', np.nan, regex=True, inplace=True)

def make_key(row):
    url = str(row.get('Amazon URL', '')).strip()
    if pd.notna(url) and url and url.lower() != 'nan':
        return url
    
    title = str(row.get('Book Title', '')).strip().lower()
    author = str(row.get('Author Name', '')).strip().lower()
    return f"{title}:::{author}"

print("Aligning rows based on unique Amazon URLs & Book Titles...")
df1['merge_key'] = df1.apply(make_key, axis=1)
df2['merge_key'] = df2.apply(make_key, axis=1)

# Drop any accidental exact duplicates
df1 = df1.drop_duplicates(subset=['merge_key'])
df2 = df2.drop_duplicates(subset=['merge_key'])

# Set index for the magic merge
df1.set_index('merge_key', inplace=True)
df2.set_index('merge_key', inplace=True)

print("Merging and filling in all the blanks...")
# df1.combine_first(df2) means: Keep everything in df1. If df1 is blank, grab the data from df2!
merged_df = df1.combine_first(df2)

# Reset index and make sure columns are in the original order
merged_df.reset_index(drop=True, inplace=True)

# Reorder columns to match original
original_cols = [c for c in df1.columns if c != 'merge_key']
# Add any new columns that might have existed only in df2 (just in case)
for c in df2.columns:
    if c not in original_cols and c != 'merge_key':
        original_cols.append(c)

merged_df = merged_df[original_cols]

print(f"Saving merged mega-file to {output_file}...")
merged_df.to_excel(output_file, index=False)
print(f"Merge Complete! Mega list created with {len(merged_df)} total rows.")
