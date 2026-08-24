import pandas as pd

merged_file = 'Merged_Romance_Keywords.xlsx'

print(f"Loading {merged_file}...")
df = pd.read_excel(merged_file)

# Find rows where Book Title or Author Name is missing
missing_title = df[df['Book Title'].isna() | (df['Book Title'].astype(str).str.strip() == '') | (df['Book Title'].astype(str).str.lower() == 'nan')]
missing_author = df[df['Author Name'].isna() | (df['Author Name'].astype(str).str.strip() == '') | (df['Author Name'].astype(str).str.lower() == 'nan')]

print(f"Total rows in sheet: {len(df)}")
print(f"Rows missing 'Book Title': {len(missing_title)}")
print(f"Rows missing 'Author Name': {len(missing_author)}")

# Let's see if there are completely empty rows at the end
empty_rows = df[df.isna().all(axis=1)]
print(f"Completely empty rows: {len(empty_rows)}")

if len(missing_title) > 0:
    print("\nSample of rows missing Book Title (showing index + Amazon URL or relevant columns):")
    cols_to_show = ['Amazon URL'] if 'Amazon URL' in df.columns else df.columns[:2]
    print(missing_title[cols_to_show].tail(10))

if len(missing_author) > 0:
    print("\nSample of rows missing Author Name:")
    cols_to_show = ['Book Title', 'Amazon URL'] if 'Amazon URL' in df.columns else df.columns[:2]
    print(missing_author[cols_to_show].tail(10))
