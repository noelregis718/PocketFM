import pandas as pd
import glob
import os

merged_file = 'Merged_Romance_Keywords.xlsx'
individual_files = [
    'Deadly Trials Romance.xlsx',
    'Demon Romance.xlsx',
    'Dungeon Trials Romance.xlsx',
    'Fantasy Tournament Romance.xlsx',
    'Fated Mates.xlsx',
    'Greek Mythology Romance.xlsx',
    'Progression Fantasy Romance.xlsx',
    'Shifter Romance.xlsx',
    'Vampire Romance.xlsx',
    'Werewolf Romance.xlsx',
    'Witch Romance.xlsx'
]

def get_title_col(df):
    for col in df.columns:
        if col.lower().strip() in ['title', 'book title', 'book_title', 'name']:
            return col
    # Fallback: find first col with title or book in it
    for col in df.columns:
        if 'title' in col.lower() or 'book' in col.lower():
            if 'url' not in col.lower() and 'link' not in col.lower() and 'id' not in col.lower():
                return col
    return df.columns[0] # fallback

print("Loading merged sheet...")
merged_df = pd.read_excel(merged_file)
merged_title_col = get_title_col(merged_df)
# We use lower case and strip for robust comparison
merged_titles = set(merged_df[merged_title_col].dropna().astype(str).str.strip().str.lower())
print(f"Merged sheet has {len(merged_titles)} unique titles. (Column: {merged_title_col})")

total_missing = 0
for file in individual_files:
    if not os.path.exists(file):
        print(f"File not found: {file}")
        continue
    df = pd.read_excel(file)
    title_col = get_title_col(df)
    titles = set(df[title_col].dropna().astype(str).str.strip().str.lower())
    
    missing = titles - merged_titles
    if missing:
        print(f"\n[MISSING] {file}: {len(missing)} titles missing from Merged Sheet! (Total from sheet: {len(titles)})")
        total_missing += len(missing)
        for t in list(missing)[:5]:
            print(f"  - {t}")
        if len(missing) > 5:
            print(f"  - ... and {len(missing) - 5} more")
    else:
        print(f"[OK] {file}: All {len(titles)} titles are present in the Merged Sheet.")

print(f"\nTotal missing titles across all 11 sheets: {total_missing}")
