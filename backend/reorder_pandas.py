import pandas as pd

file_path = r'e:\Internship\PocketFM\Merged_Romance_Keywords.xlsx'
print(f"Loading {file_path} into pandas...")
df = pd.read_excel(file_path)

# Extract column names
cols = df.columns.tolist()

# Define the block of columns to move
amazon_cols = [
    'Amazon Num_Primary_Books_in_Series',
    'Amazon Total_Page_Count_of_Primary_Books',
    'Amazon Book1_Rating',
    'Amazon Book1_Num_Ratings'
]

goodreads_cols = [
    'Goodreads Rating',
    'Goodreads No. of Ratings',
    'Goodreads Link'
]

# Remove them from the list
for c in amazon_cols + goodreads_cols:
    if c in cols:
        cols.remove(c)

# Find the index of GoodReads_Series_URL in the remaining list
insert_idx = cols.index('GoodReads_Series_URL')

# Insert Amazon columns BEFORE GoodReads_Series_URL
# Insert Goodreads columns AFTER GoodReads_Series_URL
new_order = cols[:insert_idx] + amazon_cols + ['GoodReads_Series_URL'] + goodreads_cols + cols[insert_idx+1:]

# Reorder dataframe
df = df[new_order]

print("Saving reordered workbook...")
df.to_excel(file_path, index=False)
print("Columns reordered successfully.")
