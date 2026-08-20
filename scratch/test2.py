import pandas as pd
df = pd.read_excel(r'E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx')
last_rows = df.iloc[10290:]
missing = last_rows[last_rows['Goodreads Rating Book 1'].isna()]
for idx, row in missing.iterrows():
    title = row['Book Title']
    author = row['Author Name']
    print(f'Row {idx}: {title} by {author}')
