import pandas as pd
df = pd.read_excel(r'E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx')
last_rows = df.iloc[10290:]
missing = last_rows[last_rows['Goodreads Rating Book 1'].isna() | last_rows['Goodreads No. of Ratings Book 1'].isna() | last_rows['Goodreads Primary Books Number'].isna()]
print(f'Found {len(missing)} rows with missing data.')
for idx, row in missing.iterrows():
    print(f'Row {idx}: {row["Book Title"]}')
    print(f'  - Rating: {row["Goodreads Rating Book 1"]}')
    print(f'  - Num Ratings: {row["Goodreads No. of Ratings Book 1"]}')
    print(f'  - Books in Series: {row["Goodreads Primary Books Number"]}')
    print(f'  - Goodreads Link: {row["Goodreads Link"]}')
