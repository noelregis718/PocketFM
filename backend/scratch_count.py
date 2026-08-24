import pandas as pd

df = pd.read_excel(r'E:\Internship\PocketFM\Combined List of Titles.xlsx')
def is_missing(val):
    return pd.isna(val) or str(val).strip() == '' or str(val).lower() == 'nan'

missing = 0
for idx, row in df.iterrows():
    url = row.get('Amazon URL')
    if pd.notna(url) and str(url).startswith('http'):
        if (is_missing(row.get('Amazon Book1_Rating')) or 
            is_missing(row.get('Amazon Book1_Num_Ratings')) or 
            is_missing(row.get('Amazon Num_Primary_Books_in_Series')) or 
            is_missing(row.get('Amazon Total_Page_Count_of_Primary_Books'))):
            missing += 1

print(f'REMAINING_MISSING: {missing}')
