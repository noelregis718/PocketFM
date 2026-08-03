import pandas as pd

file = r"e:\Internship\PocketFM\noel_part1.xlsx"
print(f"Scanning {file} for rows with 0 primary books...")

df = pd.read_excel(file)

columns_to_clear = [
    'GoodReads_Series_URL', 
    'Num_Primary_Books_in_Series', 
    'Total_Page_Count_of_Primary_Books', 
    'Book1_Rating', 
    'Book1_Num_Ratings', 
    'Genre Tags', 
    'Romantasy Checker', 
    'Synopsis'
]

count = 0
for index, row in df.iterrows():
    val = row.get('Num_Primary_Books_in_Series')
    # Check if the value is explicitly 0, '0', or 0.0
    if pd.notna(val) and (str(val).strip() == '0' or val == 0 or val == 0.0):
        for col in columns_to_clear:
            if col in df.columns:
                df.at[index, col] = ""
        count += 1

if count > 0:
    df.to_excel(file, index=False)
    print(f"Successfully cleared Goodreads data for {count} rows.")
else:
    print("No rows with 0 books found.")
