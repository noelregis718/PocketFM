import pandas as pd

file = r"e:\Internship\PocketFM\noel_part2.xlsx"

df = pd.read_excel(file)

initial_count = len(df)

# Filter out rows where Num_Primary_Books_in_Series is explicitly 0, '0', or 0.0
# The tilde (~) means "Keep rows that DO NOT match this condition"
condition = (
    (df['Num_Primary_Books_in_Series'].astype(str).str.strip() == '0') | 
    (df['Num_Primary_Books_in_Series'] == 0) | 
    (df['Num_Primary_Books_in_Series'] == 0.0)
)

df_filtered = df[~condition]

final_count = len(df_filtered)
deleted = initial_count - final_count

if deleted > 0:
    df_filtered.to_excel(file, index=False)
    print(f"Successfully deleted {deleted} entire rows from {file}.")
else:
    print("No rows with 0 primary books found to delete.")
