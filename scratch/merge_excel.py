import pandas as pd
import os

file1 = 'Combined List of Titles.xlsx'
file2 = 'Combined_List_of_Titles (1).xlsx'
output_file = 'Combined_List_of_Titles_Merged.xlsx'

df1 = pd.read_excel(file1)
df2 = pd.read_excel(file2)

# Merge the dataframes
merged_df = pd.concat([df1, df2], ignore_index=True)
merged_df = merged_df.drop_duplicates()

merged_df.to_excel(output_file, index=False)

# Delete the original files
os.remove(file1)
os.remove(file2)

print(f"Successfully merged {len(df1)} and {len(df2)} rows into {len(merged_df)} rows in {output_file} and deleted originals.")
