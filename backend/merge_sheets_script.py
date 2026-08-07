import pandas as pd
import glob
import os

folder_path = r"E:\Internship\PocketFM"
excel_files = glob.glob(os.path.join(folder_path, "*.xlsx"))
# Add the fae court one as well if it exists in backend
if os.path.exists(r"E:\Internship\PocketFM\backend\Amazon_Scraping_Fae_Court_Romance.xlsx"):
    excel_files.append(r"E:\Internship\PocketFM\backend\Amazon_Scraping_Fae_Court_Romance.xlsx")

print("Files to merge:")
for f in excel_files:
    print(f)

dataframes = []
all_columns = set()

for file in excel_files:
    df = pd.read_excel(file)
    print(f"{os.path.basename(file)} columns: {len(df.columns)}")
    all_columns.update(df.columns)
    dataframes.append(df)

print(f"Total unique columns across all sheets: {len(all_columns)}")

merged_df = pd.concat(dataframes, ignore_index=True)
print(f"Merged dataframe shape: {merged_df.shape}")

merged_file_path = os.path.join(folder_path, "Merged_Romance_Keywords.xlsx")
merged_df.to_excel(merged_file_path, index=False)
print(f"Merged file saved to {merged_file_path}")
