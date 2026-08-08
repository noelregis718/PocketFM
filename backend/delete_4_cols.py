import openpyxl

file_path = r'e:\Internship\PocketFM\Merged_Romance_Keywords.xlsx'
print(f"Loading {file_path}...")
wb = openpyxl.load_workbook(file_path)
ws = wb.active

# Columns to delete: AA (27), Z (26), Y (25), X (24)
# We delete in descending order so the indices of remaining columns don't shift while deleting.
cols_to_delete = [27, 26, 25, 24]

print("Deleting the 4 redundant columns...")
for col_idx in cols_to_delete:
    print(f"Deleting column {col_idx} ({ws.cell(row=1, column=col_idx).value})...")
    ws.delete_cols(col_idx)

print("Saving workbook...")
wb.save(file_path)
print("The 4 columns have been successfully deleted.")
