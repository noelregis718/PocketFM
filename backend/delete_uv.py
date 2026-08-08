import openpyxl

file_path = r'e:\Internship\PocketFM\Merged_Romance_Keywords.xlsx'
print(f"Loading {file_path}...")
wb = openpyxl.load_workbook(file_path)
ws = wb.active

# Columns to delete: V (22), U (21)
# Deleting in descending order
cols_to_delete = [22, 21]

print("Deleting columns U and V...")
for col_idx in cols_to_delete:
    print(f"Deleting column {col_idx} ({ws.cell(row=1, column=col_idx).value})...")
    ws.delete_cols(col_idx)

print("Saving workbook...")
wb.save(file_path)
print("Columns U and V have been successfully deleted.")
