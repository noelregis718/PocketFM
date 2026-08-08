import openpyxl

file_path = r'e:\Internship\PocketFM\Merged_Romance_Keywords.xlsx'
print(f"Loading {file_path}...")
wb = openpyxl.load_workbook(file_path)
ws = wb.active

col_to_delete = 18

print(f"Deleting the extra Sub-Genre column at index {col_to_delete} (Header: '{ws.cell(row=1, column=col_to_delete).value}')...")
ws.delete_cols(col_to_delete)

print("Saving workbook...")
wb.save(file_path)
print("Extra column successfully deleted.")
