import openpyxl

file_path = "E:\\Internship\\PocketFM\\Romantasy _ Self Publication Master.xlsx"
wb = openpyxl.load_workbook(file_path)
ws = wb.active

# Keep row 1 (header)
# Delete from row 2 up to row 2413
# Row 2414 will shift up to become the new row 2

start_row_to_delete = 2
end_row_to_delete = 2413
amount_to_delete = end_row_to_delete - start_row_to_delete + 1

ws.delete_rows(start_row_to_delete, amount_to_delete)

wb.save(file_path)
print(f"Successfully deleted rows {start_row_to_delete} to {end_row_to_delete}. New total rows: {ws.max_row}")
