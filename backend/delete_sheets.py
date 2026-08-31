import openpyxl

excel_path = r'e:\Internship\PocketFM\US_Licensing_Lifecycle_Tracker.xlsx'
print(f"Loading {excel_path}...")

wb = openpyxl.load_workbook(excel_path)
target_sheet = "Lifecycle Tracker - Master"

sheets_to_remove = []
for sheet in wb.sheetnames:
    if sheet.strip().lower() != target_sheet.lower():
        sheets_to_remove.append(sheet)

print(f"Found {len(sheets_to_remove)} sheets to delete.")

for sheet_name in sheets_to_remove:
    print(f"Deleting {sheet_name}...")
    del wb[sheet_name]

print(f"Remaining sheets: {wb.sheetnames}")
print("Saving workbook...")
wb.save(excel_path)
print("Done!")
