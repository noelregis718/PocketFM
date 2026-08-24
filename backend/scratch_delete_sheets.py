import openpyxl

file_path = r'E:\Internship\PocketFM\Romantasy _ Self Publication Master.xlsx'

print(f"Loading workbook: {file_path}")
wb = openpyxl.load_workbook(file_path)

keep_sheet = 'Picks for Licensing'

for sheet_name in wb.sheetnames:
    if sheet_name != keep_sheet:
        print(f"Removing sheet: {sheet_name}")
        del wb[sheet_name]

print("Saving workbook...")
wb.save(file_path)
print("Done.")
