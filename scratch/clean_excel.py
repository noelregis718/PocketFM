import openpyxl
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

file_path = "E:\\Internship\\PocketFM\\Romantasy _ Self Publication Master.xlsx"
wb = openpyxl.load_workbook(file_path)

keep_sheet_name = 'picks for licensing'
sheet_names = wb.sheetnames
sheet_to_keep = None

for sheet in sheet_names:
    if sheet.strip().lower() == keep_sheet_name.lower():
        sheet_to_keep = sheet
        break

if not sheet_to_keep:
    sheet_to_keep = sheet_names[0]
    print(f"Sheet '{keep_sheet_name}' not found. Keeping the first sheet instead: '{sheet_to_keep}'")
else:
    print(f"Keeping sheet: '{sheet_to_keep}'")

for sheet in sheet_names:
    if sheet != sheet_to_keep:
        del wb[sheet]
        print(f"Deleted sheet: '{sheet}'")

# Unprotect the sheet just in case
wb[sheet_to_keep].protection.sheet = False

# Unprotect workbook
if wb.security:
    wb.security.lockStructure = False

wb.save(file_path)
print(f"Successfully cleaned '{file_path}'.")
