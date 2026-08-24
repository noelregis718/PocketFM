import openpyxl
import csv

excel_file = r'E:\Internship\PocketFM\Romantasy _ Self Publication Master.xlsx'
csv_file = r'E:\Internship\PocketFM\Romantasy _ Self Publication Master.csv'

print(f"Loading Excel file: {excel_file}")
wb = openpyxl.load_workbook(excel_file, data_only=True)
sheet = wb.active

print(f"Writing to CSV file: {csv_file}")
with open(csv_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    for row in sheet.values:
        writer.writerow(row)

print("Done.")
