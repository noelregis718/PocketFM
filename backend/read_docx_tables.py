import docx

doc_path = r'E:\Internship\PocketFM\Amazon Scraping Expansion Report.docx'
doc = docx.Document(doc_path)
for table in doc.tables:
    for row in table.rows:
        row_text = " | ".join([cell.text.replace("\n", " ") for cell in row.cells])
        print(row_text)
