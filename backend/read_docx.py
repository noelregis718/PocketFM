import docx

doc_path = r'E:\Internship\PocketFM\Amazon Scraping Expansion Report.docx'
doc = docx.Document(doc_path)
for para in doc.paragraphs:
    print(para.text)
