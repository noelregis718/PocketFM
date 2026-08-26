from pdf2docx import Converter

pdf_file = "Murder with malice (Blake, Nicholas, 1904-1972) (z-library.sk, 1lib.sk, z-lib.sk)-compressed.pdf"
docx_file = "Murder with malice.docx"

cv = Converter(pdf_file)
cv.convert(docx_file)
cv.close()
