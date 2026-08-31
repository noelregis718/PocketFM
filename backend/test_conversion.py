import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend.utils.pdf_converter import convert_pdf_to_docx_fast, convert_pdf_to_docx

if len(sys.argv) < 2:
    print("Usage: python test_conversion.py <path_to_pdf>")
    sys.exit(1)

pdf_path = sys.argv[1]

if not os.path.exists(pdf_path):
    print(f"File not found: {pdf_path}")
    sys.exit(1)

file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
print(f"File Size: {file_size_mb:.2f} MB")

if file_size_mb > 10:
    print("PDF is over 10MB! Testing FAST Text-Only Extraction Mode...")
    try:
        docx_path = convert_pdf_to_docx_fast(pdf_path)
        print(f"SUCCESS! Fast conversion saved to: {docx_path}")
    except Exception as e:
        print(f"Fast conversion failed: {e}")
else:
    print("PDF is under 10MB! Testing Standard Layout Extraction Mode...")
    try:
        docx_path = convert_pdf_to_docx(pdf_path)
        print(f"SUCCESS! Standard conversion saved to: {docx_path}")
    except Exception as e:
        print(f"Standard conversion failed: {e}")
