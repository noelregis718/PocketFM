import os
from pdf2docx import Converter
from backend.models.book_task import BookDownloadTask
from typing import List

def convert_pdf_to_docx(pdf_path: str) -> str:
    """
    Converts a PDF file to a Word Docx file.
    Returns the path to the newly created Docx file.
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
    docx_path = pdf_path.replace(".pdf", ".docx")
    
    cv = Converter(pdf_path)
    cv.convert(docx_path, start=0, end=None)
    cv.close()
    
    return docx_path

def process_pdf_conversions(books: List[BookDownloadTask]):
    """
    Takes a list of BookDownloadTasks and converts downloaded PDFs to DOCX.
    Updates the task models with docx_path and status.
    """
    for book in books:
        if book.status == "downloaded" and book.pdf_path:
            print(f"[PDF2DOCX] [{book.title}] Converting PDF to DOCX...")
            try:
                docx_path = convert_pdf_to_docx(book.pdf_path)
                
                # Verify that the DOCX file was actually created and is not empty
                if os.path.exists(docx_path) and os.path.getsize(docx_path) > 0:
                    book.docx_path = docx_path
                    book.status = "completed"
                    
                    # Delete original PDF to save space only if conversion succeeded
                    try:
                        os.remove(book.pdf_path)
                        print(f"[PDF2DOCX] [{book.title}] Deleted original PDF.")
                    except Exception as e:
                        print(f"[PDF2DOCX] [{book.title}] Could not delete PDF: {e}")
                        
                    print(f"[PDF2DOCX] [{book.title}] Conversion successful: {docx_path}")
                else:
                    book.status = "conversion_failed"
                    book.error_message = "PDF conversion failed silently (DOCX file not created or empty)"
                    print(f"[PDF2DOCX] [{book.title}] Conversion failed: Output DOCX not created or empty. Original PDF kept.")
            except Exception as e:
                book.status = "conversion_failed"
                book.error_message = f"PDF Conversion failed: {str(e)}"
                print(f"[PDF2DOCX] [{book.title}] Conversion failed: {e}. Original PDF kept.")
