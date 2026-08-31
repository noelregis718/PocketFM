import os
from pdf2docx import Converter
import fitz  # PyMuPDF
from docx import Document
from backend.models.book_task import BookDownloadTask
from typing import List

def convert_pdf_to_docx(pdf_path: str) -> str:
    """
    Converts a PDF file to a Word Docx file using standard layout extraction.
    Returns the path to the newly created Docx file.
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
    docx_path = pdf_path.replace(".pdf", ".docx")
    
    cv = Converter(pdf_path)
    cv.convert(docx_path, start=0, end=None)
    cv.close()
    
    return docx_path

def convert_pdf_to_docx_fast(pdf_path: str) -> str:
    """
    Converts a PDF to DOCX instantly by extracting raw text, skipping layout/images.
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
    docx_path = pdf_path.replace(".pdf", ".docx")
    
    pdf_doc = fitz.open(pdf_path)
    word_doc = Document()
    
    for i in range(len(pdf_doc)):
        page = pdf_doc[i]
        text = page.get_text("text")
        if text.strip():
            word_doc.add_paragraph(text)
        if i < len(pdf_doc) - 1:
            word_doc.add_page_break()
            
    pdf_doc.close()
    word_doc.save(docx_path)
    return docx_path

def process_pdf_conversions(books: List[BookDownloadTask]):
    """
    Takes a list of BookDownloadTasks and converts downloaded PDFs to DOCX.
    Updates the task models with docx_path and status.
    """
    for book in books:
        if book.status == "downloaded" and book.pdf_path:
            file_size_mb = os.path.getsize(book.pdf_path) / (1024 * 1024)
            if file_size_mb > 10:
                print(f"[PDF2DOCX] [{book.title}] PDF is very large ({file_size_mb:.2f}MB). Using FAST text-only conversion...")
                converter_func = convert_pdf_to_docx_fast
            else:
                print(f"[PDF2DOCX] [{book.title}] Converting PDF to DOCX...")
                converter_func = convert_pdf_to_docx
                
            try:
                docx_path = converter_func(book.pdf_path)
                
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
