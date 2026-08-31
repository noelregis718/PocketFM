import os
import pypandoc
import pymupdf  # fitz
from typing import List
from backend.models.book_task import BookDownloadTask

def ensure_pandoc():
    """Ensures pandoc is available."""
    try:
        pypandoc.get_pandoc_version()
    except OSError:
        print("[Optimizer] Pandoc not found. Downloading...")
        pypandoc.download_pandoc()

def compress_pdf(pdf_path: str, max_size_mb: float = 5.0) -> bool:
    """
    Compresses a PDF if it exceeds max_size_mb.
    Returns True if compression was performed or not needed, False if it failed.
    """
    if not os.path.exists(pdf_path):
        return False
        
    size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
    if size_mb <= max_size_mb:
        return True
        
    print(f"[Optimizer] PDF size ({size_mb:.2f}MB) > {max_size_mb}MB. Compressing...")
    temp_path = pdf_path + ".tmp.pdf"
    
    try:
        doc = pymupdf.open(pdf_path)
        # Save with compression options
        doc.save(temp_path, deflate=True, garbage=4)
        doc.close()
        
        # Replace original with compressed if it's actually smaller
        new_size_mb = os.path.getsize(temp_path) / (1024 * 1024)
        if new_size_mb < size_mb:
            os.replace(temp_path, pdf_path)
            print(f"[Optimizer] Compressed down to {new_size_mb:.2f}MB")
        else:
            os.remove(temp_path)
            print(f"[Optimizer] Compression didn't help, kept original.")
        return True
    except Exception as e:
        print(f"[Optimizer] PDF compression failed: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False

def convert_epub_to_docx(epub_path: str) -> str:
    """
    Converts an EPUB file to DOCX using pandoc.
    Returns the path to the newly created Docx file.
    """
    if not os.path.exists(epub_path):
        raise FileNotFoundError(f"EPUB file not found: {epub_path}")
        
    ensure_pandoc()
    
    docx_path = epub_path.replace(".epub", ".docx")
    
    # Use pypandoc to convert directly from epub to docx
    pypandoc.convert_file(epub_path, 'docx', outputfile=docx_path)
    
    return docx_path

def process_file_optimizations(books: List[BookDownloadTask]):
    """
    Processes all downloaded files for a series:
    - Compresses PDFs if they are too large
    - Converts EPUBs directly to DOCX
    """
    for book in books:
        if book.status != "downloaded":
            continue
            
        if book.pdf_path:
            compress_pdf(book.pdf_path)
            
        elif book.epub_path:
            print(f"[Optimizer] [{book.title}] Converting EPUB to DOCX...")
            try:
                docx_path = convert_epub_to_docx(book.epub_path)
                
                if os.path.exists(docx_path) and os.path.getsize(docx_path) > 0:
                    book.docx_path = docx_path
                    book.status = "completed"
                    
                    # Delete original EPUB to save space
                    try:
                        os.remove(book.epub_path)
                        print(f"[Optimizer] [{book.title}] Deleted original EPUB.")
                    except Exception as e:
                        print(f"[Optimizer] [{book.title}] Could not delete EPUB: {e}")
                        
                    print(f"[Optimizer] [{book.title}] Conversion successful: {docx_path}")
                else:
                    book.status = "conversion_failed"
                    book.error_message = "EPUB conversion failed silently (DOCX file not created or empty)"
            except Exception as e:
                book.status = "conversion_failed"
                book.error_message = f"EPUB Conversion failed: {str(e)}"
                print(f"[Optimizer] [{book.title}] Conversion failed: {e}. Original EPUB kept.")
