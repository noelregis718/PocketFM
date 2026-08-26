import os
import openpyxl
from backend.scrapers.goodreads_series import get_primary_books_from_goodreads
from backend.scrapers.ocean_downloader import process_ocean_downloads
from backend.utils.pdf_converter import process_pdf_conversions
from urllib.parse import urlparse

def sanitize_folder_name(name: str) -> str:
    """Removes invalid characters for folder names"""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, '')
    return name.strip()

import sys

def run_pipeline(start_row: int, end_row: int):
    excel_path = "e:\\Internship\\PocketFM\\CT _ US _ Pipeline Master Sheet new.xlsx"
    downloads_base = "e:\\Internship\\PocketFM\\downloads"
    
    if not os.path.exists(downloads_base):
        os.makedirs(downloads_base)
        
    print(f"Loading Excel file: {excel_path}")
    wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
    
    if "Self-Pub Prioritization" not in wb.sheetnames:
        print("Error: 'Self-Pub Prioritization' sheet not found in the Excel file.")
        return
        
    ws = wb["Self-Pub Prioritization"]
    
    # Process rows in the specified range
    for row_idx, row in enumerate(ws.iter_rows(min_row=start_row, max_row=end_row), start=start_row):
        series_name = row[4].value
        goodreads_link = row[5].value
        
        if not goodreads_link or not isinstance(goodreads_link, str):
            print(f"[Row {row_idx}] Skipping missing or invalid link.")
            continue
            
        if 'goodreads.com/series' not in goodreads_link:
            print(f"[Row {row_idx}] Skipping non-series link: {goodreads_link}")
            continue
            
        # 1. Scrape Goodreads for book list and true series name
        books, series_name = get_primary_books_from_goodreads(goodreads_link)
        if not books:
            print(f"[Row {row_idx}] No primary books found or failed to scrape series.")
            continue
            
        clean_series_name = sanitize_folder_name(series_name)
        series_dir = os.path.join(downloads_base, clean_series_name)
        
        if not os.path.exists(series_dir):
            os.makedirs(series_dir)
            
        print(f"\n{'='*50}")
        print(f"Processing Series: {clean_series_name}")
        print(f"Link: {goodreads_link}")
        print(f"Directory: {series_dir}")
        print(f"{'='*50}")
            
        max_retries = 3
        for attempt in range(max_retries):
            # Check if there are books that still need to be processed (failed or not started)
            pending_books = [b for b in books if b.status != "completed"]
            if not pending_books:
                break
                
            if attempt > 0:
                print(f"\n[Row {row_idx}] Retrying {len(pending_books)} failed books (Attempt {attempt+1}/{max_retries})...")
                for b in pending_books:
                    b.status = "pending"
                    b.error_message = ""
                    # If it previously failed conversion, delete the bad PDF so it can be re-downloaded
                    if hasattr(b, 'pdf_path') and b.pdf_path and os.path.exists(b.pdf_path):
                        try:
                            os.remove(b.pdf_path)
                            b.pdf_path = None
                        except Exception:
                            pass
                            
            # 2. Download PDFs from OceanOfPDF
            process_ocean_downloads(books, series_dir)
            
            # 3. Convert downloaded PDFs to Word docs
            process_pdf_conversions(books)
        
        success_count = sum(1 for b in books if b.status == "completed")
        failed_count = sum(1 for b in books if b.status == "conversion_failed")
        download_failed = sum(1 for b in books if b.status == "failed")
        
        print(f"\n[Row {row_idx}] Finished processing series: {clean_series_name}")
        print(f"  - Successfully downloaded & converted to DOCX: {success_count}")
        print(f"  - Failed DOCX conversion (PDF kept): {failed_count}")
        print(f"  - Failed to download: {download_failed}")
        
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the downloader pipeline for a range of rows.")
    parser.add_argument("--start", type=int, required=True, help="Starting row index (e.g. 216)")
    parser.add_argument("--end", type=int, required=True, help="Ending row index (e.g. 220)")
    args = parser.parse_args()
    
    run_pipeline(args.start, args.end)
