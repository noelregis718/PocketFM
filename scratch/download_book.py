import sys
from ocean_scraper import download_book_ocean
from zlib_scraper import download_book_zlib

def main():
    if len(sys.argv) < 5:
        print("Usage: python download_book.py 'Book Name' 'Series Name' 'Z-Lib Email' 'Z-Lib Password'")
        sys.exit(1)
        
    book_name = sys.argv[1]
    series_name = sys.argv[2]
    
    zlib_email = sys.argv[3]
    zlib_password = sys.argv[4]
    
    print("==================================================")
    print(f"Starting Download Process for: {book_name}")
    print("==================================================")
    
    # 1. Try OceanOfPDF First (No Limits)
    print("\n>>> ATTEMPT 1: OceanOfPDF (Unlimited) <<<")
    ocean_success = download_book_ocean(book_name, series_name)
    
    if ocean_success:
        print("\n[SUCCESS] Book successfully downloaded from OceanOfPDF!")
        print("==================================================")
        return
        
    print("\n[WARNING] OceanOfPDF failed to find or download the book.")
    
    # 2. Fallback to Z-Library (10 books/day limit)
    print("\n>>> ATTEMPT 2: Z-Library Fallback (10/day Limit) <<<")
    zlib_success = download_book_zlib(book_name, series_name, zlib_email, zlib_password)
    
    if zlib_success:
        print("\n[SUCCESS] Book successfully downloaded from Z-Library!")
    else:
        print("\n[FAILURE] Both OceanOfPDF and Z-Library failed to download the book.")
        
    print("==================================================")

if __name__ == "__main__":
    main()
