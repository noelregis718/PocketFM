import re
import requests
from bs4 import BeautifulSoup
from typing import List, Tuple
from backend.models.book_task import BookDownloadTask

def get_primary_books_from_goodreads(goodreads_url: str) -> Tuple[List[BookDownloadTask], str]:
    """
    Scrapes a Goodreads series link to extract the first 10 primary books and the series name.
    """
    books = []
    series_name = "Unknown_Series"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    print(f"[Goodreads] Fetching: {goodreads_url}")
    try:
        r = requests.get(goodreads_url, headers=headers, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[Goodreads] Error fetching page: {e}")
        return books, series_name
        
    soup = BeautifulSoup(r.text, 'html.parser')
    
    h1 = soup.find('h1')
    if h1:
        series_name = h1.text.strip()
        if series_name.lower().endswith(" series"):
            series_name = series_name[:-7].strip()
    
    # We find all spans with itemprop="name"
    spans = soup.find_all('span', itemprop='name')
    
    seen_titles = set()
    
    for span in spans:
        if len(books) >= 10:
            break
            
        h3 = span.find_previous('h3')
        if not h3:
            continue
            
        book_label = h3.text.strip()
        match = re.search(r"Book\s+([\d\.]+)", book_label, re.IGNORECASE)
        
        if match:
            num_str = match.group(1)
            # Only primary books (whole numbers)
            if "." not in num_str:
                book_number = int(num_str)
                if book_number > 10:
                    continue
                title = span.text.strip()
                
                # Because author also has itemprop="name", we check if we already added a book for this number
                # The title always appears before the author in the DOM
                if book_number not in [b.number for b in books] and title not in seen_titles:
                    books.append(BookDownloadTask(title=title, number=book_number))
                    seen_titles.add(title)
                    print(f"  -> Found Primary Book {book_number}: {title}")
                    
    return books, series_name

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        res = get_primary_books_from_goodreads(sys.argv[1])
        print("Final extracted books:")
        for b in res:
            print(b)
