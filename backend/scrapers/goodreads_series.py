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
    
    print(f"[Goodreads] Fetching: {goodreads_url}")
    
    import time
    from playwright.sync_api import sync_playwright
    
    max_retries = 3
    for attempt in range(max_retries):
        books = []
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                page = browser.new_page()
                page.goto(goodreads_url, timeout=30000)
                
                # Give page time to load and bypass Cloudflare if present
                try:
                    page.wait_for_selector('h1', timeout=5000)
                except:
                    page.wait_for_timeout(3000)
                    
                html_content = page.content()
                browser.close()
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
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
                            author = ""
                            parent = h3.parent
                            if parent:
                                parent_spans = parent.find_all('span', itemprop='name')
                                if len(parent_spans) > 1:
                                    author = parent_spans[1].text.strip()
                                    
                            books.append(BookDownloadTask(title=title, number=book_number, author=author))
                            seen_titles.add(title)
                            
            if len(books) > 0:
                print(f"  -> Found {len(books)} primary books!")
                break
            else:
                print(f"[Goodreads] Attempt {attempt + 1} failed to find books (likely blocked). Retrying...")
                time.sleep(2)
                
        except Exception as e:
            print(f"[Goodreads] Error fetching page on attempt {attempt + 1}: {e}")
            time.sleep(2)
            
    return books, series_name

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        res = get_primary_books_from_goodreads(sys.argv[1])
        print("Final extracted books:")
        for b in res:
            print(b)
