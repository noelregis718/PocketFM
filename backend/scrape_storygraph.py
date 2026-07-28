from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

EXCEL_PATH = r"e:\Internship\PocketFM\Scraping_Sheet.xlsx"

def scrape():
    data = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        try:
            print("Navigating to The StoryGraph...")
            page.goto("https://app.thestorygraph.com/browse?search_term=Dark%20Academia", wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            
            # Keep scrolling until no more results load
            last_count = 0
            while True:
                # Scroll down to bottom
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                
                # The user suggested waiting 10 seconds to allow the page to load more books
                print("Scrolling down and waiting 10 seconds for more books to load...")
                page.wait_for_timeout(10000)
                
                # Check for "Load More" button again just in case
                try:
                    load_more = page.query_selector("button:has-text('Load more')")
                    if load_more and load_more.is_visible():
                        load_more.click()
                        page.wait_for_timeout(5000)
                except Exception:
                    pass
                    
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                results = soup.find_all("div", class_="book-title-author-and-series")
                if not results:
                    results = soup.find_all("div", class_="book-pane")
                    
                current_count = len(results)
                print(f"Loaded {current_count} results so far...")
                
                if current_count == last_count:
                    print("No more results loaded after scrolling and waiting 10 seconds. Stopping.")
                    break
                last_count = current_count
                
                # Limit to 1000 items to avoid infinite loops, but realistically it will stop when no more load
                if current_count > 1000:
                    print("Reached 1000 items, stopping to avoid infinite loop.")
                    break
            
            # Final HTML parsing
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            results = soup.find_all("div", class_="book-title-author-and-series")
            if not results:
                results = soup.find_all("div", class_="book-pane")
                
            for res in results:
                title_element = res.find("a", href=lambda href: href and "/books/" in href)
                author_element = res.find("a", href=lambda href: href and "/authors/" in href)
                series_element = res.find("a", href=lambda href: href and "/series/" in href)
                
                title = title_element.text.strip() if title_element else ""
                author = author_element.text.strip() if author_element else ""
                series = series_element.text.strip() if series_element else title
                
                data.append({
                    "Series Name": series,
                    "Author Name": author,
                    "GR Links": "",
                    "Publisher": "",
                    "Goodreads Series URL": "",
                    "Book 1 Ratings (stars)": "",
                    "No. of Goodreads Ratings": "",
                    "No. of Primary Books": "",
                    "Book 1 page count": "",
                    "Total Page Count (of primary books only)": "",
                    "Genre": "Dark Academia",
                    "Genre Tags": "",
                    "Sub Genre": ""
                })
                
        except Exception as e:
            print(f"Error during scraping: {e}")
        finally:
            browser.close()
            
    if data:
        print(f"Saving {len(data)} rows to Excel...")
        
        # Overwrite the Excel sheet since we're scraping everything from the start
        df_new = pd.DataFrame(data)
        df_new.to_excel(EXCEL_PATH, index=False)
        print("Done!")
    else:
        print("No data extracted.")

if __name__ == "__main__":
    scrape()
