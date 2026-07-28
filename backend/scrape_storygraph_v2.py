from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import time

EXCEL_PATH = r"e:\Internship\PocketFM\Scraping_Sheet.xlsx"

def scrape_pages():
    data = []
    
    print("Starting page-by-page scraping with Playwright...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
        
        try:
            page_num = 1
            while True:
                url = f"https://app.thestorygraph.com/browse?search_term=Dark+Academia&page={page_num}"
                print(f"Fetching {url}")
                
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(4000) # give it extra time to render
                
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                
                results = soup.find_all("div", class_="book-title-author-and-series")
                if not results:
                    results = soup.find_all("div", class_="book-pane")
                    
                if not results:
                    print(f"No results found on page {page_num}. Stopping.")
                    break
                    
                print(f"Found {len(results)} books on page {page_num}.")
                
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
                    
                page_num += 1
                
        except Exception as e:
            print(f"Error during scraping: {e}")
        finally:
            browser.close()

    if data:
        print(f"Saving {len(data)} total rows to Excel...")
        df_new = pd.DataFrame(data)
        df_new.to_excel(EXCEL_PATH, index=False)
        print("Done!")
    else:
        print("No data extracted.")

if __name__ == "__main__":
    scrape_pages()
