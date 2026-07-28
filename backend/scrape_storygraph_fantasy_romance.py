from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

EXCEL_PATH = r"e:\Internship\PocketFM\Scraping_Sheet_Next_Genre.xlsx"

def scrape_pages():
    data = []
    
    print("Starting page-by-page scraping with Playwright (Visible Browser) for Fantasy Romance...")
    with sync_playwright() as p:
        # Launch non-headless so Cloudflare is less suspicious
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        page = context.new_page()
        
        try:
            page_num = 1
            while page_num <= 200:
                url = f"https://app.thestorygraph.com/browse?search_term=Fantasy+Romance&page={page_num}"
                print(f"Fetching {url}")
                
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                
                # Wait for either the book pane OR the 'No results' indicator
                # If Cloudflare challenges us, the user can manually click it in the visible window!
                print("Waiting for page to fully load (please solve CAPTCHA if it appears)...")
                try:
                    # Wait for results or timeout
                    page.wait_for_selector(".book-pane", timeout=30000)
                except:
                    pass # We will check the HTML next to see if it's really empty or blocked
                
                # Check for Cloudflare text
                html = page.content()
                if "This website uses a security service to protect against malicious bots" in html:
                    print("Cloudflare detected! Waiting an additional 15 seconds...")
                    page.wait_for_timeout(15000)
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
                        "Genre": "Fantasy Romance",
                        "Genre Tags": "",
                        "Sub Genre": ""
                    })
                    
                page_num += 1
                time.sleep(2)
                
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
