import asyncio
import os
import sys
import pandas as pd
from urllib.parse import urlparse
from playwright.async_api import async_playwright

# Ensure backend folder is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper import AmazonScraper, GoodreadsScraper, AuthorScraper
from excel_utility import save_to_excel

# --- CONFIGURATION ---
TARGET_URL = "https://www.amazon.com/best-sellers-books-Amazon/zgbs/books/"
# You can change this URL to any Amazon Bestseller category
BATCH_SIZE = 50 
EXCEL_FILE = "scraped_data.xlsx"

# Emoji-Shield: Force UTF-8 for Windows Console to prevent crashes on special characters
try:
    if sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
except AttributeError: pass

async def run_unified_stepper():
    # 1. SETUP SCRAPERS
    amazon_scraper = AmazonScraper(headless=False)
    goodreads_scraper = GoodreadsScraper(headless=False)
    author_scraper = AuthorScraper(headless=False)
    
    # 2. CALCULATE OFFSET
    skip_offset = 0
    if os.path.exists(EXCEL_FILE):
        try:
            df_existing = pd.read_excel(EXCEL_FILE)
            skip_offset = len(df_existing)
        except: pass
    
    print("\n" + "="*50)
    print(f" UNIFIED INDUSTRIAL STEPPER: Starting Batch at Rank #{skip_offset + 1}")
    print("="*50 + "\n")

    async with async_playwright() as p:
        # Launch A SINGLE browser for everything
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        # --- TAB 1: AMAZON DISCOVERY ---
        discovery_page = await context.new_page()
        print(f"  [Phase 1] Discovering next {BATCH_SIZE} books on Amazon...")
        
        # Use the unified discovery mode
        book_list = await amazon_scraper.scrape_bestseller_list(
            TARGET_URL, limit=BATCH_SIZE, skip_offset=skip_offset, external_page=discovery_page
        )
        
        if not book_list:
            print("  [Done] No more books found to process.")
            await browser.close()
            return

        # --- TAB 2: LOGIN GATE (Goodreads) ---
        print("\n" + "!"*60)
        print("  ACTION REQUIRED: LOG IN TO GOODREADS IN THE NEW TAB")
        print("!"*60 + "\n")
        
        login_page = await context.new_page()
        await login_page.goto("https://www.goodreads.com/user/sign_in")
        
        try:
            # Wait for login success
            await login_page.wait_for_selector('a[href*="/user/sign_out"], .Header_userProfile', timeout=300000)
            print("  [OK] Goodreads Login detected. Starting enrichment...")
            await login_page.close()
        except:
            print("  [Warning] Login wait timed out. Proceeding...")

        # --- PHASE 2: DEEP DIVE (Reuse the same context!) ---
        sem = asyncio.Semaphore(30)
        author_cache = {}
        final_results = []
        total = len(book_list)
        completed = [0]
        
        parsed = urlparse(TARGET_URL)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        async def process_book(book):
            async with sem:
                try:
                    title = book.get('Book Title', 'N/A')
                    url = book.get('Amazon URL', '')
                    if url and not url.startswith('http'):
                        url = base_url.rstrip('/') + (url if url.startswith('/') else '/' + url)
                    
                    # product_details_tab opens its own tab in the shared context
                    details = await amazon_scraper.scrape_product_details_tab(context, url, base_url=base_url)
                    
                    author = details.get('Author Name', book.get('Author Name', 'N/A'))
                    gr_data = await goodreads_scraper.scrape_goodreads_data(context, title, author)
                    
                    author_key = author.strip().lower()
                    if author_key not in author_cache:
                        author_cache[author_key] = await author_scraper.find_author_details(context, author)
                    ath = author_cache[author_key]

                    mapped = {
                        "Sub_Genre":                 gr_data.get('Sub_Genre', details.get('Sub_Genre_Candidate', 'N/A')),
                        "Price_Tier":                details.get('Price', book.get('Price', 'N/A')),
                        "Amazon URL":                url,
                        "Book Title":                title,
                        "Book Number in Series":     details.get('Book Number', 'N/A'),
                        "Series Name":               details.get('Series Name', 'N/A'),
                        "Author Name":               author,
                        "Amazon Stars":              book.get('Rating', 0),
                        "Amazon Ratings":            book.get('Number of Reviews', 0),
                        "Number of Books in Series": details.get('Total Books', 'N/A'),
                        "Genre":                     gr_data.get('Genre', 'N/A'),
                        "Publisher":                 details.get('Publisher', 'N/A'),
                        "Publication Date":          details.get('Publication Date', 'N/A'),
                        "Print Length / Pages":      details.get('Pages', 'N/A'),
                        "Best Sellers Rank":         details.get('Inner Rank', book.get('Rank', 'N/A')),
                        "Licensing Status":          "N/A",
                        "Part of a Series?":         "Yes" if details.get('Series Name') and details.get('Series Name') != 'N/A' else "No",
                        "Part_of_Series":            "Yes" if details.get('Series Name') and details.get('Series Name') != 'N/A' else "No",
                        "GoodReads_Series_URL":      gr_data.get('GoodReads_Series_URL', 'N/A'),
                        "Num_Primary_Books":         gr_data.get('Num_Primary_Books', 'N/A'),
                        "Total_Pages_Primary_Books": gr_data.get('Total_Pages_Primary_Books', 'N/A'),
                        "Book1_Rating":              gr_data.get('Book1_Rating', 'N/A'),
                        "Book1_Num_Ratings":         gr_data.get('Book1_Num_Ratings', 'N/A'),
                        "Logline":                   details.get('Description', 'N/A'),
                        "One_Sentence_Logline":      (details.get('Description', '').split('.')[0] + '.') if details.get('Description') else 'N/A',
                        "Romantasy_Subgenre":        gr_data.get('Romantasy_Subgenre', 'N/A'),
                        "Author_Email":              ath.get('Author_Email', 'N/A'),
                        "Agent_Email":               ath.get('Agent_Email', 'N/A'),
                        "Facebook":                  ath.get('Facebook', 'N/A'),
                        "Twitter":                   ath.get('Twitter', 'N/A'),
                        "Instagram":                 ath.get('Instagram', 'N/A'),
                        "Website":                   ath.get('Website', 'N/A'),
                        "Other_Contact":             ath.get('Other_Contact', 'N/A')
                    }
                    final_results.append(mapped)
                    completed[0] += 1
                    safe_title = title.encode('ascii', 'ignore').decode('ascii')
                    print(f"  [{completed[0]}/{total}] Perfected: {safe_title[:30]}")
                except Exception as e:
                    print(f"  [Error] Processing {book.get('Book Title')}: {e}")

        tasks = [process_book(b) for b in book_list]
        await asyncio.gather(*tasks)
        await browser.close()

    # 5. SAVE AND FINISH
    save_to_excel(final_results, EXCEL_FILE)
    print(f"\nBATCH COMPLETE! Total books now in Excel: {skip_offset + len(final_results)}")
    
    if os.name == 'nt':
        os.startfile(EXCEL_FILE)

if __name__ == "__main__":
    asyncio.run(run_unified_stepper())
