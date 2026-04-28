import sys
import os
import asyncio
import re
import pandas as pd
import random
from playwright.async_api import async_playwright
from scraper import GoodreadsScraper
from excel_utility import save_to_excel

# Configuration
INPUT_FILE = r"e:\Internship\PocketFM\Amazon Keyword - Fantasy Romance.xlsx"
OUTPUT_FILE = r"e:\Internship\PocketFM\Amazon Keyword - Fantasy Romance.xlsx"
START_EXCEL_ROW = 999  # Start of today's scraping
MAX_CONCURRENT_TABS = 12

def extract_asin(url):
    if not url or pd.isna(url) or url == "N/A":
        return "N/A"
    match = re.search(r'/(?:dp|product|gp/product)/([A-Z0-9]{10})', str(url))
    return match.group(1) if match else "N/A"

async def repair_row(index, row, context, semaphore, gr_scraper, df, total_missing, progress_counter):
    async with semaphore:
        await asyncio.sleep(random.uniform(0.5, 2.0))
        
        title = str(row.get("Book Title", "N/A"))
        author = str(row.get("Author Name", "N/A"))
        amazon_url = str(row.get("Amazon URL", "N/A"))
        asin = extract_asin(amazon_url)
        
        progress_counter[0] += 1
        curr = progress_counter[0]
        print(f"[{curr}/{total_missing}] Repairing: {title[:40]} (ASIN: {asin})")
        
        try:
            gr_data = await gr_scraper.scrape_goodreads_data(context, title, author, asin=asin)
            
            if gr_data:
                # Update specific columns
                updates = {
                    "Sub_Genre": gr_data.get('Sub_Genre', row.get('Sub_Genre', 'N/A')),
                    "Genre": gr_data.get('Genre', row.get('Genre', 'N/A')),
                    "GoodReads_Series_URL": gr_data.get('GoodReads_Series_URL', gr_data.get('GoodReads_Book_URL', 'N/A')),
                    "Num_Primary_Books": gr_data.get('Num_Primary_Books', 'N/A'),
                    "Total_Pages_Primary_Books": gr_data.get('Total_Pages_Primary_Books', 'N/A'),
                    "Book1_Rating": gr_data.get('Book1_Rating', 'N/A'),
                    "Book1_Num_Ratings": gr_data.get('Book1_Num_Ratings', 'N/A'),
                    "Romantasy_Subgenre": gr_data.get('Romantasy_Subgenre', 'N/A')
                }
                
                for col, val in updates.items():
                    if col in df.columns:
                        df.at[index, col] = val
                print(f"  -> SUCCESS: Updated metadata.")
            else:
                print(f"  -> FAILED: No data found.")
        except Exception as e:
            print(f"  -> ERROR: {e}")

async def perform_repair():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    print(f"Loading {INPUT_FILE}...")
    df = pd.read_excel(INPUT_FILE)
    
    # Identify rows needing repair (starting from START_EXCEL_ROW)
    start_idx = max(0, START_EXCEL_ROW - 2)
    
    def needs_repair(row):
        # We repair if Goodreads metadata is missing or N/A
        cols = ["Book1_Rating", "GoodReads_Series_URL"]
        for col in cols:
            val = str(row.get(col, "N/A")).lower()
            if val in ["n/a", "nan", "", "none"]:
                return True
        return False

    repair_mask = df.index >= start_idx
    to_repair_df = df[repair_mask & df.apply(needs_repair, axis=1)]
    
    if to_repair_df.empty:
        print("No rows need repair in the specified range.")
        return

    total_to_repair = len(to_repair_df)
    print(f"Found {total_to_repair} rows to repair starting from Row {START_EXCEL_ROW}.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        # Simple wait for manual login if needed
        page = await context.new_page()
        await page.goto("https://www.goodreads.com/user/sign_in")
        print("Please log in to Goodreads if prompted. Waiting 15 seconds...")
        await asyncio.sleep(15)

        gr_scraper = GoodreadsScraper()
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)
        progress_counter = [0]
        
        tasks = [repair_row(idx, row, context, semaphore, gr_scraper, df, total_to_repair, progress_counter) for idx, row in to_repair_df.iterrows()]
        await asyncio.gather(*tasks)
        
        print("Saving repaired data...")
        save_to_excel(df.to_dict('records'), OUTPUT_FILE)
        await browser.close()
        print("Repair complete!")

if __name__ == "__main__":
    asyncio.run(perform_repair())
