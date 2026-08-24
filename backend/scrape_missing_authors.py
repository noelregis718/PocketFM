import asyncio
import os
import sys
import pandas as pd
from playwright.async_api import async_playwright
import csv

# Add current directory to path to import local modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from goodreads_scraper import GoodreadsScraper
from scraper import AmazonScraper

file_lock = asyncio.Lock()

async def safe_save_csv(df, csv_path):
    async with file_lock:
        try:
            # Save without index
            df.to_csv(csv_path, index=False)
        except Exception as e:
            print(f"Error saving CSV: {e}", flush=True)

async def scrape_author_for_row(index, row, df, csv_path, gr_scraper, amz_scraper, context, semaphore):
    series_name = str(row.get("Book Series Name", "")).strip()
    if not series_name or series_name.lower() == 'nan':
        return

    async with semaphore:
        print(f"[{index}] Fetching author for series: '{series_name}'", flush=True)
        
        # 1. Try Goodreads First
        print(f"[{index}] Checking Goodreads...", flush=True)
        # Using a dummy author to reuse existing logic for search
        gr_data = await gr_scraper.scrape_goodreads_data(context, title=series_name, author="")
        
        author_found = None
        
        details = None
        source_used = "None"
        
        if gr_data and gr_data.get("Author_Found") and gr_data.get("Author_Found") != "N/A" and gr_data.get("Author_Found") != "Unknown":
            author_found = gr_data.get("Author_Found")
            print(f"[{index}] Goodreads SUCCESS: Found author '{author_found}'", flush=True)
            
        # 2. Try Amazon Fallback if not found
        if not author_found:
            print(f"[{index}] Goodreads failed. Falling back to Amazon...", flush=True)
            amz_data = await amz_scraper.get_book1_details(context, series_name, "")
            if amz_data and amz_data.get("Author Name") and amz_data.get("Author Name") != "N/A":
                author_found = amz_data.get("Author Name")
                print(f"[{index}] Amazon SUCCESS: Found author '{author_found}'", flush=True)
        
        # 3. Save if found
        if author_found:
            highlighted_name = f"[NEW] {author_found}"
            df.at[index, "Author Name"] = highlighted_name
            await safe_save_csv(df, csv_path)
            print(f"[{index}] Saved {highlighted_name} to CSV.", flush=True)
        else:
            print(f"[{index}] FAILED to find author on both platforms.", flush=True)


async def main():
    csv_path = r"E:\Internship\PocketFM\Romantasy _ Self Publication Master.csv"
    
    print("Loading CSV file...", flush=True)
    df = pd.read_csv(csv_path)
    
    # Clean up column names in case there are leading/trailing spaces
    df.columns = df.columns.str.strip()
    
    # Ensure Author Name column exists
    if "Author Name" not in df.columns:
        print("Error: 'Author Name' column not found in CSV. Existing columns:", df.columns.tolist())
        return

    # Find rows where Author Name is missing AND Book Series Name is present
    has_series = df['Book Series Name'].notna() & (df['Book Series Name'] != '') & (df['Book Series Name'].astype(str).str.strip() != '') & (df['Book Series Name'].astype(str).str.lower() != 'nan')
    missing_author = df["Author Name"].isna() | (df["Author Name"] == "") | (df["Author Name"].astype(str).str.strip() == "") | (df['Author Name'].astype(str).str.lower() == 'nan')
    
    missing_mask = has_series & missing_author
    missing_indices = df[missing_mask].index.tolist()
    
    if not missing_indices:
        print("No missing authors found in the dataset with a valid series name.")
        return
        
    # Process all of them
    target_indices = missing_indices
    print(f"Found {len(target_indices)} missing authors with valid series names. Starting batch processing...", flush=True)
    
    gr_scraper = GoodreadsScraper(headless=False)
    amz_scraper = AmazonScraper(headless=False)
    
    semaphore = asyncio.Semaphore(8)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        # Login to Goodreads first using an initial page
        login_page = await context.new_page()
        logged_in = await gr_scraper.login_to_goodreads(login_page)
        if not logged_in:
            print("Warning: Goodreads login failed. Scraping might be limited.", flush=True)
        await login_page.close()
        
        # Prepare Amazon location using a temporary page
        amz_page = await context.new_page()
        await amz_page.goto("https://www.amazon.com", wait_until="domcontentloaded", timeout=60000)
        await amz_scraper.set_amazon_location(amz_page, "90016")
        await amz_page.close()
        
        print("\nStarting concurrent scraping...", flush=True)
        tasks = []
        for idx in target_indices:
            row = df.loc[idx]
            tasks.append(scrape_author_for_row(idx, row, df, csv_path, gr_scraper, amz_scraper, context, semaphore))
            
        await asyncio.gather(*tasks)
        
        await browser.close()
        print("\nScraping complete!", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
