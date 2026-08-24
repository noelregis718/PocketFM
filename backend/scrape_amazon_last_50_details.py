import asyncio
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scraper import AmazonScraper
from playwright.async_api import async_playwright

EXCEL_FILE = r"E:\Internship\PocketFM\Combined List of Titles.xlsx"
CONCURRENCY = 8

async def process_row(idx, row, df, context, scraper, sem):
    async with sem:
        url = row.get('Amazon URL')
        
        if pd.isna(url) or not str(url).startswith('http'):
            return

        print(f"[{idx}] Scraping Amazon URL: {url}")
        
        try:
            # Reusing the existing scraper function
            amz_details = await scraper.scrape_product_details_tab(context, url)
            
            # FALLBACK FOR SERIES PAGES
            original_description = amz_details.get("Description", "N/A")
            if original_description == "N/A" or amz_details.get("Publication Date", "N/A") == "N/A":
                print(f"  -> [{idx}] Missing details, checking if this is a series page for Book 1 fallback...")
                fallback_page = await context.new_page()
                try:
                    import re
                    match = re.search(r'/dp/([A-Z0-9]+)', url)
                    url_asin = match.group(1) if match else "UNKNOWN_ASIN"
                    
                    await fallback_page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    try:
                        continue_btn = await fallback_page.query_selector('text="Continue shopping"')
                        if continue_btn:
                            await continue_btn.click()
                            await asyncio.sleep(3)
                    except Exception:
                        pass
                        
                    await asyncio.sleep(4)
                    
                    selectors = [
                        '#series-page-product-list div[id^="item-"] a[href*="/dp/"]',
                        '#series-page-product-list a[href*="/dp/"]',
                        'div[id^="item-1"] a[href*="/dp/"]',
                        'div[id="item-list"] a[href*="/dp/"]',
                        '.item-title a[href*="/dp/"]'
                    ]
                    
                    links = []
                    for sel in selectors:
                        links = await fallback_page.query_selector_all(sel)
                        if links:
                            break
                            
                    book1_url = None
                    for link in links:
                        href = await link.get_attribute("href")
                        if href and "/dp/" in href and "sspa" not in href.lower() and "review" not in href.lower() and url_asin not in href:
                            if not href.startswith('http'):
                                href = "https://www.amazon.com" + href
                            book1_url = href
                            break
                            
                    if book1_url:
                        print(f"  -> [{idx}] Found Book 1 Fallback URL: {book1_url}")
                        book1_details = await scraper.scrape_product_details_tab(context, book1_url)
                        if original_description != "N/A":
                            book1_details["Description"] = original_description
                        amz_details = book1_details
                except Exception as e:
                    print(f"  -> [{idx}] Fallback failed: {e}")
                finally:
                    await fallback_page.close()
            
            # Extract Logline
            logline = amz_details.get("Description", "")
            if logline and logline != "N/A":
                df.at[idx, 'Logline'] = logline
                
            # Extract Publication Date
            pub_date = amz_details.get("Publication Date", "")
            if pub_date and pub_date != "N/A":
                df.at[idx, 'Publication Date'] = pub_date

            # Extract Best Sellers Rank
            rank = amz_details.get("Inner Rank", "")
            if rank and rank != "N/A":
                df.at[idx, 'Best Sellers Rank'] = rank
                
            print(f"  -> [{idx}] Fetched - Date: {pub_date} | Rank: {rank}")
                
        except Exception as e:
            print(f"  -> [{idx}] Failed to scrape: {e}")

async def main():
    print(f"Loading {EXCEL_FILE}...")
    try:
        df = pd.read_excel(EXCEL_FILE)
        
        # Ensure target columns can accept mixed types
        for col in ['Logline', 'Publication Date', 'Best Sellers Rank']:
            if col not in df.columns:
                df[col] = None
            df[col] = df[col].astype(object)
            
    except Exception as e:
        print(f"Failed to load Excel file: {e}")
        return
        
    scraper = AmazonScraper()
    scraper.headless = False

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        sem = asyncio.Semaphore(CONCURRENCY)
        
        tasks = []
        missing_count = 0
        
        # Only process the last 50 rows
        start_idx = max(0, len(df) - 50)
        for idx in range(start_idx, len(df)):
            row = df.iloc[idx]
            url = row.get('Amazon URL')
            
            # Skip if no URL
            if pd.isna(url) or not str(url).startswith('http'):
                continue
                
            # Filter for missing target columns
            logline = row.get('Logline')
            pub_date = row.get('Publication Date')
            rank = row.get('Best Sellers Rank')
            
            def is_missing(val):
                return pd.isna(val) or str(val).strip() == '' or str(val).lower() == 'nan'
                
            if is_missing(logline) or is_missing(pub_date) or is_missing(rank):
                tasks.append(process_row(idx, row, df, context, scraper, sem))
                missing_count += 1
                
        print(f"Found {missing_count} rows in the last 50 with missing details.")
        
        chunk_size = 25
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i+chunk_size]
            print(f"Processing chunk {i//chunk_size + 1}...")
            await asyncio.gather(*chunk)
            
            print("Saving partial updates to Excel...")
            try:
                df.to_excel(EXCEL_FILE, index=False)
            except Exception as e:
                print(f"Failed to save to Excel: {e}")
            
        await browser.close()
        
    print("\nExtraction complete for the last 50 rows!")

if __name__ == "__main__":
    asyncio.run(main())
