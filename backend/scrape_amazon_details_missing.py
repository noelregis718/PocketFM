import asyncio
import pandas as pd
import os
import sys
import random

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scraper import AmazonScraper
from playwright.async_api import async_playwright

EXCEL_FILE = r"E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx"
CONCURRENCY = 3
BATCH_SIZE = 50

async def process_row(idx, row, df, context, scraper, sem, fixed_counter):
    async with sem:
        url = row.get('Amazon URL')
        
        if pd.isna(url) or not str(url).startswith('http'):
            return

        print(f"[{idx}] Scraping missing details from Amazon: {url}")
        
        # Add slight random jitter
        await asyncio.sleep(random.uniform(1.0, 3.0))
        
        try:
            amz_details = await scraper.scrape_product_details_tab(context, url)
            
            original_description = amz_details.get("Description")
            
            # FALLBACK FOR SERIES PAGES
            if original_description == "N/A" or amz_details.get("Publisher") == "N/A":
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
                            print(f"  -> [{idx}] 🔄 Clicking 'Continue shopping' button on fallback page...", flush=True)
                            await continue_btn.click()
                            await asyncio.sleep(3)
                    except Exception:
                        pass
                    # Wait a bit more for lazy-loaded book list
                    await asyncio.sleep(4)
                    
                    # Target specific series item lists to avoid footer/carousel links
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
            
            # 1. Logline (Description)
            if amz_details.get("Description") and amz_details.get("Description") != "N/A":
                df.at[idx, 'Logline'] = amz_details.get("Description")
                
            # 2. Publisher
            if amz_details.get("Publisher") and amz_details.get("Publisher") != "N/A":
                pub = amz_details.get("Publisher")
                if '\n' in pub:
                    pub = pub.split('\n')[0].strip()
                df.at[idx, 'Publisher'] = pub
                
            # 3. Publication Date
            if amz_details.get("Publication Date") and amz_details.get("Publication Date") != "N/A":
                df.at[idx, 'Publication Date'] = amz_details.get("Publication Date")
                
            print(f"  -> [{idx}] Fetched Details - Pub: {df.at[idx, 'Publisher']} | Date: {amz_details.get('Publication Date')} | Logline: {str(amz_details.get('Description'))[:20]}...")
            
            fixed_counter[0] += 1
            if fixed_counter[0] % 10 == 0:
                print("Saving progress...")
                df.to_excel(EXCEL_FILE, index=False)
                
        except Exception as e:
            print(f"  -> [{idx}] Failed to scrape: {e}")

async def main():
    print(f"Loading {EXCEL_FILE}...")
    df = pd.read_excel(EXCEL_FILE)
    
    # Ensure columns exist
    for col in ['Logline', 'Publisher', 'Publication Date']:
        if col not in df.columns:
            df[col] = None
            
    scraper = AmazonScraper()
    scraper.headless = False

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        sem = asyncio.Semaphore(CONCURRENCY)
        fixed_counter = [0]
        
        missing_indices = []
        for idx, row in df.iterrows():
            url = row.get('Amazon URL')
            if pd.isna(url) or not str(url).startswith('http'):
                continue
                
            needs_scrape = False
            if pd.isna(row.get('Logline')) or str(row.get('Logline', '')).strip() == '' or str(row.get('Logline', '')).lower() == 'nan':
                needs_scrape = True
            if pd.isna(row.get('Publisher')) or str(row.get('Publisher', '')).strip() == '' or str(row.get('Publisher', '')).lower() == 'nan':
                needs_scrape = True
            if pd.isna(row.get('Publication Date')) or str(row.get('Publication Date', '')).strip() == '' or str(row.get('Publication Date', '')).lower() == 'nan':
                needs_scrape = True
                
            if needs_scrape:
                missing_indices.append(idx)
                
        print(f"Found {len(missing_indices)} rows missing Amazon details.")
        
        # LIMIT TO 10 for testing
        missing_indices = missing_indices[:10]
        print(f"Testing the first {len(missing_indices)} rows...")
        
        if missing_indices:
            tasks = []
            for idx in missing_indices:
                row = df.iloc[idx]
                tasks.append(process_row(idx, row, df, context, scraper, sem, fixed_counter))
                
            await asyncio.gather(*tasks)
            df.to_excel(EXCEL_FILE, index=False)
            
        await browser.close()
        
    print("\nExtraction of missing Amazon details complete!")

if __name__ == "__main__":
    asyncio.run(main())
