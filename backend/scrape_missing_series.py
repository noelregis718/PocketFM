import asyncio
import pandas as pd
import time
import re
from playwright.async_api import async_playwright

async def process_series(url, context, sem):
    async with sem:
        if not str(url).startswith('http'):
            return None
            
        page = await context.new_page()
        try:
            # Mask ourselves slightly
            await page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            
            series_name = None
            
            # Check for CAPTCHA first
            has_captcha = await page.query_selector('#captcha-image, .captcha, iframe[src*="captcha"]')
            if has_captcha:
                print(f"[!] CAPTCHA detected on {url}. Waiting 5 minutes for manual solve...")
                try:
                    await page.wait_for_selector('h1', timeout=300000)
                except:
                    print(f"[-] CAPTCHA not solved in time for {url}")
            
            # Try h1 first
            h1 = await page.query_selector('h1')
            if h1:
                series_name = await h1.inner_text()
                
            if not series_name:
                # Fallback to page title
                page_title = await page.title()
                if page_title:
                    series_name = page_title
            
            if series_name:
                series_name = series_name.strip()
                # Clean up "Series Name Series by Author Name"
                # e.g. "Hot Damned Series by Robyn Peterman" -> "Hot Damned"
                if " Series by " in series_name:
                    series_name = series_name.split(" Series by ")[0].strip()
                elif " by " in series_name:
                    series_name = series_name.split(" by ")[0].strip()
                
                # if it says "Book Title (Series, #1)", extract just Series
                match = re.search(r'\(([^,]+),\s*#\d', series_name)
                if match:
                    series_name = match.group(1).strip()
            
            await page.close()
            return series_name
            
        except Exception as e:
            print(f"[-] Error scraping {url}: {e}")
            await page.close()
            return None

async def run_scraper():
    csv_path = r"E:\Internship\PocketFM\Romantasy _ Self Publication Master.csv"
    print("Loading CSV...", flush=True)
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    
    missing_series = df['Book Series Name'].isna() | (df['Book Series Name'] == '') | (df['Book Series Name'].astype(str).str.strip() == '') | (df['Book Series Name'].astype(str).str.lower() == 'nan')
    has_link = df['Source URL'].notna() & (df['Source URL'] != '') & (df['Source URL'].astype(str).str.strip() != '') & (df['Source URL'].astype(str).str.lower() != 'nan')
    
    target_mask = missing_series & has_link
    target_indices = df[target_mask].index.tolist()
    
    if not target_indices:
        print("No missing series with valid URLs found!")
        return
        
    print(f"Found {len(target_indices)} missing series to scrape.", flush=True)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        sem = asyncio.Semaphore(8)
        
        tasks = []
        for idx in target_indices:
            url = df.at[idx, 'Source URL']
            tasks.append(process_series(url, context, sem))
            
        print("Starting concurrent scraping...", flush=True)
        results = await asyncio.gather(*tasks)
        
        updates = 0
        for idx, res in zip(target_indices, results):
            if res and res != "N/A" and res != "":
                df.at[idx, 'Book Series Name'] = f"[NEW] {res}"
                updates += 1
                print(f"[{idx}] Found Series: {res}")
            else:
                print(f"[{idx}] Could not find series name.")
                
        if updates > 0:
            df.to_csv(csv_path, index=False)
            print(f"Saved {updates} new series names to CSV!")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_scraper())
