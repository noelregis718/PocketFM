import asyncio
import pandas as pd
import re
from playwright.async_api import async_playwright

EXCEL_FILE = r"E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx"
START_ROW = 10290

async def extract_amazon_data(page, url):
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await asyncio.sleep(2)
    
    rating = None
    num_ratings = None
    
    # 1. Rating
    rating_el = await page.query_selector('#acrPopover, [data-hook="rating-out-of-text"]')
    if rating_el:
        txt = await rating_el.get_attribute('title')
        if not txt:
            txt = await rating_el.inner_text()
        if txt:
            match = re.search(r'([\d\.]+)\s+out of', txt)
            if match: rating = float(match.group(1))
            
    # 2. Num Ratings
    count_el = await page.query_selector('#acrCustomerReviewText, [data-hook="total-review-count"]')
    if count_el:
        txt = await count_el.inner_text()
        match = re.search(r'([\d,]+)', txt)
        if match: num_ratings = int(match.group(1).replace(',', ''))
        
    return rating, num_ratings

CONCURRENCY_LIMIT = 8

async def process_row(idx, url, df, context, semaphore, fixed_counter):
    async with semaphore:
        page = await context.new_page()
        try:
            print(f"[{idx}] Amazon Scrape: {url}")
            rating, num_ratings = await extract_amazon_data(page, url)
            
            if rating: 
                df.at[idx, 'Amazon Book1_Rating'] = rating
                print(f"  -> [{idx}] Rating: {rating}")
            if num_ratings:
                df.at[idx, 'Amazon Book1_Num_Ratings'] = num_ratings
                print(f"  -> [{idx}] Num Ratings: {num_ratings}")
                
            fixed_counter[0] += 1
            if fixed_counter[0] % 5 == 0:
                df.to_excel(EXCEL_FILE, index=False)
        finally:
            await page.close()

async def main():
    print(f"Loading {EXCEL_FILE}...")
    df = pd.read_excel(EXCEL_FILE)
    
    if 'Amazon Book1_Rating' not in df.columns:
        df['Amazon Book1_Rating'] = None
    if 'Amazon Book1_Num_Ratings' not in df.columns:
        df['Amazon Book1_Num_Ratings'] = None
        
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        fixed_counter = [0]
        tasks = []
        
        for idx in range(START_ROW, len(df)):
            url = df.at[idx, 'Amazon URL']
            if pd.isna(url) or not str(url).startswith('http'):
                continue
                
            tasks.append(process_row(idx, url, df, context, semaphore, fixed_counter))
            
        await asyncio.gather(*tasks)
        await browser.close()
        
    df.to_excel(EXCEL_FILE, index=False)
    print("Amazon tail scraping complete!")

if __name__ == "__main__":
    asyncio.run(main())
