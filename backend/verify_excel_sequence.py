import asyncio
import os
import sys
import pandas as pd
import re
from playwright.async_api import async_playwright

# Add parent dir to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper import AmazonScraper

async def verify_sequence():
    INPUT_FILE = r"E:\Internship\PocketFM\Amazon Keyword - Romantasy.xlsx"
    SEARCH_URL = "https://www.amazon.com/s?k=Romantasy&i=stripbooks&crid=3JGEQS9FA3ITT&sprefix=romantasy%2Cstripbooks%2C376&ref=nb_sb_noss_1"
    
    print(f"Loading Excel...")
    df = pd.read_excel(INPUT_FILE)
    
    # Get last two books
    last_two = df.dropna(subset=['Book Title', 'Amazon URL']).tail(2)
    if len(last_two) < 2:
        print("Not enough books in Excel to check sequence.")
        return

    book2 = last_two.iloc[1] # Last
    book1 = last_two.iloc[0] # Second to last
    
    def get_asin(url):
        m = re.search(r'/(?:dp|product|gp/product)/([A-Z0-9]{10})', str(url))
        return m.group(1) if m else None

    asin1 = get_asin(book1['Amazon URL'])
    asin2 = get_asin(book2['Amazon URL'])
    title1 = book1['Book Title']
    title2 = book2['Book Title']

    print(f"Target 1 (Second-to-Last): {title1[:40]}... (ASIN: {asin1})")
    print(f"Target 2 (Last): {title2[:40]}... (ASIN: {asin2})")
    print("-" * 50)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        
        amz = AmazonScraper()
        await page.goto("https://www.amazon.com")
        await amz.set_amazon_location(page, "90016")
        
        found1_page = None
        found2_page = None
        
        # We know they should be around Page 22-25 based on previous counts
        for p_num in range(1, 101):
            url = f"{SEARCH_URL}&page={p_num}"
            print(f"[Page {p_num}] Scanning...")
            
            await page.goto(url, wait_until="load", timeout=60000)
            await asyncio.sleep(4)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
            
            items = await page.query_selector_all('[data-asin]')
            asins_on_page = []
            for item in items:
                a = await item.get_attribute('data-asin')
                if a and a != 'N/A': asins_on_page.append(a)
            
            if asin1 in asins_on_page:
                found1_page = p_num
                print(f"  -> FOUND Target 1 on Page {p_num}")
            if asin2 in asins_on_page:
                found2_page = p_num
                print(f"  -> FOUND Target 2 on Page {p_num}")
            
            if found1_page and found2_page:
                break
            if p_num > 5 and not found1_page and not found2_page and p_num % 10 == 0:
                print(f"  ...still searching (Cumulative books scanned: {p_num * 48})")

        print("\n" + "="*50)
        print("SEQUENCE VERIFICATION RESULTS:")
        if found1_page and found2_page:
            print(f"Book 1 (Second-to-last): Page {found1_page}")
            print(f"Book 2 (Last):           Page {found2_page}")
            if found1_page == found2_page:
                print("CONCLUSION: Both books are on the SAME page.")
            elif found2_page == found1_page + 1:
                print("CONCLUSION: Sequential pages (Page X and Page X+1).")
            else:
                print(f"CONCLUSION: Non-sequential pages (Gap of {found2_page - found1_page} pages).")
        else:
            if not found1_page: print(f"Target 1 (ASIN {asin1}) NOT FOUND in first 100 pages.")
            if not found2_page: print(f"Target 2 (ASIN {asin2}) NOT FOUND in first 100 pages.")
        print("="*50)
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(verify_sequence())
