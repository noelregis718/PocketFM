import asyncio
import pandas as pd
from playwright.async_api import async_playwright
import re

EXCEL_FILE = r"E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx"

async def run_flow():
    print("Loading Excel...")
    df = pd.read_excel(EXCEL_FILE)
    
    missing_indices = []
    for idx, row in df.iterrows():
        url = row.get('Amazon URL')
        if pd.isna(url) or not str(url).startswith('http'):
            continue
            
        if pd.isna(row.get('Logline')) or str(row.get('Logline')).strip() == '' or str(row.get('Logline')).lower() == 'nan' or \
           pd.isna(row.get('Publisher')) or str(row.get('Publisher')).strip() == '' or str(row.get('Publisher')).lower() == 'nan':
            missing_indices.append(idx)
            
    missing_indices = missing_indices[:10]
    print(f"Found missing rows, processing first {len(missing_indices)}...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        for idx in missing_indices:
            row = df.iloc[idx]
            url = row['Amazon URL']
            print(f"\n[{idx}] Navigating to: {url}")
            
            try:
                await page.goto(url, wait_until='domcontentloaded', timeout=45000)
                await asyncio.sleep(3)
                
                # Scroll down to find "Books in this series"
                await page.evaluate("window.scrollBy(0, 800)")
                await asyncio.sleep(2)
                
                # The user says: "find - Books in this series (9 books) under that the book is there click on it"
                print("Looking for Book 1 in the series list...")
                book1_clicked = False
                
                # First try to find the specific series list items
                book_links = await page.query_selector_all('#series-page-product-list div[id^="item-"] a[href*="/dp/"], div[id^="item-"] a.a-link-normal[href*="/dp/"]')
                for link in book_links:
                    href = await link.get_attribute("href")
                    if href and "review" not in href.lower() and "sspa" not in href.lower():
                        print(f"Clicking Book 1: {href}")
                        await link.click()
                        await page.wait_for_load_state("domcontentloaded")
                        book1_clicked = True
                        break
                
                if not book1_clicked:
                    print("Could not find a book to click under 'Books in this series'!")
                    # We might already be on a book page, let's proceed anyway
                    
                await asyncio.sleep(3)
                
                # The user says: "below the ratings you will find the logline"
                print("Extracting Logline...")
                logline = "N/A"
                desc_el = await page.query_selector('#bookDescription_feature_div .a-expander-content, #productDescription')
                if desc_el:
                    logline = (await desc_el.inner_text()).strip()
                    df.at[idx, 'Logline'] = logline
                    print(f"Got Logline: {logline[:30]}...")
                
                # The user says: "below it you will find the see all details button click on that"
                print("Looking for 'See all details' button...")
                details_clicked = False
                for sel in ['a:has-text("See all details")', 'span:has-text("See all details")', '#seeAllDetailsBtn']:
                    btn = await page.query_selector(sel)
                    if btn:
                        print("Clicking 'See all details'...")
                        await btn.click()
                        await asyncio.sleep(3)
                        details_clicked = True
                        break
                        
                # Extract Publisher and Publication Date
                print("Extracting Publisher details...")
                pub = "N/A"
                date = "N/A"
                
                # After clicking 'See all details', it usually reveals standard bullets or RPI table
                els = await page.query_selector_all('#detailBullets_feature_div li, #rpiTable tr, .rpi-attribute-value, .a-list-item')
                for el in els:
                    text = (await el.inner_text()).strip()
                    text = re.sub(r'[\u200e\u200f\u200b]+', ':', text)
                    text = re.sub(r'\s*:\s*', ': ', text)
                    
                    if pub == "N/A" and re.search(r'publisher\s*:\s*(.+)', text, re.IGNORECASE):
                        m = re.search(r'publisher\s*:\s*(.+)', text, re.IGNORECASE)
                        val = m.group(1).strip()
                        val = re.sub(r'\s*\(\d+.*?\)\s*$', '', val).strip()
                        pub = val
                        df.at[idx, 'Publisher'] = pub
                        print(f"Got Publisher: {pub}")
                        
                    if date == "N/A" and re.search(r'publication\s*date\s*:\s*(.+)', text, re.IGNORECASE):
                        m = re.search(r'publication\s*date\s*:\s*(.+)', text, re.IGNORECASE)
                        date = m.group(1).strip()
                        df.at[idx, 'Publication Date'] = date
                        print(f"Got Date: {date}")
                        
            except Exception as e:
                print(f"Failed on row {idx}: {e}")
                
        await browser.close()
        
    print("Saving to Excel...")
    df.to_excel(EXCEL_FILE, index=False)
    print("Done!")

if __name__ == "__main__":
    asyncio.run(run_flow())
