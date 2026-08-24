import asyncio
import pandas as pd
from playwright.async_api import async_playwright

EXCEL_FILE = r"E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx"
CONCURRENCY_LIMIT = 8

async def get_amazon_details(context, url):
    page = await context.new_page()
    try:
        await page.goto(url, timeout=60000, wait_until='domcontentloaded')
        await page.wait_for_timeout(3000) # Give extra time for React/JS to render
        
        title = None
        author = None
        
        # Multiple selectors for title (book pages, series pages, varied layouts)
        for sel in ['#productTitle', '#collection-title', 'h1.a-size-large', 'span.a-size-extra-large']:
            el = await page.query_selector(sel)
            if el:
                title = await el.inner_text()
                break
                
        # Multiple selectors for author
        for sel in ['span.author a', 'a.contributorNameID', '#bylineInfo a', '.author .a-link-normal']:
            el = await page.query_selector(sel)
            if el:
                author = await el.inner_text()
                if author: break
                
        # Robust fallback: extract from the <title> tag
        if not title or not author:
            page_title = await page.title()
            if page_title and "Amazon.com" in page_title:
                # e.g., "Amazon.com: Book Title eBook : Author Name: Kindle Store"
                parts = page_title.replace("Amazon.com:", "").strip().split(":")
                if len(parts) >= 2:
                    if not title: title = parts[0].replace('eBook', '').replace('Kindle edition', '').strip()
                    if not author: author = parts[1].strip()
            elif page_title and "- Kindle edition by" in page_title:
                # e.g., "Book Title - Kindle edition by Author Name."
                parts = page_title.split("- Kindle edition by")
                if len(parts) == 2:
                    if not title: title = parts[0].strip()
                    if not author: author = parts[1].split(".")[0].strip()
                    
        if title: title = title.strip()
        if author: author = author.strip()
            
        return title, author
    except Exception as e:
        print(f"  [Error scraping {url}] {e}")
        return None, None
    finally:
        await page.close()

async def process_row(idx, url, df, context, semaphore):
    async with semaphore:
        print(f"[{idx}] Scraping {url} ...")
        title, author = await get_amazon_details(context, url)
        
        updated = False
        if title and (pd.isna(df.at[idx, 'Book Title']) or str(df.at[idx, 'Book Title']).strip() == ''):
            df.at[idx, 'Book Title'] = title
            print(f"  -> [{idx}] Found Title: {title}")
            updated = True
        if author and (pd.isna(df.at[idx, 'Author Name']) or str(df.at[idx, 'Author Name']).strip() == ''):
            df.at[idx, 'Author Name'] = author
            print(f"  -> [{idx}] Found Author: {author}")
            updated = True
            
        return updated

async def main():
    print(f"Loading {EXCEL_FILE}...")
    df = pd.read_excel(EXCEL_FILE)
    
    missing_indices = df[df['Book Title'].isna() | (df['Book Title'].astype(str).str.strip() == '') | df['Author Name'].isna() | (df['Author Name'].astype(str).str.strip() == '')].index[:10]
    print(f"Found {len(missing_indices)} rows with missing Title or Author (limiting to first 10).")
    
    if len(missing_indices) == 0:
        print("Nothing to fix.")
        return
        
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        tasks = []
        for idx in missing_indices:
            url = df.at[idx, 'Amazon URL']
            if pd.isna(url) or not str(url).startswith('http'):
                continue
            
            task = asyncio.create_task(process_row(idx, url, df, context, semaphore))
            tasks.append(task)
            
        results = await asyncio.gather(*tasks)
        fixed_count = sum(results)
        
        await browser.close()
        
    if fixed_count > 0:
        print(f"Saving changes to {EXCEL_FILE}...")
        df.to_excel(EXCEL_FILE, index=False)
        print("Done! You may need to run the styling script again on this file.")
    else:
        print("No new details were extracted. Amazon might be blocking the scraper with a Captcha.")

if __name__ == "__main__":
    asyncio.run(main())
