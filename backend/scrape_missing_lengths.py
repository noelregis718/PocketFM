import asyncio
import pandas as pd
import time
import re
import urllib.request
import urllib.parse
import json
from playwright.async_api import async_playwright

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
}

def get_autocomplete_book_url(query):
    api_url = f"https://www.goodreads.com/book/auto_complete?format=json&q={urllib.parse.quote_plus(query)}"
    for attempt in range(3):
        try:
            req = urllib.request.Request(api_url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as response:
                data = json.loads(response.read().decode("utf-8", errors="ignore"))
                if data and len(data) > 0:
                    book_path = data[0].get('bookUrl', '')
                    if book_path:
                        return "https://www.goodreads.com" + book_path
                return None
        except Exception as e:
            time.sleep(1)
    return None

async def extract_pages_from_book(b_url, context, book_title=None, book_num=None):
    # Use the Autocomplete API to bypass Playwright and UI blocks completely!
    if book_title:
        try:
            api_url = f"https://www.goodreads.com/book/auto_complete?format=json&q={urllib.parse.quote_plus(book_title)}"
            for attempt in range(3):
                req = urllib.request.Request(api_url, headers=HEADERS)
                with urllib.request.urlopen(req, timeout=15) as response:
                    data = json.loads(response.read().decode("utf-8", errors="ignore"))
                    if data and len(data) > 0:
                        # Find the match
                        for res in data:
                            if 'numPages' in res and res['numPages']:
                                return int(res['numPages']), book_num
                await asyncio.sleep(1)
        except Exception as e:
            pass
            
    # Fallback to Playwright ONLY if API fails
    b_page = await context.new_page()
    extracted_pages = 0
    try:
        await b_page.goto(b_url, wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(1)
        
        page_title = await b_page.title()
        if "Just a moment" in page_title or "403 Forbidden" in page_title or "Access Denied" in page_title:
            print(f"Goodreads blocked access to {b_url}")
            return -1, book_num
            
        # 1. Try JSON-LD
        try:
            ld_el = await b_page.query_selector('script[type="application/ld+json"]')
            if ld_el:
                data = json.loads(await ld_el.inner_text())
                if isinstance(data, list): data = data[0]
                if 'numberOfPages' in data:
                    extracted_pages = int(data['numberOfPages'])
        except: pass
        
        # 2. Try simple data-testid
        if not extracted_pages:
            p_el = await b_page.query_selector('[data-testid="pagesFormat"]')
            if p_el:
                p_match2 = re.search(r'(\d+)\s*pages', await p_el.inner_text(), re.IGNORECASE)
                if p_match2: extracted_pages = int(p_match2.group(1))
    except Exception as e:
        print(f"Error scraping book page {b_url}: {e}")
    finally:
        await b_page.close()
        
    return extracted_pages, book_num

async def process_length(row_data, context):
    idx, series_name, author_name, source_url = row_data
    
    try:
        target_url = None
        series_url = None
        
        # If we have a Goodreads series URL directly, use it
        if str(source_url).startswith('http') and '/series/' in str(source_url):
            series_url = str(source_url)
        elif str(source_url).startswith('http') and '/book/' in str(source_url):
            target_url = str(source_url)
        else:
            # Use Autocomplete to find Book 1
            clean_series = str(series_name).replace('[NEW]', '').strip()
            clean_author = str(author_name).replace('[NEW]', '').strip()
            query = f"{clean_series} {clean_author}".strip()
            if query == "nan nan" or query == "":
                return idx, None, None
            target_url = await asyncio.to_thread(get_autocomplete_book_url, query)
            
        if target_url and not series_url:
            # Go to book page to find series URL
            page = await context.new_page()
            try:
                await page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(1)
                
                series_tag = await page.query_selector('h3.Text__title3 a[href*="/series/"], [data-testid="series"] a, div.BookPageTitleSection__title a[href*="/series/"], a.infoBoxRowItem[href*="/series/"]')
                if series_tag:
                    series_url = await series_tag.evaluate("el => el.href")
                else:
                    # Standalone book! Get its page count
                    pages, _ = await extract_pages_from_book(target_url, context, book_num="1")
                    await page.close()
                    if pages == -1:
                        return -1
                    return idx, 1, pages
            except Exception as e:
                pass
            finally:
                if not page.is_closed():
                    await page.close()
                    
        if not series_url:
            return idx, None, None
            
        # Now we have series_url, let's process the series list
        page = await context.new_page()
        total_page_count = 0
        num_primary_books = 0
        try:
            await page.goto(series_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(1)
            
            book_items = await page.query_selector_all('.listWithDividers__item, .seriesWork')
            
            book_tasks = []
            
            for item in book_items:
                item_text = await item.inner_text()
                match = re.search(r'Book\s+([0-9a-zA-Z\.\-]+)', item_text, re.IGNORECASE)
                if match:
                    book_num = match.group(1)
                    if book_num.isdigit() and int(book_num) > 0:
                        num_primary_books += 1
                        b_link = await item.query_selector('a.bookTitle, a[href*="/book/show"]')
                        if b_link:
                            b_url = await b_link.evaluate("el => el.href")
                            b_title = (await b_link.inner_text()).strip()
                            book_tasks.append(extract_pages_from_book(b_url, context, book_title=b_title, book_num=book_num))
                            
            if book_tasks:
                # Open all books within this series AT THE EXACT SAME TIME
                results = await asyncio.gather(*book_tasks)
                for pages, b_num in results:
                    if pages == -1:
                        print(f"[{idx}] Block/Error detected on Book {b_num}!")
                        return -1 # Abort entire scraper
                    else:
                        total_page_count += pages
                        print(f"[{idx}] Book {b_num} -> {pages} pages")
                        
        finally:
            await page.close()
            
        return idx, num_primary_books, total_page_count
        
    except Exception as e:
        print(f"[-] Error processing idx {idx}: {e}")
        return idx, None, None

async def run_scraper():
    csv_path = r"E:\Internship\PocketFM\Romantasy _ Self Publication Master.csv"
    print("Loading CSV...", flush=True)
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    
    col = 'Approx Length (Hrs)'
    missing_mask = df[col].isna() | (df[col] == '') | (df[col].astype(str).str.strip() == '') | (df[col].astype(str).str.lower() == 'nan')
    zero_mask = (df[col] == 0) | (df[col] == '0') | (df[col] == '0.0') | (df[col] == 0.0)
    target_mask = missing_mask | zero_mask
    
    target_indices = df[target_mask].index.tolist()
    
    # Filter for rows >= 1000 as requested
    target_indices = [idx for idx in target_indices if idx >= 1000]
    
    if not target_indices:
        print("No missing lengths found!")
        return
        
    print(f"Found {len(target_indices)} missing lengths to calculate.", flush=True)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        updates = 0
        for idx in target_indices:
            row_data = (
                idx,
                df.at[idx, 'Book Series Name'] if pd.notna(df.at[idx, 'Book Series Name']) else "",
                df.at[idx, 'Author Name'] if pd.notna(df.at[idx, 'Author Name']) else "",
                df.at[idx, 'Source URL'] if pd.notna(df.at[idx, 'Source URL']) else ""
            )
            
            # Process one series at a time sequentially
            res = await process_length(row_data, context)
            
            if res == -1:
                print("\n[!] Goodreads block detected. SHUTTING DOWN SCRAPER GRACEFULLY. Run again later.")
                break
                
            idx, num_books, total_pages = res
            
            if total_pages and total_pages > 0:
                length_hrs = round((total_pages * 250) / 10000, 1)
                df.at[idx, 'Approx Length (Hrs)'] = f"[NEW] {length_hrs}"
                if '# of books in series' in df.columns and num_books:
                    df.at[idx, '# of books in series'] = num_books
                updates += 1
                print(f"[{idx}] Calculated Length: {length_hrs} hrs (from {total_pages} pages, {num_books} books)")
                
                # Save progressively so we don't lose data if we get blocked later
                df.to_csv(csv_path, index=False)
            else:
                print(f"[{idx}] Could not calculate length.")
                
        print(f"Finished session. Saved {updates} new lengths to CSV.")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_scraper())
