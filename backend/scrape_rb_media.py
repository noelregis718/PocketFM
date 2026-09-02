import asyncio
import pandas as pd
import os
import json
import urllib.request
import urllib.parse
from playwright.async_api import async_playwright
import re

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9',
}

CONCURRENCY = 5

def get_autocomplete_book_url(query):
    """Uses raw HTTP to bypass WAF and get the direct Book URL."""
    import time
    api_url = f"https://www.goodreads.com/book/auto_complete?format=json&q={urllib.parse.quote_plus(query)}"
    for attempt in range(3):
        try:
            req = urllib.request.Request(api_url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as response:
                data = json.loads(response.read().decode("utf-8", errors="ignore"))
                if data and len(data) > 0:
                    book_path = data[0].get('bookUrl', '')
                    if book_path:
                        return "https://www.goodreads.com" + book_path.split('?')[0]
                return None
        except Exception as e:
            time.sleep(1)
    return None

async def process_row(index, row, df, context, sem):
    async with sem:
        series_name = str(row.get('Series Name', ''))
        author_name = str(row.get('Author Name', ''))
        search_query = f"{series_name} by {author_name}".strip()
        
        if not search_query or search_query.lower() == 'nan':
            return
            
        existing_link = str(row.get('GR series link', '')).strip()
        book_url = None
        series_url = None
        
        if existing_link.startswith('http'):
            print(f"\n[{index}] Using existing link from CSV: {existing_link}")
            if '/series/' in existing_link:
                series_url = existing_link
            else:
                book_url = existing_link
        else:
            print(f"\n[{index}] Searching for: {search_query}")
            book_url = await asyncio.to_thread(get_autocomplete_book_url, search_query)
            
            if not book_url:
                print(f"[{index}] Not found in Autocomplete API. Falling back to Aggressive UI Search...")
                search_page = await context.new_page()
                try:
                    fallback_query = series_name.strip()
                    if fallback_query.lower().endswith(" series"):
                        fallback_query = fallback_query[:-7].strip()
                    
                    print(f"[{index}] Aggressive Search Query: '{fallback_query}'")
                    search_url = f"https://www.goodreads.com/search?q={urllib.parse.quote_plus(fallback_query)}"
                    await search_page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(2)
                    
                    book_links = await search_page.query_selector_all('a.bookTitle, a[href*="/book/show/"]')
                    best_link = None
                    
                    for link in book_links:
                        title_text = (await link.inner_text()).strip()
                        if not title_text:
                            continue
                            
                        title_lower = title_text.lower()
                        bundle_pattern = re.search(r'(books?\s*\d+\s*-\s*\d+|collection|box set|boxed set)', title_lower)
                        
                        if not bundle_pattern and len(title_text) > 2:
                            best_link = link
                            print(f"[{index}] Selected from Search: {title_text}")
                            break
                            
                    if best_link:
                        book_url = await best_link.evaluate("el => el.href")
                        print(f"[{index}] Found Book URL via Aggressive Search: {book_url}")
                    else:
                        print(f"[{index}] Still not found via Aggressive Search.")
                        if not existing_link.startswith('http'):
                            df.at[index, 'GR series link'] = "Not Found"
                        return
                except Exception as e:
                    print(f"[{index}] Aggressive Search failed: {e}")
                    if not existing_link.startswith('http'):
                        df.at[index, 'GR series link'] = "Not Found"
                    return
                finally:
                    await search_page.close()
                
            print(f"[{index}] Found Book URL: {book_url}")
            
        page = await context.new_page()
        try:
            if book_url and not series_url:
                await page.goto(book_url, wait_until="domcontentloaded", timeout=45000)
                series_tag = await page.query_selector('h3.Text__title3 a[href*="/series/"], [data-testid="series"] a, div.BookPageTitleSection__title a[href*="/series/"], a.infoBoxRowItem[href*="/series/"]')
                if series_tag:
                    try:
                        href = await series_tag.get_attribute('href')
                        if href:
                            series_url = href if href.startswith('http') else "https://www.goodreads.com" + href
                    except: pass
            
            
            if not series_url:
                print(f"[{index}] No Series URL found. Treating initial book as Book 1.")
                # Extractor block for single book fallback
                rating_text, ratings_count_text, pages = "Unknown", "Unknown", 0
                primary_book_count = 1
                try:
                    r_el = await page.query_selector('div.RatingStatistics__rating')
                    if r_el: rating_text = (await r_el.inner_text()).strip()
                    
                    rc_el = await page.query_selector('[data-testid="ratingsCount"]')
                    if rc_el: ratings_count_text = (await rc_el.inner_text()).replace(',', '').split()[0]
                    
                    # Robust single page extraction
                    p_el = await page.query_selector('[data-testid="pagesFormat"]')
                    if p_el:
                        pages_text = await p_el.inner_text()
                        p_match = re.search(r'(\d+)\s*pages', pages_text, re.IGNORECASE)
                        if p_match: pages = int(p_match.group(1))
                        
                    if pages == 0:
                        ld_el = await page.query_selector('script[type="application/ld+json"]')
                        if ld_el:
                            import json
                            data = json.loads(await ld_el.inner_text())
                            if isinstance(data, list): data = data[0]
                            if 'numberOfPages' in data:
                                pages = int(data['numberOfPages'])
                except: pass
                
                df.at[index, 'GR Book 1 rating'] = rating_text
                df.at[index, 'GR Book 1 #ratings'] = ratings_count_text
                df.at[index, 'Total page count'] = pages if pages > 0 else "Unknown"
                df.at[index, '*Total No. of Books'] = primary_book_count
                df.at[index, 'GR series link'] = book_url
                await page.close()
                return

            print(f"[{index}] Found Series URL: {series_url}")
            df.at[index, 'GR series link'] = series_url
            
            # Navigate to Series Page
            await page.goto(series_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(1)
            
            book_items = await page.query_selector_all('.listWithDividers__item, .seriesWork')
            
            total_pages = 0
            rating_text = "Unknown"
            ratings_count_text = "Unknown"
            scrape_failed = False
            primary_book_count = 0
            
            for item in book_items:
                try:
                    item_text = await item.inner_text()
                    
                    # Extract the exact book number string (e.g., '1', '1.5', '1-3', '1A')
                    match = re.search(r'Book\s+([0-9a-zA-Z\.\-]+)', item_text, re.IGNORECASE)
                    if match:
                        book_num = match.group(1)
                        # Check if it is purely a whole number (primary book)
                        if book_num.isdigit():
                            primary_book_count += 1
                            b_link = await item.query_selector('a.bookTitle, a[href*="/book/show"]')
                            if b_link:
                                b_url = await b_link.evaluate("el => el.href")
                                b_page = await context.new_page()
                                try:
                                    print(f"    -> [{index}] Extracting Book {book_num}...")
                                    await b_page.goto(b_url, wait_until="domcontentloaded", timeout=30000)
                                    await asyncio.sleep(1)
                                    
                                    # Check for bot block
                                    body_text = await b_page.inner_text("body")
                                    if "Checking if the site connection is secure" in body_text or "Robot Check" in body_text:
                                        print(f"    -> [{index}] Blocked by Goodreads Captcha. Skipping entire row.")
                                        scrape_failed = True
                                        break
                                    
                                    # If this is Book 1, scrape the definitive rating
                                    if book_num == "1":
                                        b_rating_el = await b_page.query_selector('div.RatingStatistics__rating')
                                        if b_rating_el:
                                            try: rating_text = (await b_rating_el.inner_text()).strip()
                                            except: pass
                                            
                                        b_count_el = await b_page.query_selector('[data-testid="ratingsCount"]')
                                        if b_count_el:
                                            try: ratings_count_text = (await b_count_el.inner_text()).replace(',', '').split()[0]
                                            except: pass
                                            
                                    # Robust Page Extraction
                                    extracted_pages = 0
                                    
                                    for attempt in range(2):
                                        if extracted_pages > 0:
                                            break
                                            
                                        if attempt == 1:
                                            print(f"[{index}]   - Book {book_num} pages was 0. Rechecking (Attempt 2)...")
                                            await b_page.reload(wait_until="domcontentloaded")
                                            await asyncio.sleep(2)
                                            
                                        # 1. Try JSON-LD
                                        try:
                                            ld_el = await b_page.query_selector('script[type="application/ld+json"]')
                                            if ld_el:
                                                import json
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
                                        
                                        # 3. Try clicking Book details
                                        if not extracted_pages:
                                            try:
                                                btn = await b_page.query_selector('button:has-text("Book details")')
                                                if btn:
                                                    await btn.click(force=True)
                                                    await asyncio.sleep(1)
                                                    p_el = await b_page.query_selector('[data-testid="pagesFormat"]')
                                                    if p_el:
                                                        p_match2 = re.search(r'(\d+)\s*pages', await p_el.inner_text(), re.IGNORECASE)
                                                        if p_match2: extracted_pages = int(p_match2.group(1))
                                            except: pass
                                            
                                        # 4. Fallback Regex across entire content
                                        if not extracted_pages:
                                            try:
                                                content = await b_page.content()
                                                matches = re.findall(r'(\d+)\s*pages', content, re.IGNORECASE)
                                                if matches:
                                                    extracted_pages = int(matches[0])
                                            except: pass
                                        
                                    if extracted_pages == 0:
                                        print(f"    -> [{index}] Could not find page count for Book {book_num} (possibly Unknown Binding). Treating as 0 pages and continuing.")
                                        
                                    total_pages += extracted_pages
                                    print(f"[{index}]   - Primary Book {book_num} | {extracted_pages} pages (fetched)")
                                    
                                except Exception as e:
                                    print(f"    -> [{index}] Failed to load Book {book_num}: {e}. Skipping entire row.")
                                    scrape_failed = True
                                    break
                                finally:
                                    await b_page.close()
                                    
                except Exception:
                    pass

            if scrape_failed:
                print(f"[{index}] Row aborted. Leaving columns untouched.")
                return
                
            if primary_book_count == 0:
                primary_book_count = max(1, len(book_items))
                
            print(f"[{index}] Series Scrape Complete! Primary Books: {primary_book_count} | Total Pages: {total_pages} | Book 1 Rating: {rating_text}")
            df.at[index, 'Total page count'] = total_pages
            df.at[index, '*Total No. of Books'] = primary_book_count
            df.at[index, 'GR Book 1 rating'] = rating_text
            df.at[index, 'GR Book 1 #ratings'] = ratings_count_text
            
        except Exception as e:
            print(f"[{index}] Failed to process series logic: {e}")
        finally:
            await page.close()

async def run_scraper():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(base_dir)
    target_path = os.path.join(parent_dir, 'RB Media _ Pocket FM List - 2 (Testing Final) _ Internal Copy - Noel.csv')
    user_data_dir = os.path.join(parent_dir, 'playwright_goodreads_profile')
    
    print(f"Loading {target_path}...")
    try:
        df = pd.read_csv(target_path)
    except Exception as e:
        print(f"Failed to load CSV: {e}")
        return
        
    cols_to_convert = ['GR series link', 'GR Book 1 rating', 'GR Book 1 #ratings', 'Total page count', '*Total No. of Books']
    for col in cols_to_convert:
        if col in df.columns:
            df[col] = df[col].astype('object')
            
    indices_to_scrape = []
    for idx, row in df.iterrows():
        link_val = str(row.get('GR series link', '')).strip().lower()
        pages_val = str(row.get('Total page count', '')).strip().lower()
        rating_val = str(row.get('GR Book 1 rating', '')).strip().lower()
        
        missing_link = link_val == '' or link_val == 'nan' or link_val == 'not found'
        missing_pages = pages_val == '' or pages_val == 'nan' or pages_val == 'unknown' or pages_val == '0'
        missing_rating = rating_val == '' or rating_val == 'nan' or rating_val == 'unknown'
        
        if missing_link or missing_pages or missing_rating:
            indices_to_scrape.append(idx)
            
    print(f"Found {len(indices_to_scrape)} series needing Goodreads scraping.")
    if not indices_to_scrape:
        return
        
    TEST_LIMIT = 500
    indices_to_scrape = indices_to_scrape[:TEST_LIMIT]
    print(f"Running on up to {TEST_LIMIT} missing rows...")
        
    async with async_playwright() as p:
        print("Launching Playwright Context with Persistent Login...")
        context = await p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=False,
            args=['--disable-blink-features=AutomationControlled'],
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 800}
        )
        
        page = await context.new_page()
        await page.goto("https://www.goodreads.com/")
        try:
            await page.wait_for_selector('a[href="/user/sign_out"], .headerPersonalNav', timeout=5000)
            print("Successfully verified Goodreads Login.")
        except:
            print("Not logged in! Pausing script so you can log in.")
            await page.pause()
            
        await page.close()
        
        sem = asyncio.Semaphore(CONCURRENCY)
        
        tasks = []
        for idx in indices_to_scrape:
            row = df.iloc[idx]
            tasks.append(process_row(idx, row, df, context, sem))
            
        CHUNK_SIZE = 10
        for i in range(0, len(tasks), CHUNK_SIZE):
            chunk = tasks[i:i + CHUNK_SIZE]
            await asyncio.gather(*chunk)
            df.to_csv(target_path, index=False)
            print(f"Saved progress (chunk {i} to {i + len(chunk)})")

        await context.close()
        print("Scraping Complete!")
        
if __name__ == "__main__":
    asyncio.run(run_scraper())
