import asyncio
import pandas as pd
import re
import os
import urllib.request
import urllib.parse
import json
from playwright.async_api import async_playwright
import format_excel

EXCEL_FILE = r"e:\Internship\PocketFM\Publishers Master Sheet - Noah Lukeman - No GR data.xlsx"
START_ROW = 0
TARGET_ROWS = 150
CONCURRENCY = 5
BATCH_SIZE = 50

# We use urllib for the Autocomplete API to bypass AWS WAF
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
}

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
                        return "https://www.goodreads.com" + book_path
                return None
        except Exception as e:
            time.sleep(1)
    return None

async def process_row(index, row, df, context, sem):
    async with sem:
        rating = row.get("Goodreads Rating (Book 1)")
        count = row.get("Goodreads Ratings Count (Book 1)")
        books = row.get("Primary Books Count")
        pages = row.get("Total Primary Pages")
        
        has_rating = pd.notna(rating) and str(rating).strip().lower() not in ['0.0', '0', 'nan', 'none', '']
        has_count = pd.notna(count) and str(count).strip().lower() not in ['0.0', '0', 'nan', 'none', '']
        has_books = pd.notna(books) and str(books).strip().lower() not in ['0.0', '0', 'nan', 'none', '']
        has_pages = pd.notna(pages) and str(pages).strip().lower() not in ['0.0', '0', 'nan', 'none', '']
        
        # Aggressive Mode: Skip only if ALL 4 data points are perfectly complete
        if has_rating and has_count and has_books and has_pages:
            print(f"[{index}] Row already has Rating ({rating}) and Count ({count}). Skipping.")
            return

        print(f"\n--- Processing Row {index + 1} (Missing Ratings) ---")

        book_name = str(row.get("Title", "")).strip()
        if ":" in book_name:
            book_name = book_name.split(":")[0].strip()
        author_name = str(row.get("Author", "")).strip()
        
        if not book_name or book_name.lower() == 'nan':
            return
            
        existing_val = str(row.get("Goodreads Link", "")).strip()
        has_url = existing_val and existing_val.lower() != 'nan' and existing_val != 'none'
        
        book_url = None
        
        if has_url:
            book_url = existing_val
            print(f"[{index}] Directly using existing Goodreads Link: {book_url}")
        else:
            print(f"[{index}] No Goodreads Link exists! Switching to AGGRESSIVE SEARCH MODE.")
            # 1. Build Query
            if not author_name or author_name.lower() == 'nan' or author_name.lower() == 'none':
                search_query = book_name
            else:
                search_query = f"{book_name} {author_name}"

            # 2. Bypass WAF using Autocomplete API
            book_url = await asyncio.to_thread(get_autocomplete_book_url, search_query)
            if not book_url:
                print(f"[{index}] Book not found in Autocomplete API. Falling back to Aggressive UI Search...")
                search_page = await context.new_page()
                try:
                    search_url = f"https://www.goodreads.com/search?q={urllib.parse.quote_plus(search_query)}"
                    await search_page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(2)
                    
                    b_link = await search_page.query_selector('a.bookTitle')
                    if b_link:
                        book_url = await b_link.evaluate("el => el.href")
                        print(f"[{index}] Found Book URL via Aggressive Search: {book_url}")
                    else:
                        print(f"[{index}] Still not found via Aggressive Search. Leaving completely blank for future retry.")
                        return
                except Exception as e:
                    print(f"[{index}] Aggressive Search failed: {e}. Leaving completely blank for future retry.")
                    return
                finally:
                    await search_page.close()
            else:
                print(f"[{index}] Bypassed WAF. Found Book URL: {book_url}")
        
        # 3. Load Book Page or Search Page
        page = await context.new_page()
        try:
            await page.goto(book_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2)
            
            is_search_link = "/search?" in book_url or "/search/" in book_url
            search_book_hrefs = []
            
            if is_search_link:
                b_links = await page.query_selector_all('a.bookTitle')
                for b in b_links:
                    h = await b.evaluate("el => el.href")
                    if h: search_book_hrefs.append(h)
                    
                if not search_book_hrefs:
                    print(f"[{index}] Search returned no books. Skipping.")
                    return
                
                # Navigate to the first book in the search results to extract ratings and check for official series
                book_url = search_book_hrefs[0]
                print(f"[{index}] Search link detected. Extracted {len(search_book_hrefs)} books. Selected top book: {book_url}")
                await page.goto(book_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2)
            
            # Now we are either on a Book page or a direct Series page
            is_series_link = "/series/" in book_url
            
            if not is_series_link:
                # EXTRACT RATINGS FOR STEP 4
                avg_rating = 0.0
                total_ratings = 0
                
                rating_el = await page.query_selector('div.RatingStatistics__rating')
                if rating_el:
                    try: avg_rating = float((await rating_el.inner_text()).strip())
                    except: pass
                
                count_el = await page.query_selector('[data-testid="ratingsCount"]')
                if count_el:
                    try:
                        count_txt = (await count_el.inner_text()).replace(',', '').split()[0]
                        total_ratings = int(count_txt)
                    except: pass
                    
                df.at[index, "Goodreads Rating (Book 1)"] = avg_rating
                df.at[index, "Goodreads Ratings Count (Book 1)"] = total_ratings
                    
                # 4. Find Official Series Link dynamically
                series_tag = await page.query_selector('h3.Text__title3 a[href*="/series/"], [data-testid="series"] a, div.BookPageTitleSection__title a[href*="/series/"], a.infoBoxRowItem[href*="/series/"]')
                
                if not series_tag:
                    print(f"[{index}] No Official Series link found on Book Page.")
                    df.at[index, "Goodreads Link"] = book_url
                    
                    # Extra Functionality: Check if Book Title explicitly implies a series
                    title_el = await page.query_selector('h1[data-testid="bookTitle"]')
                    is_explicit_installment = False
                    base_title = book_name
                    
                    if title_el:
                        full_title = await title_el.inner_text()
                        if re.search(r'\b(Book|Vol|Volume|Part)\s+[0-9A-Za-z]+\b', full_title, re.IGNORECASE):
                            is_explicit_installment = True
                            base_title = full_title.split('(')[0].split(':')[0].strip()
                            print(f"[{index}] Book Title explicitly implies a series: '{full_title}'. Base Title: '{base_title}'")
                    
                    unofficial_hrefs = []
                    
                    if is_search_link and len(search_book_hrefs) > 1:
                        print(f"[{index}] Falling back to SEARCH RESULTS as an unofficial series ({len(search_book_hrefs)} books).")
                        unofficial_hrefs = search_book_hrefs
                    elif is_explicit_installment:
                        print(f"[{index}] Triggering dynamic search for unofficial series: '{base_title} {author_name}'")
                        search_url2 = f"https://www.goodreads.com/search?q={urllib.parse.quote_plus(base_title + ' ' + author_name)}"
                        s_page = await context.new_page()
                        try:
                            await s_page.goto(search_url2, wait_until="domcontentloaded", timeout=45000)
                            await asyncio.sleep(2)
                            b_links2 = await s_page.query_selector_all('a.bookTitle')
                            for b in b_links2:
                                h = await b.evaluate("el => el.href")
                                if h: unofficial_hrefs.append(h)
                        except Exception as e:
                            print(f"[{index}] Dynamic search failed: {e}")
                        finally:
                            await s_page.close()
                            
                    if unofficial_hrefs and len(unofficial_hrefs) > 1:
                        num_primary_books = len(unofficial_hrefs)
                        total_page_count = 0
                        
                        # Loop through all books in search results to sum pages
                        for i, href in enumerate(unofficial_hrefs):
                            b_page = await context.new_page()
                            try:
                                await b_page.goto(href, wait_until="domcontentloaded", timeout=30000)
                                await asyncio.sleep(1)
                                
                                extracted_pages = 0
                                p_el = await b_page.query_selector('[data-testid="pagesFormat"]')
                                if p_el:
                                    p_match = re.search(r'(\d+)\s*pages', await p_el.inner_text(), re.IGNORECASE)
                                    if p_match: extracted_pages = int(p_match.group(1))
                                    
                                if not extracted_pages:
                                    try:
                                        content = await b_page.content()
                                        matches = re.findall(r'(\d+)\s*pages', content, re.IGNORECASE)
                                        if matches: extracted_pages = int(matches[0])
                                    except: pass
                                    
                                if extracted_pages:
                                    total_page_count += extracted_pages
                                    print(f"[{index}]   - Unofficial Book {i+1} | {extracted_pages} pages (fetched)")
                                else:
                                    print(f"[{index}]   - Unofficial Book {i+1} | 0 pages (not found)")
                            except Exception as e:
                                print(f"[{index}]   - Failed to scrape Unofficial Book {i+1}: {e}")
                            finally:
                                await b_page.close()
                                
                        print(f"[{index}] Unofficial Series Scraping Complete. Primary Books: {num_primary_books} | Total Pages: {total_page_count}")
                        df.at[index, "Primary Books Count"] = num_primary_books
                        df.at[index, "Total Primary Pages"] = total_page_count
                        return
                    else:
                        print(f"[{index}] Treating as standalone.")
                        # Fallback to book's own page count if standalone
                        pages_el = await page.query_selector('[data-testid="pagesFormat"]')
                        if pages_el:
                            pages_text = await pages_el.inner_text()
                            p_match = re.search(r'(\d+)\s*pages', pages_text, re.IGNORECASE)
                            if p_match:
                                df.at[index, "Primary Books Count"] = 1
                                df.at[index, "Total Primary Pages"] = int(p_match.group(1))
                                print(f"[{index}] Standalone Book Pages: {p_match.group(1)}")
                        return
                    
                # 5. Extract Series URL and navigate
                series_url = await series_tag.evaluate("el => el.href")
                print(f"[{index}] Found Official Series Link via React: {series_url}")
                df.at[index, "Goodreads Link"] = series_url
                
                await page.goto(series_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(1)
            else:
                print(f"[{index}] Link is already a Series page. Skipping Rating extraction for Book 1 (unless it is Book 1 below).")
            
            # 6. Parse Series List
            num_primary_books = 0
            total_page_count = 0
            
            book_items = await page.query_selector_all('.listWithDividers__item, .seriesWork')
            
            for item in book_items:
                item_text = await item.inner_text()
                
                match = re.search(r'Book\s+([0-9a-zA-Z\.\-]+)', item_text, re.IGNORECASE)
                if match:
                    book_num = match.group(1)
                    if book_num.isdigit():
                        num_primary_books += 1
                        
                        b_link = await item.query_selector('a.bookTitle, a[href*="/book/show"]')
                        if b_link:
                            b_url = await b_link.evaluate("el => el.href")
                            b_page = await context.new_page()
                            try:
                                await b_page.goto(b_url, wait_until="domcontentloaded", timeout=30000)
                                await asyncio.sleep(1)
                                
                                if book_num == "1":
                                    b_rating_el = await b_page.query_selector('div.RatingStatistics__rating')
                                    if b_rating_el:
                                        try: df.at[index, "Goodreads Rating (Book 1)"] = float((await b_rating_el.inner_text()).strip())
                                        except: pass
                                        
                                    b_count_el = await b_page.query_selector('[data-testid="ratingsCount"]')
                                    if b_count_el:
                                        try:
                                            b_count_txt = (await b_count_el.inner_text()).replace(',', '').split()[0]
                                            df.at[index, "Goodreads Ratings Count (Book 1)"] = int(b_count_txt)
                                        except: pass
                                        
                                extracted_pages = 0
                                
                                for attempt in range(2):
                                    if extracted_pages > 0: break
                                        
                                    if attempt == 1:
                                        print(f"[{index}]   - Book {book_num} pages was 0. Rechecking (Attempt 2)...")
                                        await b_page.reload(wait_until="domcontentloaded")
                                        await asyncio.sleep(2)
                                        
                                    try:
                                        ld_el = await b_page.query_selector('script[type="application/ld+json"]')
                                        if ld_el:
                                            import json
                                            data = json.loads(await ld_el.inner_text())
                                            if isinstance(data, list): data = data[0]
                                            if 'numberOfPages' in data:
                                                extracted_pages = int(data['numberOfPages'])
                                    except: pass
                                    
                                    if not extracted_pages:
                                        p_el = await b_page.query_selector('[data-testid="pagesFormat"]')
                                        if p_el:
                                            p_match2 = re.search(r'(\d+)\s*pages', await p_el.inner_text(), re.IGNORECASE)
                                            if p_match2: extracted_pages = int(p_match2.group(1))
                                    
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
                                        
                                    if not extracted_pages:
                                        try:
                                            content = await b_page.content()
                                            matches = re.findall(r'(\d+)\s*pages', content, re.IGNORECASE)
                                            if matches:
                                                extracted_pages = int(matches[0])
                                        except: pass

                                if extracted_pages:
                                    total_page_count += extracted_pages
                                    print(f"[{index}]   - Primary Book {book_num} | {extracted_pages} pages (fetched)")
                                else:
                                    print(f"[{index}]   - Primary Book {book_num} | 0 pages (not found)")
                            except Exception as e:
                                print(f"[{index}]   - Failed to scrape Book {book_num}: {e}")
                            finally:
                                await b_page.close()
                                    
            if num_primary_books == 0:
                num_primary_books = max(1, len(book_items))
                print(f"[{index}]   - Warning: 0 primary books parsed. Defaulting to {num_primary_books}.")
                
            print(f"[{index}] Aggressive Scraping Complete. Primary Books: {num_primary_books} | Total Pages: {total_page_count}")
            df.at[index, "Primary Books Count"] = num_primary_books
            df.at[index, "Total Primary Pages"] = total_page_count

        except Exception as e:
            print(f"[{index}] Error rendering page via Playwright: {e}")
        finally:
            await page.close()

async def run_scraper():
    print(f"Loading {EXCEL_FILE}...")
    try:
        df = pd.read_excel(EXCEL_FILE)
    except Exception as e:
        print(f"Excel load error: {e}")
        return

    # Ensure all target columns exist
    for col in ["Goodreads Rating (Book 1)", "Goodreads Ratings Count (Book 1)", "Primary Books Count", "Total Primary Pages", "Goodreads Link"]:
        if col not in df.columns:
            df[col] = None

    print(f"Running HYBRID Scraper on FIRST {TARGET_ROWS} ROWS with CONCURRENCY {CONCURRENCY} and BATCH_SIZE {BATCH_SIZE}...")

    user_data_dir = os.path.join(r"e:\Internship\PocketFM", "playwright_goodreads_profile")
    
    total_to_process = min(TARGET_ROWS, len(df))
    
    for batch_start in range(START_ROW, total_to_process, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_to_process)
        print(f"\n=======================================================")
        print(f"STARTING BATCH {batch_start} to {batch_end} (Cooldown Architecture)")
        print(f"=======================================================\n")
        
        # Fast-forward check: strictly check if ratings are missing
        needs_processing = False
        for i in range(batch_start, batch_end):
            row = df.iloc[i]
            rating = row.get("Goodreads Rating (Book 1)")
            count = row.get("Goodreads Ratings Count (Book 1)")
            books = row.get("Primary Books Count")
            pages = row.get("Total Primary Pages")
            
            has_rating = pd.notna(rating) and str(rating).strip().lower() not in ['0.0', '0', 'nan', 'none', '']
            has_count = pd.notna(count) and str(count).strip().lower() not in ['0.0', '0', 'nan', 'none', '']
            has_books = pd.notna(books) and str(books).strip().lower() not in ['0.0', '0', 'nan', 'none', '']
            has_pages = pd.notna(pages) and str(pages).strip().lower() not in ['0.0', '0', 'nan', 'none', '']
            
            if not (has_rating and has_count and has_books and has_pages):
                needs_processing = True
                break
                
        if not needs_processing:
            print(f"Entire batch {batch_start}-{batch_end} already has all ratings. Skipping.")
            continue

        async with async_playwright() as p:
            print("Launching Fresh Playwright Context...")
            context = await p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=False,
                args=['--disable-blink-features=AutomationControlled'],
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={'width': 1280, 'height': 800}
            )
            
            # --- AUTOMATED LOGIN ---
            login_page = await context.new_page()
            try:
                print("Forcing fresh session... Navigating to sign-out...")
                await login_page.goto("https://www.goodreads.com/user/sign_out", wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2)
                
                print("Navigating to sign-in page...")
                await login_page.goto("https://www.goodreads.com/user/sign_in", wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2)
                
                print("Attempting automated login...")
                email_btn = login_page.locator('a:has-text("Sign in with email")')
                if await email_btn.is_visible():
                    await email_btn.click()
                    await asyncio.sleep(2)
                
                if await login_page.locator("#ap_email").is_visible():
                    await login_page.fill("#ap_email", "noel.regis04@gmail.com")
                    await login_page.fill("#ap_password", "Noel@1024")
                    await login_page.click("#signInSubmit")
                    print("Credentials submitted. Waiting 10 seconds for you to solve any CAPTCHAs...")
                    await asyncio.sleep(10)
                else:
                    print("Email input not found, might already be logged in.")
            except Exception as e:
                print(f"Auto-login skipped or failed: {e}")
            finally:
                await login_page.close()
            
            # Limit concurrency to exactly CONCURRENCY active tasks
            sem = asyncio.Semaphore(CONCURRENCY)
            
            # Build tasks for this batch
            tasks = []
            for index in range(batch_start, batch_end):
                row = df.iloc[index]
                tasks.append(process_row(index, row, df, context, sem))
                
            # Run them simultaneously
            await asyncio.gather(*tasks)

            await context.close()
            
        # Securely save after every batch
        try:
            df.to_excel(EXCEL_FILE, index=False)
            print(f"\nBatch {batch_start}-{batch_end} saved successfully.")
        except Exception as e:
            print(f"Failed to save batch {batch_start}-{batch_end}: {e}")
            
        # Cooldown sleep if there are more batches left
        if batch_end < total_to_process:
            print("Cooling down WAF for 10 seconds...")
            await asyncio.sleep(10)

    try:
        print("\nAll batches complete for the target rows.")
        # print("Applying automatic Excel styling...")
        # format_excel.apply_styling(EXCEL_FILE)
        print("Scraping finished. Check output above for any errors.")
    except Exception as e:
        print(f"Failed to finish script cleanly: {e}")

if __name__ == "__main__":
    asyncio.run(run_scraper())
