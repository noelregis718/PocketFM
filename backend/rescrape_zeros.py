import asyncio
import pandas as pd
import re
import os
import urllib.request
import urllib.parse
import json
from playwright.async_api import async_playwright
import format_excel

EXCEL_FILE = r"e:\Internship\PocketFM\vanshika_part1.xlsx"
START_ROW = 0
TARGET_ROWS = 2500
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
        book_name = str(row.get("Book Title", row.get("Book Name", row.get("Book 1 Title", "")))).strip()
        
        # SANITIZE BOOK NAME: Take only the part before a colon to avoid confusing the search
        if ":" in book_name:
            book_name = book_name.split(":")[0].strip()
            
        author_name = str(row.get("Author Name", "")).strip()
        
        if not book_name or book_name.lower() == 'nan':
            return
            
        num = row.get("Num_Primary_Books_in_Series")
        if str(num) != '0' and str(num) != '0.0':
            # Skip if it is not a 0
            return

        print(f"\n--- Processing Row {index + 1} ---")

        # 1. Build Query
        if not author_name or author_name.lower() == 'nan' or author_name.lower() == 'none':
            print(f"[{index}] No valid author found. Querying: '{book_name}'")
            search_query = book_name
        else:
            print(f"[{index}] Querying Book + Author: '{book_name}' by '{author_name}'")
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
        
        # 3. Load Book Page using Playwright to render React
        page = await context.new_page()
        try:
            await page.goto(book_url, wait_until="domcontentloaded", timeout=45000)
            # Short sleep to allow React to inject data
            await asyncio.sleep(2)
            
            # EXPAND GENRES (Click "...more" if present)
            try:
                genre_btns = await page.query_selector_all('[data-testid="genresList"] button, [data-testid="genresList"] [role="button"]')
                for btn in genre_btns:
                    if "...more" in (await btn.inner_text()).lower():
                        await btn.click(force=True)
                        await asyncio.sleep(0.5)
                        break
            except Exception:
                pass

            # EXTRACT GENRES
            genres = []
            genre_els = await page.query_selector_all('[data-testid="genresList"] .Button__labelItem, .BookPageMetadataSection__genre a')
            for gel in genre_els:
                txt = (await gel.inner_text()).strip()
                if txt and txt not in genres: genres.append(txt)
            if genres:
                df.at[index, "Genre Tags"] = ", ".join(genres)
                
            # EXTRACT SYNOPSIS
            desc_el = await page.query_selector('[data-testid="description"] .Formatted, .readable')
            if desc_el: 
                df.at[index, "Synopsis"] = (await desc_el.inner_text()).strip()
                
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
                
            df.at[index, "Book1_Rating"] = avg_rating
            df.at[index, "Book1_Num_Ratings"] = total_ratings
                
            # Romantasy classification will happen at the end to include series length
            
            # 4. Find Series Link dynamically
            series_tag = await page.query_selector('h3.Text__title3 a[href*="/series/"], [data-testid="series"] a, div.BookPageTitleSection__title a[href*="/series/"], a.infoBoxRowItem[href*="/series/"]')
            if not series_tag:
                print(f"[{index}] No Series link found on Book Page. It is likely a standalone.")
                df.at[index, "GoodReads_Series_URL"] = book_url
                
                # Fallback to book's own page count if standalone
                pages_el = await page.query_selector('[data-testid="pagesFormat"]')
                if pages_el:
                    pages_text = await pages_el.inner_text()
                    p_match = re.search(r'(\d+)\s*pages', pages_text, re.IGNORECASE)
                    if p_match:
                        df.at[index, "Num_Primary_Books_in_Series"] = 1
                        df.at[index, "Total_Page_Count_of_Primary_Books"] = int(p_match.group(1))
                        print(f"[{index}] Standalone Book Pages: {p_match.group(1)}")
                
                _apply_romantasy_checker(df, index, row, genres)
                return
                
            # 5. Extract Series URL and navigate
            series_url = await series_tag.evaluate("el => el.href")
            print(f"[{index}] Found Series Link via React: {series_url}")
            df.at[index, "GoodReads_Series_URL"] = series_url
            
            await page.goto(series_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(1)
            
            # 6. Parse Series List
            num_primary_books = 0
            total_page_count = 0
            
            book_items = await page.query_selector_all('.listWithDividers__item, .seriesWork')
            
            for item in book_items:
                item_text = await item.inner_text()
                
                # Extract the exact book number string (e.g., '1', '1.5', '1-3', '1A')
                match = re.search(r'Book\s+([0-9a-zA-Z\.\-]+)', item_text, re.IGNORECASE)
                if match:
                    book_num = match.group(1)
                    # Check if it is purely a whole number (primary book)
                    if book_num.isdigit():
                        num_primary_books += 1
                        
                        # Aggressive scraping: Always visit the book page to get accurate page count
                        b_link = await item.query_selector('a.bookTitle, a[href*="/book/show"]')
                        if b_link:
                            b_url = await b_link.evaluate("el => el.href")
                            b_page = await context.new_page()
                            try:
                                await b_page.goto(b_url, wait_until="domcontentloaded", timeout=30000)
                                await asyncio.sleep(1)
                                
                                # If this is Book 1, scrape the definitive rating and replace the default one
                                if book_num == "1":
                                    b_rating_el = await b_page.query_selector('div.RatingStatistics__rating')
                                    if b_rating_el:
                                        try: df.at[index, "Book1_Rating"] = float((await b_rating_el.inner_text()).strip())
                                        except: pass
                                        
                                    b_count_el = await b_page.query_selector('[data-testid="ratingsCount"]')
                                    if b_count_el:
                                        try:
                                            b_count_txt = (await b_count_el.inner_text()).replace(',', '').split()[0]
                                            df.at[index, "Book1_Num_Ratings"] = int(b_count_txt)
                                        except: pass
                                        
                                # Robust Page Extraction with Retry
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
            df.at[index, "Num_Primary_Books_in_Series"] = num_primary_books
            df.at[index, "Total_Page_Count_of_Primary_Books"] = total_page_count
            
            _apply_romantasy_checker(df, index, row, genres)

        except Exception as e:
            print(f"[{index}] Error rendering page via Playwright: {e}")
        finally:
            await page.close()

def _apply_romantasy_checker(df, index, row, genres):
    import pandas as pd
    classification = "Fail"
    all_tags = []
    
    def add_tags(val):
        if pd.isna(val) or not val: return
        parts = [p.strip() for p in str(val).split(',')]
        for p in parts:
            if p and p not in all_tags: all_tags.append(p)
            
    add_tags(row.get('Genre'))
    for g in genres: add_tags(g)
    add_tags(row.get('Keyword'))
    
    fantasy_kws = ['fantasy', 'paranormal', 'supernatural', 'magic', 'fae', 'witch', 'vampire', 'dragon', 'mythology', 'fairy tale', 'monster', 'isekai', 'reincarnation', 'sci-fi', 'beast']
    romance_kws = ['romance', 'romantasy', 'romantic', 'love', 'mate', 'heart', 'beauty']
    
    idx_f, idx_r = -1, -1
    for i, tag in enumerate(all_tags):
        tag_lower = tag.lower()
        if idx_f == -1 and any(kw in tag_lower for kw in fantasy_kws): idx_f = i
        if idx_r == -1 and any(kw in tag_lower for kw in romance_kws): idx_r = i
    for i, tag in enumerate(all_tags):
        if 'romantasy' in tag.lower():
            if idx_f == -1 or i < idx_f: idx_f = i
            if idx_r == -1 or i < idx_r: idx_r = i
            
    if idx_f != -1 and idx_r != -1:
        rank = max(idx_f, idx_r)
        if rank < 5: classification = "Strong Match"
        elif rank < 9: classification = "Confirmed Match"
        else: classification = "Weak Match"
        
    num_books = df.at[index, "Num_Primary_Books_in_Series"]
    if pd.notna(num_books):
        try:
            if float(num_books) < 3:
                classification = "Weak Match"
        except: pass
        
    df.at[index, "Romantasy Checker"] = classification
    print(f"[{index}] Romantasy Checker: {classification}")

async def run_scraper():
    print(f"Loading {EXCEL_FILE}...")
    try:
        df = pd.read_excel(EXCEL_FILE)
    except Exception as e:
        print(f"Excel load error: {e}")
        return

    # Ensure all target columns exist
    for col in ["Num_Primary_Books_in_Series", "Total_Page_Count_of_Primary_Books", "Genre Tags", "Synopsis", "Romantasy Checker", "Book1_Rating", "Book1_Num_Ratings", "GoodReads_Series_URL"]:
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
        
        # Rescrape Zeros Check: skip batch if it has no zeros
        needs_processing = False
        for i in range(batch_start, batch_end):
            num = df.iloc[i].get("Num_Primary_Books_in_Series")
            if str(num) == '0' or str(num) == '0.0':
                needs_processing = True
                break
                
        if not needs_processing:
            print(f"Entire batch {batch_start}-{batch_end} has no zeros to re-scrape. Skipping.")
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
        print("Applying automatic Excel styling...")
        format_excel.apply_styling(EXCEL_FILE)
    except Exception as e:
        print(f"Failed to apply styling: {e}")

if __name__ == "__main__":
    asyncio.run(run_scraper())
