import asyncio
import pandas as pd
import re
import os
from playwright.async_api import async_playwright
import format_excel

EXCEL_FILE = r"e:\Internship\PocketFM\vanshika_part2.xlsx"
START_ROW = 0
TARGET_ROWS = 2400
CONCURRENCY = 5
BATCH_SIZE = 50

async def extract_pages(page):
    # Give the page a bit more time to render React elements
    await asyncio.sleep(2)
    
    # 1. Try JSON-LD
    try:
        ld_el = await page.query_selector('script[type="application/ld+json"]')
        if ld_el:
            import json
            data = json.loads(await ld_el.inner_text())
            if isinstance(data, list): data = data[0]
            if 'numberOfPages' in data:
                return int(data['numberOfPages'])
    except: pass
    
    # 2. Try simple data-testid
    try:
        p_el = await page.query_selector('[data-testid="pagesFormat"]')
        if p_el:
            text = await p_el.inner_text()
            p_match2 = re.search(r'(\d+)\s*pages', text, re.IGNORECASE)
            if p_match2: return int(p_match2.group(1))
    except: pass
            
    # 3. Try clicking Book details
    try:
        btn = await page.query_selector('button:has-text("Book details")')
        if btn:
            await btn.click(force=True)
            await asyncio.sleep(1.5)
            p_el = await page.query_selector('[data-testid="pagesFormat"]')
            if p_el:
                text = await p_el.inner_text()
                p_match2 = re.search(r'(\d+)\s*pages', text, re.IGNORECASE)
                if p_match2: return int(p_match2.group(1))
    except: pass
    
    # 4. Try looking for metadata properties
    try:
        meta = await page.query_selector('meta[property="books:page_count"]')
        if meta:
            content = await meta.get_attribute('content')
            if content and content.isdigit():
                return int(content)
    except: pass
    
    # 5. Fallback Regex across entire content
    try:
        content = await page.content()
        matches = re.findall(r'(\d+)\s*pages', content, re.IGNORECASE)
        if matches:
            return int(matches[0])
    except: pass
    
    return 0

async def process_row(index, row, df, context, sem):
    async with sem:
        existing_url = str(row.get("GoodReads_Series_URL", "")).strip()
        num_books = row.get("Num_Primary_Books_in_Series")
        num_pages = row.get("Total_Page_Count_of_Primary_Books")
        
        has_url = existing_url and existing_url.lower() != 'nan' and existing_url != 'none'
        
        if not has_url:
            return

        # TARGET CONDITION: Only process if it has books, but pages is 0 or NaN
        has_books = str(num_books) not in ['0', '0.0', 'nan', 'None', ''] and pd.notna(num_books)
        is_missing_pages = str(num_pages) in ['0', '0.0', 'nan', 'None', ''] or not pd.notna(num_pages)
        
        if not (has_books and is_missing_pages):
            return

        print(f"\n--- Row {index + 1}: Found Books={num_books} but Pages={num_pages}. Re-scraping ---")
        print(f"[{index}] URL: {existing_url}")

        page = await context.new_page()
        try:
            await page.goto(existing_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(2)
            
            is_series = "/series/" in existing_url
            
            if not is_series:
                pages = await extract_pages(page)
                df.at[index, "Num_Primary_Books_in_Series"] = 1
                if pages: df.at[index, "Total_Page_Count_of_Primary_Books"] = pages
                print(f"[{index}] Standalone Pages updated to: {pages}")
                
            else:
                book_items = await page.query_selector_all('.listWithDividers__item, .seriesWork')
                
                total_page_count = 0
                
                for item in book_items:
                    item_text = await item.inner_text()
                    match = re.search(r'Book\s+([0-9a-zA-Z\.\-]+)', item_text, re.IGNORECASE)
                    if match:
                        book_num = match.group(1)
                        if book_num.isdigit():
                            b_link = await item.query_selector('a.bookTitle, a[href*="/book/show"]')
                            if b_link:
                                b_url = await b_link.evaluate("el => el.href")
                                b_page = await context.new_page()
                                try:
                                    await b_page.goto(b_url, wait_until="domcontentloaded", timeout=45000)
                                    extracted_pages = await extract_pages(b_page)
                                    if extracted_pages:
                                        total_page_count += extracted_pages
                                        print(f"[{index}] Book {book_num} Pages: {extracted_pages}")
                                    else:
                                        print(f"[{index}] Book {book_num} Pages: STILL NOT FOUND (Might be Audiobook edition)")
                                except Exception as e:
                                    print(f"[{index}] Error on Book {book_num}: {e}")
                                finally:
                                    await b_page.close()
                                    
                df.at[index, "Total_Page_Count_of_Primary_Books"] = total_page_count
                print(f"[{index}] Series Pages updated to: {total_page_count}")
                
        except Exception as e:
            print(f"[{index}] Error processing {existing_url}: {e}")
        finally:
            await page.close()


async def run_scraper():
    print(f"Loading {EXCEL_FILE}...")
    try:
        df = pd.read_excel(EXCEL_FILE)
    except Exception as e:
        print(f"Excel load error: {e}")
        return

    user_data_dir = os.path.join(r"e:\Internship\PocketFM", "playwright_goodreads_profile_missing")
    
    total_to_process = min(TARGET_ROWS, len(df))
    
    for batch_start in range(START_ROW, total_to_process, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_to_process)
        
        needs_processing = False
        for i in range(batch_start, batch_end):
            row = df.iloc[i]
            existing_url = str(row.get("GoodReads_Series_URL", "")).strip()
            num_books = row.get("Num_Primary_Books_in_Series")
            num_pages = row.get("Total_Page_Count_of_Primary_Books")
            
            has_url = existing_url and existing_url.lower() != 'nan' and existing_url != 'none'
            if has_url:
                has_books = str(num_books) not in ['0', '0.0', 'nan', 'None', ''] and pd.notna(num_books)
                is_missing_pages = str(num_pages) in ['0', '0.0', 'nan', 'None', ''] or not pd.notna(num_pages)
                if has_books and is_missing_pages:
                    needs_processing = True
                    break
                    
        if not needs_processing:
            continue
            
        print(f"\n=======================================================")
        print(f"STARTING BATCH {batch_start} to {batch_end} for 0 Pages Only")
        print(f"=======================================================\n")

        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=False,
                args=['--disable-blink-features=AutomationControlled'],
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={'width': 1280, 'height': 800}
            )
            
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = []
            for index in range(batch_start, batch_end):
                row = df.iloc[index]
                tasks.append(process_row(index, row, df, context, sem))
                
            await asyncio.gather(*tasks)
            await context.close()
            
        try:
            df.to_excel(EXCEL_FILE, index=False)
            print(f"Batch {batch_start}-{batch_end} saved successfully.")
        except Exception as e:
            print(f"Failed to save batch {batch_start}-{batch_end}: {e}")

    try:
        format_excel.apply_styling(EXCEL_FILE)
    except Exception as e:
        print(f"Failed to apply styling: {e}")

if __name__ == "__main__":
    asyncio.run(run_scraper())
