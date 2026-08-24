import asyncio
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from goodreads_scraper import GoodreadsScraper
from playwright.async_api import async_playwright

EXCEL_FILE = r"E:\Internship\PocketFM\Combined List of Titles.xlsx"
CONCURRENCY = 4  # Lower concurrency for Goodreads to prevent blocks

async def process_row(idx, row, df, context, scraper, sem):
    async with sem:
        url = row.get('GoodReads_Series_URL')
        
        # If Series URL is missing, try to extract it from the Goodreads Book Link
        if pd.isna(url) or not str(url).startswith('http'):
            book_url = row.get('Goodreads Link')
            if pd.notna(book_url) and str(book_url).startswith('http'):
                print(f"[{idx}] Finding Series URL from Book URL...")
                bp = await context.new_page()
                try:
                    await bp.goto(book_url, wait_until="domcontentloaded", timeout=45000)
                    series_link = await bp.query_selector('h3.Text__title3 a[href*="/series/"], [data-testid="series"] a')
                    if series_link:
                        url = await series_link.evaluate("el => el.href")
                except Exception:
                    pass
                finally:
                    await bp.close()
            
        if pd.isna(url) or not str(url).startswith('http'):
            print(f"[{idx}] No valid Goodreads Series URL found. Skipping.")
            return

        print(f"[{idx}] Scraping Goodreads Series URL: {url}")
        
        try:
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2)
            
            primary_links = []
            items = await page.query_selector_all('.listWithDividers__item, .seriesWork')
            for item in items:
                h3 = await item.query_selector('h3')
                if not h3: continue
                text = await h3.inner_text()
                import re
                if re.search(r'Book\s+\d+\.\d+', text):
                    continue
                    
                a_tag = await item.query_selector('a[href*="/book/show"]')
                if a_tag:
                    link = await a_tag.get_attribute('href')
                    if link and not link.startswith('http'):
                        link = "https://www.goodreads.com" + link
                    if link:
                        primary_links.append(link)
                        
            await page.close()
            
            num_books = len(primary_links)
            if num_books > 0:
                df.at[idx, 'Goodreads Primary Books Number'] = float(num_books)
            else:
                print(f"  -> [{idx}] Failed to find primary books in series.")
                return
                
            print(f"  -> [{idx}] Found {num_books} primary books. Fetching page counts...")
            
            total_pages = 0
            has_missing = False
            
            book_sem = asyncio.Semaphore(4)
            async def fetch_pages(book_url):
                async with book_sem:
                    bp = await context.new_page()
                    try:
                        await bp.goto(book_url, wait_until="domcontentloaded", timeout=45000)
                        import re
                        pages_format = await bp.query_selector('[data-testid="pagesFormat"]')
                        if pages_format:
                            text = await pages_format.inner_text()
                            match = re.search(r'(\d+)\s+pages', text)
                            if match: return int(match.group(1))
                            
                        full_text = await bp.evaluate("() => document.body.innerText")
                        match = re.search(r'(\d+)\s+pages', full_text)
                        if match: return int(match.group(1))
                        return None
                    except Exception:
                        return None
                    finally:
                        await bp.close()
                        
            results = await asyncio.gather(*(fetch_pages(link) for link in primary_links))
            
            for p in results:
                if p is None:
                    has_missing = True
                    break
                total_pages += p
                
            if has_missing:
                print(f"  -> [{idx}] ABORTED SUM: Missing page count for one or more primary books.")
            else:
                df.at[idx, 'Goodreads Primary Books Page Count'] = float(total_pages)
                print(f"  -> [{idx}] Fetched - Goodreads Num Books: {num_books} | EXACT TOTAL Pages: {total_pages}")
                
        except Exception as e:
            print(f"  -> [{idx}] Failed to scrape series: {e}")

async def main():
    print(f"Loading {EXCEL_FILE}...")
    try:
        df = pd.read_excel(EXCEL_FILE)
        
        if 'Goodreads Primary Books Number' not in df.columns:
            df['Goodreads Primary Books Number'] = None
        if 'Goodreads Primary Books Page Count' not in df.columns:
            df['Goodreads Primary Books Page Count'] = None
            
        # Ensure column accepts mixed types to prevent pandas dtype assignment error
        df['Goodreads Primary Books Number'] = df['Goodreads Primary Books Number'].astype(object)
        df['Goodreads Primary Books Page Count'] = df['Goodreads Primary Books Page Count'].astype(object)
            
    except Exception as e:
        print(f"Failed to load Excel file: {e}")
        return
        
    scraper = GoodreadsScraper()
    scraper.headless = False

    async with async_playwright() as p:
        # Launch persistent context to use existing Goodreads cookies
        user_data_dir = os.path.join(r"E:\Internship\PocketFM", "playwright_goodreads_profile")
        context = await p.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            no_viewport=True
        )
        
        # Log into Goodreads first using a dummy page
        login_page = await context.new_page()
        await scraper.login_to_goodreads(login_page)
        await login_page.close()
        
        sem = asyncio.Semaphore(CONCURRENCY)
        
        tasks = []
        found_count = 0
        for idx in range(len(df)):
            row = df.iloc[idx]
            amz_books = row.get('Amazon Num_Primary_Books_in_Series')
            amz_pages = row.get('Amazon Total_Page_Count_of_Primary_Books')
            
            # Convert amz_books to float safely
            amz_val = 0
            if pd.notna(amz_books):
                try:
                    amz_val = float(amz_books)
                except:
                    pass
                    
            # Check if page count is missing or zero
            pages_missing_or_zero = False
            if pd.isna(amz_pages) or str(amz_pages).strip() == '' or str(amz_pages).lower() == 'nan':
                pages_missing_or_zero = True
            else:
                try:
                    if float(amz_pages) == 0:
                        pages_missing_or_zero = True
                except:
                    pass
            
            # Focus precisely on rows with books >= 4
            if amz_val >= 4:
                # SKIP if we already successfully scraped Goodreads Page Count
                gr_pages = row.get('Goodreads Primary Books Page Count')
                gr_pages_missing = True
                if pd.notna(gr_pages) and str(gr_pages).strip() != '' and str(gr_pages).lower() != 'nan':
                    try:
                        if float(gr_pages) > 0:
                            gr_pages_missing = False
                    except:
                        pass
                        
                if gr_pages_missing:
                    tasks.append(process_row(idx, row, df, context, scraper, sem))
                    found_count += 1
                
        print(f"Found {found_count} rows with Amazon books >= 2 needing Goodreads scraping.")
        
        # Process in chunks and save periodically
        chunk_size = 20
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i+chunk_size]
            print(f"Processing chunk {i//chunk_size + 1}...")
            await asyncio.gather(*chunk)
            
            print("Saving partial updates to Excel...")
            try:
                df.to_excel(EXCEL_FILE, index=False)
            except Exception as e:
                print(f"Failed to save to Excel: {e}")
            
        await context.close()
        
    print("\nExtraction complete!")

if __name__ == "__main__":
    asyncio.run(main())
