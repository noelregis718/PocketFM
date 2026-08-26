import asyncio
import pandas as pd
import argparse
import os
import sys

# Ensure backend modules can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))
from scraper import AmazonScraper, clean_text
from playwright.async_api import async_playwright

MAX_TABS = 8

async def check_captcha(page):
    while True:
        try:
            if await page.query_selector('form[action="/errors/validateCaptcha"], input#captchacharacters'):
                print("🚨 CAPTCHA DETECTED! Please solve it in the browser window...", flush=True)
                await asyncio.sleep(5)
            else:
                break
        except:
            break

async def process_book(context, book_data, row_metadata):
    amazon = AmazonScraper()
    url = book_data.get("Amazon URL")
    
    amz_details = await amazon.scrape_product_details_tab(context, url)
    
    desc = amz_details.get("Description", "")
    price_tier = amz_details.get("Price", "").replace('\n', ' | ') if amz_details.get("Price") not in ["N/A", ""] else ""
    is_series = "Yes" if amz_details.get("Series Name") and amz_details.get("Series Name") not in ["N/A", ""] else "No"
    part_of_series_text = f"{amz_details.get('Series Name')} Book {amz_details.get('Book Number')}" if is_series == "Yes" else ""

    amz_title = amz_details.get("Book Title")
    final_title = amz_title if (amz_title and amz_title != "N/A") else book_data.get("Book Title", "")

    result = {
        "Genre": row_metadata.get("Genre", ""),
        "Sub-Genre": row_metadata.get("Sub-Genre", ""),
        "Sub-Sub-Genre": row_metadata.get("Sub-Sub-Genre", ""),
        "Book Title": final_title,
        "Series": amz_details.get("Series Name", ""),
        "Author": amz_details.get("Author Name", ""),
        "Amazon URL": url,
        "Browse Category": row_metadata.get("Browse Node ID", ""),
        "ASIN": book_data.get("asin", ""),
        "Books in Series": amz_details.get("Total Books", ""),
        "Product Description": desc[:1500] if desc != "N/A" else "",
        "Publisher": amz_details.get("Publisher", ""),
        "Star Rating": amz_details.get("Rating", ""),
        "Ratings Count": amz_details.get("Number of Reviews", ""),
    }
    
    for k, v in result.items():
        if v == "N/A":
            result[k] = ""
            
    return result

async def scrape_category(p, context, row, known_asins, output_file):
    search_url = row.get('URL')
    if pd.isna(search_url) or not search_url:
        return
        
    print(f"\n--- Scraping Category: {row.get('Sub-Sub-Genre', 'Unknown')} ---", flush=True)
    
    page = await context.new_page()
    await page.goto("https://www.amazon.com", wait_until="domcontentloaded", timeout=60000)
    amazon_scraper = AmazonScraper()
    await amazon_scraper.set_amazon_location(page, "90016")
    
    print(f"Navigating to URL: {search_url}", flush=True)
    await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    await check_captcha(page)
    
    discovery_links = []
    page_num = 1
    
    # Scrape first 3 pages of the category
    while page_num <= 3:
        print(f"Scanning Page {page_num}...", flush=True)
        for _ in range(5):
            await page.evaluate("window.scrollBy(0, 1500)")
            await asyncio.sleep(1)
            
        items = await page.query_selector_all("div[data-asin]")
        for item in items:
            asin = await item.get_attribute('data-asin')
            if not asin or len(asin) < 5: continue
            
            # DEDUPLICATION CHECK
            if asin in known_asins:
                continue
                
            if any(x['asin'] == asin for x in discovery_links): 
                continue
            
            title = "N/A"
            for sel in ["h2 a span", ".a-size-medium", ".a-size-base-plus"]:
                try:
                    t_el = await item.query_selector(sel)
                    if t_el:
                        t = clean_text(await t_el.inner_text())
                        if t: 
                            title = t
                            break
                except: pass
            
            href = None
            for l_sel in ["h2 a", "a.a-link-normal[href*='/dp/']"]:
                try:
                    l_el = await item.query_selector(l_sel)
                    if l_el:
                        h = await l_el.evaluate("el => el.href")
                        if h and "/dp/" in h: 
                            href = h
                            break
                except: pass
            
            if href and title != "N/A":
                discovery_links.append({"asin": asin, "Amazon URL": href, "Book Title": title})
        
        try:
            next_btn = await page.query_selector('a.s-pagination-next')
            if next_btn and await next_btn.is_visible() and not await next_btn.get_attribute('aria-disabled'):
                print("Clicking Next Page...", flush=True)
                await next_btn.click()
                await asyncio.sleep(3)
                await check_captcha(page)
                page_num += 1
            else:
                print("No more pages found or Next button hidden.", flush=True)
                break
        except:
            print("Failed to navigate to next page.", flush=True)
            break
            
    await page.close()
    
    print(f"Discovered {len(discovery_links)} NEW books (not in database)!", flush=True)
    if not discovery_links:
        return
        
    print("\n--- Starting Deep Extraction ---", flush=True)
    final_rows = []
    for i in range(0, len(discovery_links), MAX_TABS):
        batch = discovery_links[i : i + MAX_TABS]
        print(f"Processing Batch {i//MAX_TABS + 1} ({len(batch)} books concurrently)...", flush=True)
        tasks = [asyncio.wait_for(process_book(context, book, row), timeout=120) for book in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, dict):
                final_rows.append(res)
                known_asins.add(res["ASIN"])
            else:
                print(f"Error in task: {res}", flush=True)
    
    if final_rows:
        df = pd.DataFrame(final_rows)
        # Reorder to match main sheet if necessary
        cols = ['Genre', 'Sub-Genre', 'Sub-Sub-Genre', 'Book Title', 'Series', 'Author', 'Amazon URL', 'Browse Category', 'ASIN', 'Books in Series', 'Product Description', 'Publisher', 'Star Rating', 'Ratings Count']
        df = df.reindex(columns=cols)
        
        # Append to new_scraped_books.csv
        file_exists = os.path.isfile(output_file)
        df.to_csv(output_file, mode='a', index=False, header=not file_exists)
        print(f"\nSaved {len(final_rows)} books to {output_file}!", flush=True)

async def main(args):
    # 1. Build Deduplication Set
    known_asins = set()
    master_file = 'consolidated_books_scraped.csv'
    if os.path.exists(master_file):
        print(f"Loading master database '{master_file}' for deduplication...", flush=True)
        try:
            df_master = pd.read_csv(master_file, usecols=['ASIN'])
            known_asins = set(df_master['ASIN'].dropna().astype(str).tolist())
            print(f"Loaded {len(known_asins)} known ASINs.", flush=True)
        except Exception as e:
            print(f"Error loading master database: {e}", flush=True)
            
    # Load previously scraped books from output file too
    output_file = 'new_scraped_books.csv'
    if os.path.exists(output_file):
        try:
            df_existing = pd.read_csv(output_file, usecols=['ASIN'])
            known_asins.update(df_existing['ASIN'].dropna().astype(str).tolist())
        except:
            pass

    # 2. Read Target Categories
    print(f"Loading target categories from '{args.input_csv}'...", flush=True)
    try:
        categories_df = pd.read_csv(args.input_csv)
    except Exception as e:
        print(f"Failed to read input CSV: {e}")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        # 3. Process Categories
        for index, row in categories_df.iterrows():
            if pd.isna(row.get('URL')): continue
            print(f"\n=== Processing Category {index + 1}/{len(categories_df)} ===")
            await scrape_category(p, context, row, known_asins, output_file)
            
        await browser.close()
        
    print("\n--- ALL CATEGORIES PROCESSED ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Amazon Category Book Scraper")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to input categories CSV")
    args = parser.parse_args()
    
    asyncio.run(main(args))
