import asyncio
import pandas as pd
import argparse
import os
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

async def process_book(context, book_data, genre_name):
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
        "Book Title": final_title,
        "Author Name": amz_details.get("Author Name", ""),
        "Genre": genre_name,
        "Sub_Genre": "",
        "Part of a Series?": is_series,
        "Part_of_Series": part_of_series_text,
        "Series Name": amz_details.get("Series Name", ""),
        "Book Number in Series": amz_details.get("Book Number", ""),
        "Number of Books in Series": amz_details.get("Total Books", ""),
        "Publisher": amz_details.get("Publisher", ""),
        "Publication Date": amz_details.get("Publication Date", ""),
        "Print Length / Pages": amz_details.get("Pages", ""),
        "Price_Tier": price_tier,
        "Amazon URL": url,
        "Amazon Stars": amz_details.get("Rating", ""),
        "Amazon Ratings": amz_details.get("Number of Reviews", ""),
        "Logline": desc[:1500] if desc != "N/A" else "",
        "Best Sellers Rank": amz_details.get("Inner Rank", ""),
        "GoodReads_Series_URL": "",
        "Num_Primary_Books": "",
        "Total_Pages_Primary_Books": "",
        "Book1_Rating": "",
        "Book1_Num_Ratings": "",
        "Licensing Status": "Available"
    }
    
    for k, v in result.items():
        if v == "N/A":
            result[k] = ""
            
    return result

async def run_scraper(search_url, genre_name, limit, output_file):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        page = await context.new_page()
        
        # 1. Location Sync
        print("Navigating to Amazon and syncing US location...", flush=True)
        await page.goto("https://www.amazon.com", wait_until="domcontentloaded", timeout=60000)
        amazon_scraper = AmazonScraper()
        await amazon_scraper.set_amazon_location(page, "90016")
        
        # 2. Discovery Phase
        print(f"\nNavigating to Search URL: {search_url}", flush=True)
        await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
        await check_captcha(page)
        
        discovery_links = []
        page_num = 1
        
        while len(discovery_links) < limit:
            print(f"Scanning Page {page_num}...", flush=True)
            for i in range(5):
                await page.evaluate("window.scrollBy(0, 1500)")
                await asyncio.sleep(1)
                
            items = await page.query_selector_all("div[data-asin]")
            for item in items:
                asin = await item.get_attribute('data-asin')
                if not asin or len(asin) < 5: continue
                if any(x['asin'] == asin for x in discovery_links): continue
                
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
                if len(discovery_links) >= limit: break
            
            print(f" -> Discovered {len(discovery_links)}/{limit} links so far.", flush=True)
            
            if len(discovery_links) < limit:
                try:
                    next_btn = await page.query_selector('a.s-pagination-next')
                    if next_btn and await next_btn.is_visible():
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
        
        print("\n--- Starting Deep Extraction ---", flush=True)
        final_rows = []
        for i in range(0, len(discovery_links), MAX_TABS):
            batch = discovery_links[i : i + MAX_TABS]
            print(f"Processing Batch {i//MAX_TABS + 1} ({len(batch)} books concurrently)...", flush=True)
            tasks = [asyncio.wait_for(process_book(context, book, genre_name), timeout=120) for book in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, dict):
                    final_rows.append(res)
                else:
                    print(f"Error in task: {res}", flush=True)
        
        # Save to excel
        if final_rows:
            df = pd.DataFrame(final_rows)
            try:
                old_df = pd.read_excel(output_file)
                combined = pd.concat([old_df, df], ignore_index=True)
            except:
                combined = df
            combined.to_excel(output_file, index=False)
            print(f"\nSaved {len(final_rows)} books to {output_file}!", flush=True)
            
        await browser.close()

def ensure_excel(output_file):
    if not os.path.exists(output_file):
        columns = [
            "Book Title", "Author Name", "Genre", "Sub_Genre", "Part of a Series?", 
            "Part_of_Series", "Series Name", "Book Number in Series", "Number of Books in Series",
            "Publisher", "Publication Date", "Print Length / Pages", "Price_Tier", "Amazon URL",
            "Amazon Stars", "Amazon Ratings", "Logline", "Best Sellers Rank", "GoodReads_Series_URL",
            "Num_Primary_Books", "Total_Pages_Primary_Books", "Book1_Rating", "Book1_Num_Ratings",
            "Licensing Status"
        ]
        df = pd.DataFrame(columns=columns)
        df.to_excel(output_file, index=False)
        print(f"Created fresh Excel sheet: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Universal Amazon Book Scraper")
    parser.add_argument("--url", type=str, help="Amazon search URL")
    parser.add_argument("--genre", type=str, help="Genre Name (e.g. Dark Romance)")
    parser.add_argument("--limit", type=int, help="Number of books to scrape", default=20)
    args = parser.parse_args()

    search_url = args.url
    genre_name = args.genre
    limit = args.limit

    if not search_url:
        search_url = input("Enter Amazon Search URL: ").strip()
    if not genre_name:
        genre_name = input("Enter Genre Name (e.g. Fae Court Romance): ").strip()
    if not args.limit:
        try:
            limit_input = input(f"Enter number of titles to scrape [default: 20]: ").strip()
            limit = int(limit_input) if limit_input else 20
        except:
            limit = 20

    # Auto-format the Excel filename
    safe_genre = genre_name.replace(" ", "_")
    output_file = rf"E:\Internship\PocketFM\Amazon_Scraping_{safe_genre}.xlsx"

    ensure_excel(output_file)
    asyncio.run(run_scraper(search_url, genre_name, limit, output_file))
