import asyncio
import pandas as pd
import re
import os
import json
from scraper import AmazonScraper, clean_text
from playwright.async_api import async_playwright

# ==========================================
# UNIVERSAL SCRAPER SETTINGS
# Edit these variables for each new genre!
# ==========================================
SEARCH_URL = "https://www.amazon.com/s?k=kingdom+romance&i=stripbooks&crid=1PGGOQ90W5JP3&sprefix=kingdom+roman%2Cstripbooks%2C380&ref=nb_sb_noss_2"
GENRE_NAME = "Kingdom Romance"
LIMIT = 500
MAX_TABS = 8

safe_genre = GENRE_NAME.replace(" ", "_")
OUTPUT_FILE = rf"E:\Internship\PocketFM\Amazon_Scraping_{safe_genre}.xlsx"
STATE_FILE = OUTPUT_FILE.replace(".xlsx", "_state.json")

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return {"last_page_number": 1, "last_asin": None}

def save_state(page_num, last_asin):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_page_number": page_num, "last_asin": last_asin}, f)

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

async def process_book(context, book_data):
    amazon = AmazonScraper()
    url = book_data.get("Amazon URL")
    
    amz_details = await amazon.scrape_product_details_tab(context, url)
    
    desc = amz_details.get("Description", "")
    publisher = amz_details.get("Publisher", "")
    pub_date = amz_details.get("Publication Date", "")
    pages = amz_details.get("Pages", "")
    amz_title = amz_details.get("Book Title")
    
    is_series = "Yes" if amz_details.get("Series Name") and amz_details.get("Series Name") not in ["N/A", ""] else "No"
    
    # --- BOOK 1 FALLBACK TRIGGER ---
    if is_series == "Yes" and str(amz_details.get("Book Number")) != "1":
        if not publisher or publisher == "N/A" or not pub_date or pub_date == "N/A" or not pages or pages == "N/A" or not desc or desc == "N/A" or len(desc) < 20:
            print(f"    -> Missing details for '{amz_title}'. Triggering Book 1 Fallback...", flush=True)
            book1_details = await amazon.get_book1_details(context, amz_details.get("Series Name"), amz_details.get("Author Name", ""))
            if book1_details:
                if not publisher or publisher == "N/A":
                    amz_details["Publisher"] = book1_details.get("Publisher", "")
                if not pub_date or pub_date == "N/A":
                    amz_details["Publication Date"] = book1_details.get("Publication Date", "")
                if not pages or pages == "N/A":
                    amz_details["Pages"] = book1_details.get("Pages", "")
                if not desc or desc == "N/A" or len(desc) < 20:
                    amz_details["Description"] = book1_details.get("Description", "")
                    
    # Re-evaluate variables after potential fallback
    desc = amz_details.get("Description", "")
    publisher = amz_details.get("Publisher", "")
    pub_date = amz_details.get("Publication Date", "")
    pages = amz_details.get("Pages", "")
    
    price_tier = amz_details.get("Price", "").replace('\n', ' | ') if amz_details.get("Price") not in ["N/A", ""] else ""
    part_of_series_text = f"{amz_details.get('Series Name')} Book {amz_details.get('Book Number')}" if is_series == "Yes" else ""

    final_title = amz_title if (amz_title and amz_title != "N/A") else book_data.get("Book Title", "")

    result = {
        "Book Title": final_title,
        "Author Name": amz_details.get("Author Name", ""),
        "Genre": GENRE_NAME,
        "Sub_Genre": "",
        "Part of a Series?": is_series,
        "Part_of_Series": part_of_series_text,
        "Series Name": amz_details.get("Series Name", ""),
        "Book Number in Series": amz_details.get("Book Number", ""),
        "Number of Books in Series": amz_details.get("Total Books", ""),
        "Publisher": publisher,
        "Publication Date": pub_date,
        "Print Length / Pages": pages,
        "Price_Tier": price_tier,
        "Amazon URL": url,
        "Amazon Stars": amz_details.get("Rating", ""),
        "Amazon Ratings": amz_details.get("Number of Reviews", ""),
        "Number of reviews": amz_details.get("Actual Reviews", ""),
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

async def run_scraper():
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
        
        # Load existing titles to skip them during discovery
        existing_titles = set()
        try:
            old_df = pd.read_excel(OUTPUT_FILE)
            existing_titles = set(old_df['Book Title'].dropna().tolist())
        except:
            pass
            
        state = load_state()
        page_num = state["last_page_number"]
        last_asin = state["last_asin"]
        
        # 2. Discovery Phase
        if page_num > 1:
            if "&page=" in SEARCH_URL:
                target_url = re.sub(r'&page=\d+', f'&page={page_num}', SEARCH_URL)
            else:
                target_url = SEARCH_URL + f"&page={page_num}"
        else:
            target_url = SEARCH_URL
            
        print(f"\nNavigating to Search URL: {target_url}", flush=True)
        await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
        await check_captcha(page)
        
        discovery_links = []
        
        skipping = True if last_asin else False
        if skipping:
            print(f"Skipping items until we find ASIN: {last_asin}", flush=True)
        
        while len(discovery_links) < LIMIT:
            print(f"Scanning Page {page_num}...", flush=True)
            # Scroll to load thumbnails
            for i in range(5):
                await page.evaluate("window.scrollBy(0, 1500)")
                await asyncio.sleep(1)
                
            items = await page.query_selector_all("div[data-asin]")
            for item in items:
                asin = await item.get_attribute('data-asin')
                if not asin or len(asin) < 5: continue
                
                if skipping:
                    if asin == last_asin:
                        print("    -> Found last ASIN! Resuming collection from next item...", flush=True)
                        skipping = False
                    continue
                
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
                
                # Skip if we already scraped this title!
                if title in existing_titles:
                    continue
                
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
                if len(discovery_links) >= LIMIT: 
                    save_state(page_num, discovery_links[-1]["asin"])
                    break
            
            print(f" -> Discovered {len(discovery_links)}/{LIMIT} links so far.", flush=True)
            
            # Check if page is empty (end of line)
            items_check = await page.query_selector_all("div[data-asin]")
            if not items_check:
                print("No items found on this page. Reached the absolute end of Amazon results.", flush=True)
                break
                
            if page_num >= 110:
                print(f"Reached maximum page limit of 110. Forcefully stopping discovery and proceeding to extraction.", flush=True)
                break
            
            if len(discovery_links) < LIMIT:
                try:
                    if skipping:
                        print("    -> Warning: Did not find exact ASIN on this page. Forcefully unlocking collection for next page.", flush=True)
                        skipping = False
                        
                    if discovery_links:
                        save_state(page_num, discovery_links[-1]["asin"])
                    
                    page_num += 1
                    print(f"Bypassing UI and jumping straight to Page {page_num}...", flush=True)
                    
                    if "&page=" in SEARCH_URL:
                        next_url = re.sub(r'&page=\d+', f'&page={page_num}', SEARCH_URL)
                    else:
                        next_url = SEARCH_URL + f"&page={page_num}"
                        
                    await page.goto(next_url, wait_until="domcontentloaded", timeout=60000)
                    await asyncio.sleep(3)
                    await check_captcha(page)
                except Exception as e:
                    print(f"Failed to navigate to next page: {e}", flush=True)
                    break
                    
        await page.close()
        
        print("\n--- Starting Deep Extraction ---", flush=True)
        final_rows = []
        for i in range(0, len(discovery_links), MAX_TABS):
            batch = discovery_links[i : i + MAX_TABS]
            print(f"Processing Batch {i//MAX_TABS + 1} ({len(batch)} books concurrently)...", flush=True)
            tasks = [asyncio.wait_for(process_book(context, book), timeout=120) for book in batch]
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
                old_df = pd.read_excel(OUTPUT_FILE)
                combined = pd.concat([old_df, df], ignore_index=True)
                combined = combined.drop_duplicates(subset=["Book Title"], keep="last")
            except:
                combined = df
            combined.to_excel(OUTPUT_FILE, index=False)
            print(f"\nSaved {len(final_rows)} books to {OUTPUT_FILE}!", flush=True)
            
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
    ensure_excel(OUTPUT_FILE)
    asyncio.run(run_scraper())
