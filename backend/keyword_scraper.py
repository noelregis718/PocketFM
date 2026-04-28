import asyncio
import json
import os
import pandas as pd
import re
from scraper import AmazonScraper, GoodreadsScraper, AuthorScraper, clean_text, normalize_title_for_search
from excel_utility import save_to_excel
from playwright.async_api import async_playwright

# Configuration
STATE_FILE = r"e:\Internship\PocketFM\backend\keyword_state.json"
OUTPUT_FILE = r"E:\Internship\PocketFM\Amazon Keyword - Werewolves & Shifters.xlsx"
BATCH_SIZE = 50
MAX_TABS = 8
SEARCH_URL = "https://www.amazon.com/s?k=Werewolves+%26+Shifters&i=stripbooks&crid=1VFS1NEXMRVWD&sprefix=werewolves+%26+shifters%2Cstripbooks%2C432&ref=nb_sb_noss_1"

COLUMNS = [
    "Sub_Genre", "Price_Tier", "Amazon URL", "Book Title", "Book Number in Series",
    "Series Name", "Author Name", "Amazon Stars", "Amazon Ratings", "Number of Books in Series",
    "Genre", "Publisher", "Publication Date", "Print Length / Pages", "Best Sellers Rank",
    "Licensing Status", "Part of a Series?", "Part_of_Series", "GoodReads_Series_URL",
    "Num_Primary_Books", "Total_Pages_Primary_Books", "Book1_Rating", "Book1_Num_Ratings",
    "Logline", "One_Sentence_Logline", "Romantasy_Subgenre", "Author_Email", "Agent_Email",
    "Facebook", "Twitter", "Instagram", "Website", "Other_Contact"
]

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {
        "last_page_scanned": 0,
        "last_book_title": "N/A",
        "total_processed_global": 0,
        "next_batch_start": 1
    }

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

async def process_book(context, book_data):
    """Full 33-column extraction for a single book (Amazon + Goodreads + Author)."""
    amazon = AmazonScraper()
    goodreads = GoodreadsScraper()
    author_scr = AuthorScraper()

    url = book_data.get("Amazon URL")
    discovery_title = book_data.get("Book Title", "N/A")
    asin = book_data.get("asin", "N/A")
    
    # 1. Amazon Details
    amz_details = await amazon.scrape_product_details_tab(context, url)
    
    # --- USD HEARTBEAT CHECK ---
    price_raw = amz_details.get("Price", "N/A")
    if "INR" in price_raw or "₹" in price_raw or "\u20b9" in price_raw or "Rs" in price_raw:
        print(f"    [Heartbeat] Non-USD detected ({price_raw[:15]}). Forcing Location Sync and retrying...")
        try:
            temp_page = await context.new_page()
            await temp_page.goto("https://www.amazon.com", wait_until="domcontentloaded", timeout=45000)
            await amazon.set_amazon_location(temp_page, "90016")
            for _ in range(3):
                await asyncio.sleep(2)
                loc_text = await temp_page.evaluate("() => { const el = document.querySelector('#glow-ingress-line2'); return el ? el.innerText : ''; }")
                if "90016" in loc_text or "Los Angeles" in loc_text:
                    break
                await amazon.set_amazon_location(temp_page, "90016")
            await temp_page.close()
            # Retry extraction
            amz_details = await amazon.scrape_product_details_tab(context, url)
        except Exception as e:
            print(f"    [Heartbeat] Failed to fix location: {e}")
    
    # Final Title for Goodreads search
    actual_title = amz_details.get("Book Title") if (amz_details.get("Book Title") and amz_details.get("Book Title") != "N/A") else discovery_title
    author_name = amz_details.get("Author Name", "N/A")
    print(f"  [Task] Processing: {actual_title[:40]}... (ASIN: {asin})", flush=True)

    # 2. Goodreads Details (INTEGRATED SCAN)
    if actual_title and actual_title != "N/A":
        print(f"    [Goodreads] Initiating search for '{actual_title[:25]}' by '{author_name}'...")
        gr_details = await goodreads.scrape_goodreads_data(
            context, 
            actual_title, 
            author_name, 
            asin=asin 
        )
        if gr_details.get("GoodReads_Book_URL"):
            print(f"    [Goodreads] Found: {gr_details.get('GoodReads_Rating')} stars | {gr_details.get('GoodReads_Book_URL')[:35]}...")
        else:
            print(f"    [Goodreads] Warning: No match found.")
    else:
        print(f"    [Goodreads] Error: Cannot search without a title.")
        gr_details = {}

    # 3. Author Details
    ath_details = await author_scr.find_author_details(context, author_name)

    # Mapping to 33 Columns
    desc = amz_details.get("Description", "N/A")
    one_sentence = desc.split('.')[0] + '.' if desc != "N/A" else "N/A"
    
    # Logic for Price Tier
    price_raw = amz_details.get("Price", "N/A")
    price_tier = price_raw.replace('\n', ' | ') if price_raw != "N/A" else "N/A"

    # Logic for Part of Series
    is_series = "Yes" if amz_details.get("Series Name") and amz_details.get("Series Name") != "N/A" else "No"
    part_of_series_text = f"{amz_details.get('Series Name')} Book {amz_details.get('Book Number')}" if is_series == "Yes" else "N/A"

    return {
        "Sub_Genre": gr_details.get("Sub_Genre", amz_details.get("Sub_Genre_Candidate", "N/A")),
        "Price_Tier": price_tier,
        "Amazon URL": url,
        "Book Title": actual_title,
        "Book Number in Series": amz_details.get("Book Number", "N/A"),
        "Series Name": amz_details.get("Series Name", "N/A"),
        "Author Name": author_name,
        "Amazon Stars": amz_details.get("Rating", "N/A"),
        "Amazon Ratings": amz_details.get("Number of Reviews", "N/A"),
        "Number of Books in Series": amz_details.get("Total Books", "N/A"),
        "Genre": gr_details.get("Genre", "N/A"),
        "Publisher": amz_details.get("Publisher", "N/A"),
        "Publication Date": amz_details.get("Publication Date", "N/A"),
        "Print Length / Pages": amz_details.get("Pages", "N/A"),
        "Best Sellers Rank": amz_details.get("Inner Rank", "N/A"),
        "Licensing Status": "Available",
        "Part of a Series?": is_series,
        "Part_of_Series": part_of_series_text,
        "GoodReads_Series_URL": gr_details.get("GoodReads_Series_URL", "N/A"),
        "Num_Primary_Books": gr_details.get("Num_Primary_Books", "N/A"),
        "Total_Pages_Primary_Books": gr_details.get("Total_Pages_Primary_Books", "N/A"),
        "Book1_Rating": gr_details.get("Book1_Rating", "N/A"),
        "Book1_Num_Ratings": gr_details.get("Book1_Num_Ratings", "N/A"),
        "Logline": desc[:1500] if desc != "N/A" else "N/A", 
        "One_Sentence_Logline": one_sentence,
        "Romantasy_Subgenre": gr_details.get("Romantasy_Subgenre", "No"),
        "Author_Email": ath_details.get("Author_Email", "N/A"),
        "Agent_Email": ath_details.get("Agent_Email", "N/A"),
        "Facebook": ath_details.get("Facebook", "N/A"),
        "Twitter": ath_details.get("Twitter", "N/A"),
        "Instagram": ath_details.get("Instagram", "N/A"),
        "Website": ath_details.get("Website", "N/A"),
        "Other_Contact": ath_details.get("Other_Contact", "N/A")
    }

LOCK_FILE = r"e:\Internship\PocketFM\keyword_scraper.lock"

async def _run_keyword_mission_core():
    state = load_state()
    # MISSION TARGET: Scale by another 500 titles
    MISSION_TARGET = state['total_processed_global'] + 50
    
    print(f"\n{'='*60}", flush=True)
    print(f"INDUSTRIAL SCALING MISSION: Target {MISSION_TARGET} Titles", flush=True)
    print(f"Current Progress: {state['total_processed_global']} | Starting from Page {state['last_page_scanned'] + 1}", flush=True)
    print(f"{'='*60}\n", flush=True)

    # --- GLOBAL ASIN PROTECTION ---
    global_seen_asins = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            existing_df = pd.read_excel(OUTPUT_FILE)
            if 'Amazon URL' in existing_df.columns:
                # Extract ASINs from URLs
                for url in existing_df['Amazon URL'].dropna():
                    match = re.search(r'/(?:dp|product|gp/product)/([A-Z0-9]{10})', str(url))
                    if match: global_seen_asins.add(match.group(1))
            print(f"  [Protection] Loaded {len(global_seen_asins)} existing ASINs to prevent duplicates.")
        except Exception as e:
            print(f"  [Protection] Warning: Could not load existing data: {e}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        print("  [System] Browser launched in Stable mode.")
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/Los_Angeles"
        )
        
        # Process only one batch for this run
        while state['total_processed_global'] < MISSION_TARGET:
            curr_batch_start = state['next_batch_start']
            print(f"\n>>> [MISSION BATCH] Processing {curr_batch_start} to {curr_batch_start + BATCH_SIZE - 1}...")
            
            page = await context.new_page()
            
            # --- Discovery Phase ---
            print(f"  Navigating to Amazon Search (Page {state['last_page_scanned'] + 1})...")
            search_url = SEARCH_URL
            try:
                # [FIXED] Navigate to base search URL first to set location.
                print(f"  Navigating directly to base Amazon Search to set location...")
                await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=60000)
                
                amazon_scraper = AmazonScraper()
                await amazon_scraper.set_amazon_location(page, "90016")
                
                # VERIFICATION LOOP
                location_verified = False
                for _ in range(3):
                    await asyncio.sleep(2)
                    loc_text = await page.evaluate("() => { const el = document.querySelector('#glow-ingress-line2'); return el ? el.innerText : ''; }")
                    if "90016" in loc_text or "Los Angeles" in loc_text:
                        location_verified = True
                        break
                    print("    [Location] Verification failed, retrying...")
                    await amazon_scraper.set_amazon_location(page, "90016")
                
                if not location_verified:
                    print("  [Warning] Could not verify US location, but proceeding anyway.")

                # NOW navigate to the target page to ensure it doesn't drop pagination
                target_page = state['last_page_scanned'] + 1
                if target_page > 1:
                    # Robustly replace or append page parameters
                    if "page=" in search_url:
                        search_url = re.sub(r'page=\d+', f'page={target_page}', search_url)
                    else:
                        search_url += f"&page={target_page}"
                    
                    if "ref=sr_pg_" in search_url:
                        search_url = re.sub(r'ref=sr_pg_\d+', f'ref=sr_pg_{target_page}', search_url)
                    else:
                        search_url += f"&ref=sr_pg_{target_page}"
                        
                    print(f"  Navigating directly to Target Amazon Search (Page {target_page})...")
                    await page.goto(search_url, wait_until="load", timeout=60000)
                else:
                    print(f"  Navigating to base Amazon Search...")
                    await page.goto(SEARCH_URL, wait_until="load", timeout=60000)
                
                all_discovery_links = []
                page_count = state['last_page_scanned'] + 1
                seen_titles = [] 
                consecutive_empty_pages = 0
                
                mission_aborted_end_of_results = False
                while len(all_discovery_links) < BATCH_SIZE and consecutive_empty_pages < 3:
                    print(f"\n  [Discovery] Scanning Page {page_count} for new titles...")
                    
                    # Check for "No results" message
                    no_results_text = await page.evaluate("""() => {
                        const text = document.body.innerText;
                        return text.includes("No results for") || 
                               text.includes("Try checking your spelling") ||
                               text.includes("did not match any products") ||
                               text.includes("No more results");
                    }""")
                    if no_results_text:
                        print(f"    [Discovery] End of results detected on Page {page_count}. Stopping mission.")
                        mission_aborted_end_of_results = True
                        break

                    for i in range(6):
                        print(f"    [Discovery] Scrolling... ({i+1}/6)")
                        await page.evaluate("window.scrollBy(0, 1500)")
                        await asyncio.sleep(2.0)
                    
                    items = await page.query_selector_all("[data-asin]")
                    found_this_page = 0
                    
                    for item in items:
                        asin = await item.get_attribute('data-asin')
                        if not asin or asin == "N/A" or len(asin) < 5: continue
                        if asin in global_seen_asins: continue # Global Duplicate Protection
                        if any(x.get("asin") == asin for x in all_discovery_links): continue

                        title = "N/A"
                        title_selectors = ["h2 a span", ".a-size-medium", ".a-size-base-plus", "h2 a"]
                        for t_sel in title_selectors:
                            try:
                                t_el = await item.query_selector(t_sel)
                                if t_el:
                                    title = clean_text(await t_el.inner_text())
                                    if title and title != "N/A": break
                            except: continue
                        
                        clean_title = normalize_title_for_search(title)
                        if clean_title in seen_titles: continue

                        href = None
                        link_selectors = ["h2 a", "a.a-link-normal[href*='/dp/']"]
                        for l_sel in link_selectors:
                            try:
                                l_el = await item.query_selector(l_sel)
                                if l_el:
                                    href = await l_el.evaluate("el => el.href")
                                    if href and "/dp/" in href: break
                            except: continue

                        if href and title != "N/A":
                            all_discovery_links.append({"asin": asin, "Amazon URL": href, "Book Title": title})
                            seen_titles.append(clean_title)
                            found_this_page += 1
                        if len(all_discovery_links) >= BATCH_SIZE: break
                    
                    print(f"    -> Captured {found_this_page} titles on Page {page_count}. (Total Discovery: {len(all_discovery_links)})", flush=True)

                    if found_this_page == 0:
                        consecutive_empty_pages += 1
                        print(f"    [Warning] Page {page_count} had no new titles. ({consecutive_empty_pages}/3 empty pages)")
                    else:
                        consecutive_empty_pages = 0

                    if len(all_discovery_links) < BATCH_SIZE and consecutive_empty_pages < 3:
                        # Improved Pagination Logic
                        page_count += 1
                        print(f"    [Pagination] Attempting move to Page {page_count}...")
                        
                        # Try direct URL navigation as it is more reliable for Amazon than clicking Next
                        if "page=" in search_url:
                            search_url = re.sub(r'page=\d+', f'page={page_count}', search_url)
                        else:
                            search_url += f"&page={page_count}"
                        
                        if "ref=sr_pg_" in search_url:
                            search_url = re.sub(r'ref=sr_pg_\d+', f'ref=sr_pg_{page_count}', search_url)
                        else:
                            search_url += f"&ref=sr_pg_{page_count}"
                        
                        try:
                            print(f"    [Pagination] Navigating to {search_url[:60]}...")
                            await page.goto(search_url, wait_until="load", timeout=60000)
                            # Update state immediately so we don't loop on the same page if a retry happens
                            state['last_page_scanned'] = page_count - 1
                            save_state(state)
                            await asyncio.sleep(5)
                        except Exception as e:
                            print(f"    [Pagination] Direct navigation failed: {e}. Falling back to Next button click.")
                            next_btn = None
                            pagination_selectors = ['a.s-pagination-next', 'li.a-last a', 'a:has-text("Next")']
                            for p_sel in pagination_selectors:
                                try:
                                    next_btn = await page.query_selector(p_sel)
                                    if next_btn: break
                                except: continue
                            
                            if next_btn:
                                await next_btn.click()
                                state['last_page_scanned'] = page_count - 1
                                save_state(state)
                                await asyncio.sleep(8)
                            else:
                                print(f"    [Pagination] Critical: Next button not found and navigation failed. Stopping discovery.")
                                break
                
                # --- Extraction Phase ---
                final_rows = []
                for i in range(0, len(all_discovery_links), MAX_TABS):
                    batch = all_discovery_links[i : i + MAX_TABS]
                    print(f"  Batch {i//MAX_TABS + 1}: Handling {len(batch)} titles...")
                    # --- ANTI-STALL: 120s Global Timeout per Book ---
                    tasks = [asyncio.wait_for(process_book(context, book), timeout=120) for book in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    valid = [res for res in results if isinstance(res, dict)]
                    final_rows.extend(valid)
                    save_to_excel(valid, OUTPUT_FILE)

                # --- State Sync ---
                state['last_page_scanned'] = page_count
                state['total_processed_global'] += len(final_rows)
                state['next_batch_start'] += len(final_rows)
                if final_rows:
                    state['last_book_title'] = final_rows[-1]['Book Title']
                save_state(state)
                
                if mission_aborted_end_of_results or (not final_rows and consecutive_empty_pages >= 3):
                    if mission_aborted_end_of_results:
                        print(f"\n[Terminating] Reached the end of Amazon results. No more books to scrape.")
                        break
                    else:
                        print(f"\n[Notice] Hit a wall of 3 empty pages. Advancing starting page to {page_count} and continuing...")
                        state['last_page_scanned'] = page_count
                        save_state(state)
                
                print(f"\n[OK] Batch Complete. Total Processed: {state['total_processed_global']}/{MISSION_TARGET}")
                
                # --- AUTO-OPEN (USER REQUESTED: Per Batch) ---
                if os.name == 'nt' and os.path.exists(OUTPUT_FILE):
                    print(f"  [Mission] Opening updated results for index {curr_batch_start}-{curr_batch_start+BATCH_SIZE-1}...")
                    os.startfile(os.path.abspath(OUTPUT_FILE))
                
            except Exception as e:
                print(f"[CRITICAL ERROR] Batch failed: {e}. Retrying after short nap...")
                await asyncio.sleep(5)
            
            await page.close()
            
            # Continue until MISSION_TARGET is reached
            if state['total_processed_global'] >= MISSION_TARGET:
                print("\n[OK] Mission target reached.")
                break

        await browser.close()
        print(f"\n{'='*60}")
        print(f"MISSION ACCOMPLISHED: {state['total_processed_global']} Titles Total.")
        print(f"Final Data: {os.path.abspath(OUTPUT_FILE)}")
        print(f"{'='*60}\n")
        
        if os.name == 'nt' and os.path.exists(OUTPUT_FILE):
            os.startfile(os.path.abspath(OUTPUT_FILE))

async def run_keyword_mission():
    if os.path.exists(LOCK_FILE):
        print(f"[ERROR] Scraper is already running (lock file {LOCK_FILE} exists).")
        return
    with open(LOCK_FILE, 'w') as f:
        f.write("locked")
    try:
        await _run_keyword_mission_core()
    finally:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)

if __name__ == "__main__":
    asyncio.run(run_keyword_mission())
