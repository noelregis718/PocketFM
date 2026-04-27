import asyncio
import os
import sys
import pandas as pd
from playwright.async_api import async_playwright

# Add parent dir to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper import AmazonScraper

async def find_resume_point():
    TARGET_COUNT = 1084  # Number of books in the Excel
    SEARCH_URL = "https://www.amazon.com/s?k=Romantasy&i=stripbooks&crid=3JGEQS9FA3ITT&sprefix=romantasy%2Cstripbooks%2C376&ref=nb_sb_noss_1"
    
    print(f"============================================================")
    print(f"RESUME POINT DISCOVERY MISSION")
    print(f"Target Count: {TARGET_COUNT} books")
    print(f"Goal: Find the exact page where book #{TARGET_COUNT} is located.")
    print(f"============================================================\n")

    async with async_playwright() as p:
        # Using non-headless to ensure stability and user visibility
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = await context.new_page()
        
        amz = AmazonScraper()
        
        # 1. Set Location (90016)
        print("[System] Setting US Location to ensure correct results...")
        try:
            await page.goto("https://www.amazon.com", timeout=60000)
            await amz.set_amazon_location(page, "90016")
            print("[System] Location synced to Zip 90016.")
        except Exception as e:
            print(f"[Warning] Location sync failed: {e}. Continuing...")
        
        total_found_global = 0
        
        # 2. Iterate and Count
        for p_num in range(1, 401):
            url = f"{SEARCH_URL}&page={p_num}"
            print(f"\n[Page {p_num}] Navigating to search results...")
            
            try:
                await page.goto(url, wait_until="load", timeout=90000)
                # Wait for results to settle
                await asyncio.sleep(4)
                
                # Fast Scroll to reveal all lazy-loaded books
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)

                # Find all book items with ASINs
                items = await page.query_selector_all('[data-asin]')
                
                # Filter out empty/ad items
                asins_on_page = []
                for item in items:
                    asin = await item.get_attribute('data-asin')
                    if asin and asin != 'N/A' and len(asin) == 10:
                        asins_on_page.append(asin)
                
                page_count = len(asins_on_page)
                total_found_global += page_count
                
                print(f"  -> Found {page_count} books on Page {p_num}")
                print(f"  -> Cumulative Count: {total_found_global} / {TARGET_COUNT}")

                if total_found_global >= TARGET_COUNT:
                    print(f"\n" + "!"*60)
                    print(f"MISSION ACCOMPLISHED!")
                    print(f"The {TARGET_COUNT}th book is located on Page {p_num}.")
                    print(f"Recommended Resume Page: {p_num}")
                    print("!"*60)
                    
                    # Update the state file automatically
                    state = {
                        "last_page_scanned": p_num,
                        "last_book_title": "N/A (Found via Book Finder)",
                        "total_processed_global": TARGET_COUNT,
                        "next_batch_start": TARGET_COUNT + 1
                    }
                    with open("keyword_state.json", "w") as f:
                        import json
                        json.dump(state, f, indent=4)
                    print(f"\n[State] Automatically updated 'keyword_state.json' to resume from Page {p_num}.")
                    break
                    
                if page_count == 0:
                    print(f"  [End] Reached end of results at Page {p_num} before hitting target.")
                    break
                    
            except Exception as e:
                print(f"  [Error] Navigation error on Page {p_num}: {e}")
                break
                
        print("\nClosing browser...")
        await browser.close()
        print("Discovery complete.")

if __name__ == "__main__":
    asyncio.run(find_resume_point())
