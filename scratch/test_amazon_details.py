import asyncio
import pandas as pd
import os
import sys

sys.path.append(r"E:\Internship\PocketFM\backend")
from scraper import AmazonScraper
from playwright.async_api import async_playwright

async def main():
    print("Finding 2 rows missing details...")
    df = pd.read_excel(r'E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx')
    
    # Find Frostbound Court (Index 10343)
    test_rows = df.iloc[[10343]]
    
    scraper = AmazonScraper()
    scraper.headless = False
    
    async with async_playwright() as p:
        user_data_dir = os.path.join(r"E:\Internship\PocketFM", "playwright_goodreads_profile")
        context = await p.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        for idx, row in test_rows.iterrows():
            url = row['Amazon URL']
            print(f"\n--- Testing Row {idx}: {row['Book Title']} ---")
            print(f"URL: {url}")
            
            try:
                amz_details = await scraper.scrape_product_details_tab(context, url)
                
                original_description = amz_details.get("Description")
                
                # FALLBACK FOR SERIES PAGES
                if original_description == "N/A" or amz_details.get("Publisher") == "N/A":
                    print(f"  -> Missing details, checking if this is a series page for Book 1 fallback...")
                    fallback_page = await context.new_page()
                    try:
                        import re
                        match = re.search(r'/dp/([A-Z0-9]+)', url)
                        url_asin = match.group(1) if match else "UNKNOWN_ASIN"
                        
                        await fallback_page.goto(url, wait_until="domcontentloaded", timeout=45000)
                        try:
                            continue_btn = await fallback_page.query_selector('text="Continue shopping"')
                            if continue_btn:
                                print(f"  -> 🔄 Clicking 'Continue shopping' button on fallback page...", flush=True)
                                await continue_btn.click()
                                await asyncio.sleep(3)
                        except Exception:
                            pass
                        await asyncio.sleep(2)
                        links = await fallback_page.query_selector_all('.item-title a[href*="/dp/"], #item-list a[href*="/dp/"], div[id^="item-"] a[href*="/dp/"]')
                        if not links:
                            links = await fallback_page.query_selector_all('a[href*="/dp/"]')
                        
                        book1_url = None
                        print(f"  -> Found {len(links)} candidate links")
                        for link in links:
                            href = await link.get_attribute("href")
                            if href and "/dp/" in href and "sspa" not in href.lower() and "review" not in href.lower() and url_asin not in href:
                                if not href.startswith('http'):
                                    href = "https://www.amazon.com" + href
                                book1_url = href
                                break
                            else:
                                print(f"     [Skip] {href}")
                        if book1_url:
                            print(f"  -> Found Book 1 Fallback URL: {book1_url}")
                            book1_details = await scraper.scrape_product_details_tab(context, book1_url)
                            if original_description != "N/A":
                                book1_details["Description"] = original_description
                            amz_details = book1_details
                    except Exception as e:
                        print(f"  -> Fallback failed: {e}")
                    finally:
                        await fallback_page.close()
                
                print(f"EXTRACTED LOGLINE (Description):")
                desc = amz_details.get('Description', 'N/A')
                print(f"{desc[:150]}..." if desc != 'N/A' else "N/A")
                
                print(f"EXTRACTED PUBLISHER: {amz_details.get('Publisher')}")
                print(f"EXTRACTED PUBLICATION DATE: {amz_details.get('Publication Date')}")
                
            except Exception as e:
                print(f"Error scraping {url}: {e}")
                await context.close()

if __name__ == "__main__":
    asyncio.run(main())
