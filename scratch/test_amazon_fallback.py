import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from backend.scraper import AmazonScraper
from playwright.async_api import async_playwright

async def run_test():
    scraper = AmazonScraper()
    scraper.headless = True
    
    url = "https://www.amazon.com/dp/B0CQ3HNKXV" # Sins of the Zodiac Series Page
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        print(f"Scraping Series URL: {url}")
        amz_details = await scraper.scrape_product_details_tab(context, url)
        
        original_description = amz_details.get("Description")
        print(f"Series Page Description: {original_description[:50] if original_description != 'N/A' else 'N/A'}")
        print(f"Series Page Publisher: {amz_details.get('Publisher')}")
        
        if original_description == "N/A" or amz_details.get("Publisher") == "N/A":
            print("Triggering Fallback to Book 1...")
            fallback_page = await context.new_page()
            
            # Simulated fallback logic from scrape_amazon_details_missing.py
            await fallback_page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(4)
            
            selectors = [
                '#series-page-product-list div[id^="item-"] a[href*="/dp/"]',
                '#series-page-product-list a[href*="/dp/"]',
                'div[id^="item-1"] a[href*="/dp/"]',
                'div[id="item-list"] a[href*="/dp/"]',
                '.item-title a[href*="/dp/"]'
            ]
            
            links = []
            for sel in selectors:
                links = await fallback_page.query_selector_all(sel)
                if links:
                    print(f"Found {len(links)} links with selector: {sel}")
                    break
                    
            book1_url = None
            url_asin = "B0CQ3HNKXV"
            for link in links:
                href = await link.get_attribute("href")
                if href and "/dp/" in href and "sspa" not in href.lower() and "review" not in href.lower() and url_asin not in href:
                    if not href.startswith('http'):
                        href = "https://www.amazon.com" + href
                    book1_url = href
                    break
                    
            if book1_url:
                print(f"Book 1 URL resolved to: {book1_url}")
                book1_details = await scraper.scrape_product_details_tab(context, book1_url)
                print("--- Book 1 Extracted Details ---")
                print(f"Publisher: {book1_details.get('Publisher')}")
                print(f"Publication Date: {book1_details.get('Publication Date')}")
                print(f"Description length: {len(str(book1_details.get('Description')))}")
            else:
                print("Failed to find Book 1 URL.")
                
            await fallback_page.close()
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_test())
