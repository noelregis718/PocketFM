import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from backend.scraper import AmazonScraper
from playwright.async_api import async_playwright

async def run_test():
    scraper = AmazonScraper()
    scraper.headless = True
    
    url = "https://www.amazon.com/dp/B0CZ84HSPL"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        print(f"Scraping Book URL: {url}")
        amz_details = await scraper.scrape_product_details_tab(context, url)
        
        print("--- Extracted Details ---")
        print(f"Publisher: {amz_details.get('Publisher')}")
        print(f"Publication Date: {amz_details.get('Publication Date')}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_test())
