import asyncio
from scraper import AmazonScraper
from playwright.async_api import async_playwright

async def main():
    url = "https://www.amazon.com/dp/B0863VYNW9"
    print(f"Debugging Extraction for URL: {url}\n")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        amazon = AmazonScraper()
        
        # Set location
        page = await context.new_page()
        await page.goto("https://www.amazon.com", wait_until="domcontentloaded")
        await amazon.set_amazon_location(page, "90016")
        await page.close()
        
        # Scrape
        amz_details = await amazon.scrape_product_details_tab(context, url)
        
        print("--- EXTRACTED DETAILS ---")
        for key, value in amz_details.items():
            # truncate logline for easy reading
            if key == "Description" and len(str(value)) > 100:
                print(f"{key}: {str(value)[:100]}...")
            else:
                print(f"{key}: {value}")
                
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
