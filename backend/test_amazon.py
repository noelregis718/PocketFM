import asyncio
from playwright.async_api import async_playwright

async def check_amazon():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        url = "https://www.amazon.com/s?k=dark+romance&i=stripbooks&page=12"
        print(f"Navigating to {url}...")
        await page.goto(url, wait_until="load", timeout=60000)
        
        # Check for captcha
        if "api-services-support@amazon.com" in await page.content():
            print("CAPTCHA DETECTED!")
        else:
            items = await page.query_selector_all("[data-asin]")
            print(f"Found {len(items)} items with [data-asin]")
            for item in items[:5]:
                asin = await item.get_attribute("data-asin")
                print(f"  ASIN: {asin}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(check_amazon())
