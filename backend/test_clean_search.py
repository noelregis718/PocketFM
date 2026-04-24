import asyncio
from playwright.async_api import async_playwright

async def test_clean_search():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        page = await context.new_page()
        # Try a cleaner URL
        url = "https://www.amazon.com/s?k=dark+romance&i=stripbooks&page=21"
        print(f"Navigating to {url}...")
        await page.goto(url, wait_until="load", timeout=60000)
        items = await page.query_selector_all("[data-asin]")
        print(f"Found {len(items)} items on page 21")
        
        next_btn = await page.query_selector('a.s-pagination-next')
        print(f"Next button found: {next_btn is not None}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_clean_search())
