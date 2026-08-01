import asyncio
import urllib.parse
from playwright.async_api import async_playwright

async def test_search():
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=r"e:\Internship\PocketFM\playwright_goodreads_profile",
            headless=False
        )
        page = await context.new_page()
        
        query = "A Court of Thorns and Roses Sarah J. Maas"
        search_url = f"https://www.goodreads.com/search?q={urllib.parse.quote_plus(query)}"
        
        print(f"Navigating to {search_url}")
        await page.goto(search_url)
        await asyncio.sleep(3)
        
        b_link = await page.query_selector('a.bookTitle')
        if b_link:
            url = await b_link.evaluate("el => el.href")
            print("FOUND:", url)
        else:
            print("NOT FOUND")
            
        await context.close()

if __name__ == "__main__":
    asyncio.run(test_search())
