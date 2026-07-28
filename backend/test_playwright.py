import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        await page.goto("https://www.goodreads.com/series/313590", timeout=60000)
        
        content = await page.content()
        with open("goodreads_series.html", "w", encoding="utf-8") as f:
            f.write(content)
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
