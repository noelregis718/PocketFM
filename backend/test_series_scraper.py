import asyncio
from playwright.async_api import async_playwright
import re

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://www.goodreads.com/series/313590", timeout=60000)
        
        works = await page.query_selector_all('.listWithDividers__item')
        print(f"Found {len(works)} works")
        
        for work in works[:5]:
            title_el = await work.query_selector('a[href*="/book/show/"]')
            title = await title_el.inner_text() if title_el else "Unknown"
            link = await title_el.evaluate("el => el.href") if title_el else "Unknown"
            
            h3 = await work.query_selector('h3')
            h3_text = await h3.inner_text() if h3 else ""
            
            print(f"Work: {h3_text} | {title} | {link}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
