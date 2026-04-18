import asyncio
from playwright.async_api import async_playwright

async def debug_dom():
    url = "https://www.amazon.com/s?k=fantasy+romance&i=stripbooks&crid=28AEJRI2U74G8&qid=1776231644&sprefix=fantasy+romance%2Cstripbooks%2C364&xpid=roloQs4pN_CDY&ref=sr_pg_1"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        page = await context.new_page()
        await page.goto(url, wait_until="load")
        await page.wait_for_selector("[data-asin]")
        
        # Grab first item with a title
        items = await page.query_selector_all(".s-result-item[data-asin]")
        for item in items:
            html = await item.evaluate("el => el.outerHTML")
            text = await item.inner_text()
            if len(text) > 20: # skip ads/empty
                print("--- CORE ITEM DOM ---")
                print(html[:2000]) # first 2k chars
                break
        await browser.close()

if __name__ == '__main__':
    asyncio.run(debug_dom())
