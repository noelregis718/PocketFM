import asyncio
from playwright.async_api import async_playwright

async def main():
    url = 'https://www.amazon.com/dp/B0F8L7LLLM?binding=kindle_edition&searchxofy=true&ref_=dbs_s_bs_series_rwt_tkin&qid=1785216976&sr=1-3-e169343e-09af-4d41-85b1-8335fe8f32d0'
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(2)
        
        # Selectors for books in series list
        book_links = await page.query_selector_all('a.a-link-normal[href*="/dp/"]')
        print(f"Found {len(book_links)} links matching selector")
        for link in book_links:
            href = await link.evaluate("el => el.href")
            text = await link.inner_text()
            print(f"Found link: {text.strip()} -> {href}")
            
        await page.screenshot(path=r"E:\Internship\PocketFM\scratch\screenshot.png")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
