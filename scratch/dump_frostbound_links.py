import asyncio
from playwright.async_api import async_playwright
import re

async def main():
    url = 'https://www.amazon.com/dp/B0F8L7LLLM?binding=kindle_edition&searchxofy=true&ref_=dbs_s_bs_series_rwt_tkin&qid=1785216976&sr=1-3-e169343e-09af-4d41-85b1-8335fe8f32d0'
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        # Click Continue shopping if present
        try:
            continue_btn = await page.query_selector('text="Continue shopping"')
            if continue_btn:
                print("Clicking Continue shopping...")
                await continue_btn.click()
                await asyncio.sleep(3)
        except:
            pass
            
        await asyncio.sleep(3)
        
        # Extract ASIN from URL
        match = re.search(r'/dp/([A-Z0-9]+)', url)
        series_asin = match.group(1) if match else "UNKNOWN"
        print(f"Series ASIN: {series_asin}")
        
        # Get all links containing /dp/
        links = await page.query_selector_all('a[href*="/dp/"]')
        print(f"Found {len(links)} links with /dp/")
        
        for i, link in enumerate(links[:15]):
            href = await link.get_attribute("href")
            text = (await link.inner_text()).strip()
            # Try to get aria-label if text is empty (for images)
            if not text:
                text = await link.get_attribute("aria-label") or "IMAGE_LINK"
            print(f"[{i}] {text} -> {href}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
