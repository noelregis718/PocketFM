import asyncio
from playwright.async_api import async_playwright
import re

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto('https://www.goodreads.com/book/show/62004185-a-dream-so-wicked', wait_until='networkidle')
        
        btn = await page.query_selector('button:has-text("Book details")')
        if btn:
            await btn.click()
            await asyncio.sleep(1)
            
        content = await page.content()
        matches = re.findall(r'.{0,30}\s*(?:pages|Pages)\s*.{0,30}', content)
        for m in set(matches):
            print('NEAR PAGES:', m.strip())
            
        print("Now looking for JSON-LD data...")
        ld_el = await page.query_selector('script[type="application/ld+json"]')
        if ld_el:
            text = await ld_el.inner_text()
            print("JSON-LD found.")
            if "numberOfPages" in text:
                import json
                data = json.loads(text)
                if isinstance(data, list): data = data[0]
                print("numberOfPages in JSON:", data.get('numberOfPages', 'Not found'))
            else:
                print("No numberOfPages in JSON-LD")
                
        await browser.close()

asyncio.run(main())
