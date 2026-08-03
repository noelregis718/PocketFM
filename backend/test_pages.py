import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto('https://www.goodreads.com/book/show/62004185-a-dream-so-wicked')
        await asyncio.sleep(2)
        
        # Test 1: data-testid='pagesFormat'
        p_el = await page.query_selector('[data-testid=\"pagesFormat\"]')
        if p_el: print('pagesFormat:', await p_el.inner_text())
        else: print('No pagesFormat found')
        
        # Let's dump all text that has 'pages' in it
        content = await page.content()
        import re
        matches = re.findall(r'[^>]*\d+\s+pages[^<]*', content, re.IGNORECASE)
        print('Regex matches:', set(matches))
        
        await browser.close()

asyncio.run(main())
