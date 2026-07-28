import asyncio
from playwright.async_api import async_playwright
import re

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print("Visiting series page...")
        await page.goto('https://www.goodreads.com/series/167354-', wait_until="domcontentloaded")
        
        book_links = await page.query_selector_all('.listWithDividers__item a.bookTitle, .listWithDividers__item a[href*="/book/show/"]')
        if not book_links:
            print("No book links found!")
            return
            
        href = await book_links[0].evaluate("el => el.href")
        print("First book href:", href)
        
        print("Visiting book page...")
        await page.goto(href, wait_until="domcontentloaded")
        await asyncio.sleep(2) # wait for render
        
        body_text = await page.evaluate("document.body.innerText")
        print("--- BODY TEXT SNIPPET ---")
        lines = body_text.split('\n')
        for i, line in enumerate(lines):
            if 'Published' in line or 'Headline' in line or 'Berkley' in line or 'Amber Quill' in line:
                # print the context around it
                start = max(0, i-2)
                end = min(len(lines), i+3)
                print("\n".join(lines[start:end]))
                print("------")
                
        # Try extracting
        agency = ""
        # 1. New UI
        pub_info = await page.query_selector('[data-testid="publicationInfo"]')
        if pub_info:
            txt = await pub_info.inner_text()
            print("Found [data-testid='publicationInfo']:", txt)
        # 2. Classic UI
        rows = await page.query_selector_all('#details .row')
        for r in rows:
            txt = await r.inner_text()
            if "Published" in txt:
                print("Found #details .row with Published:", txt)
                
        # Regex
        match = re.search(r'Published.{1,50}?by\s+([^\n]+)', body_text, re.IGNORECASE | re.DOTALL)
        if match:
            print("Regex match:", match.group(1).strip())
        else:
            print("Regex failed.")
            
        await browser.close()

asyncio.run(main())
