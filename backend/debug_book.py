import asyncio
from playwright.async_api import async_playwright
import re

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        # Series URL
        await page.goto('https://www.goodreads.com/series/428079-once-upon-a-time---cameron-dokey')
        await asyncio.sleep(2)
        
        book_items = await page.query_selector_all('.listWithDividers__item, .seriesWork')
        book_url = None
        for item in book_items:
            item_text = await item.inner_text()
            match = re.search(r'Book\s+([0-9a-zA-Z\.\-]+)', item_text, re.IGNORECASE)
            if match and match.group(1).isdigit():
                b_link = await item.query_selector('a.bookTitle, a[href*="/book/show"]')
                if b_link:
                    book_url = await b_link.evaluate('el => el.href')
                    break
        
        if not book_url:
            print("Book 1 not found")
            return
            
        print("Navigating to:", book_url)
        await page.goto(book_url)
        await asyncio.sleep(3)
        
        details = await page.query_selector('.FeaturedDetails')
        if details:
            print("FeaturedDetails:")
            print(await details.inner_text())
            
        try:
            btn = await page.query_selector('button:has-text("Book details")')
            if btn:
                await btn.click()
                await asyncio.sleep(1)
                
            editions = await page.query_selector('.EditionDetails')
            if editions:
                print("\nEditionDetails:")
                print(await editions.inner_text())
        except Exception as e:
            print("Error expanding details:", e)
        await asyncio.sleep(3)
        
        # Check pagesFormat
        p_el = await page.query_selector('[data-testid="pagesFormat"]')
        if p_el:
            print("pagesFormat text:", await p_el.inner_text())
        else:
            print("No pagesFormat element found")
            
        # Check title / format
        format_el = await page.query_selector('.BookPageMetadataSection__format')
        if format_el:
            print("Format:", await format_el.inner_text())
            
        await browser.close()

asyncio.run(main())
