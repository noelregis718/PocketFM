import asyncio
from playwright.async_api import async_playwright
import re, json

async def scrape_pages(b_page):
    extracted = 0
    # JSON-LD
    try:
        ld_el = await b_page.query_selector('script[type="application/ld+json"]')
        if ld_el:
            data = json.loads(await ld_el.inner_text())
            if isinstance(data, list): data = data[0]
            if 'numberOfPages' in data:
                return int(data['numberOfPages']), 'JSON-LD'
    except Exception as e:
        pass

    # data-testid
    try:
        p_el = await b_page.query_selector('[data-testid="pagesFormat"]')
        if p_el:
            m = re.search(r'(\d+)\s*pages', await p_el.inner_text(), re.IGNORECASE)
            if m: return int(m.group(1)), 'pagesFormat-direct'
    except Exception as e:
        pass

    # Click Book details
    try:
        btn = await b_page.query_selector('button:has-text("Book details")')
        if btn:
            await btn.click(force=True)
            await asyncio.sleep(1)
            p_el = await b_page.query_selector('[data-testid="pagesFormat"]')
            if p_el:
                m = re.search(r'(\d+)\s*pages', await p_el.inner_text(), re.IGNORECASE)
                if m: return int(m.group(1)), 'pagesFormat-clicked'
    except Exception as e:
        pass
    
    # Just grab all text and look for it as a fallback test
    try:
        content = await b_page.content()
        matches = re.findall(r'.{0,30}(\d+)\s*pages.{0,30}', content, re.IGNORECASE)
        if matches:
            return matches[0], 'regex-fallback'
    except Exception as e:
        pass

    return 0, 'none'

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36')
        page = await context.new_page()
        
        await page.goto('https://www.goodreads.com/series/159227-beastly-tales')
        await asyncio.sleep(2)
        
        items = await page.query_selector_all('.listWithDividers__item, .seriesWork')
        for item in items:
            item_text = await item.inner_text()
            match = re.search(r'Book\s+([0-9a-zA-Z\.\-]+)', item_text, re.IGNORECASE)
            if match and match.group(1).isdigit():
                book_num = match.group(1)
                b_link = await item.query_selector('a.bookTitle, a[href*="/book/show"]')
                if b_link:
                    b_url = await b_link.evaluate('el => el.href')
                    b_page = await context.new_page()
                    await b_page.goto(b_url, wait_until='domcontentloaded')
                    await asyncio.sleep(1.5)
                    pages, method = await scrape_pages(b_page)
                    print(f'Book {book_num}: {pages} pages (via {method})')
                    await b_page.close()
                    
        await browser.close()

asyncio.run(main())
