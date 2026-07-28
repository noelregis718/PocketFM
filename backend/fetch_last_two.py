import asyncio
from playwright.async_api import async_playwright
import re
import openpyxl

EXCEL_FILE = "Agency _ Publishers Crawl - 1852, Bent, Penzler, Biagi, AULit .xlsx"
SHEET_NAME = "1852 Literary Agent"

async def get_page_count(page, url):
    await page.goto(url, wait_until="domcontentloaded")
    try:
        await page.wait_for_selector('[data-testid="pagesFormat"]', timeout=5000)
    except:
        pass
    text = await page.content()
    match = re.search(r'(\d+)\s+pages', text)
    return int(match.group(1)) if match else None

async def main():
    print("Launching Playwright...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        wb = openpyxl.load_workbook(EXCEL_FILE)
        ws = wb[SHEET_NAME]
        
        for row, url in [(23, 'https://www.goodreads.com/book/show/52943067-filthy'), (121, 'https://www.goodreads.com/book/show/251303722-whiskey-salvation')]:
            print(f"Fetching Row {row}...")
            pages = await get_page_count(page, url)
            print(f"Row {row} Pages: {pages}")
            if pages:
                ws.cell(row=row, column=16, value=pages)
            await asyncio.sleep(2)
            
        wb.save(EXCEL_FILE)
        await browser.close()
        print("Done! Saved to Excel.")

if __name__ == "__main__":
    asyncio.run(main())
