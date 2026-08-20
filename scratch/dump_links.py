import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto('https://www.amazon.com/dp/B0CZR55GN5', wait_until='domcontentloaded', timeout=45000)
        await asyncio.sleep(4)
        
        links = await page.evaluate('''() => {
            return Array.from(document.querySelectorAll('a')).map(a => ({
                text: a.innerText.replace(/\\n/g, ' ').substring(0, 30),
                href: a.href
            })).filter(l => l.href.includes('/dp/') || l.href.includes('/gp/product/'));
        }''')
        
        for i, l in enumerate(links[:50]):
            print(f"[{i}] '{l['text']}' -> {l['href']}")
            
        await browser.close()

asyncio.run(run())
