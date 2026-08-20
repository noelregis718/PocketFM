import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        print("Navigating...")
        await page.goto('https://www.amazon.com/dp/B0CZ84HSPL', wait_until='domcontentloaded', timeout=45000)
        
        # Scroll down to trigger lazy loading
        print("Scrolling...")
        await page.evaluate('''async () => {
            window.scrollBy(0, 1500);
            await new Promise(r => setTimeout(r, 1000));
            window.scrollBy(0, 1500);
        }''')
        await asyncio.sleep(5)
        
        text = await page.evaluate('() => document.body.innerText')
        if "King's Hollow" in text:
            print("FOUND KING'S HOLLOW IN PAGE TEXT!")
            lines = text.split('\n')
            for i, line in enumerate(lines):
                if "King's Hollow" in line:
                    print(f"Context: {lines[i-2:i+3]}")
        else:
            print("Publisher not found anywhere in body text!")
            
        await browser.close()

asyncio.run(run())
