import asyncio
from playwright.async_api import async_playwright
import re

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        
        url = "https://www.amazon.com/dp/B07HJYM1SD?binding=kind"
        print(f"Loading {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(5)
        
        links = await page.query_selector_all('a[href*="/dp/"]')
        
        match = re.search(r'/dp/([A-Z0-9]+)', url)
        url_asin = match.group(1) if match else "UNKNOWN_ASIN"
        
        valid = []
        for link in links:
            href = await link.get_attribute("href")
            if not href: continue
            
            # Basic exclusions
            if "sspa" in href.lower() or "review" in href.lower() or url_asin in href:
                continue
            if "product/" in href: # Often footer links
                continue
                
            text = await link.inner_text()
            title = await link.get_attribute("title")
            
            # Print candidate links to debug
            print(f"Candidate: {text[:30].strip() or title} | {href}")
            valid.append(href)

        if valid:
            print(f"Chosen fallback: {valid[0]}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
