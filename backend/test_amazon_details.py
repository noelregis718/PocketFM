import asyncio
from playwright.async_api import async_playwright

async def check_amazon_details():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        url = "https://www.amazon.com/s?k=dark+romance&i=stripbooks&page=12"
        print(f"Navigating to {url}...")
        await page.goto(url, wait_until="load", timeout=60000)
        
        await page.evaluate("window.scrollBy(0, 5000)")
        await asyncio.sleep(2)
        
        items = await page.query_selector_all("[data-asin]")
        print(f"Found {len(items)} items")
        
        captured = 0
        for item in items:
            asin = await item.get_attribute('data-asin')
            if not asin or len(asin) < 5: continue
            
            title = "N/A"
            title_selectors = ["h2 a span", ".a-size-medium", ".a-size-base-plus", "h2 a"]
            for t_sel in title_selectors:
                t_el = await item.query_selector(t_sel)
                if t_el:
                    title = await t_el.inner_text()
                    break
            
            href = "N/A"
            link_selectors = ["h2 a", "a.a-link-normal[href*='/dp/']"]
            for l_sel in link_selectors:
                l_el = await item.query_selector(l_sel)
                if l_el:
                    href = await l_el.evaluate("el => el.href")
                    break
            
            if title != "N/A" and href != "N/A":
                captured += 1
                if captured <= 5:
                    print(f"  Captured: {title[:30]} | ASIN: {asin}")
        
        print(f"Total Captured with selectors: {captured}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(check_amazon_details())
