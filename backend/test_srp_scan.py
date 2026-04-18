import asyncio
from playwright.async_api import async_playwright
import sys

async def test_scan():
    # Use the user's specific keyword URL
    url = "https://www.amazon.com/s?k=fantasy+romance&i=stripbooks&crid=28AEJRI2U74G8&qid=1776231644&sprefix=fantasy+romance%2Cstripbooks%2C364&xpid=roloQs4pN_CDY&ref=sr_pg_1"
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            
            print(f"Opening URL: {url}")
            await page.goto(url, wait_until="load", timeout=60000)
            
            # Wait for any result item
            try:
                await page.wait_for_selector("[data-asin]", timeout=15000)
            except:
                print("Wait for [data-asin] failed. Scanning raw DOM...")

            # INDUSTRIAL SCROLL
            for i in range(6):
                print(f"  Scroll step {i+1}/6...")
                await page.evaluate("window.scrollBy(0, 1500)")
                await asyncio.sleep(1.5)
                
            # TEST SELECTORS
            print("\n--- SELECTOR AUDIT ---")
            
            # 1. Main Grid items
            items = await page.query_selector_all("[data-asin]")
            print(f"Found {len(items)} items using '[data-asin]'")
            
            # 2. Result items (filter out ads/widgets)
            result_items = await page.query_selector_all(".s-result-item[data-asin]")
            print(f"Found {len(result_items)} items using '.s-result-item[data-asin]'")
            
            if result_items:
                for i, item in enumerate(result_items[:10]):
                    asin = await item.get_attribute('data-asin')
                    
                    # Try title selectors
                    title = "N/A"
                    title_selectors = ["h2 a span", ".a-size-medium", ".a-size-base-plus", "h2 a"]
                    for t_sel in title_selectors:
                        t_el = await item.query_selector(t_sel)
                        if t_el:
                            title = (await t_el.inner_text()).strip()
                            if title: break
                    
                    # Try Link selectors
                    link = "N/A"
                    l_el = await item.query_selector("h2 a")
                    if l_el:
                        link = await l_el.evaluate("el => el.href")
                        
                    print(f"  [{i+1}] {title[:30]}... | ASIN: {asin} | Link: {'Yes' if link != 'N/A' else 'No'}")
            
            await browser.close()
        except Exception as e:
            print(f"Error: {e}")

if __name__ == '__main__':
    asyncio.run(test_scan())
