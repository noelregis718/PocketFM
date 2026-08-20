import asyncio
from playwright.async_api import async_playwright
import re

async def run():
    url = "https://www.amazon.com/dp/B0CZR55GN5"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        print(f"STEP 1: Navigating to {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(3)
        input("\n[PAUSED] Look at the browser. Is it on the series page? Press Enter to continue and click Book 1...")
        
        # Click Book 1
        print("\nSTEP 2: Trying to find 'Books in this series' and click Book 1...")
        first_book_url = await page.evaluate('''() => {
            let els = Array.from(document.querySelectorAll('*'));
            let header = els.find(el => el.innerText && el.innerText.includes('Books in this series') && el.children.length === 0);
            if (!header) return null;
            
            let container = header.closest('div, section');
            for (let i = 0; i < 5; i++) {
                if (!container) break;
                let links = Array.from(container.querySelectorAll('a[href*="/dp/"]')).filter(a => !a.href.includes('review') && !a.href.includes('sspa'));
                if (links.length > 0) return links[0].href;
                container = container.parentElement;
            }
            return null;
        }''')
        
        if first_book_url:
            print(f"-> Found Book 1 URL: {first_book_url}")
            await page.goto(first_book_url, wait_until="domcontentloaded")
            await asyncio.sleep(3)
            print("-> Successfully navigated to Book 1.")
        else:
            print("-> ERROR: Could not find Book 1 link! You can manually click it now.")
            
        input("\n[PAUSED] Are we on Book 1's page now? Press Enter to extract Logline and click 'See all details'...")
        
        # Extract Logline
        print("\nSTEP 3: Extracting Logline from below ratings...")
        logline = "N/A"
        desc_el = await page.query_selector('#bookDescription_feature_div .a-expander-content, #productDescription')
        if desc_el:
            logline = (await desc_el.inner_text()).strip()
            print(f"-> Found Logline: {logline[:50]}...")
        else:
            print("-> ERROR: Could not find Logline element.")
            
        print("\nSTEP 4: Clicking 'See all details'...")
        btn = await page.query_selector('a:has-text("See all details"), span:has-text("See all details"), #seeAllDetailsBtn')
        if btn:
            await btn.click()
            await asyncio.sleep(4)
            print("-> Clicked successfully.")
        else:
            print("-> ERROR: Could not find 'See all details' button! You can manually click it now.")
            
        input("\n[PAUSED] Is the details section open? Press Enter to extract Publisher and Date...")
        
        # Extract Publisher and Date
        print("\nSTEP 5: Extracting Publisher and Date...")
        text = await page.evaluate('() => document.body.innerText')
        lines = [l.strip() for l in text.split('\\n') if l.strip()]
        
        pub = "N/A"
        date = "N/A"
        
        # Try RPI table directly first
        rpi_pub = await page.evaluate('() => { let e = document.querySelector("#rpi-attribute-book_details-publisher .rpi-attribute-value"); return e ? e.innerText : null; }')
        if rpi_pub: pub = rpi_pub.strip()
        
        rpi_date = await page.evaluate('() => { let e = document.querySelector("#rpi-attribute-book_details-publication_date .rpi-attribute-value"); return e ? e.innerText : null; }')
        if rpi_date: date = rpi_date.strip()
        
        # Fallback to scanning lines
        if pub == "N/A" or date == "N/A":
            for i, line in enumerate(lines):
                if pub == "N/A":
                    m = re.search(r'publisher\s*:\s*(.+)', line, re.IGNORECASE)
                    if m: pub = re.sub(r'\s*\(\d+.*?\)\s*$', '', m.group(1)).strip()
                    elif line.lower() == 'publisher' and i+1 < len(lines): pub = lines[i+1]
                if date == "N/A":
                    m = re.search(r'publication\s*date\s*:\s*(.+)', line, re.IGNORECASE)
                    if m: date = m.group(1).strip()
                    elif line.lower() == 'publication date' and i+1 < len(lines): date = lines[i+1]
                    
        print(f"-> FINAL PUBLISHER: {pub}")
        print(f"-> FINAL DATE: {date}")
        
        print("\nAll done! Close the browser to exit.")
        await page.wait_for_timeout(300000)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
