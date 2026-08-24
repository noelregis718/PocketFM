import asyncio
import pandas as pd
from playwright.async_api import async_playwright
import re

EXCEL_FILE = r"E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx"

async def run_flow():
    print("Loading Excel...")
    df = pd.read_excel(EXCEL_FILE)
    
    # We will process ONLY row 2 (which is index 0 or 1 depending on pandas)
    # The user's first missing row is index 2 (row 4 in excel if header is 1)
    # Let's find the exact first missing row dynamically, but limit to exactly ONE.
    missing_indices = []
    for idx, row in df.iterrows():
        url = row.get('Amazon URL')
        if pd.isna(url) or not str(url).startswith('http'):
            continue
        if pd.isna(row.get('Logline')) or pd.isna(row.get('Publisher')):
            missing_indices.append(idx)
            
    if not missing_indices:
        print("No missing rows found!")
        return
        
    idx = missing_indices[0]
    row = df.iloc[idx]
    url = row['Amazon URL']
    
    print(f"\n--- PROCESSING EXACTLY 1 BOOK (Row index: {idx}) ---")
    print(f"URL: {url}")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            print("1. Opening Amazon URL...")
            await page.goto(url, wait_until='domcontentloaded', timeout=45000)
            await asyncio.sleep(4)
            
            print("2. Scrolling down to find 'Books in this series'...")
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, 1000)")
                await asyncio.sleep(1)
            
            print("3. Locating the 'Books in this series' heading and clicking the first book...")
            try:
                # Use substring match (no quotes around the text inside the locator)
                heading = page.locator('text=Books in this series').first
                await heading.wait_for(state="visible", timeout=10000)
                
                # Use robust xpath to find the very first book link that appears AFTER this heading
                book_link = heading.locator('xpath=following::a[contains(@href, "/dp/") and not(contains(@href, "review")) and not(contains(@href, "customer"))]').first
                await book_link.click(timeout=10000)
                print("-> Automatically clicked Book 1!")
                
                await page.wait_for_load_state("domcontentloaded")
                await asyncio.sleep(4)
                
            except Exception as e:
                print(f"-> Could not automatically click Book 1 (Error: {e}). You have 15 seconds to click it manually!")
                await asyncio.sleep(15)
                
            print("4. On Book Page: Extracting Logline (below ratings)...")
            logline = "N/A"
            desc_el = await page.query_selector('#bookDescription_feature_div .a-expander-content, #productDescription')
            if desc_el:
                logline = (await desc_el.inner_text()).strip()
                df.at[idx, 'Logline'] = logline
                print(f"-> Extracted Logline: {logline[:50]}...")
            else:
                print("-> Could not find logline!")
                
            print("5. Looking for 'See all details' button (above 'Report an issue')...")
            try:
                details_btn = page.locator('text=See all details').first
                await details_btn.click(timeout=10000)
                print("-> Clicked 'See all details'!")
                await asyncio.sleep(4)
            except Exception as e:
                print("-> Could not find 'See all details' button. Trying to proceed anyway...")
                
            print("6. Extracting Publisher details...")
            details = await page.evaluate('''() => {
                let pub = "N/A";
                let date = "N/A";
                
                // Check Rich Product Information first
                let rpiPub = document.querySelector('#rpi-attribute-book_details-publisher .rpi-attribute-value');
                if(rpiPub) pub = rpiPub.innerText.trim();
                
                let rpiDate = document.querySelector('#rpi-attribute-book_details-publication_date .rpi-attribute-value');
                if(rpiDate) date = rpiDate.innerText.trim();
                
                // Fallback to text scanning
                if(pub === "N/A" || date === "N/A") {
                    let text = document.body.innerText;
                    let lines = text.split('\\n').map(l => l.trim()).filter(l => l.length > 0);
                    for(let i=0; i<lines.length; i++) {
                        if(pub === "N/A" && lines[i].match(/publisher\\s*:\\s*(.+)/i)) {
                            pub = lines[i].match(/publisher\\s*:\\s*(.+)/i)[1].replace(/\\(\\d+.*?\\)$/, '').trim();
                        }
                        if(date === "N/A" && lines[i].match(/publication\\s*date\\s*:\\s*(.+)/i)) {
                            date = lines[i].match(/publication\\s*date\\s*:\\s*(.+)/i)[1].trim();
                        }
                    }
                }
                return {pub: pub, date: date};
            }''')
            
            if details['pub'] != "N/A":
                df.at[idx, 'Publisher'] = details['pub']
                print(f"-> Extracted Publisher: {details['pub']}")
            else:
                print("-> Failed to find Publisher.")
                
            if details['date'] != "N/A":
                df.at[idx, 'Publication Date'] = details['date']
                print(f"-> Extracted Publication Date: {details['date']}")
                
            print("7. Saving exact details back to Excel...")
            df.to_excel(EXCEL_FILE, index=False)
            print("--- DONE ---")
            
        except Exception as e:
            print(f"Failed with error: {e}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_flow())
