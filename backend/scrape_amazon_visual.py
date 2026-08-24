import asyncio
import pandas as pd
from playwright.async_api import async_playwright
import re

EXCEL_FILE = r"E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx"

async def run_flow():
    print(f"Loading Excel: {EXCEL_FILE}")
    df = pd.read_excel(EXCEL_FILE)
    
    missing_indices = []
    for idx, row in df.iterrows():
        url = row.get('Amazon URL')
        if pd.isna(url) or not str(url).startswith('http'):
            continue
            
        if pd.isna(row.get('Logline')) or str(row.get('Logline')).strip() == '' or str(row.get('Logline')).lower() == 'nan' or \
           pd.isna(row.get('Publisher')) or str(row.get('Publisher')).strip() == '' or str(row.get('Publisher')).lower() == 'nan':
            missing_indices.append(idx)
            
    print(f"Found {len(missing_indices)} missing rows.")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        for idx in missing_indices:
            row = df.iloc[idx]
            url = row['Amazon URL']
            print(f"\n[{idx}] Navigating to Series Page: {url}")
            
            try:
                await page.goto(url, wait_until='domcontentloaded', timeout=45000)
                await asyncio.sleep(4)
                
                # 1. Scroll down to find "Books in this series"
                for _ in range(5):
                    await page.evaluate("window.scrollBy(0, 800)")
                    await asyncio.sleep(1)
                
                # 2. Find heading "Books in this series" and click the first book beneath it
                print("Looking for heading 'Books in this series' and clicking the first book beneath it...")
                clicked_book = await page.evaluate('''() => {
                    let allEls = Array.from(document.querySelectorAll('h2, h3, span, div, p'));
                    let header = allEls.find(el => el.innerText && el.innerText.toLowerCase().includes('books in this series') && el.children.length === 0);
                    
                    if (header) {
                        let allLinks = Array.from(document.querySelectorAll('a[href*="/dp/"]'));
                        let targetLink = allLinks.find(a => 
                            header.compareDocumentPosition(a) & Node.DOCUMENT_POSITION_FOLLOWING && 
                            !a.href.toLowerCase().includes('review') && 
                            !a.href.toLowerCase().includes('sspa')
                        );
                        if (targetLink) {
                            targetLink.click();
                            return true;
                        }
                    }
                    return false;
                }''')
                
                if not clicked_book:
                    print("Could not find 'Books in this series' or the book link. It might already be a book page.")
                else:
                    await page.wait_for_load_state("domcontentloaded")
                    await asyncio.sleep(4)
                
                # 3. Extract Logline
                print("Extracting Logline...")
                logline = "N/A"
                desc_el = await page.query_selector('#bookDescription_feature_div .a-expander-content, #productDescription')
                if desc_el:
                    logline = (await desc_el.inner_text()).strip()
                    df.at[idx, 'Logline'] = logline
                    print(f"Got Logline: {logline[:30]}...")
                
                # 4. Find "See all details" button (above "Report an issue") and click it
                print("Looking for 'See all details' button...")
                clicked_details = await page.evaluate('''() => {
                    let allEls = Array.from(document.querySelectorAll('a, span, button'));
                    let detailsBtn = allEls.find(el => el.innerText && el.innerText.toLowerCase().includes('see all details') && el.children.length === 0);
                    
                    if (detailsBtn) {
                        detailsBtn.click();
                        return true;
                    }
                    return false;
                }''')
                
                if clicked_details:
                    print("Clicked 'See all details' button!")
                    await asyncio.sleep(4) # Wait for panel/modal to open
                else:
                    print("Could not find 'See all details' button.")
                
                # 5. Scrape Publisher details
                print("Scraping Publisher details...")
                details = await page.evaluate('''() => {
                    let pub = "N/A";
                    let date = "N/A";
                    
                    let text = document.body.innerText;
                    let lines = text.split('\\n').map(l => l.trim()).filter(l => l.length > 0);
                    
                    for(let i=0; i<lines.length; i++) {
                        if(pub === "N/A") {
                            let m = lines[i].match(/publisher\\s*:\\s*(.+)/i);
                            if(m) pub = m[1].replace(/\\(\\d+.*?\\)$/, '').trim();
                            else if(lines[i].toLowerCase() === 'publisher') {
                                for(let j=1; j<=3; j++) {
                                    if(i+j < lines.length && lines[i+j].length > 1) { pub = lines[i+j]; break; }
                                }
                            }
                        }
                        if(date === "N/A") {
                            let m = lines[i].match(/publication\\s*date\\s*:\\s*(.+)/i);
                            if(m) date = m[1].trim();
                            else if(lines[i].toLowerCase() === 'publication date') {
                                for(let j=1; j<=3; j++) {
                                    if(i+j < lines.length && lines[i+j].length > 1) { date = lines[i+j]; break; }
                                }
                            }
                        }
                    }
                    return {pub: pub, date: date};
                }''')
                
                if details['pub'] != "N/A":
                    df.at[idx, 'Publisher'] = details['pub']
                    print(f"Got Publisher: {details['pub']}")
                if details['date'] != "N/A":
                    df.at[idx, 'Publication Date'] = details['date']
                    print(f"Got Date: {details['date']}")
                
                # 6. Save every 10 rows
                if idx % 10 == 0:
                    df.to_excel(EXCEL_FILE, index=False)
                    print("Progress saved to Excel.")
                    
            except Exception as e:
                print(f"Failed on row {idx}: {e}")
                
        await browser.close()
        
    print("Final save to Excel...")
    df.to_excel(EXCEL_FILE, index=False)
    print("All done!")

if __name__ == "__main__":
    asyncio.run(run_flow())
