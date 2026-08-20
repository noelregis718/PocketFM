import asyncio
import pandas as pd
from playwright.async_api import async_playwright
import re

EXCEL_FILE = r"E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx"

async def run_flow():
    print("Loading Excel...")
    df = pd.read_excel(EXCEL_FILE)
    
    missing_indices = []
    for idx, row in df.iterrows():
        url = row.get('Amazon URL')
        if pd.isna(url) or not str(url).startswith('http'):
            continue
            
        if pd.isna(row.get('Logline')) or str(row.get('Logline')).strip() == '' or str(row.get('Logline')).lower() == 'nan' or \
           pd.isna(row.get('Publisher')) or str(row.get('Publisher')).strip() == '' or str(row.get('Publisher')).lower() == 'nan':
            missing_indices.append(idx)
            
    missing_indices = missing_indices[:10]
    print(f"Found missing rows, processing first {len(missing_indices)}...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        for idx in missing_indices:
            row = df.iloc[idx]
            url = row['Amazon URL']
            print(f"\n[{idx}] Navigating to: {url}")
            
            try:
                await page.goto(url, wait_until='domcontentloaded', timeout=45000)
                await asyncio.sleep(4)
                
                # Scroll down in chunks to trigger lazy loading
                for _ in range(5):
                    await page.evaluate("window.scrollBy(0, 1000)")
                    await asyncio.sleep(1)
                
                print("Looking for 'Books in this series'...")
                
                # Extract first book link via Javascript to be completely robust
                first_book_url = await page.evaluate('''() => {
                    // Find any header or span containing "Books in this series"
                    let els = Array.from(document.querySelectorAll('*'));
                    let header = els.find(el => el.innerText && el.innerText.includes('Books in this series') && el.children.length === 0);
                    
                    if (!header) return null;
                    
                    // Traverse up to a container that holds the books
                    let container = header.closest('div, section');
                    for (let i = 0; i < 5; i++) {
                        if (!container) break;
                        let links = Array.from(container.querySelectorAll('a[href*="/dp/"]')).filter(a => !a.href.includes('review') && !a.href.includes('sspa'));
                        if (links.length > 0) {
                            return links[0].href;
                        }
                        container = container.parentElement;
                    }
                    
                    // Fallback: just find the first dp link below it in the DOM
                    let allLinks = Array.from(document.querySelectorAll('a[href*="/dp/"]'));
                    for(let a of allLinks) {
                        if(a.compareDocumentPosition(header) & Node.DOCUMENT_POSITION_PRECEDING) {
                            if(!a.href.includes('review') && !a.href.includes('sspa')) return a.href;
                        }
                    }
                    return null;
                }''')
                
                if first_book_url:
                    print(f"Clicking Book 1: {first_book_url}")
                    await page.goto(first_book_url, wait_until="domcontentloaded")
                    await asyncio.sleep(4)
                else:
                    print("Could not find 'Books in this series' book. Trying to proceed anyway...")
                    
                # Extract Logline
                print("Extracting Logline...")
                logline = "N/A"
                desc_el = await page.query_selector('#bookDescription_feature_div .a-expander-content, #productDescription')
                if desc_el:
                    logline = (await desc_el.inner_text()).strip()
                    df.at[idx, 'Logline'] = logline
                    print(f"Got Logline: {logline[:30]}...")
                
                # Click 'See all details'
                print("Looking for 'See all details' button...")
                for sel in ['a:has-text("See all details")', 'span:has-text("See all details")', '#seeAllDetailsBtn']:
                    btn = await page.query_selector(sel)
                    if btn:
                        print("Clicking 'See all details'...")
                        await btn.click()
                        await asyncio.sleep(4)
                        break
                        
                # Extract Publisher and Date via JS evaluation (most robust)
                print("Extracting Publisher details...")
                details = await page.evaluate('''() => {
                    let pub = "N/A";
                    let date = "N/A";
                    
                    // Try RPI tables first
                    let rpiPub = document.querySelector('#rpi-attribute-book_details-publisher .rpi-attribute-value');
                    if(rpiPub) pub = rpiPub.innerText.trim();
                    
                    let rpiDate = document.querySelector('#rpi-attribute-book_details-publication_date .rpi-attribute-value');
                    if(rpiDate) date = rpiDate.innerText.trim();
                    
                    if(pub === "N/A" || date === "N/A") {
                        let text = document.body.innerText;
                        let lines = text.split('\\n').map(l => l.trim()).filter(l => l.length > 0);
                        for(let i=0; i<lines.length; i++) {
                            if(pub === "N/A") {
                                let m = lines[i].match(/publisher\\s*:\\s*(.+)/i);
                                if(m) pub = m[1].replace(/\\(\\d+.*?\\)$/, '').trim();
                                else if(lines[i].toLowerCase() === 'publisher' && i+1 < lines.length) pub = lines[i+1];
                            }
                            if(date === "N/A") {
                                let m = lines[i].match(/publication\\s*date\\s*:\\s*(.+)/i);
                                if(m) date = m[1].trim();
                                else if(lines[i].toLowerCase() === 'publication date' && i+1 < lines.length) date = lines[i+1];
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
                    
            except Exception as e:
                print(f"Failed on row {idx}: {e}")
                
        await browser.close()
        
    print("Saving to Excel...")
    df.to_excel(EXCEL_FILE, index=False)
    print("Done!")

if __name__ == "__main__":
    asyncio.run(run_flow())
