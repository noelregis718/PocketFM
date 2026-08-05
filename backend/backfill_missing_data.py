import asyncio
import pandas as pd
import os
import sys
from playwright.async_api import async_playwright
sys.path.append('backend')
from scraper import AmazonScraper, clean_text

async def check_captcha(page):
    while True:
        try:
            if await page.query_selector('form[action="/errors/validateCaptcha"], input#captchacharacters'):
                print("🚨 CAPTCHA DETECTED! Please solve it in the browser window...", flush=True)
                await asyncio.sleep(5)
            else:
                break
        except:
            break

async def backfill_missing(file_path):
    print(f"Loading {file_path} for backfilling...")
    df = pd.read_excel(file_path)
    
    # Identify rows missing Author or Logline
    missing_mask = (df['Author Name'].isna()) | (df['Author Name'] == '') | (df['Author Name'] == 'N/A') | \
                   (df['Logline'].isna()) | (df['Logline'] == '') | (df['Logline'] == 'N/A') | \
                   (df['Logline'].str.len() < 20)
                   
    missing_indices = df[missing_mask].index.tolist()
    print(f"Found {len(missing_indices)} rows needing backfill.")
    
    if not missing_indices:
        print("Nothing to backfill!")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        amazon = AmazonScraper()
        
        # Initial sync
        page = await context.new_page()
        await page.goto("https://www.amazon.com", wait_until="domcontentloaded", timeout=60000)
        await amazon.set_amazon_location(page, "90016")
        await check_captcha(page)

        semaphore = asyncio.Semaphore(8)
        
        async def process_row(idx):
            async with semaphore:
                url = df.at[idx, 'Amazon URL']
                title = df.at[idx, 'Book Title']
                print(f"-> Starting: {title[:40]}...", flush=True)
                
                try:
                    amz_details = await amazon.scrape_product_details_tab(context, str(url))
                    
                    price_raw = amz_details.get("Price", "N/A")
                    if "INR" in price_raw or "₹" in price_raw or "\u20b9" in price_raw:
                        amz_details = await amazon.scrape_product_details_tab(context, str(url))
                    
                    new_author = amz_details.get("Author Name", "")
                    new_desc = amz_details.get("Description", "")
                    new_logline = new_desc[:1500] if new_desc != "N/A" else ""
                    
                    if new_author and new_author != "N/A":
                        df.at[idx, 'Author Name'] = new_author
                    if new_logline and len(new_logline) > 20:
                        df.at[idx, 'Logline'] = new_logline
                        
                    if pd.isna(df.at[idx, 'Series Name']) or df.at[idx, 'Series Name'] == '':
                        if amz_details.get("Series Name") and amz_details.get("Series Name") != "N/A":
                            df.at[idx, 'Series Name'] = amz_details.get("Series Name")
                    
                    if pd.isna(df.at[idx, 'Num_Primary_Books_in_Series']) or df.at[idx, 'Num_Primary_Books_in_Series'] == '':
                        if amz_details.get("Total Books") and amz_details.get("Total Books") != "N/A":
                            df.at[idx, 'Num_Primary_Books_in_Series'] = amz_details.get("Total Books")
                            
                    if pd.isna(df.at[idx, 'Total_Page_Count_of_Primary_Books']) or df.at[idx, 'Total_Page_Count_of_Primary_Books'] == '':
                        if amz_details.get("Pages") and amz_details.get("Pages") != "N/A":
                            df.at[idx, 'Total_Page_Count_of_Primary_Books'] = amz_details.get("Pages")
                            
                    print(f"<- Finished: {title[:40]}", flush=True)
                except Exception as e:
                    print(f"  Error processing {url}: {e}")

        # Process all missing items concurrently with a stagger
        tasks = []
        for i, idx in enumerate(missing_indices):
            tasks.append(asyncio.create_task(process_row(idx)))
            if i < 8: # Stagger the initial burst
                await asyncio.sleep(1.5)
                
        await asyncio.gather(*tasks)
                
        # Final save
        df.to_excel(file_path, index=False)
        print(f"\n✅ Backfill complete! Final save to {file_path}.")
        await browser.close()

if __name__ == '__main__':
    target_file = 'Dungeon Trials Romance.xlsx'
    asyncio.run(backfill_missing(target_file))
