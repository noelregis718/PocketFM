import asyncio
import pandas as pd
from scraper import AmazonScraper
from playwright.async_api import async_playwright
from format_excel import apply_styling

EXCEL_FILE = r"E:\Internship\PocketFM\Amazon_Fantasy_Tournament_Romance.xlsx"
MAX_TABS = 2

async def rescrape_row(context, amazon, idx, url, original_title):
    clean_title = str(original_title).replace('\r', '').replace('\n', '')[:30]
    clean_url = str(url).replace('\r', '')
    print(f"Rescraping: {clean_title}... ({clean_url})")
    try:
        amz_details = await amazon.scrape_product_details_tab(context, url)
        # Check heartbeat
        price_raw = amz_details.get("Price", "N/A")
        if "INR" in price_raw or "₹" in price_raw or "\u20b9" in price_raw or "Rs" in price_raw:
            try:
                temp_page = await context.new_page()
                await temp_page.goto("https://www.amazon.com", wait_until="domcontentloaded", timeout=45000)
                await amazon.set_amazon_location(temp_page, "90016")
                await asyncio.sleep(2)
                await temp_page.close()
                amz_details = await amazon.scrape_product_details_tab(context, url)
            except Exception:
                pass
                
        # Fill missing
        result = {'idx': idx}
        result['Author Name'] = amz_details.get("Author Name", "")
        result['Publisher'] = amz_details.get("Publisher", "")
        result['Logline'] = amz_details.get("Description", "")[:1500] if amz_details.get("Description") != "N/A" else ""
        result['Publication Date'] = amz_details.get("Publication Date", "")
        result['Print Length / Pages'] = amz_details.get("Pages", "")
        result['Price_Tier'] = amz_details.get("Price", "N/A").replace('\n', ' | ') if amz_details.get("Price") != "N/A" else "N/A"
        result['Series Name'] = amz_details.get("Series Name", "")
        result['Book Number in Series'] = amz_details.get("Book Number", "")
        
        # Cleanup N/A
        for k, v in result.items():
            if v == "N/A": result[k] = ""
            
        return result
    except Exception as e:
        print(f"Error rescraping {url}: {e}")
        return {'idx': idx}

async def main():
    df = pd.read_excel(EXCEL_FILE)
    
    # Get the last row index
    total_rows = len(df)
    start_idx = total_rows - 1
    print(f"\nTotal rows: {total_rows}. Rescraping from index {start_idx} to {total_rows-1} concurrently ({MAX_TABS} tabs).")
    
    tasks_list = []
    for idx in range(start_idx, total_rows):
        url = df.at[idx, 'Amazon URL']
        if pd.isna(url) or not str(url).startswith('http'):
            continue
        tasks_list.append((idx, url, df.at[idx, 'Book Title']))

    if not tasks_list:
        print("No valid URLs found in the last 50 rows.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        amazon = AmazonScraper()
        
        # Set location once
        print("Setting Amazon Location to US (90016)...")
        page = await context.new_page()
        await page.goto("https://www.amazon.com", wait_until="domcontentloaded")
        await amazon.set_amazon_location(page, "90016")
        await page.close()

        for i in range(0, len(tasks_list), MAX_TABS):
            batch = tasks_list[i:i + MAX_TABS]
            print(f"\nProcessing Batch {i//MAX_TABS + 1} ({len(batch)} items)...")
            
            coroutines = [rescrape_row(context, amazon, item[0], str(item[1]), str(item[2])) for item in batch]
            results = await asyncio.gather(*coroutines, return_exceptions=True)
            
            for res in results:
                if isinstance(res, dict) and 'idx' in res:
                    idx = res['idx']
                    if res.get('Author Name'): df.at[idx, 'Author Name'] = res['Author Name']
                    if res.get('Publisher'): df.at[idx, 'Publisher'] = res['Publisher']
                    if res.get('Logline'): df.at[idx, 'Logline'] = res['Logline']
                    if res.get('Publication Date'): df.at[idx, 'Publication Date'] = res['Publication Date']
                    if res.get('Print Length / Pages'): df.at[idx, 'Print Length / Pages'] = res['Print Length / Pages']
                    if res.get('Price_Tier'): df.at[idx, 'Price_Tier'] = res['Price_Tier']
                    if res.get('Series Name'): df.at[idx, 'Series Name'] = res['Series Name']
                    if res.get('Book Number in Series'): df.at[idx, 'Book Number in Series'] = res['Book Number in Series']
                    print(f" -> Updated row {idx}: {res.get('Author Name', 'Unknown')} | {res.get('Publisher', 'Unknown')}")
                else:
                    print(f" -> Gather Exception: {res}")
            
            # Save after every batch
            df.to_excel(EXCEL_FILE, index=False)
                
        await browser.close()
        
    df.to_excel(EXCEL_FILE, index=False)
    print("Finished rescraping last 50 rows. Formatting now...")
    apply_styling(EXCEL_FILE)

if __name__ == "__main__":
    asyncio.run(main())
