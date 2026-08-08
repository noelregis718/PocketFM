import asyncio
import os
import sys
import pandas as pd
from playwright.async_api import async_playwright

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from goodreads_scraper import GoodreadsScraper

EXCEL_FILE = r"E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx"
MAX_CONCURRENT = 5
MAX_ROWS = 20 # Processing first 20 rows

async def process_row(context, scraper, idx, title, author, df, semaphore):
    async with semaphore:
        safe_title = title.encode('ascii', 'ignore').decode('ascii') if title else 'Unknown'
        safe_author = author.encode('ascii', 'ignore').decode('ascii') if author else 'Unknown'
        
        try:
            print(f"[{idx+2}] Searching: '{safe_title}' by {safe_author}...") # +2 because pandas is 0-indexed and excel has a header
            
            # Use existing links if available to speed up
            existing_series_link = str(df.at[idx, 'GoodReads_Series_URL']).strip()
            existing_book_link = str(df.at[idx, 'Goodreads Link']).strip()
            
            existing = existing_series_link if (existing_series_link and existing_series_link != 'N/A' and existing_series_link.lower() != 'nan') else ""
            if not existing:
                existing = existing_book_link if (existing_book_link and existing_book_link != 'N/A' and existing_book_link.lower() != 'nan') else ""
                
            data = await scraper.scrape_goodreads_data(context, title, author, existing_url=existing)
            
            if data:
                # Update series URL
                s_link = data.get('GoodReads_Series_URL', '')
                if s_link and s_link != 'N/A':
                    df.at[idx, 'GoodReads_Series_URL'] = s_link
                    
                # Update book URL
                b_link = data.get('GoodReads_Book_URL', '')
                if b_link and b_link != 'N/A':
                    df.at[idx, 'Goodreads Link'] = b_link
                    
                # Update Rating
                rating = data.get('Book1_Rating', 'N/A')
                if rating == 'N/A': rating = data.get('GoodReads_Rating', 'N/A')
                if rating != 'N/A':
                    df.at[idx, 'Goodreads Rating'] = rating
                    
                # Update Num Ratings
                count = data.get('Book1_Num_Ratings', 'N/A')
                if count == 'N/A': count = data.get('GoodReads_Rating_Count', 'N/A')
                if count != 'N/A':
                    df.at[idx, 'Goodreads No. of Ratings'] = count
                    
                # Update Synopsis
                synopsis = data.get('Description', 'N/A')
                if synopsis != 'N/A':
                    df.at[idx, 'Synopsis'] = synopsis
                    
                # Update Genre Tags
                genres = data.get('Genres', [])
                if genres and isinstance(genres, list):
                    df.at[idx, 'Genre Tags'] = ", ".join(genres)
                elif genres and isinstance(genres, str):
                    df.at[idx, 'Genre Tags'] = genres
                    
                print(f"[{idx+2}] Done. Got info for '{safe_title}'.")
            else:
                print(f"[{idx+2}] Not Found on Goodreads.")
                
        except Exception as e:
            print(f"[{idx+2}] Error scraping '{safe_title}': {e}")

async def run_aggressive_scrape():
    if not os.path.exists(EXCEL_FILE):
        print(f"Error: {EXCEL_FILE} not found!")
        return

    print(f"Loading {EXCEL_FILE}...")
    df = pd.read_excel(EXCEL_FILE)
    
    # Ensure all Goodreads columns exist
    gr_cols = ['GoodReads_Series_URL', 'Goodreads Rating', 'Goodreads No. of Ratings', 'Goodreads Link', 'Genre Tags', 'Romantasy Checker', 'Synopsis']
    for col in gr_cols:
        if col not in df.columns:
            df[col] = ""
    
    # Ensure columns are strings to prevent nan floating issues
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].fillna("").astype(str)
            
    tasks = []
    scraper = GoodreadsScraper(headless=False)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        login_page = await context.new_page()
        print("Logging in to Goodreads...")
        try:
            await scraper.login_to_goodreads(login_page)
        except Exception as e:
            print(f"Login failed/skipped (might already be logged in): {e}")
        await login_page.close()
        
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        
        print(f"--- Scanning first {MAX_ROWS} rows ---")
        for idx in range(min(MAX_ROWS, len(df))):
            title = str(df.at[idx, 'Series Name']).strip()
            if not title or title.lower() == 'nan':
                title = str(df.at[idx, 'Book Title']).strip()
                
            author = str(df.at[idx, 'Author Name']).strip()
            
            if not title or title.lower() == 'nan':
                continue
                
            tasks.append(process_row(context, scraper, idx, title, author, df, semaphore))
                
        print(f"Queued {len(tasks)} books for aggressive scraping.")
        
        if tasks:
            await asyncio.gather(*tasks)
            
        await browser.close()
        
    print("--- Saving Excel ---")
    df.to_excel(EXCEL_FILE, index=False)
    
    try:
        from style_excel_fixed import style_excel
        style_excel(EXCEL_FILE)
        print("--- Applied styling ---")
    except Exception as e:
        print(f"Could not apply automated styling script (you may need to run it manually): {e}")
        
    print("ALL DONE! First 20 rows processed.")

if __name__ == '__main__':
    asyncio.run(run_aggressive_scrape())
