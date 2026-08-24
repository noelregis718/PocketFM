import asyncio
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scraper import AmazonScraper
from playwright.async_api import async_playwright

EXCEL_FILE = r"E:\Internship\PocketFM\Combined List of Titles.xlsx"
CONCURRENCY = 8

async def process_row(idx, row, df, context, scraper, sem):
    async with sem:
        url = row.get('Amazon URL')
        
        if pd.isna(url) or not str(url).startswith('http'):
            return

        print(f"[{idx}] Scraping Amazon URL: {url}")
        
        try:
            # Reusing the existing scraper function
            amz_details = await scraper.scrape_product_details_tab(context, url)
            
            # Extract Number of Books (usually fetched as Total Books)
            num_books = amz_details.get("Total Books", "")
            if num_books and num_books != "N/A":
                try:
                    df.at[idx, 'Amazon Num_Primary_Books_in_Series'] = float(num_books)
                except ValueError:
                    df.at[idx, 'Amazon Num_Primary_Books_in_Series'] = num_books
                
            # Extract Pages
            pages = amz_details.get("Pages", "")
            if pages and pages != "N/A":
                try:
                    df.at[idx, 'Amazon Total_Page_Count_of_Primary_Books'] = float(pages)
                except ValueError:
                    df.at[idx, 'Amazon Total_Page_Count_of_Primary_Books'] = pages

            # Extract Ratings
            rating = amz_details.get("Rating", "")
            if rating and rating != "N/A":
                df.at[idx, 'Amazon Book1_Rating'] = rating
                
            # Extract Number of Ratings
            num_ratings = amz_details.get("Number of Reviews", "")
            if num_ratings and num_ratings != "N/A":
                df.at[idx, 'Amazon Book1_Num_Ratings'] = num_ratings
                
            print(f"  -> [{idx}] Fetched - Num Books: {num_books} | Pages: {pages} | Rating: {rating} | Num Ratings: {num_ratings}")
                
        except Exception as e:
            print(f"  -> [{idx}] Failed to scrape: {e}")

async def main():
    print(f"Loading {EXCEL_FILE}...")
    try:
        df = pd.read_excel(EXCEL_FILE)
        
        # Ensure target columns can accept mixed types
        if 'Amazon Num_Primary_Books_in_Series' not in df.columns:
            df['Amazon Num_Primary_Books_in_Series'] = None
        if 'Amazon Total_Page_Count_of_Primary_Books' not in df.columns:
            df['Amazon Total_Page_Count_of_Primary_Books'] = None
        if 'Amazon Book1_Rating' not in df.columns:
            df['Amazon Book1_Rating'] = None
        if 'Amazon Book1_Num_Ratings' not in df.columns:
            df['Amazon Book1_Num_Ratings'] = None

        df['Amazon Num_Primary_Books_in_Series'] = df['Amazon Num_Primary_Books_in_Series'].astype(object)
        df['Amazon Total_Page_Count_of_Primary_Books'] = df['Amazon Total_Page_Count_of_Primary_Books'].astype(object)
        df['Amazon Book1_Rating'] = df['Amazon Book1_Rating'].astype(object)
        df['Amazon Book1_Num_Ratings'] = df['Amazon Book1_Num_Ratings'].astype(object)
            
    except Exception as e:
        print(f"Failed to load Excel file: {e}")
        return
        
    scraper = AmazonScraper()
    scraper.headless = False

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        sem = asyncio.Semaphore(CONCURRENCY)
        
        tasks = []
        missing_count = 0
        
        # Only process the last 100 rows
        start_idx = max(0, len(df) - 100)
        for idx in range(start_idx, len(df)):
            row = df.iloc[idx]
            url = row.get('Amazon URL')
            
            # Skip if no URL
            if pd.isna(url) or not str(url).startswith('http'):
                continue
                
            # Filter for rows missing Column H (Num Books) or Column I (Pages)
            num_books = row.get('Amazon Num_Primary_Books_in_Series')
            pages = row.get('Amazon Total_Page_Count_of_Primary_Books')
            
            def is_missing(val):
                return pd.isna(val) or str(val).strip() == '' or str(val).lower() == 'nan'
                
            if is_missing(num_books) or is_missing(pages):
                tasks.append(process_row(idx, row, df, context, scraper, sem))
                missing_count += 1
                
        print(f"Found {missing_count} rows with missing ratings.")
        
        # We can chunk them so we save progress periodically
        chunk_size = 50
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i+chunk_size]
            print(f"Processing chunk {i//chunk_size + 1}...")
            await asyncio.gather(*chunk)
            
            print("Saving partial updates to Excel...")
            try:
                df.to_excel(EXCEL_FILE, index=False)
            except Exception as e:
                print(f"Failed to save to Excel: {e}")
            
        await browser.close()
        
    print("\nExtraction complete for all missing rows!")

if __name__ == "__main__":
    asyncio.run(main())
