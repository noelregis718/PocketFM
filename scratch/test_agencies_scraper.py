import asyncio
import os
import re
from playwright.async_api import async_playwright

async def main():
    url = 'https://www.goodreads.com/book/show/75712384-bonded-by-thorns'
    print(f"Testing URL: {url}")
    
    user_data_dir = os.path.join(r"E:\Internship\PocketFM", "playwright_goodreads_profile")
    
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2)
            
            # Find the series link
            series_link = None
            series_el = await page.query_selector('a[href*="/series/"]')
            if series_el:
                series_link = await series_el.get_attribute("href")
                if not series_link.startswith('http'):
                    series_link = "https://www.goodreads.com" + series_link
                print(f"Found Series Link: {series_link}")
            
            if not series_link:
                print("Could not find series link!")
                return
                
            # Go to series link
            series_page = await context.new_page()
            try:
                await series_page.goto(series_link, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2)
                
                # Get all books in the series
                book_items = await series_page.query_selector_all('.listWithDividers__item')
                print(f"Found {len(book_items)} items in series list")
                
                total_pages = 0
                primary_books = 0
                
                for item in book_items:
                    # Check if it's a primary book (e.g., Book 1, Book 2)
                    h3_el = await item.query_selector('h3')
                    if not h3_el: continue
                    h3_text = await h3_el.inner_text()
                    
                    if not re.search(r'Book\s+\d+$', h3_text.strip(), re.IGNORECASE):
                        print(f"Skipping non-primary: {h3_text}")
                        continue
                        
                    primary_books += 1
                    
                    # Extract page count if available directly on series page? 
                    # Goodreads series page usually does NOT show page count, only rating/editions.
                    # We have to click into the book!
                    book_link_el = await item.query_selector('a[href^="/book/show/"]')
                    if not book_link_el: 
                        print(f"  -> Could not find book link for {h3_text}")
                        continue
                    
                    b_url = await book_link_el.get_attribute("href")
                    if not b_url.startswith('http'): b_url = "https://www.goodreads.com" + b_url
                    
                    b_page = await context.new_page()
                    try:
                        await b_page.goto(b_url, wait_until="domcontentloaded", timeout=30000)
                        
                        extracted_pages = 0
                        # Try LD JSON
                        ld_el = await b_page.query_selector('script[type="application/ld+json"]')
                        if ld_el:
                            import json
                            try:
                                data = json.loads(await ld_el.inner_text())
                                if isinstance(data, list): data = data[0]
                                if 'numberOfPages' in data:
                                    extracted_pages = int(data['numberOfPages'])
                            except: pass
                            
                        # Try UI fallback
                        if not extracted_pages:
                            p_el = await b_page.query_selector('[data-testid="pagesFormat"]')
                            if p_el:
                                p_match2 = re.search(r'(\d+)\s*pages', await p_el.inner_text(), re.IGNORECASE)
                                if p_match2: extracted_pages = int(p_match2.group(1))
                                
                        print(f"  -> {h3_text.strip()}: {extracted_pages} pages")
                        total_pages += extracted_pages
                    except Exception as e:
                        print(f"Error on {b_url}: {e}")
                    finally:
                        await b_page.close()
                        
                print(f"\\nFINAL RESULT:")
                print(f"Total Primary Books: {primary_books}")
                print(f"Total Pages: {total_pages}")
                
            finally:
                await series_page.close()
                
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await page.close()
            await context.close()

if __name__ == "__main__":
    asyncio.run(main())
