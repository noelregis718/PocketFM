import asyncio
import os
import re
import urllib.parse
from playwright.async_api import async_playwright

async def main():
    series_name = "Secret Games"
    author_name = ""
    
    print(f"Testing Fallback Search for '{series_name}' by '{author_name}'...")
    
    user_data_dir = os.path.join(r"E:\Internship\PocketFM", "playwright_goodreads_profile")
    
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        fallback_search_url = f"https://www.goodreads.com/search?q={urllib.parse.quote_plus(str(series_name) + ' ' + str(author_name))}"
        search_page = await context.new_page()
        try:
            await search_page.goto(fallback_search_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2)
            await search_page.screenshot(path="scratch/search_fallback.png")
            
            html = await search_page.content()
            with open("scratch/search_fallback.html", "w", encoding="utf-8") as f:
                f.write(html)
            print("Wrote HTML to scratch/search_fallback.html")
            return
            
            for item in book_items:
                title_el_2 = await item.query_selector('a.bookTitle')
                if not title_el_2: continue
                item_text = await title_el_2.inner_text()
                
                if str(series_name).lower() not in item_text.lower():
                    continue
                    
                match = re.search(r'\((?:.+?)(?:,\s*#|,\s*Book\s+|\s+Book\s+|\s+#)([0-9a-zA-Z\.\-]+)\)', item_text, re.IGNORECASE)
                if match:
                    book_num = match.group(1)
                    if book_num.isdigit():
                        b_url_2 = await title_el_2.get_attribute("href")
                        if not b_url_2.startswith('http'): b_url_2 = "https://www.goodreads.com" + b_url_2
                        
                        b_page = await context.new_page()
                        try:
                            await b_page.goto(b_url_2, wait_until="domcontentloaded", timeout=30000)
                            extracted_pages = 0
                            ld_el = await b_page.query_selector('script[type="application/ld+json"]')
                            if ld_el:
                                import json
                                try:
                                    data = json.loads(await ld_el.inner_text())
                                    if isinstance(data, list): data = data[0]
                                    if 'numberOfPages' in data:
                                        extracted_pages = int(data['numberOfPages'])
                                except: pass
                                
                            if not extracted_pages:
                                p_el = await b_page.query_selector('[data-testid="pagesFormat"]')
                                if p_el:
                                    p_match2 = re.search(r'(\d+)\s*pages', await p_el.inner_text(), re.IGNORECASE)
                                    if p_match2: extracted_pages = int(p_match2.group(1))
                                    
                            print(f"    [Fallback] Book {book_num} '{item_text.strip()}': {extracted_pages} pages")
                            total_pages += extracted_pages
                            primary_books += 1
                        except Exception as e:
                            print(f"    [Fallback Error] {b_url_2}: {e}")
                        finally:
                            await b_page.close()
            
            print(f"  -> FALLBACK FINAL: {primary_books} primary books, {total_pages} total pages")
        finally:
            await search_page.close()
            await context.close()

if __name__ == "__main__":
    asyncio.run(main())
