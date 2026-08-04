import asyncio
import pandas as pd
import re
import os
from playwright.async_api import async_playwright
import format_excel

EXCEL_FILE = r"e:\Internship\PocketFM\vanshika_part2.xlsx"
TARGET_ROWS = [394, 988, 1235, 1645, 1612, 1770, 1811, 2309]
CONCURRENCY = 3

async def extract_genres(page):
    try:
        genre_btns = await page.query_selector_all('[data-testid="genresList"] button, [data-testid="genresList"] [role="button"]')
        for btn in genre_btns:
            if "...more" in (await btn.inner_text()).lower():
                await btn.click(force=True)
                await asyncio.sleep(0.5)
                break
    except Exception:
        pass

    genres = []
    genre_els = await page.query_selector_all('[data-testid="genresList"] .Button__labelItem, .BookPageMetadataSection__genre a')
    for gel in genre_els:
        txt = (await gel.inner_text()).strip()
        if txt and txt not in genres: genres.append(txt)
    return genres

async def extract_synopsis(page):
    desc_el = await page.query_selector('[data-testid="description"] .Formatted, .readable')
    if desc_el: 
        return (await desc_el.inner_text()).strip()
    return ""

async def extract_ratings(page):
    avg_rating = 0.0
    total_ratings = 0
    rating_el = await page.query_selector('div.RatingStatistics__rating')
    if rating_el:
        try: avg_rating = float((await rating_el.inner_text()).strip())
        except: pass
    count_el = await page.query_selector('[data-testid="ratingsCount"]')
    if count_el:
        try:
            count_txt = (await count_el.inner_text()).replace(',', '').split()[0]
            total_ratings = int(count_txt)
        except: pass
    return avg_rating, total_ratings

async def extract_pages(page):
    await asyncio.sleep(2)
    try:
        ld_el = await page.query_selector('script[type="application/ld+json"]')
        if ld_el:
            import json
            data = json.loads(await ld_el.inner_text())
            if isinstance(data, list): data = data[0]
            if 'numberOfPages' in data:
                return int(data['numberOfPages'])
    except: pass
    try:
        p_el = await page.query_selector('[data-testid="pagesFormat"]')
        if p_el:
            text = await p_el.inner_text()
            p_match2 = re.search(r'(\d+)\s*pages', text, re.IGNORECASE)
            if p_match2: return int(p_match2.group(1))
    except: pass
    try:
        btn = await page.query_selector('button:has-text("Book details")')
        if btn:
            await btn.click(force=True)
            await asyncio.sleep(1.5)
            p_el = await page.query_selector('[data-testid="pagesFormat"]')
            if p_el:
                text = await p_el.inner_text()
                p_match2 = re.search(r'(\d+)\s*pages', text, re.IGNORECASE)
                if p_match2: return int(p_match2.group(1))
    except: pass
    try:
        meta = await page.query_selector('meta[property="books:page_count"]')
        if meta:
            content = await meta.get_attribute('content')
            if content and content.isdigit(): return int(content)
    except: pass
    try:
        content = await page.content()
        matches = re.findall(r'(\d+)\s*pages', content, re.IGNORECASE)
        if matches: return int(matches[0])
    except: pass
    return 0

def _apply_romantasy_checker(df, index, row, genres):
    classification = "Fail"
    all_tags = []
    def add_tags(val):
        if pd.isna(val) or not val: return
        parts = [p.strip() for p in str(val).split(',')]
        for p in parts:
            if p and p not in all_tags: all_tags.append(p)
    add_tags(row.get('Genre'))
    for g in genres: add_tags(g)
    add_tags(row.get('Keyword'))
    fantasy_kws = ['fantasy', 'paranormal', 'supernatural', 'magic', 'fae', 'witch', 'vampire', 'dragon', 'mythology', 'fairy tale', 'monster', 'isekai', 'reincarnation', 'sci-fi', 'beast']
    romance_kws = ['romance', 'romantasy', 'romantic', 'love', 'mate', 'heart', 'beauty']
    idx_f, idx_r = -1, -1
    for i, tag in enumerate(all_tags):
        tag_lower = tag.lower()
        if idx_f == -1 and any(kw in tag_lower for kw in fantasy_kws): idx_f = i
        if idx_r == -1 and any(kw in tag_lower for kw in romance_kws): idx_r = i
    for i, tag in enumerate(all_tags):
        if 'romantasy' in tag.lower():
            if idx_f == -1 or i < idx_f: idx_f = i
            if idx_r == -1 or i < idx_r: idx_r = i
    if idx_f != -1 and idx_r != -1:
        rank = max(idx_f, idx_r)
        if rank < 5: classification = "Strong Match"
        elif rank < 9: classification = "Confirmed Match"
        else: classification = "Weak Match"
    num_books = df.at[index, "Num_Primary_Books_in_Series"]
    if pd.notna(num_books):
        try:
            if float(num_books) < 3: classification = "Weak Match"
        except: pass
    df.at[index, "Romantasy Checker"] = classification

async def process_row(index, row, df, context, sem):
    async with sem:
        existing_url = str(row.get("GoodReads_Series_URL", "")).strip()
        if not existing_url or existing_url.lower() == 'nan' or existing_url == 'none':
            print(f"[{index}] No Goodreads URL for this row. Skipping.")
            return

        print(f"\n--- Scraping Row {index} / Excel {index+2} ---")
        print(f"[{index}] URL: {existing_url}")

        page = await context.new_page()
        try:
            await page.goto(existing_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(2)
            
            is_series = "/series/" in existing_url
            genres = []
            
            if not is_series:
                genres = await extract_genres(page)
                if genres: df.at[index, "Genre Tags"] = ", ".join(genres)
                synopsis = await extract_synopsis(page)
                if synopsis: df.at[index, "Synopsis"] = synopsis
                rating, count = await extract_ratings(page)
                if rating: df.at[index, "Book1_Rating"] = rating
                if count: df.at[index, "Book1_Num_Ratings"] = count
                pages = await extract_pages(page)
                df.at[index, "Num_Primary_Books_in_Series"] = 1
                if pages: df.at[index, "Total_Page_Count_of_Primary_Books"] = pages
                print(f"[{index}] Standalone Pages updated to: {pages}")
            else:
                book_items = await page.query_selector_all('.listWithDividers__item, .seriesWork')
                num_primary_books = 0
                total_page_count = 0
                book1_processed = False
                for item in book_items:
                    item_text = await item.inner_text()
                    match = re.search(r'Book\s+([0-9a-zA-Z\.\-]+)', item_text, re.IGNORECASE)
                    if match:
                        book_num = match.group(1)
                        if book_num.isdigit():
                            num_primary_books += 1
                            b_link = await item.query_selector('a.bookTitle, a[href*="/book/show"]')
                            if b_link:
                                b_url = await b_link.evaluate("el => el.href")
                                b_page = await context.new_page()
                                try:
                                    await b_page.goto(b_url, wait_until="domcontentloaded", timeout=45000)
                                    if book_num == "1":
                                        genres = await extract_genres(b_page)
                                        if genres: df.at[index, "Genre Tags"] = ", ".join(genres)
                                        synopsis = await extract_synopsis(b_page)
                                        if synopsis: df.at[index, "Synopsis"] = synopsis
                                        rating, count = await extract_ratings(b_page)
                                        if rating: df.at[index, "Book1_Rating"] = rating
                                        if count: df.at[index, "Book1_Num_Ratings"] = count
                                        book1_processed = True
                                    extracted_pages = await extract_pages(b_page)
                                    if extracted_pages:
                                        total_page_count += extracted_pages
                                        print(f"[{index}] Book {book_num} Pages: {extracted_pages}")
                                    else:
                                        print(f"[{index}] Book {book_num} Pages: Not found")
                                except Exception as e:
                                    print(f"[{index}] Error on Book {book_num}: {e}")
                                finally:
                                    await b_page.close()
                if num_primary_books == 0:
                    num_primary_books = max(1, len(book_items))
                df.at[index, "Num_Primary_Books_in_Series"] = num_primary_books
                df.at[index, "Total_Page_Count_of_Primary_Books"] = total_page_count
                print(f"[{index}] Series Scraped. Primary Books: {num_primary_books}, Total Pages: {total_page_count}")
            _apply_romantasy_checker(df, index, row, genres)
        except Exception as e:
            print(f"[{index}] Error processing {existing_url}: {e}")
        finally:
            await page.close()

async def run_scraper():
    print(f"Loading {EXCEL_FILE}...")
    try:
        df = pd.read_excel(EXCEL_FILE)
    except Exception as e:
        print(f"Excel load error: {e}")
        return

    # Create tasks only for targeted rows
    target_indices = []
    for i, row in df.iterrows():
        # Check if i or i+2 is in the user's list
        if i in TARGET_ROWS or (i+2) in TARGET_ROWS or (i+1) in TARGET_ROWS:
            target_indices.append(i)

    print(f"Targeting indices: {target_indices}")
    if not target_indices:
        print("No valid target rows found.")
        return

    user_data_dir = os.path.join(r"e:\Internship\PocketFM", "playwright_goodreads_profile_missing")
    
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=False,
            args=['--disable-blink-features=AutomationControlled'],
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 800}
        )
        
        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = []
        for index in target_indices:
            row = df.iloc[index]
            tasks.append(process_row(index, row, df, context, sem))
            
        await asyncio.gather(*tasks)
        await context.close()
        
    try:
        df.to_excel(EXCEL_FILE, index=False)
        print(f"Specific rows saved successfully.")
        format_excel.apply_styling(EXCEL_FILE)
    except Exception as e:
        print(f"Failed to save specific rows: {e}")

if __name__ == "__main__":
    asyncio.run(run_scraper())
