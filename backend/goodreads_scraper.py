import asyncio
import json
import urllib.request
import urllib.parse
import time
from playwright.async_api import async_playwright

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
}

def get_autocomplete_book_url(query):
    api_url = f"https://www.goodreads.com/book/auto_complete?format=json&q={urllib.parse.quote_plus(query)}"
    for attempt in range(3):
        try:
            req = urllib.request.Request(api_url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as response:
                data = json.loads(response.read().decode("utf-8", errors="ignore"))
                if data and len(data) > 0:
                    book_path = data[0].get('bookUrl', '')
                    if book_path:
                        return "https://www.goodreads.com" + book_path
                return None
        except Exception as e:
            time.sleep(1)
    return None

import re
import unicodedata
import os

def clean_text(text):
    if not text:
        return ""
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_series_from_title(title):
    """Matches patterns like (Series Name #1) or (Series Name, Book 1) or [Series Name]."""
    if not title: return None
    # Look for patterns inside parentheses or brackets
    patterns = [
        r'\((.*?)(?:[\s,]+#?\d+|[\s,]+Book\s+\d+)?\)',
        r'\[(.*?)(?:[\s,]+#?\d+|[\s,]+Book\s+\d+)?\]'
    ]
    for pattern in patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            name = match.group(1).strip()
            # Clean trailing fluff
            name = re.sub(r'[\s#,\d]+$', '', name)
            if len(name) > 2: return name
    return None

def normalize_title_for_search(title):
    if not title: return ""
    t = str(title).lower()
    
    # Remove obvious platform fluff
    t = re.sub(r'\(.*?\)', '', t) # Remove everything in parens
    t = re.sub(r'\[.*?\]', '', t) # Remove everything in brackets
    
    remove_patterns = [
        r':\s+a\s+novel.*', r':\s+a\s+read\s+with\s+jenna\s+pick.*',
        r':\s+a\s+memoir.*', r'\(deluxe\s+edition\).*',
        r'\(special\s+edition\).*', r'\'s\s+broken\s+mate',
        r'book\s+\d+.*', r'vol(ume)?\s+\d+.*',
    ]
    for pattern in remove_patterns:
        t = re.sub(pattern, '', t, flags=re.IGNORECASE)
        
    t = re.split(r'[:\-—]', t)[0] # Split at colon, dash, em-dash
    t = re.sub(r'[^\w\s]', '', t) # Remove punctuation
    return t.strip()

class GoodreadsScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def login_to_goodreads(self, page):
        """Logs into Goodreads using stored credentials."""
        print("[Goodreads] Attempting login...")
        # Set a realistic User-Agent to avoid 403
        await page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        try:
            creds_path = os.path.join(os.path.dirname(__file__), "gr_creds.json")
            if not os.path.exists(creds_path):
                print("  [Goodreads] Error: gr_creds.json not found.")
                return False
                
            with open(creds_path, 'r') as f:
                creds = json.load(f)
                
            await page.goto("https://www.goodreads.com/user/sign_in", wait_until="domcontentloaded", timeout=60000)
            
            # Check if already logged in
            if await page.query_selector('a.headerPersonalNav__link[href*="/user/show"]'):
                print("  [Goodreads] Already logged in.")
                return True

            # If on the initial sign-in page, click "Sign in with email"
            try:
                email_btn = page.locator('a:has-text("Sign in with email")')
                if await email_btn.is_visible():
                    await email_btn.click()
                    await page.wait_for_load_state("networkidle")
            except: pass

            # Use Amazon-style selectors for Goodreads login
            if await page.locator("#ap_email").is_visible():
                await page.fill("#ap_email", creds['email'])
                await page.fill("#ap_password", creds['password'])
                
                # Check for CAPTCHA before submitting
                if await page.query_selector('#captcha-image, .captcha'):
                    print("  [Action Required] CAPTCHA detected! Please solve it in the browser window...")
                    await page.wait_for_selector('a.headerPersonalNav__link[href*="/user/show"]', timeout=300000)
                else:
                    await page.click("#signInSubmit")
                    print("  [Auto-Login] Credentials submitted...", flush=True)
                    
                    try:
                        print("  [Goodreads] Waiting for user profile to appear...", flush=True)
                        await page.wait_for_selector('.headerPersonalNav__link[href*="/user/show"], [data-testid="userProfileMenu"]', timeout=45000)
                        print("  [Goodreads] Login confirmed!", flush=True)
                    except:
                        print("  [Goodreads] Login confirmation timed out. Proceeding cautiously.")
            
            return True
        except Exception as e:
            print(f"  [Goodreads] Login error: {e}")
            return False

    async def search_author_books(self, page, author_name, max_books=5):
        """Wrapper for backward compatibility."""
        results = await self.search_author_books_with_links(page, author_name, max_books)
        return [r['title'] for r in results]

    async def search_author_books_with_links(self, page, author_name, max_books=5):
        """Searches for an author and returns titles AND links of their first few books."""
        try:
            print(f"    [Goodreads] Searching for author: {author_name}...", flush=True)
            search_url = f"https://www.goodreads.com/search?q={author_name.replace(' ', '+')}"
            await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2.5)
            
            author_link = await page.query_selector('a[href*="/author/show/"]')
            if not author_link:
                author_link = await page.query_selector('.authorName, .authorName__container a')

            if not author_link:
                print(f"    [Goodreads] Could not find author profile for: {author_name}", flush=True)
                return []
                
            author_url = await author_link.evaluate("el => el.href")
            await page.goto(author_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2.5)
            
            book_els = await page.query_selector_all('a.bookTitle, [data-testid="bookTitle"] a')
            results = []
            for el in book_els:
                title = (await el.inner_text()).strip()
                link = await el.evaluate("el => el.href")
                if link and link not in [r['link'] for r in results]:
                    results.append({'title': title, 'link': link})
                if len(results) >= max_books:
                    break
            
            return results
        except Exception as e:
            print(f"    [Goodreads] Author search error: {e}", flush=True)
            return []

    async def extract_book_details(self, page):
        """Extracts metadata from a currently open book page."""
        try:
            await asyncio.sleep(0.5)
            genres = []
            genre_els = await page.query_selector_all('[data-testid="genresList"] .Button__labelItem, .BookPageMetadataSection__genre a')
            for gel in genre_els:
                txt = clean_text(await gel.inner_text())
                if txt and txt not in genres: genres.append(txt)
            
            is_romantasy = "Yes" if any("romantasy" in g.lower() for g in genres) else "No"
            genre_main = genres[0] if genres else "N/A"
            genre_sub = genres[1] if len(genres) > 1 else "N/A"

            avg_rating = "N/A"
            rating_count = "N/A"
            try:
                ld_el = await page.query_selector('script[type="application/ld+json"]')
                if ld_el:
                    ld_data = json.loads(await ld_el.inner_text())
                    if isinstance(ld_data, list): ld_data = ld_data[0]
                    avg_rating = str(ld_data.get('aggregateRating', {}).get('ratingValue', 'N/A'))
                    rating_count = str(ld_data.get('aggregateRating', {}).get('ratingCount', 'N/A'))
            except: pass

            description = "N/A"
            desc_el = await page.query_selector('[data-testid="description"] .Formatted, .readable')
            if desc_el: description = clean_text(await desc_el.inner_text())

            # Series & Primary Book Logic
            series_url = "N/A"
            series_link = await page.query_selector('h3.Text__title3 a[href*="/series/"], [data-testid="series"] a')
            if series_link: series_url = await series_link.evaluate("el => el.href")
            
            series_data = {"Num_Primary_Books": "1", "Book1_Rating": avg_rating, "Book1_Num_Ratings": rating_count}
            if series_url != "N/A":
                try:
                    # We skip navigating to series for speed in multi-tab, but keep primary rating
                    pass
                except: pass

            title_el = await page.query_selector('[data-testid="bookTitle"], #bookTitle')
            title = clean_text(await title_el.inner_text()) if title_el else "N/A"

            # Author Logic
            author_name = "N/A"
            try:
                author_el = await page.query_selector('.ContributorLink__name, [data-testid="authorName"]')
                if author_el:
                    author_name = clean_text(await author_el.inner_text())
            except: pass

            return {
                "GoodReads_Series_URL": series_url,
                "GoodReads_Book_URL": page.url,
                "GoodReads_Rating": avg_rating,
                "Author_Found": author_name,
                "GoodReads_Rating_Count": rating_count,
                "Genre": genre_main,
                "Sub_Genre": genre_sub,
                "Description": description,
                "Num_Primary_Books": series_data["Num_Primary_Books"],
                "Book_Title": title,
                "All_Genres": genres[:7]
            }
        except Exception as e:
            print(f"    [Error] Details extraction: {e}")
            return None

    async def scrape_goodreads_data(self, context, title, author, isbn10="N/A", isbn13="N/A", asin="N/A", existing_url="N/A"):
        page = await context.new_page()
        # If title is missing, we'll try to find the author's top book first
        if not title or title == "N/A":
            if author and author != "N/A":
                # Use the existing context/page to find their first book
                # We'll reuse the current logic but on the main page
                try:
                    books = await self.search_author_books(page, author, max_books=1)
                    if books:
                        title = books[0]
                        print(f"    [Goodreads] Found top book for {author}: {title}", flush=True)
                    else:
                        await page.close()
                        return {}
                except Exception as e:
                    print(f"    [Goodreads] Author search failed: {e}", flush=True)
                    await page.close()
                    return {}
            else:
                await page.close()
                return {}

        try:
            book_url = None
            extracted_series = extract_series_from_title(title)
            
            book_url = existing_url if (existing_url and existing_url != "N/A" and str(existing_url).startswith("http")) else None
            
            # Skip search if we already have a valid book_url
            if book_url:
                print(f"    [Goodreads] Using existing URL: {book_url}")
                
            if not book_url:
                # --- TIER 1: Internal Search (Title + Author Only) ---
                # Use a "Clean" title + Author for search query
                clean_query_title = normalize_title_for_search(title)
                query = f"{clean_query_title} {author}".strip()
                book_url = await asyncio.to_thread(get_autocomplete_book_url, query)
                if book_url:
                    print(f"    [Goodreads] Autocomplete API Success! URL: {book_url}")
                try:
                    if not book_url:
                        print(f"    [Goodreads] Searching: {query}...")
                        # Set headers for the search too
                        await page.set_extra_http_headers({
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                        })
                        search_url = f"https://www.goodreads.com/search?q={query.replace(' ', '+')}"
                        await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                    
                        # Wait for results to actually populate
                        await asyncio.sleep(2.5)
                    
                        # Broad Selection: Evaluate top results for highest rating if title matches
                        results_data = []
                        rows = await page.query_selector_all('tr[itemtype="http://schema.org/Book"], [data-testid="bookSearchResult"]')
                    
                        if rows:
                            for row in rows[:5]:
                                title_el = await row.query_selector('a.bookTitle, [data-testid="bookTitle"] a, .bookTitle, h3 a[href*="/book/show/"]')
                                rating_el = await row.query_selector('.minirating, [data-testid="rating"]')
                                if title_el:
                                    b_title = await title_el.inner_text()
                                    b_link = await title_el.evaluate("el => el.href")
                                    b_rating = 0.0
                                    if rating_el:
                                        r_text = await rating_el.inner_text()
                                        r_match = re.search(r'([\d.]+)\s*avg', r_text.lower())
                                        if r_match: b_rating = float(r_match.group(1))
                                    results_data.append({"title": b_title.strip(), "link": b_link, "rating": b_rating})
                                
                        if results_data:
                            target_t = clean_query_title.lower()
                            matches = []
                            for r in results_data:
                                rt_clean = normalize_title_for_search(r["title"]).lower()
                                if target_t in rt_clean or rt_clean in target_t:
                                    matches.append(r)
                                
                            if not matches:
                                matches = results_data[:3]
                            
                            matches.sort(key=lambda x: x["rating"], reverse=True)
                            book_url = matches[0]["link"]
                            print(f"    [Goodreads] Selected highest rated match: '{matches[0]['title']}' ({matches[0]['rating']} avg rating)")
                    
                        if not book_url:
                            first_link = await page.query_selector('a.bookTitle, [data-testid="bookTitle"] a, .bookTitle, [data-testid="bookSearchResult"] a, h3 a[href*="/book/show/"]')
                            if not first_link:
                                if await page.query_selector('#captcha-image, .captcha, iframe[src*="captcha"]'):
                                    print(f"\n    [!!! ACTION REQUIRED !!!] CAPTCHA detected for '{title}'! Please solve it in the browser window. (5 minute timeout)")
                                try:
                                    await page.wait_for_selector('a.bookTitle, [data-testid="bookTitle"] a, .bookTitle', timeout=300000)
                                    first_link = await page.query_selector('a.bookTitle, [data-testid="bookTitle"] a, .bookTitle, [data-testid="bookSearchResult"] a')
                                except:
                                    print(f"    [Timeout] CAPTCHA not solved in 5 minutes. Falling back to Tier 2...")
                            if first_link:
                                book_url = await first_link.evaluate("el => el.href")
                                print(f"    [Goodreads] Success! Captured fallback result: {book_url}")
                except Exception as e:
                    print(f"    [Goodreads] Search error: {e}")
                
                # --- TIER 2: External Fallback (Title + Author Only) ---
                if not book_url:
                    search_queries = [
                        f'"{clean_query_title}" {author} goodreads',
                        f'"{title}" {author} site:goodreads.com/book'
                    ]
                    for query in search_queries:
                        brave_url = f"https://search.brave.com/search?q={query.replace(' ', '+')}"
                        try:
                            print(f"    [Goodreads] Trying external search: {query}...")
                            await page.goto(brave_url, wait_until="domcontentloaded", timeout=30000)
                            links = await page.query_selector_all('a[href*="goodreads.com/book/show/"]')
                            if links: 
                                book_url = await links[0].evaluate("el => el.href")
                                break
                        except: continue

            if not book_url: return {}
            
            # Step 2: Book Page Extraction
            if page.url != book_url or "/series/" in page.url:
                if "/series/" in book_url or "/series/" in page.url:
                    await page.goto(book_url, wait_until="domcontentloaded", timeout=45000)
                    first_book = await page.query_selector('a.bookTitle, [data-testid="bookTitle"] a, a[href*="/book/show/"]')
                    if first_book: book_url = await first_book.evaluate("el => el.href")
                await page.goto(book_url, wait_until="domcontentloaded", timeout=60000)
            
            await asyncio.sleep(0.5)
            genres = []
            genre_els = await page.query_selector_all('[data-testid="genresList"] .Button__labelItem, .BookPageMetadataSection__genre a')
            for gel in genre_els:
                txt = clean_text(await gel.inner_text())
                if txt and txt not in genres: genres.append(txt)
            
            is_romantasy = "Yes" if any("romantasy" in g.lower() for g in genres) else "No"
            genre_main = genres[0] if genres else "N/A"
            genre_sub = genres[1] if len(genres) > 1 else "N/A"

            avg_rating = "N/A"
            rating_count = "N/A"
            try:
                ld_el = await page.query_selector('script[type="application/ld+json"]')
                if ld_el:
                    ld_data = json.loads(await ld_el.inner_text())
                    if isinstance(ld_data, list): ld_data = ld_data[0]
                    avg_rating = str(ld_data.get('aggregateRating', {}).get('ratingValue', 'N/A'))
                    rating_count = str(ld_data.get('aggregateRating', {}).get('ratingCount', 'N/A'))
            except: pass

            description = "N/A"
            desc_el = await page.query_selector('[data-testid="description"] .Formatted, .readable')
            if desc_el: description = clean_text(await desc_el.inner_text())
            
            num_pages = "N/A"
            try:
                pages_el = await page.query_selector('[data-testid="pagesFormat"]')
                if pages_el:
                    pages_text = clean_text(await pages_el.inner_text())
                    m = re.search(r'(\d+)\s+pages', pages_text, re.IGNORECASE)
                    if m: num_pages = m.group(1)
            except: pass

            # Series Logic
            series_url = "N/A"
            series_link = await page.query_selector('h3.Text__title3 a[href*="/series/"], [data-testid="series"] a')
            if series_link: series_url = await series_link.evaluate("el => el.href")
            
            series_data = {"Num_Primary_Books": "1", "Book1_Rating": avg_rating, "Book1_Num_Ratings": rating_count}
            if series_url != "N/A":
                try:
                    await page.goto(series_url, wait_until="domcontentloaded", timeout=60000)
                    content = await page.content()
                    m = re.search(r'(\d+)\s+primary\s+works', content, re.IGNORECASE)
                    if m: series_data["Num_Primary_Books"] = m.group(1)
                    
                    row1 = await page.query_selector('.listWithDividers__item, .seriesWork')
                    if row1:
                        rtxt = (await row1.inner_text()).lower()
                        r_match = re.search(r'([\d.]+)\s+avg\s+rating\s+[—\-]\s+([\d,]+)\s+ratings', rtxt)
                        if r_match:
                            series_data["Book1_Rating"] = r_match.group(1)
                            series_data["Book1_Num_Ratings"] = r_match.group(2).replace(',', '')
                except: pass

            author_found = "Unknown"
            author_el = await page.query_selector('[data-testid="authorName"], .authorName__container [itemprop="name"], span.ContributorLink__name, .ContributorLink__name, [data-testid="name"], h1 [itemprop="name"], .authorName')
            if author_el:
                author_found = clean_text(await author_el.inner_text())

            book_title_found = title # Default to search title
            title_el = await page.query_selector('[data-testid="bookTitle"], #bookTitle')
            if title_el:
                book_title_found = clean_text(await title_el.inner_text())

            return {
                "GoodReads_Series_URL": series_url,
                "GoodReads_Book_URL": page.url,
                "GoodReads_Rating": avg_rating,
                "GoodReads_Rating_Count": rating_count,
                "Genre": genre_main,
                "Sub_Genre": genre_sub,
                "Romantasy_Subgenre": is_romantasy,
                "Description": description,
                "Num_Primary_Books": series_data["Num_Primary_Books"],
                "Book1_Rating": series_data["Book1_Rating"],
                "Book1_Num_Ratings": series_data["Book1_Num_Ratings"],
                "Author_Found": author_found,
                "Book_Title": book_title_found,
                "Num_Pages": num_pages,
                "All_Genres": genres[:7]
            }
        except Exception as e:
            print(f"  Goodreads: Error: {e}")
            return {}
        finally:
            await page.close()

    async def scrape_top_books_by_author(self, context, author_name, count=2):
        """Finds the top `count` highest-rated books for an author and scrapes them."""
        page = await context.new_page()
        results = []
        top_books = []
        try:
            print(f"    [Goodreads] Searching for author: {author_name}...", flush=True)
            search_url = f"https://www.goodreads.com/search?q={author_name.replace(' ', '+')}"
            await page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })
            await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2.5)
            
            author_link = await page.query_selector('a[href*="/author/show/"]')
            if not author_link:
                author_link = await page.query_selector('.authorName, .authorName__container a')

            if not author_link:
                print(f"    [Goodreads] Could not find author profile for: {author_name}", flush=True)
                return []
                
            author_url = await author_link.evaluate("el => el.href")
            await page.goto(author_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2.5)
            
            book_els = await page.query_selector_all('tr[itemtype="http://schema.org/Book"]')
            book_candidates = []
            
            for row in book_els[:15]:
                title_el = await row.query_selector('a.bookTitle')
                rating_el = await row.query_selector('.minirating')
                if title_el and rating_el:
                    title = (await title_el.inner_text()).strip()
                    link = await title_el.evaluate("el => el.href")
                    r_text = await rating_el.inner_text()
                    r_match = re.search(r'([\d.]+)\s*avg', r_text.lower())
                    rating = float(r_match.group(1)) if r_match else 0.0
                    
                    if link not in [b['link'] for b in book_candidates]:
                        book_candidates.append({'title': title, 'link': link, 'rating': rating})
            
            if book_candidates:
                book_candidates.sort(key=lambda x: x['rating'], reverse=True)
                top_books = book_candidates[:count]
                for b in top_books:
                    print(f"    [Goodreads] Selected '{b['title']}' ({b['rating']} avg rating)")
        except Exception as e:
            print(f"    [Goodreads] Author scrape error: {e}", flush=True)
        finally:
            await page.close()
            
        for b in top_books:
            details = await self.scrape_goodreads_data(context, title=b['title'], author=author_name, existing_url=b['link'])
            if details:
                results.append(details)
                
        return results
