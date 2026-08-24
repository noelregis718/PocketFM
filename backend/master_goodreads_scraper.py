import asyncio
import pandas as pd
import re
import os
import urllib.request
import urllib.parse
import json
from playwright.async_api import async_playwright
import format_excel

from transformers import pipeline
from keybert import KeyBERT
from langdetect import detect
from deep_translator import GoogleTranslator
import warnings
warnings.filterwarnings("ignore")

# --- AI ENSEMBLE INITIALIZATION ---
print("Loading AI Ensemble Models... (This may take a moment)")
try:
    hf_classifier = pipeline("zero-shot-classification", model="cross-encoder/nli-distilroberta-base", device=-1)
    kw_extractor = KeyBERT()
    print("AI Models Loaded Successfully.")
except Exception as e:
    print(f"Failed to load AI models. Please ensure packages are installed. Error: {e}")
    hf_classifier = None
    kw_extractor = None

EXCEL_FILE = r"E:\Internship\PocketFM\Combined List of Titles.xlsx"
START_ROW = 12682
TARGET_ROWS = 13182
CONCURRENCY = 5
BATCH_SIZE = 50

# We use urllib for the Autocomplete API to bypass AWS WAF
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
}

def get_autocomplete_book_url(query):
    """Uses raw HTTP to bypass WAF and get the direct Book URL."""
    import time
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

def is_row_complete(row):
    """Checks if a row has absolutely all required fields fully populated."""
    def valid(val):
        s = str(val).strip().lower()
        return s != '' and s != 'nan' and s != 'none'
        
    return (
        valid(row.get("Goodreads Link")) and
        valid(row.get("Genre Tags")) and
        valid(row.get("Synopsis")) and
        valid(row.get("GoodReads_Series_URL")) and
        valid(row.get("Goodreads Primary Books Number")) and
        valid(row.get("Goodreads Primary Books Page Count")) and
        valid(row.get("Romantasy Checker"))
    )

async def process_row(index, row, df, context, sem):
    async with sem:
        book_name = str(row.get("Book Title", row.get("Book Name", row.get("Book 1 Title", "")))).strip()
        
        # SANITIZE BOOK NAME: Take only the part before a colon to avoid confusing the search
        if ":" in book_name:
            book_name = book_name.split(":")[0].strip()
            
        author_name = str(row.get("Author Name", "")).strip()
        
        if not book_name or book_name.lower() == 'nan':
            return
            
        if is_row_complete(row):
            print(f"[{index}] Row is fully complete (URL, Books, Pages, Genres, Synopsis, Romantasy). Skipping.")
            return

        print(f"\n--- Processing Row {index + 1} ---")

        # 1. Check if we already have the Book URL saved from a previous partial run
        book_url = str(row.get("Goodreads Link", "")).strip()
        
        if not book_url or book_url.lower() == 'nan' or book_url.lower() == 'none':
            if not author_name or author_name.lower() == 'nan' or author_name.lower() == 'none':
                print(f"[{index}] No valid author found. Querying: '{book_name}'")
                search_query = book_name
            else:
                print(f"[{index}] Querying Book + Author: '{book_name}' by '{author_name}'")
                search_query = f"{book_name} {author_name}"

            # 2. Bypass WAF using Autocomplete API
            book_url = await asyncio.to_thread(get_autocomplete_book_url, search_query)
            if not book_url:
                print(f"[{index}] Book not found in Autocomplete API. Falling back to Aggressive UI Search...")
                search_page = await context.new_page()
                try:
                    search_url = f"https://www.goodreads.com/search?q={urllib.parse.quote_plus(search_query)}"
                    await search_page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(2)
                    
                    b_link = await search_page.query_selector('a.bookTitle')
                    if b_link:
                        book_url = await b_link.evaluate("el => el.href")
                        print(f"[{index}] Found Book URL via Aggressive Search: {book_url}")
                    else:
                        print(f"[{index}] Still not found via Aggressive Search. Leaving completely blank for future retry.")
                        return
                except Exception as e:
                    print(f"[{index}] Aggressive Search failed: {e}. Leaving completely blank for future retry.")
                    return
                finally:
                    await search_page.close()
            else:
                print(f"[{index}] Bypassed WAF. Found Book URL: {book_url}")
                
            # Store the book URL explicitly!
            df.at[index, "Goodreads Link"] = book_url
        else:
            print(f"[{index}] Reusing existing Goodreads Link from previous run: {book_url}")
            
        # 3. Load Book Page using Playwright to render React
        page = await context.new_page()
        try:
            await page.goto(book_url, wait_until="domcontentloaded", timeout=45000)
            # Short sleep to allow React to inject data
            await asyncio.sleep(2)
            
            # EXPAND GENRES (Click "...more" if present)
            try:
                genre_btns = await page.query_selector_all('[data-testid="genresList"] button, [data-testid="genresList"] [role="button"]')
                for btn in genre_btns:
                    if "...more" in (await btn.inner_text()).lower():
                        await btn.click(force=True)
                        await asyncio.sleep(0.5)
                        break
            except Exception:
                pass

            # EXTRACT GENRES
            genres = []
            genre_els = await page.query_selector_all('[data-testid="genresList"] .Button__labelItem, .BookPageMetadataSection__genre a')
            for gel in genre_els:
                txt = (await gel.inner_text()).strip()
                if txt and txt not in genres: genres.append(txt)
            if genres:
                df.at[index, "Genre Tags"] = ", ".join(genres)
                
            # EXTRACT SYNOPSIS
            desc_el = await page.query_selector('[data-testid="description"] .Formatted, .readable')
            if desc_el: 
                df.at[index, "Synopsis"] = (await desc_el.inner_text()).strip()
                
            # EXTRACT RATINGS FOR STEP 4
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
                
            df.at[index, "Goodreads Rating Book 1"] = avg_rating
            df.at[index, "Goodreads No. of Ratings Book 1"] = total_ratings
                
            # Romantasy classification will happen at the end to include series length
            
            # 4. Find Series Link dynamically
            series_tag = await page.query_selector('h3.Text__title3 a[href*="/series/"], [data-testid="series"] a, div.BookPageTitleSection__title a[href*="/series/"], a.infoBoxRowItem[href*="/series/"]')
            if not series_tag:
                print(f"[{index}] No Series link found on Book Page. It is likely a standalone.")
                df.at[index, "GoodReads_Series_URL"] = book_url
                
                # Fallback to book's own page count if standalone
                df.at[index, "Goodreads Primary Books Number"] = 1
                extracted_pages = 0
                
                try:
                    ld_el = await page.query_selector('script[type="application/ld+json"]')
                    if ld_el:
                        import json
                        data = json.loads(await ld_el.inner_text())
                        if isinstance(data, list): data = data[0]
                        if 'numberOfPages' in data:
                            extracted_pages = int(data['numberOfPages'])
                except: pass
                
                if not extracted_pages:
                    pages_el = await page.query_selector('[data-testid="pagesFormat"]')
                    if pages_el:
                        pages_text = await pages_el.inner_text()
                        p_match = re.search(r'(\d+)\s*pages', pages_text, re.IGNORECASE)
                        if p_match: extracted_pages = int(p_match.group(1))
                        
                if not extracted_pages:
                    try:
                        content = await page.content()
                        matches = re.findall(r'(\d+)\s*pages', content, re.IGNORECASE)
                        if matches:
                            extracted_pages = int(matches[0])
                    except: pass
                    
                df.at[index, "Goodreads Primary Books Page Count"] = extracted_pages
                print(f"[{index}] Standalone Book Pages: {extracted_pages}")
                
                await asyncio.to_thread(_apply_romantasy_checker, df, index, row, genres)
                return
                
            # 5. Extract Series URL and navigate
            series_url = await series_tag.evaluate("el => el.href")
            print(f"[{index}] Found Series Link via React: {series_url}")
            df.at[index, "GoodReads_Series_URL"] = series_url
            
            await page.goto(series_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(1)
            
            # 6. Parse Series List
            num_primary_books = 0
            total_page_count = 0
            all_pages_successful = True
            
            book_items = await page.query_selector_all('.listWithDividers__item, .seriesWork')
            
            for item in book_items:
                item_text = await item.inner_text()
                
                # Extract the exact book number string (e.g., '1', '1.5', '1-3', '1A')
                match = re.search(r'Book\s+([0-9a-zA-Z\.\-]+)', item_text, re.IGNORECASE)
                if match:
                    book_num = match.group(1)
                    # Check if it is purely a whole number (primary book)
                    if book_num.isdigit():
                        num_primary_books += 1
                        
                        # Aggressive scraping: Always visit the book page to get accurate page count
                        b_link = await item.query_selector('a.bookTitle, a[href*="/book/show"]')
                        if b_link:
                            b_url = await b_link.evaluate("el => el.href")
                            b_page = await context.new_page()
                            try:
                                await b_page.goto(b_url, wait_until="domcontentloaded", timeout=30000)
                                await asyncio.sleep(1)
                                
                                # If this is Book 1, scrape the definitive rating and replace the default one
                                if book_num == "1":
                                    b_rating_el = await b_page.query_selector('div.RatingStatistics__rating')
                                    if b_rating_el:
                                        try: df.at[index, "Goodreads Rating Book 1"] = float((await b_rating_el.inner_text()).strip())
                                        except: pass
                                        
                                    b_count_el = await b_page.query_selector('[data-testid="ratingsCount"]')
                                    if b_count_el:
                                        try:
                                            b_count_txt = (await b_count_el.inner_text()).replace(',', '').split()[0]
                                            df.at[index, "Goodreads No. of Ratings Book 1"] = int(b_count_txt)
                                        except: pass
                                        
                                # Robust Page Extraction with Retry
                                extracted_pages = 0
                                
                                for attempt in range(2):
                                    if extracted_pages > 0:
                                        break
                                        
                                    if attempt == 1:
                                        print(f"[{index}]   - Book {book_num} pages was 0. Rechecking (Attempt 2)...")
                                        await b_page.reload(wait_until="domcontentloaded")
                                        await asyncio.sleep(2)
                                        
                                    # 1. Try JSON-LD
                                    try:
                                        ld_el = await b_page.query_selector('script[type="application/ld+json"]')
                                        if ld_el:
                                            import json
                                            data = json.loads(await ld_el.inner_text())
                                            if isinstance(data, list): data = data[0]
                                            if 'numberOfPages' in data:
                                                extracted_pages = int(data['numberOfPages'])
                                    except: pass
                                    
                                    # 2. Try simple data-testid
                                    if not extracted_pages:
                                        p_el = await b_page.query_selector('[data-testid="pagesFormat"]')
                                        if p_el:
                                            p_match2 = re.search(r'(\d+)\s*pages', await p_el.inner_text(), re.IGNORECASE)
                                            if p_match2: extracted_pages = int(p_match2.group(1))
                                    
                                    # 3. Try clicking Book details
                                    if not extracted_pages:
                                        try:
                                            btn = await b_page.query_selector('button:has-text("Book details")')
                                            if btn:
                                                await btn.click(force=True)
                                                await asyncio.sleep(1)
                                                p_el = await b_page.query_selector('[data-testid="pagesFormat"]')
                                                if p_el:
                                                    p_match2 = re.search(r'(\d+)\s*pages', await p_el.inner_text(), re.IGNORECASE)
                                                    if p_match2: extracted_pages = int(p_match2.group(1))
                                        except: pass
                                        
                                    # 4. Fallback Regex across entire content
                                    if not extracted_pages:
                                        try:
                                            content = await b_page.content()
                                            matches = re.findall(r'(\d+)\s*pages', content, re.IGNORECASE)
                                            if matches:
                                                extracted_pages = int(matches[0])
                                        except: pass

                                if extracted_pages:
                                    total_page_count += extracted_pages
                                    print(f"[{index}]   - Primary Book {book_num} | {extracted_pages} pages (fetched)")
                                else:
                                    print(f"[{index}]   - Primary Book {book_num} | 0 pages (not found)")
                                    all_pages_successful = False
                            except Exception as e:
                                print(f"[{index}]   - Failed to scrape Book {book_num}: {e}")
                            finally:
                                await b_page.close()
                                    
            if num_primary_books == 0:
                num_primary_books = max(1, len(book_items))
                print(f"[{index}]   - Warning: 0 primary books parsed. Defaulting to {num_primary_books}.")
                
            if not all_pages_successful:
                print(f"[{index}] Aggressive Scraping Aborted. Goodreads blocked or failed to load a book's pages.")
                return
                
            print(f"[{index}] Aggressive Scraping Complete. Primary Books: {num_primary_books} | Total Pages: {total_page_count}")
            df.at[index, "Goodreads Primary Books Number"] = num_primary_books
            df.at[index, "Goodreads Primary Books Page Count"] = total_page_count
            
            await asyncio.to_thread(_apply_romantasy_checker, df, index, row, genres)

        except Exception as e:
            print(f"[{index}] Error rendering page via Playwright: {e}")
        finally:
            await page.close()

def _apply_romantasy_checker(df, index, row, genres):
    import pandas as pd
    import re
    
    # 1. Negative Dealbreakers & Gold Authors
    negative_dealbreakers = ["science fiction", "sci-fi", "childrens", "middle grade", "historical non-fiction", "biography", "office romance", "billionaire romance"]
    gold_authors = ["sarah j. maas", "rebecca yarros", "jennifer l. armentrout", "laura thalassa", "raven kennedy", "carissa broadbent", "scarlett st. clair", "lexi ryan", "elise kova", "ali hazelwood", "kresley cole", "s.j. maas", "jennifer armentrout"]
    
    author_name = str(row.get('Author Name', '')).lower().strip()
    is_gold_author = any(gold in author_name for gold in gold_authors if gold)
    
    # Text Extraction & Normalization
    def clean_text(text):
        if pd.isna(text) or text is None: return ""
        text = str(text).lower()
        text = re.sub(r'[^\w\s]', ' ', text)
        return text

    col_title = clean_text(row.get('Book Title'))
    col_genres = clean_text(row.get('Genre')) + " " + clean_text(row.get('Genre Tags')) + " " + " ".join([clean_text(g) for g in genres])
    col_series = clean_text(row.get('Series Name'))
    col_logline = clean_text(row.get('Logline'))
    col_synopsis = clean_text(row.get('Synopsis'))
    
    all_text = f"{col_title} {col_genres} {col_series} {col_logline} {col_synopsis}"
    
    # Step 2: Negative Screen
    if not is_gold_author:
        for db in negative_dealbreakers:
            if re.search(r'\b' + re.escape(db) + r'(?:s|es)?\b', all_text):
                df.at[index, "Romantasy Checker"] = "No"
                print(f"[{index}] Romantasy Checker: No (Dealbreaker) -> '{db}'")
                return

    # Subgenre Dictionaries
    subgenres = [
        {
            "name": "High Fantasy Court Adventure",
            "confirmed": ["fae court romance", "royal fantasy romance", "fantasy court romance", "kingdom romance", "royal heir romance", "noble fantasy romance", "unseelie court", "seelie court"],
            "master": ["royal court fantasy", "court intrigue", "political fantasy", "kingdom politics", "noble houses", "royal succession", "crown politics", "throne war", "castle court", "imperial court", "court mage", "royal heir", "kingdom alliance", "noble romance", "palace fantasy", "throne of", "crown of", "king and queen", "royal romance", "fae king", "prince romance", "princess romance", "royal intrigue", "court secrets", "fae prince"]
        },
        {
            "name": "Gothic Dark Romantasy",
            "confirmed": ["gothic romance", "dark fantasy romance", "cursed castle romance", "gothic fantasy romance", "gothic thriller"],
            "master": ["gothic romance", "haunted castle", "haunted manor", "dark curse", "cursed castle", "victorian fantasy", "grim romance", "gothic horror fantasy", "dark cathedral", "shadow manor", "tragic love fantasy", "macabre romance", "gothic manor", "dark estate", "gothic supernatural", "death romance", "forbidden desire", "undead romance", "haunted house romance", "dark lord", "forbidden love", "dark obsession", "monstrous lover", "dark magic", "shadow magic", "blood magic", "grimdark romance", "villain romance"]
        },
        {
            "name": "Dark Academia Romantasy",
            "confirmed": ["dark academia romance", "occult academy romance", "forbidden magic academy", "secret society romance", "cursed university romance"],
            "master": ["dark academia", "secret society", "forbidden knowledge", "occult studies", "elite academy", "cursed library", "forbidden ritual", "ancient grimoire", "magic university", "arcane college", "sinister campus", "forbidden spellwork", "hidden magic society", "dangerous knowledge", "magical conspiracy", "ink magic", "tarot academy", "clandestine order", "ivy league magic", "magical underground", "magical school", "dark school", "ancient magic", "secret magic", "deadly academy", "magic scholars", "scholar romance"]
        },
        {
            "name": "Monster Romance (Non-Shifter)",
            "confirmed": ["monster romance", "demon romance", "monster boyfriend", "alien monster romance", "non-human romance", "orc romance", "gargoyle romance", "naga romance"],
            "master": ["monster romance", "demon romance", "monster boyfriend", "vampire romance", "fae romance", "mer romance", "kraken romance", "minotaur romance", "alien romance", "creature romance", "non-human love interest", "monstrous partner", "immortal captor", "beast romance", "alien mate", "spider romance", "monster lover", "creature feature romance", "human and monster", "symbiote romance"]
        },
        {
            "name": "Werewolf / Shifter Romance",
            "confirmed": ["werewolf romance", "shifter romance", "fated mates", "wolf shifter romance", "rejected mate romance", "shifter mate"],
            "master": ["werewolf romance", "shifter romance", "fated mates", "rejected mate", "alpha romance", "lycan romance", "wolf pack romance", "bear shifter romance", "dragon shifter romance", "omegaverse romance", "pack romance", "mate bond", "true mate", "shifter pack", "animal shifter", "werebear", "werelion", "werepanther", "werecat", "alpha male", "luna", "pack dynamics", "wolf pack"]
        },
        {
            "name": "High-Stakes Games & Deadly Trials",
            "confirmed": ["fantasy tournament romance", "progression fantasy romance", "dungeon trials romance", "deadly trials romance", "death game romance", "magic tournament", "deadly games"],
            "master": ["deadly trials", "magical tournament", "battle tournament", "survival trials", "death competition", "trial by combat", "fantasy arena", "magic competition", "elimination tournament", "dungeon trials", "labyrinth challenge", "champion tournament", "fantasy contest", "survival arena", "final trial", "death game", "magical contest", "forced competition", "win or die", "chosen champion", "trial of", "survival romance", "blood tournament", "combat trials", "gladiator romance"]
        },
        {
            "name": "Mythology, Legend & Fairy Tale Retelling",
            "confirmed": ["beauty and the beast retelling", "hades and persephone", "greek mythology romance", "fairy tale retelling romance", "myth retelling romance"],
            "master": ["greek myth retelling", "norse myth retelling", "celtic legend", "arthurian legend", "fairy tale retelling", "hades and persephone", "cinderella retelling", "sleeping beauty retelling", "red riding hood retelling", "snow white retelling", "rapunzel retelling", "medusa retelling", "persephone retelling", "mythic retelling", "chinese mythology romance", "folklore retelling", "legend reimagined", "fairy tale romance", "myth romance", "goddess romance", "greek myth", "roman myth", "norse myth", "egyptian myth", "celtic myth", "arthurian romance", "camelot romance", "robin hood retelling", "peter pan retelling", "alice in wonderland retelling", "beauty and the beast", "rumplestiltskin retelling"]
        },
        {
            "name": "War College / Military Academy",
            "confirmed": ["magic academy romance", "military fantasy romance", "war college romance", "dragon rider academy romance", "combat academy romance"],
            "master": ["war college", "military academy", "officer academy", "cadet academy", "battle school", "combat academy", "warrior academy", "military fantasy", "officer training", "academy cadets", "battle training", "tactical academy", "war strategy", "military campaign", "combat training", "rider academy", "dragon rider", "flight academy", "wingleader", "cadet romance", "dragon school", "battle academy", "warrior school", "magic soldier", "warrior romance"]
        },
        {
            "name": "Korean Romance Fantasy / Isekai",
            "confirmed": ["villainess romance", "isekai romance", "litrpg romance", "transmigration romance", "reincarnation fantasy romance"],
            "master": ["otome isekai", "villainess", "reincarnated heroine", "transmigration", "regression fantasy", "reborn noblewoman", "korean web novel", "manhwa romance", "duke of the north", "tyrant romance", "second chance fantasy", "possession fantasy", "system fantasy romance", "korean fantasy novel", "isekai romance", "portal fantasy romance", "game world romance", "reincarnated into novel", "manhwa adaptation", "web novel romance", "transmigrated", "otome game", "webtoon romance", "manhwa", "regression", "time travel romance", "reborn as"]
        },
        {
            "name": "Paranormal Romance",
            "confirmed": ["vampire romance", "witch romance", "angel romance", "ghost romance", "psychic romance", "warlock romance"],
            "master": ["vampire romance", "witch romance", "demon romance", "angel romance", "ghost romance", "psychic romance", "necromancer romance", "fallen angel romance", "supernatural romance", "immortal romance", "soul bond", "witch coven romance", "vampire mate", "dark angel", "haunting romance", "vampire hunter", "witch coven", "demon hunter", "angel and demon", "nephilim", "shifter and witch"]
        },
        {
            "name": "Cozy / Cottagecore Romantasy",
            "confirmed": ["cozy fantasy romance", "cottagecore romance", "cozy witch romance", "magical small town romance", "cozy romantasy", "cozy magic", "slice of life fantasy", "witchy cozy"],
            "master": ["cozy fantasy", "cottagecore", "small town magic", "cozy witch", "magical bakery", "enchanted garden", "herbalist romance", "cottage magic", "whimsical romance", "gentle magic", "warm magic", "magical community", "village witch", "low-stakes magic", "healing magic", "nature magic romance", "magical shop", "bookshop magic", "woodland cottage", "cozy magical world", "low stakes fantasy", "magical baking", "magical tea shop", "cozy mystery with magic"]
        },
        {
            "name": "Urban / Contemporary Fantasy Romance",
            "confirmed": ["urban fantasy romance", "hidden magic romance", "urban coven romance", "secret magic world", "contemporary fantasy romance"],
            "master": ["urban fantasy", "contemporary fantasy", "modern magic", "hidden magic society", "magical city", "supernatural detective", "paranormal investigation", "secret magic world", "modern sorcerer", "magical crime", "city witch", "urban coven", "magical apartment", "urban spellcaster", "metropolitan fantasy", "hidden world", "magic underground", "supernatural organisation", "modern witch", "city fae", "magic in the city", "masquerade", "supernatural city", "modern day magic", "detective witch", "paranormal PI"]
        }
    ]

    def count_matches(text, keywords):
        count = 0
        if not text: return 0
        for kw in keywords:
            # NLP Regex Suffix Expansion
            pattern = r'\b' + re.escape(clean_text(kw)) + r'(?:s|es|ed|ing|ly|al)?\b'
            count += len(re.findall(pattern, text))
        return count

    results = []
    for sub in subgenres:
        conf_count = 0
        mast_count = 0
        points = 0
        
        # Critical columns (x3)
        for col in [col_title, col_genres]:
            c = count_matches(col, sub['confirmed'])
            m = count_matches(col, sub['master'])
            conf_count += c; mast_count += m
            points += (c * 3 * 3) + (m * 1 * 3)
            
        # High columns (x2)
        for col in [col_series, col_logline]:
            c = count_matches(col, sub['confirmed'])
            m = count_matches(col, sub['master'])
            conf_count += c; mast_count += m
            points += (c * 3 * 2) + (m * 1 * 2)
            
        # Normal columns (x1)
        c = count_matches(col_synopsis, sub['confirmed'])
        m = count_matches(col_synopsis, sub['master'])
        conf_count += c; mast_count += m
        points += (c * 3 * 1) + (m * 1 * 1)
        
        if points > 0:
            results.append({
                "name": sub["name"],
                "points": points,
                "conf_count": conf_count,
                "mast_count": mast_count
            })

    if not results:
        if is_gold_author:
            classification = "Yes"
            print(f"[{index}] Romantasy Checker: {classification} (Gold Author Rescue)")
        else:
            classification = "No"
            print(f"[{index}] Romantasy Checker: {classification}")
        df.at[index, "Romantasy Checker"] = classification
        return

    # Tie-Breaking Hierarchy
    results.sort(key=lambda x: (x['points'], x['conf_count'], x['mast_count']), reverse=True)
    winner = results[0]
    
    score = winner['points']
    num_books = df.at[index, "Goodreads Primary Books Number"]
    is_standalone = False
    try:
        if pd.notna(num_books) and float(num_books) < 3:
            is_standalone = True
    except: pass
    
    if score >= 3 and not is_standalone:
        classification = "Yes"
        df.at[index, "Romantasy Checker"] = classification
        print(f"[{index}] Romantasy Checker: {classification} ({winner['name']}) | Score: {score}")
        return

    # Ensemble Rescue for Weak Matches / Standalones
    print(f"[{index}] Romantasy Checker: V3 score {score}. Engaging AI Ensemble Rescue...")
    
    translated_synopsis = col_synopsis
    try:
        if col_synopsis and detect(col_synopsis) != 'en':
            print(f"[{index}]   - Foreign language detected. Translating to English...")
            translated_synopsis = GoogleTranslator(source='auto', target='en').translate(col_synopsis)
    except: pass
    
    super_paragraph = f"{col_title} {col_series} {col_genres} {col_logline} {translated_synopsis}"
    
    theme_matches = 0
    if kw_extractor:
        try:
            kw_results = kw_extractor.extract_keywords(super_paragraph, keyphrase_ngram_range=(1, 2), stop_words='english', top_n=5)
            extracted_keywords = [kw[0] for kw in kw_results]
            for kw in extracted_keywords:
                for sub in subgenres:
                    if kw in sub['confirmed'] or kw in sub['master']:
                        theme_matches += 1
        except: pass
        
    hf_score = 0.0
    if hf_classifier:
        try:
            trunc_text = super_paragraph[:1500] 
            result = hf_classifier(trunc_text, candidate_labels=["Romantic Fantasy", "Standard Fiction"])
            idx = result['labels'].index("Romantic Fantasy")
            hf_score = result['scores'][idx]
        except Exception as e:
            print(f"[{index}]   - HF Error: {e}")
            
    if hf_score > 0.60 or theme_matches >= 2:
        classification = "Yes"
        print(f"[{index}] Romantasy Checker: AI Rescued -> {classification} ({winner['name']}) | HF Score: {hf_score:.2f} | KeyBERT matches: {theme_matches}")
    else:
        classification = "No"
        print(f"[{index}] Romantasy Checker: AI Rejected -> {classification} | HF Score: {hf_score:.2f} | KeyBERT matches: {theme_matches}")
        
    df.at[index, "Romantasy Checker"] = classification

async def run_scraper():
    print(f"Loading {EXCEL_FILE}...")
    try:
        df = pd.read_excel(EXCEL_FILE)
    except Exception as e:
        print(f"Excel load error: {e}")
        return

    # Ensure all target columns exist
    for col in ["GoodReads_Series_URL", "Goodreads Primary Books Number", "Goodreads Primary Books Page Count", "Goodreads Rating Book 1", "Goodreads No. of Ratings Book 1", "Goodreads Link", "Genre Tags", "Synopsis", "Romantasy Checker"]:
        if col not in df.columns:
            df[col] = None

    print(f"Running MASTER Scraper on FIRST {TARGET_ROWS} ROWS with CONCURRENCY {CONCURRENCY} and BATCH_SIZE {BATCH_SIZE}...")

    user_data_dir = os.path.join(r"e:\Internship\PocketFM", "playwright_goodreads_profile")
    
    total_to_process = min(TARGET_ROWS, len(df))
    
    for batch_start in range(START_ROW, total_to_process, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_to_process)
        print(f"\n=======================================================")
        print(f"STARTING BATCH {batch_start} to {batch_end} (Cooldown Architecture)")
        print(f"=======================================================\n")
        
        # Fast-forward check: skip launching browser if the whole batch is fully completed
        needs_processing = False
        for i in range(batch_start, batch_end):
            if not is_row_complete(df.iloc[i]):
                needs_processing = True
                break
                
        if not needs_processing:
            print(f"Entire batch {batch_start}-{batch_end} is already fully complete. Skipping.")
            continue

        async with async_playwright() as p:
            print("Launching Fresh Playwright Context...")
            context = await p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=False,
                args=['--disable-blink-features=AutomationControlled'],
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={'width': 1280, 'height': 800}
            )
            
            # Limit concurrency to exactly CONCURRENCY active tasks
            sem = asyncio.Semaphore(CONCURRENCY)
            
            # Build tasks for this batch
            tasks = []
            for index in range(batch_start, batch_end):
                row = df.iloc[index]
                tasks.append(process_row(index, row, df, context, sem))
                
            # Run them simultaneously
            await asyncio.gather(*tasks)

            await context.close()
            
        # Securely save after every batch
        try:
            df.to_excel(EXCEL_FILE, index=False)
            print(f"\nBatch {batch_start}-{batch_end} saved successfully.")
        except Exception as e:
            print(f"Failed to save batch {batch_start}-{batch_end}: {e}")
            
        # Cooldown sleep if there are more batches left
        if batch_end < total_to_process:
            print("Cooling down WAF for 10 seconds...")
            await asyncio.sleep(10)

    try:
        print("\nAll batches complete for the target rows.")
        print("Applying automatic Excel styling...")
        format_excel.apply_styling(EXCEL_FILE)
    except Exception as e:
        print(f"Failed to apply styling: {e}")

if __name__ == "__main__":
    asyncio.run(run_scraper())
