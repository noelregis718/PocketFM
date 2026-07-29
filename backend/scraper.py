import asyncio
import unicodedata
import re
from playwright.async_api import async_playwright


def clean_text(text):
    if not text:
        return ""
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def clean_numeric(text):
    if not text:
        return 0
    match = re.search(r'[\d,.]+', text)
    if match:
        clean_val = match.group(0).replace(',', '')
        try:
            if clean_val == '.':
                return 0
            return float(clean_val)
        except (ValueError, TypeError):
            return 0
    return 0


def extract_series_from_title(title):
    """Matches patterns like (Series Name #1) or (Series Name Book 1)."""
    if not title: return None
    match = re.search(r'\((.*?)(?:\s+#?\d+|\s+Book\s+\d+)?\)', title, re.IGNORECASE)
    if match:
        name = match.group(1).strip()
        # Clean up common garbage at end of series name in title
        name = re.sub(r'[\s#]+$', '', name)
        return name if len(name) > 2 else None
    return None

def normalize_title_for_search(title):
    if not title:
        return ""
    # 1. Clean de-duplication (e.g. "Title Title" -> "Title")
    words = title.split()
    half = len(words) // 2
    if len(words) >= 4 and words[:half] == words[half:]:
        title = " ".join(words[:half])
    
    # Standard cleanup
    t = title.lower()
    
    # Remove common Amazon subtitles and fluff
    remove_patterns = [
        r':\s+a\s+novel.*', 
        r':\s+a\s+read\s+with\s+jenna\s+pick.*',
        r':\s+a\s+memoir.*',
        r'\(deluxe\s+edition\).*',
        r'\(special\s+edition\).*',
        r'\'s\s+broken\s+mate',
        r'\[.*\]', 
        r'book\s+\d+.*',
        r'\d+\s+of\s+\d+.*',
        r'a\s+dark\s+fantasy.*',
        r'an\s+addictive\s+fantasy.*',
    ]
    
    for pattern in remove_patterns:
        t = re.sub(pattern, '', t, flags=re.IGNORECASE)
    
    # Take only part before first colon/dash for broad search
    t = re.split(r'[:\-—\(]', t)[0]
    
    # Clean up punctuation
    t = re.sub(r'[^\w\s]', '', t)
    return t.strip()


class AmazonScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def set_amazon_location(self, page, zip_code="90016"):
        """Automates setting the Amazon delivery location to a US zip code (ensures USD)."""
        print(f"  [Location] Setting Amazon location to US Zip: {zip_code}...")
        try:
            # Wait for any of the common location selectors to appear
            selectors = [
                '#nav-global-location-popover-link',
                '#nav-packard-glow-loc-icon',
                '#glow-ingress-block',
                '#nav-global-location-slot'
            ]
            
            loc_button = None
            for sel in selectors:
                try:
                    loc_button = await page.wait_for_selector(sel, timeout=10000)
                    if loc_button: break
                except: continue

            if loc_button:
                await loc_button.click()
                await asyncio.sleep(3) # Wait for popover
                
                # Enter Zip Code if input is visible
                zip_input = await page.query_selector('#GLUXZipUpdateInput')
                if zip_input:
                    await zip_input.fill(zip_code)
                    await asyncio.sleep(1)
                    
                    # Click Apply
                    apply_btn = await page.query_selector('#GLUXZipUpdate .a-button-input, #GLUXZipUpdate input')
                    if apply_btn:
                        await apply_btn.click()
                        await asyncio.sleep(3)
                
                # Check for "Continue" or "Done" button
                continue_btn = await page.query_selector('span[id="GLUXConfirmClose"] input, [name="glowDoneButton"], #GLUXConfirmClose-announce')
                if continue_btn and await continue_btn.is_visible():
                    await continue_btn.click()
                    await asyncio.sleep(2)
                else:
                    # Alternative: If it's a "Done" button
                    done_btn = await page.query_selector('button[name="glowDoneButton"]')
                    if done_btn and await done_btn.is_visible():
                        await done_btn.click()
                        await asyncio.sleep(2)
                    else:
                        # Final Fallback: Refresh the page to lock in cookies
                        await page.reload(wait_until="domcontentloaded")
                
                print(f"  [Location] Done. Verified location: {zip_code}")
            else:
                print("  [Location] Warning: Could not find location button with standard selectors.")
                # Strategy 2: Try to find by text if IDs failed
                try:
                    text_loc = await page.locator('text="Deliver to"').first
                    if await text_loc.is_visible():
                        await text_loc.click()
                        await asyncio.sleep(2)
                        print("  [Location] Found location button via Text Search.")
                except: pass
        except Exception as e:
            print(f"  [Location] Error setting location: {e}")

    async def scrape_bestseller_list(self, url, limit=10, skip_offset=0, external_page=None):
        if external_page:
            # UNIFIED SESSION MODE: Use the page provided by the caller
            return await self._execute_discovery(external_page, url, limit, skip_offset)
            
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            try:
                return await self._execute_discovery(page, url, limit, skip_offset)
            finally:
                await browser.close()

    async def _execute_discovery(self, page, url, limit, skip_offset):
        try:
            unique_results = []
            seen_asin = set()
            # Queue of category URLs to explore
            category_queue = [url]
            seen_categories = {url}

            # --- AMAZON LOGIN GATE (Only once at the start) ---
            print(f"Opening Amazon: {url}")
            await page.goto(url, wait_until="load", timeout=60000)
            
            # --- NEW: Set US Location (90016) ---
            await self.set_amazon_location(page, "90016")

            print("\n" + "!" * 60)
            print("  ACTION REQUIRED: MANUALLY CLEAR AMAZON BLOCKS")
            print("  1. Solve any CAPTCHAs.")
            print("  2. Navigate to your target START category.")
            print("  The engine will then auto-dive into sub-categories if needed.")
            print("!" * 60 + "\n")

            try:
                await page.wait_for_selector('#twotabsearchtextbox, #nav-logo-sprites, [data-asin]', timeout=300000)
                print("  [OK] Amazon page detected. Starting Deep Scrape...")
            except Exception:
                print("  [Time Out] Wait exceeded. Proceeding with visible content...")

            global_found_count = 0
            if skip_offset > 0:
                print(f"  [Resuming] Industrial Stepper: Skipping the first {skip_offset} books (already in Excel)...")

            while len(unique_results) < limit and category_queue:
                current_cat_url = category_queue.pop(0)
                print(f"\n" + "-"*30)
                print(f"[Category Pivot] Exploring: {current_cat_url}")
                print(f"  (Target: {limit} | Current: {len(unique_results)})")
                print("-"*30)
                
                try:
                    # Use a 45s timeout for navigation as category switching can be slow
                    await page.goto(current_cat_url, wait_until="load", timeout=45000)
                except Exception as e:
                    print(f"  [Skip] Navigation failed: {e}")
                    continue

                # --- DETECT LIST TYPE ---
                # If this is a search result page vs a Bestseller page, pagination differs
                is_bestseller = "/zgbs/" in current_cat_url or "/best-sellers/" in current_cat_url

                page_num = 1
                while True:
                    # --- AGGRESSIVE FAST SCROLL ---
                    print(f"  [Page {page_num}] Scrolling to reveal all content...")
                    await page.evaluate("""async () => {
                        for (let i = 0; i < 5; i++) {
                            window.scrollBy(0, document.body.scrollHeight / 5);
                            await new Promise(r => setTimeout(r, 700));
                        }
                        window.scrollTo(0, document.body.scrollHeight);
                    }""")
                    await asyncio.sleep(2)

                    # --- DISCOVERY SCAN ---
                    items = await page.query_selector_all('[data-asin]')
                    page_asins = []
                    found_on_page = 0
                    
                    for item in items:
                        asin = await item.get_attribute('data-asin') or "N/A"
                        
                        # Stepper logic: Increment counter for every book found
                        global_found_count += 1
                        if global_found_count <= skip_offset:
                            if global_found_count % 10 == 0:
                                print(f"  [Skip] Skipping Rank #{global_found_count}...")
                            continue

                        if not asin or asin == "N/A" or asin in seen_asin:
                            continue
                        
                        title_el = await item.query_selector('.p13n-sc-untruncated-desktop-title, ._cDE_gridItem_truncate-title, img')
                        raw_title = "N/A"
                        if title_el:
                            tag = await title_el.evaluate("el => el.tagName")
                            if tag == 'IMG': raw_title = clean_text(await title_el.get_attribute('alt'))
                            else: raw_title = clean_text(await title_el.inner_text())

                        # Extract Rank (Optional for discovery, but helpful)
                        rank_el = await item.query_selector('.zg-bdg-text, .p13n-sc-badge-label-size-base, span.zg-badge-text, .s-badge-text')
                        rank_text = clean_text(await rank_el.inner_text()).lstrip('#').strip() if rank_el else "N/A"

                        link_el = await item.query_selector('a.a-link-normal[href*="/dp/"], a.a-link-normal')
                        raw_href = await link_el.evaluate("el => el.href") if link_el else ""

                        if not raw_href or "javascript" in raw_href or not raw_title:
                            continue

                        unique_results.append({
                            "Rank": rank_text,
                            "Book Title": raw_title,
                            "Author Name": "N/A",
                            "Rating": 0,
                            "Number of Reviews": 0,
                            "Price": "N/A",
                            "Amazon URL": raw_href
                        })
                        seen_asin.add(asin)
                        page_asins.append(asin)
                        found_on_page += 1
                        if len(unique_results) >= limit: break

                    print(f"  -> +{found_on_page} unique books. (Progress: {len(unique_results)}/{limit})")
                    if len(unique_results) >= limit: 
                        print("  [OK] Limit satisfied!")
                        break

                    # --- UNIVERSAL PAGINATION ---
                    next_btn = None
                    selectors = [
                        'li.a-last a', 
                        'a.s-pagination-next', 
                        '.zg-pagination-next a',
                        'a:has-text("Next")',
                        '#p_n_feature_nine_browse-bin-title + ul li a' # Sub-category fallback if pag fails
                    ]
                    for sel in selectors:
                        try:
                            btn = await page.query_selector(sel)
                            if btn and await btn.is_visible():
                                next_btn = btn
                                break
                        except: continue

                    if next_btn:
                        first_asin_before = page_asins[0] if page_asins else None
                        print(f"  Flipping Page {page_num}...")
                        await next_btn.click()
                        page_num += 1
                        await asyncio.sleep(4) # Industrial safety delay
                        
                        # Verify page turn
                        new_items = await page.query_selector_all('[data-asin]')
                        if new_items:
                            current_asin = await new_items[0].get_attribute('data-asin')
                            if current_asin == first_asin_before:
                                print("  [Warning] Page turn failed. Retrying click...")
                                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                await asyncio.sleep(1)
                                await next_btn.click()
                    else:
                        print(f"  [End of Category] No more pages in this branch.")
                        break

                    # --- AGGRESSIVE SUB-CATEGORY DETECTION ---
                    if len(unique_results) < limit:
                        print("  [Searching Sidebar] Scanning for pivot-links in the category tree...")
                    
                    # Massive selector array for all possible sidebar/pivot link locations
                    pivot_selectors = [
                        '#zg_left_col2 a', 
                        'ul[role="group"] li a',
                        '._p13n-zg-nav-tree-all_style_zg-selected__199v3 + ul a', # Children
                        '._p13n-zg-nav-tree-all_style_zg-selected__199v3 ~ li a', # Siblings
                        '.zg-nav-tree a',
                        '.s-navigation-item a'
                    ]
                    
                    discovered_cats = 0
                    for sel in pivot_selectors:
                        try:
                            links = await page.query_selector_all(sel)
                            for link in links:
                                href = await link.evaluate("el => el.href")
                                txt = await link.inner_text()
                                txt_clean = txt.strip().lower()
                                
                                # Filter: Ignore parent nodes like "Books", "Any Department" or current page
                                ignore_names = ['books', 'all books', 'any department', 'all departments', 'home']
                                if href and href not in seen_categories and any(x in href for x in ['/zgbs/', '/best-sellers/']):
                                    if not any(ign in txt_clean for ign in ignore_names):
                                        print(f"    -> Queuing Pivot: {txt.strip()[:30]}")
                                        category_queue.append(href)
                                        seen_categories.add(href)
                                        discovered_cats += 1
                        except: continue
                        
                        print(f"  -> Discovery found {discovered_cats} potential new branches.")

            print(f"Discovery Phase Finished! Total gathered: {len(unique_results)} items.")
            return unique_results
        except Exception as e:
            print(f"  [Critical] Discovery Error: {e}")
            return []

    async def get_book1_details(self, context, series_name, author_name):
        if not series_name or series_name == "N/A": return None
        
        from urllib.parse import quote
        a_name = author_name if author_name != "N/A" else ""
        query = quote(f"{series_name} book 1 {a_name}")
        search_url = f"https://www.amazon.com/s?k={query}"
        
        page = await context.new_page()
        print(f"    -> [Fallback] Searching Book 1 for series: {series_name}", flush=True)
        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(2)
            
            href = None
            for i in range(3):
                await page.evaluate("window.scrollBy(0, 1000)")
                await asyncio.sleep(1)
                
            items = await page.query_selector_all("div[data-asin]")
            for item in items:
                asin = await item.get_attribute('data-asin')
                if not asin or len(asin) < 5: continue
                l_el = await item.query_selector("h2 a")
                if l_el:
                    h = await l_el.evaluate("el => el.href")
                    if h and "/dp/" in h: 
                        href = h
                        break
            
            await page.close()
            
            if href:
                print(f"    -> [Fallback] Found Book 1 link, scraping...", flush=True)
                return await self.scrape_product_details_tab(context, href)
        except Exception as e:
            print(f"    -> [Fallback] Error: {e}", flush=True)
            try: await page.close()
            except: pass
        return None

    async def scrape_product_details_tab(self, context, url, base_url="https://www.amazon.com"):
        if not url:
            return {
                "Description": "N/A", "Publisher": "N/A", "Publication Date": "N/A", 
                "Author Name": "N/A", "Price": "N/A", "Series": "N/A", 
                "Pages": "N/A", "Inner Rank": "N/A"
            }
        if not url.startswith('http'):
            url = base_url.rstrip('/') + url

        page = await context.new_page()
        try:
            # domcontentloaded is fast and sufficient — detail elements are in DOM immediately
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            # Give lazy sections 2s to render without waiting for full networkidle
            await asyncio.sleep(2)

            # --- Description ---
            description = "N/A"
            for desc_sel in [
                '#bookDescription_feature_div noscript',
                '#bookDescription_feature_div',
                '#productDescription',
                '#bookDescription',
                '#book-description-widget',
                '[data-a-expander-name="book_description_expander"]',
            ]:
                desc_el = await page.query_selector(desc_sel)
                if desc_el:
                    description = clean_text(await desc_el.inner_text())
                    if description and len(description) > 10:
                        break

            # --- Author ---
            author = "N/A"
            for sel in [
                '#bylineInfo .author a',
                '#bylineInfo a.contributorNameID',
                '#bylineInfo .a-link-normal',
                '#byline a',
                '.author .a-link-normal',
                'span.author a',
                '#authorName',
                '.contributorNameID',
                '#author-follow-button',
                'a[data-asin*="B0"]',
                '.a-link-normal.contributorNameID'
            ]:
                el = await page.query_selector(sel)
                if el:
                    text = clean_text(await el.inner_text())
                    # Filter out non-author strings like "Visit Amazon's..."
                    text = re.sub(r"Visit Amazon's\s+", "", text, flags=re.IGNORECASE)
                    text = re.sub(r"\s+Page", "", text, flags=re.IGNORECASE)
                    text = re.sub(r"Search results for this author", "", text, flags=re.IGNORECASE)
                    
                    if (text and len(text) > 1
                        and not re.match(r'^[\d\.\$,]+$', text)
                        and 'out of' not in text.lower()
                        and 'stars' not in text.lower()
                        and 'ratings' not in text.lower()):
                        author = text
                        break

            if author == "N/A":
                byline_el = await page.query_selector('#bylineInfo, #byline')
                if byline_el:
                    byline_text = clean_text(await byline_el.inner_text())
                    m = re.search(r'\bby\s+([A-Z][A-Za-z\s\.\-\']+?)(?:\s*[\(,;|]|$)', byline_text, re.IGNORECASE)
                    if m:
                        author = m.group(1).strip()

            # --- Publisher & Publication Date ---
            # Strategy 1: bullet list items (most common layout)
            publisher = "N/A"
            pub_date = "N/A"

            for sel in [
                '#detailBullets_feature_div li',
                '#productDetails_detailBullets_sections1 tr',
                '#productDetails_techSpec_section_1 tr',
                '#productDetailsTable tr',
                '.detail-bullet-list li',
                '#bookDetails_feature_div .a-list-item',
                '#richProductInformation_feature_div .a-section',
                '#rpiTable tr',
                '.rpi-attribute-value',
            ]:
                items = await page.query_selector_all(sel)
                for item in items:
                    text = clean_text(await item.inner_text())
                    # Normalize separators: replace Unicode LRM/RLM with colon
                    text = re.sub(r'[\u200e\u200f\u200b]+', ':', text)
                    text = re.sub(r'\s*:\s*', ': ', text)

                    if publisher == "N/A" and re.search(r'\bpublisher\b', text, re.IGNORECASE):
                        # Extract value after "Publisher :"
                        m = re.search(r'publisher\s*:\s*(.+)', text, re.IGNORECASE)
                        if m:
                            val = m.group(1).strip().lstrip(':').strip()
                            # Remove trailing junk like "(15 January 2017)"
                            val = re.sub(r'\s*\(\d+.*?\)\s*$', '', val).strip()
                            if val and len(val) > 1:
                                publisher = val

                    if pub_date == "N/A" and re.search(r'publication\s*date', text, re.IGNORECASE):
                        m = re.search(r'publication\s*date\s*:\s*(.+)', text, re.IGNORECASE)
                        if m:
                            val = m.group(1).strip().lstrip(':').strip()
                            if val and len(val) > 1:
                                pub_date = val

                if publisher != "N/A" and pub_date != "N/A":
                    break

            # Strategy 2: if still N/A, scan all text nodes on the page line by line
            if publisher == "N/A" or pub_date == "N/A":
                try:
                    page_text = await page.evaluate("() => document.body.innerText")
                    lines = page_text.split('\n')
                    for i, line in enumerate(lines):
                        line = line.strip()
                        if not line:
                            continue

                        if publisher == "N/A" and re.search(r'\bpublisher\b', line, re.IGNORECASE):
                            m = re.search(r'publisher\s*[:\u200e\u200f]?\s*(.+)', line, re.IGNORECASE)
                            if m:
                                val = m.group(1).strip().lstrip(':').strip()
                                val = re.sub(r'\s*\(\d+.*?\)\s*$', '', val).strip()
                                if val and len(val) > 1:
                                    publisher = val
                            elif line.lower().strip() == 'publisher' and i + 1 < len(lines):
                                next_val = lines[i + 1].strip()
                                if next_val and len(next_val) > 1:
                                    publisher = next_val

                        if pub_date == "N/A" and re.search(r'publication\s*date', line, re.IGNORECASE):
                            m = re.search(r'publication\s*date\s*[:\u200e\u200f]?\s*(.+)', line, re.IGNORECASE)
                            if m:
                                val = m.group(1).strip().lstrip(':').strip()
                                if val and len(val) > 1:
                                    pub_date = val
                            elif i + 1 < len(lines):
                                next_val = lines[i + 1].strip()
                                if next_val and len(next_val) > 1:
                                    pub_date = next_val
                except Exception as e:
                    print(f"Text scan error: {e}")

            # Strategy 3: Aggressive JS scan of ALL elements on the page
            # Handles audiobook pages, carousel grids, and any other unknown layout
            if publisher == "N/A" or pub_date == "N/A":
                try:
                    deep_scan = await page.evaluate("""() => {
                        const result = { publisher: null, pubDate: null };
                        
                        // Scan EVERY element on the page
                        const allElements = document.querySelectorAll('*');
                        for (const el of allElements) {
                            // Only check leaf-ish elements (avoid huge containers)
                            if (el.children.length > 10) continue;
                            const txt = (el.textContent || '').trim();
                            if (txt.length > 500 || txt.length < 3) continue;
                            
                            const lower = txt.toLowerCase();
                            
                            // Publication date: look for element whose text IS "Publication date"
                            if (!result.pubDate && lower === 'publication date') {
                                // Value is in the next sibling or parent's next child
                                let next = el.nextElementSibling;
                                if (next) {
                                    const val = next.textContent.trim();
                                    if (val.length > 3 && val.length < 50) result.pubDate = val;
                                }
                                if (!result.pubDate && el.parentElement) {
                                    const parent = el.parentElement;
                                    const siblings = parent.parentElement ? parent.parentElement.children : [];
                                    let found = false;
                                    for (const sib of siblings) {
                                        if (found) {
                                            const val = sib.textContent.trim();
                                            if (val.length > 3 && val.length < 50) {
                                                result.pubDate = val;
                                                break;
                                            }
                                        }
                                        if (sib === parent) found = true;
                                    }
                                }
                            }
                            
                            // Publisher: look for element whose text IS "Publisher"
                            if (!result.publisher && lower === 'publisher') {
                                let next = el.nextElementSibling;
                                if (next) {
                                    const val = next.textContent.trim();
                                    if (val.length > 1 && val.length < 100) result.publisher = val;
                                }
                                if (!result.publisher && el.parentElement) {
                                    const parent = el.parentElement;
                                    const siblings = parent.parentElement ? parent.parentElement.children : [];
                                    let found = false;
                                    for (const sib of siblings) {
                                        if (found) {
                                            const val = sib.textContent.trim();
                                            if (val.length > 1 && val.length < 100) {
                                                result.publisher = val;
                                                break;
                                            }
                                        }
                                        if (sib === parent) found = true;
                                    }
                                }
                            }
                        }
                        
                        return result;
                    }""")
                    if deep_scan.get('publisher') and publisher == "N/A":
                        publisher = clean_text(deep_scan['publisher'])
                    if deep_scan.get('pubDate') and pub_date == "N/A":
                        pub_date = clean_text(deep_scan['pubDate'])
                except Exception as e:
                    print(f"Deep scan error: {e}")

            # Strategy 4: Python regex on full page text for date patterns near "publication date"
            if pub_date == "N/A":
                try:
                    full_text = await page.evaluate("() => document.body.innerText")
                    # Look for date patterns like "September 14, 2021" near "Publication date"
                    m = re.search(
                        r'publication\s*date[\s\S]{0,50}?'
                        r'((?:January|February|March|April|May|June|July|August|September|October|November|December)'
                        r'\s+\d{1,2},?\s+\d{4})',
                        full_text, re.IGNORECASE
                    )
                    if m:
                        pub_date = m.group(1).strip()
                except Exception:
                    pass

            # ====== STRUCTURED PRICE EXTRACTION ======
            # Extract all format prices from the product page
            price_lines = []
            seen_formats = set()
            try:
                # 1. Primary Hunt: Format Swatch Buttons
                format_items = await page.query_selector_all(
                    '#tmmSwatches .a-button-inner, '
                    '[id*="tmm-grid-swatch"] .a-button-inner, '
                    '.swatchElement .a-button-inner, '
                    '.a-button-inner:has(.a-price)'
                )
                for fi in format_items:
                    text = clean_text(await fi.inner_text())
                    if not text: continue
                    
                    parts = [p.strip() for p in text.split('\n') if p.strip()]
                    if len(parts) >= 1:
                        format_name = "Price"
                        price_val = "N/A"
                        for ftype in ["Kindle", "Paperback", "Hardcover", "Audiobook", "Audio CD", "Multimedia CD"]:
                            if any(ftype.lower() in p.lower() for p in parts):
                                format_name = ftype
                                break
                        for p in parts:
                            if re.search(r'[\d,\.]+', p) and re.search(r'[\u20b9\$\£\€]|INR|USD|GBP|EUR|Rs\.?', p, re.IGNORECASE):
                                price_val = p
                                break
                        if price_val != "N/A" and format_name.lower() not in seen_formats:
                            seen_formats.add(format_name.lower())
                            price_lines.append(f"{format_name} - {price_val}")

                # 2. Secondary Hunt: Core Price Selectors
                if not price_lines:
                    for p_sel in [
                        '#corePrice_feature_div .a-price .a-offscreen',
                        '#corePrice_desktop .a-price .a-offscreen',
                        '#kindle-price', '#price', '.a-price .a-offscreen', '.slot-price .a-offscreen'
                    ]:
                        try:
                            p_el = await page.query_selector(p_sel)
                            if p_el:
                                p_val = clean_text(await p_el.inner_text())
                                if p_val and re.search(r'\d', p_val):
                                    f_name = "Price"
                                    if "kindle" in url.lower(): f_name = "Kindle"
                                    elif "paperback" in url.lower(): f_name = "Paperback"
                                    price_lines.append(f"{f_name} - {p_val}")
                                    break
                        except: continue

                # 3. Tertiary Hunt: List-based formats
                if len(price_lines) < 2:
                    format_links = await page.query_selector_all('li.swatchElement a, .olp-text-box a')
                    for flnk in format_links:
                        raw_t = await flnk.inner_text()
                        cleaned_t = clean_text(raw_t)
                        for ftype in ["Paperback", "Hardcover", "Audiobook", "Kindle", "Mass Market Paperback"]:
                            if ftype.lower() in cleaned_t.lower() and ftype.lower() not in seen_formats:
                                p_el = await flnk.query_selector('.a-color-secondary, .a-size-mini, .a-price')
                                if p_el:
                                    p_val = clean_text(await p_el.inner_text())
                                    if re.search(r'\d', p_val):
                                        price_lines.append(f"{ftype} - {p_val}")
                                        seen_formats.add(ftype.lower())

                # 4. Fallback: Full Page Text Search
                if not price_lines:
                    full_txt = await page.evaluate("() => document.body.innerText")
                    for ftype in ["Kindle", "Paperback", "Hardcover", "Audiobook"]:
                        m = re.search(ftype + r'[\s\S]{0,50}?((?:\$|INR|₹|Rs\.?)\s*[\d,]+\.?\d*)', full_txt, re.IGNORECASE)
                        if m and ftype.lower() not in seen_formats:
                            price_lines.append(f"{ftype} - {m.group(1).strip()}")
                            seen_formats.add(ftype.lower())

            except Exception as e:
                print(f"Price extraction error: {e}")

            price_str = "\n".join(price_lines) if price_lines else "N/A"

            # ====== AMAZON STARS AND RATINGS ======
            rating = "N/A"
            reviews = "N/A"
            actual_reviews = "N/A"
            try:
                # Stars
                star_el = await page.query_selector('#acrPopoverTitle, [data-hook="rating-out-of-text"], .a-icon-star span')
                if star_el:
                    star_text = clean_text(await star_el.inner_text())
                    # Format: "4.5 out of 5 stars" -> "4.5"
                    m = re.search(r'([\d.]+)', star_text)
                    if m: rating = m.group(1)

                # Review Count
                review_el = await page.query_selector('#acrCustomerReviewText, [data-hook="total-review-count"]')
                if review_el:
                    review_text = clean_text(await review_el.inner_text())
                    # Format: "1,234 ratings" -> "1234"
                    m = re.search(r'([\d,]+)', review_text)
                    if m: reviews = m.group(1).replace(',', '')
                    
                # Aggressive fallback for review count
                if reviews == "N/A":
                    try:
                        rev_scan = await page.evaluate("""() => {
                            const elements = document.querySelectorAll('span.a-size-base, a.a-link-normal');
                            for (const el of elements) {
                                const text = (el.textContent || '').trim().toLowerCase();
                                if (text.includes('ratings') || text.includes('reviews')) {
                                    const match = text.match(/([\\d,]+)\\s*(?:ratings|reviews)/);
                                    if (match && match[1]) {
                                        return match[1].replace(/,/g, '');
                                    }
                                }
                            }
                            return "N/A";
                        }""")
                        if rev_scan != "N/A":
                            reviews = rev_scan
                    except Exception:
                        pass
            except Exception as e:
                print(f"Rating extraction error: {e}")

            # ====== NEW: SERIES, PAGES, INNER RANK EXTRACTION ======
            series_name = "N/A"
            book_number = "N/A"
            total_books_in_series = "N/A"
            pages = "N/A"
            inner_rank = "N/A"

            try:
                # Strategy 1: Title parsing
                raw_title_el = await page.query_selector('#productTitle')
                raw_title = clean_text(await raw_title_el.inner_text()) if raw_title_el else ""
                
                if raw_title:
                    m = re.search(r'\((.*?)(?:\s+#?\d+|\s+Book\s+\d+)?\)', raw_title, re.IGNORECASE)
                    if m:
                        s_name = re.sub(r'[\s#]+$', '', m.group(1).strip())
                        if len(s_name) > 2 and 'edition' not in s_name.lower():
                            series_name = s_name
                    m2 = re.search(r'Book\s+(\d+)', raw_title, re.IGNORECASE)
                    if m2: book_number = m2.group(1)

                # Strategy 2: Parse standard series block text
                series_el = await page.query_selector('#seriesBulletWidget_feature_div, #bookSeries_feature_div')
                if series_el:
                    series_text = clean_text(await series_el.inner_text())
                    
                    m = re.search(r'Book\s+(\d+)\s+of\s+(\d+)\s*:\s*(.+)', series_text, re.IGNORECASE)
                    if m:
                        book_number = m.group(1).strip()
                        total_books_in_series = m.group(2).strip()
                        series_name = m.group(3).strip()
                    else:
                        m3 = re.search(r'Book\s+(\d+)\s+of\s*:\s*(.+)', series_text, re.IGNORECASE)
                        if m3:
                            book_number = m3.group(1).strip()
                            series_name = m3.group(2).strip()
                        
                        m2 = re.search(r'Part\s+of\s*:\s*(.+)', series_text, re.IGNORECASE)
                        if m2 and series_name == "N/A": 
                            series_name = m2.group(1).strip()
                            
                # Strategy 3: Scan for all series links
                if series_name == "N/A" or "book" in series_name.lower():
                    series_links = await page.query_selector_all('a[href*="/series/"], .series-link')
                    for link in series_links:
                        txt = clean_text(await link.inner_text())
                        if txt and len(txt) > 2 and "visit amazon's" not in txt.lower():
                            series_name = txt
                            break

                # Strategy 4: Deep DOM string matching
                if series_name == "N/A":
                    all_spans = await page.query_selector_all('span')
                    for span in all_spans:
                        txt = clean_text(await span.inner_text())
                        if 'Book' in txt and 'of' in txt and ':' in txt and len(txt) < 100:
                            m = re.search(r'Book\s+(\d+)\s+of\s+\d*\s*:\s*(.+)', txt, re.IGNORECASE)
                            if m:
                                book_number = m.group(1).strip()
                                series_name = m.group(2).strip()
                                break

                # 2. Pages (Print length)
                for page_sel in ['#detailBullets_feature_div li', '#rpiTable tr', '.rpi-attribute-value']:
                    els = await page.query_selector_all(page_sel)
                    for el in els:
                        t = clean_text(await el.inner_text())
                        if 'print length' in t.lower() or 'pages' in t.lower():
                            m = re.search(r'(\d+)\s*pages', t, re.IGNORECASE)
                            if m:
                                pages = m.group(1).strip()
                                break
                    if pages != "N/A": break

                # 3. Best Sellers Rank (inner)
                rank_container = await page.query_selector('#detailBullets_feature_div, #productDetails_db_sections')
                if rank_container:
                    rank_text_full = clean_text(await rank_container.inner_text())
                    rank_matches = re.findall(r'#[\d,]+\s+in\s+[^(\n]+', rank_text_full)
                    if rank_matches:
                        inner_rank = " | ".join(rank_matches[:3])
            except Exception as e:
                print(f"Detail enrichment error: {e}")

            # ====== THE ULTIMATE AGGRESSIVE REGEX FALLBACK ======
            try:
                full_text = await page.evaluate("() => document.body.innerText")
                # 1. Total books in series
                if total_books_in_series == "N/A":
                    # Look for "Book X of Y [junk] Series Name"
                    m = re.search(r'Book\s+(\d+)\s+of\s+(\d+)[\s\S]{1,50}?([A-Za-z0-9\s,&.-]+?)(?=\n|Print|Language|Previous)', full_text, re.IGNORECASE)
                    if m: 
                        if book_number == "N/A": book_number = m.group(1)
                        total_books_in_series = m.group(2)
                        if series_name == "N/A": series_name = m.group(3).strip()
                
                # 2. Publisher
                if publisher == "N/A":
                    m = re.search(r'Publisher[\s\S]{1,40}?([A-Za-z0-9\s,&.-]+?)(?:\n|\(|;)', full_text, re.IGNORECASE)
                    if m: publisher = m.group(1).strip()
                    elif re.search(r'Independently published', full_text, re.IGNORECASE):
                        publisher = "Independently published"
                
                # 3. Publication Date
                if pub_date == "N/A":
                    m = re.search(r'(?:Publication date|Release date)[\s\S]{1,40}?([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})', full_text, re.IGNORECASE)
                    if m: pub_date = m.group(1).strip()
                
                # 4. Pages (or Audiobook Length)
                if pages == "N/A":
                    m = re.search(r'(?:Print length|Length|Listening Length)[\s\S]{1,40}?(\d+(?:\.\d+)?)\s*(?:pages|hours|minutes|hrs|mins)', full_text, re.IGNORECASE)
                    if m: pages = m.group(1).strip()
                
                # 5. Amazon Stars
                if rating == "N/A":
                    m = re.search(r'([\d.]+)\s*out of 5 stars', full_text, re.IGNORECASE)
                    if m: rating = m.group(1).strip()
                
                # 6. Amazon Ratings (Reviews)
                if reviews == "N/A":
                    m = re.search(r'([\d,]+)\s*ratings', full_text, re.IGNORECASE)
                    if m: reviews = m.group(1).replace(',', '').strip()
                
                # 6.5. Actual Reviews (Text Reviews)
                if actual_reviews == "N/A":
                    m = re.search(r'([\d,]+)\s*(?:global\s+)?reviews', full_text, re.IGNORECASE)
                    if m: actual_reviews = m.group(1).replace(',', '').strip()
                
                # 7. Best Sellers Rank
                if inner_rank == "N/A":
                    rank_matches = re.findall(r'#[\d,]+\s+in\s+[^\n]+', full_text)
                    if rank_matches:
                        inner_rank = " | ".join(rank_matches[:3])
                        
                # 8. Logline / Description Fallback
                # If desc wasn't picked up by standard selectors, grab a chunk of text that looks like a story summary
                desc = locals().get('description', "N/A")
                if desc == "N/A" or len(desc) < 20:
                    # Look for paragraph-style text
                    m = re.search(r'\n([A-Z][\s\S]{200,1500}?)(?:\n\n|\n[A-Z][a-z]+(?:\s[A-Z][a-z]+)?\n|Read less|Read more)', full_text)
                    if m: desc = m.group(1).strip()
                # Update locals description so it carries over
                locals()['description'] = desc
            except Exception as e:
                print(f"Fallback regex error: {e}")

            # Final cleanup: strip any leading colons, spaces, Unicode markers from all values
            publisher = re.sub(r'^[\s:;\u200e\u200f\u200b]+', '', publisher).strip() if publisher != "N/A" else "N/A"
            pub_date = re.sub(r'^[\s:;\u200e\u200f\u200b]+', '', pub_date).strip() if pub_date != "N/A" else "N/A"

            title_safe = url.split('/dp/')[0].rsplit('/', 1)[-1][:40] if '/dp/' in url else 'unknown'
            print(f"  [{title_safe[:30]}] Author={author} | Pub={publisher} | Date={pub_date} | Prices={len(price_lines)}")

            return {
                "Description":      description,
                "Publisher":        publisher,
                "Publication Date": pub_date,
                "Author Name":      author,
                "Price":            price_str,
                "Rating":           rating,
                "Number of Reviews": reviews,
                "Actual Reviews":   actual_reviews,
                "Amazon URL":       page.url,
                "Series Name":      series_name,
                "Book Number":      book_number,
                "Total Books":      total_books_in_series,
                "Pages":            pages,
                "Inner Rank":       inner_rank
            }
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return {
                "Description": "N/A", "Publisher": "N/A", "Publication Date": "N/A", 
                "Author Name": "N/A", "Price": "N/A", "Rating": "N/A", "Number of Reviews": "N/A", "Actual Reviews": "N/A",
                "Series Name": "N/A",
                "Book Number": "N/A", "Total Books": "N/A", "Pages": "N/A", "Inner Rank": "N/A"
            }
        finally:
            await page.close()


# GoodreadsScraper moved to goodreads_scraper.py


class AuthorScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def find_author_details(self, context, author_name):
        if not author_name or author_name == "N/A":
            return {}

        # Handle Multiple Authors (e.g., "A + B")
        authors = [a.strip() for a in re.split(r'\s*\+\s*|\s*\&\s*|\s+and\s+', str(author_name)) if a.strip()]
        primary_author = authors[0] if authors else str(author_name)

        page = await context.new_page()
        details = {
            "Author_Email": "N/A",
            "Agent_Email": "N/A",
            "Facebook": "N/A",
            "Twitter": "N/A",
            "Instagram": "N/A",
            "Website": "N/A",
            "Contact_Website": "N/A",
            "Other_Contact": "N/A"
        }

        try:
            # Step 1: Find Official Website (Using DuckDuckGo for better bot resilience)
            print(f"  Author: Searching for '{primary_author}' official website...")
            search_query = f"{primary_author} author official website contact"
            ddg_url = f"https://html.duckduckgo.com/html/?q={search_query.replace(' ', '+')}"
            
            website_url = None
            try:
                await page.goto(ddg_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2)
                
                # Look for results
                links = await page.query_selector_all('.result__a')
                for link in links:
                    if page.is_closed(): break
                    href = await link.evaluate("el => el.href")
                    if any(x in href for x in ['facebook.com', 'twitter.com', 'instagram.com', 'wikipedia.org', 'goodreads.com', 'amazon.com', 'linkedin.com']):
                        continue
                    if 'brave.com' in href or 'search.yahoo.com' in href:
                        continue
                    
                    website_url = href
                    print(f"  Author: Potential website found: {website_url}")
                    break
            except Exception: pass

            if website_url:
                details["Website"] = website_url
                details["Contact_Website"] = website_url # Default
                
                # Step 2: Scrape the website
                await page.goto(website_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2)
                
                # Scroll to bottom to capture footers/lazy-loaded emails
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1)
                
                content = await page.content()
                
                # Enhanced Email Extraction (handles [at] and (at) patterns)
                def extract_emails(text):
                    # Standard regex with negative lookahead to exclude common image/file extensions (false positives like @2x.png)
                    standard = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.(?!png|jpg|jpeg|gif|svg|webp|bmp|ico|tiff|pdf|mp4|mov|avi|zip|tar|gz|exe|dmg)[a-zA-Z]{2,}', text)
                    # Obfuscated patterns
                    obfuscated = re.findall(r'[a-zA-Z0-9._%+-]+\s*[\[\(]\s*at\s*[\]\)]\s*[a-zA-Z0-9.-]+\s*[\[\(]\s*dot\s*[\]\)]\s*[a-zA-Z]{2,}', text, re.I)
                    decoded = [e.replace(' [at] ', '@').replace(' (at) ', '@').replace(' [dot] ', '.').replace(' (dot) ', '.') for e in obfuscated]
                    return list(set(standard + decoded))

                emails = extract_emails(content)
                
                def classify_emails(found_emails, text_context):
                    lower_context = text_context.lower()
                    for email in set(found_emails):
                        lower_email = email.lower()
                        # Agency/Agent Indicators
                        agent_keywords = ['agent', 'literary', 'representation', 'rights', 'press', 'publicist', 'media', 'inquiry']
                        # Check proximity in text (rough check via index)
                        email_idx = lower_context.find(lower_email)
                        context_snippet = lower_context[max(0, email_idx-100):email_idx+100]
                        
                        if any(kw in lower_email for kw in ['agent', 'press', 'rights']) or any(kw in context_snippet for kw in agent_keywords):
                            if details["Agent_Email"] == "N/A": details["Agent_Email"] = email
                        # Personal Indicators
                        else:
                            if details["Author_Email"] == "N/A": details["Author_Email"] = email

                classify_emails(emails, content)

                # Socials
                links = await page.query_selector_all('a[href]')
                for link in links:
                    href = await link.evaluate("el => el.href")
                    if 'facebook.com' in href and details["Facebook"] == "N/A": details["Facebook"] = href
                    elif ('twitter.com' in href or 'x.com' in href) and details["Twitter"] == "N/A": details["Twitter"] = href
                    elif 'instagram.com' in href and details["Instagram"] == "N/A": details["Instagram"] = href
                
                # Step 3: Targeted Contact Page
                contact_link = await page.query_selector('a:has-text("Contact"), a:has-text("About"), a:has-text("Reach")')
                if contact_link:
                    contact_url = await contact_link.evaluate("el => el.href")
                    details["Contact_Website"] = contact_url
                    await page.goto(contact_url, wait_until="domcontentloaded", timeout=30000)
                    c_content = await page.content()
                    c_emails = extract_emails(c_content)
                    classify_emails(c_emails, c_content)

            return details
        except Exception as e:
            print(f"  Author: Error for '{author_name}': {e}")
            return details
        finally:
            await page.close()


