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


def normalize_title_for_search(title):
    if not title:
        return ""
    # Standard cleanup
    t = title.lower()
    
    # Remove common Amazon subtitles and fluff
    remove_patterns = [
        r':\s+a\s+novel.*', 
        r':\s+a\s+read\s+with\s+jenna\s+pick.*',
        r':\s+a\s+memoir.*',
        r'\(deluxe\s+edition\).*',
        r'\(special\s+edition\).*',
        r'\(.*book\s+\d+.*\)', # Remove (Series Name Book 1)
        r'\(.*series.*\)',
        r'\[.*\]', # Remove [Brackets]
    ]
    
    for pattern in remove_patterns:
        t = re.sub(pattern, '', t)
    
    # Clean up punctuation and extra spaces
    t = re.sub(r'[:\-—].*', '', t) # Take only part before first colon/dash for broad search
    t = re.sub(r'[^\w\s]', '', t)
    return t.strip()


class AmazonScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def scrape_bestseller_list(self, url, limit=10):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            try:
                # 'load' waits for full page including any redirect chains
                await page.goto(url, wait_until="load", timeout=60000)

                # Wait for network to settle (redirect chains, CAPTCHA checks)
                try:
                    await page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    pass

                # Confirm the bestseller grid is actually present
                try:
                    await page.wait_for_selector(
                        '[data-asin], .zg-grid-general-faceout',
                        timeout=20000
                    )
                except Exception:
                    print("Warning: bestseller grid not found - page may be CAPTCHA or redirect")

                # Scroll to trigger lazy-loading
                try:
                    await page.evaluate("""async () => {
                        await new Promise((resolve) => {
                            let totalHeight = 0;
                            let distance = 100;
                            let timer = setInterval(() => {
                                let scrollHeight = document.body.scrollHeight;
                                window.scrollBy(0, distance);
                                totalHeight += distance;
                                if (totalHeight >= scrollHeight) {
                                    clearInterval(timer);
                                    resolve();
                                }
                            }, 100);
                        });
                    }""")
                    await asyncio.sleep(1)
                except Exception as scroll_err:
                    print(f"Scroll skipped: {scroll_err}")

                try:
                    await page.wait_for_load_state("networkidle", timeout=8000)
                except Exception:
                    pass

                # --- Item selection ---
                # [data-asin] is the broadest selector but guaranteed to hit every book card.
                # We skip items with no title (those are outer wrapper divs, not book cards).
                # Final deduplication is done by Book Title to remove any remaining duplicates.
                items = await page.query_selector_all('[data-asin]')
                print(f"Found {len(items)} [data-asin] elements on page")

                results = []

                for item in items:
                    # --- Best Effort Title Extraction ---
                    # 1. Try specific bestseller title classes
                    title_el = await item.query_selector('.p13n-sc-untruncated-desktop-title, ._cDE_gridItem_truncate-title')
                    raw_title = ""
                    if title_el:
                        raw_title = clean_text(await title_el.inner_text())
                    
                    # 2. High-reliability fallback: Image alt text
                    if not raw_title or "formats available" in raw_title.lower() or re.search(r'(INR|USD|\$|£|€|₹|Rs\.?)\s*[\d,\.]+', raw_title, re.IGNORECASE):
                        img_el = await item.query_selector('img')
                        if img_el:
                            alt_val = await img_el.get_attribute('alt')
                            if alt_val:
                                # Sometimes alt text is just 'image' or 'poster', we need to check length
                                if len(alt_val) > 3:
                                    raw_title = clean_text(alt_val)

                    if not raw_title:
                        continue
                    
                    # Reject if title is still junk (price only or 'formats available')
                    if re.search(r'(INR|USD|\$|£|€|₹|Rs\.?)\s*[\d,\.]+', raw_title, re.IGNORECASE) or \
                       re.match(r'^[\d,\.]+$', raw_title) or \
                       "formats available" in raw_title.lower():
                        continue

                    # ====== RANK EXTRACTION (cascade, same pattern as author) ======
                    rank_text = "N/A"

                    # Strategy 1: known Amazon rank badge selectors
                    for rank_sel in [
                        '.zg-bdg-text',                    # Classic bestseller badge
                        '.p13n-sc-badge-label-size-base',  # New UI badge
                        'span.zg-badge-text',              # Alt class
                        '[class*="badge"] span',           # Generic badge span
                        '[class*="rank"]',                 # Any rank element
                        'span.a-size-base.a-color-secondary.a-text-bold',  # Bold rank number
                    ]:
                        el = await item.query_selector(rank_sel)
                        if el:
                            t = clean_text(await el.inner_text())
                            # Must look like a rank: #1, 1, No. 1, etc.
                            if t and re.search(r'\d', t):
                                rank_text = t.lstrip('#').strip()
                                break

                    # Strategy 2: JS scan -- find a small element whose text is just a number
                    if rank_text == "N/A":
                        try:
                            js_rank = await item.evaluate("""(el) => {
                                const spans = el.querySelectorAll('span, div');
                                for (const s of spans) {
                                    const txt = (s.textContent || '').trim();
                                    // Rank is typically a 1-2 digit number, maybe prefixed with #
                                    if (/^#?\\d{1,2}$/.test(txt)) return txt.replace('#','').trim();
                                }
                                return null;
                            }""")
                            if js_rank:
                                rank_text = js_rank
                        except Exception:
                            pass
                    rating_el  = await item.query_selector('.a-icon-star-small .a-icon-alt, [class*="star"]')
                    reviews_el = await item.query_selector('[aria-label*="ratings"], [aria-label*="reviews"]')
                    price_el   = await item.query_selector('.p13n-sc-price, [class*="price"]')
                    link_el    = await item.query_selector('a.a-link-normal[href*="/dp/"], a.a-link-normal')

                    # ====== AUTHOR EXTRACTION (5-strategy cascade) ======
                    author_name = "N/A"

                    # Strategy 1: Amazon author links (multiple patterns for .com, .in, etc.)
                    for author_selector in [
                        'a[href*="/e/"]',                    # Amazon author page (/e/BXXXXXX)
                        'div.a-row.a-size-small a',           # "by Author" row link
                        '.a-row a.a-link-normal',             # Generic row link
                        'span.a-size-small.a-color-base + a', # Span "by" followed by author link
                        '[class*="contributor"] a',
                        '[class*="author"] a',
                    ]:
                        try:
                            el = await item.query_selector(author_selector)
                            if el:
                                text = clean_text(await el.inner_text())
                                # Reject if it looks like a rating, price, or generic text
                                if (text and text != "N/A" and len(text) > 1
                                    and not re.match(r'^[\d\.\$,]+$', text)
                                    and 'out of' not in text.lower()
                                    and 'stars' not in text.lower()
                                    and 'star' not in text.lower()
                                    and 'ratings' not in text.lower()):
                                    author_name = text
                                    break
                        except Exception:
                            continue

                    # Strategy 2: JS-based -- get text of all a.size-small rows and find "by" pattern
                    if not author_name or author_name == "N/A":
                        try:
                            js_author = await item.evaluate("""(el) => {
                                // Look for 'by Author' pattern in the card's text nodes
                                const rows = el.querySelectorAll('.a-row, .a-size-small, div');
                                for (const row of rows) {
                                    const txt = row.textContent || '';
                                    const m = txt.match(/\\bby\\s+([A-Z][A-Za-z .'-]+)/i);
                                    if (m && m[1].trim().length > 2) {
                                        // Verify it's not a price or rating
                                        const candidate = m[1].trim();
                                        if (!/^[\\d.$,]+$/.test(candidate)) return candidate;
                                    }
                                }
                                // Fallback: look in anchor text that's not a title or price
                                const links = el.querySelectorAll('a.a-link-normal');
                                for (const link of links) {
                                    const href = link.getAttribute('href') || '';
                                    const txt = (link.textContent || '').trim();
                                    if (href.includes('/e/') && txt.length > 1) return txt;
                                }
                                return null;
                            }""")
                            if js_author:
                                author_name = clean_text(js_author)
                        except Exception:
                            pass

                    # Strategy 3: parse "by [Author]" from the title text itself
                    if not author_name or author_name == "N/A":
                        by_match = re.search(
                            r'\bby\s+([A-Z][^\|,\[\]]+?)(?:\s*[\|,\[]|$)',
                            raw_title, re.IGNORECASE
                        )
                        if by_match:
                            author_name = by_match.group(1).strip()

                    # Strategy 4: scan the full card text for "by [Name]" pattern
                    if not author_name or author_name == "N/A":
                        try:
                            item_text = clean_text(await item.inner_text())
                            by_match = re.search(
                                r'\bby\s+([A-Z][A-Za-z\s\.\-\']+?)(?:\s*[\|\n,;(]|\d|$)',
                                item_text
                            )
                            if by_match:
                                candidate = by_match.group(1).strip()
                                if not re.match(r'^[\d\.]+$', candidate) and len(candidate) > 2:
                                    author_name = candidate
                        except Exception:
                            pass

                    # ====== ASIN & URL EXTRACTION ======
                    asin = await item.get_attribute('data-asin')
                    if not asin:
                        # Fallback: try to find an element with data-asin
                        asin_el = await item.query_selector('[data-asin]')
                        if asin_el:
                            asin = await asin_el.get_attribute('data-asin')

                    # Extract absolute href using el.href property (resolves relative links automatically)
                    raw_href = ""
                    if link_el:
                        try:
                            raw_href = await link_el.evaluate("el => el.href")
                        except Exception:
                            raw_href = await link_el.get_attribute('href')

                    # Normalize: cleanest URL is /dp/ASIN. 
                    # If we have an absolute href but it's relative, we prefix the base domain later in app.py.
                    # But if we have ASIN, we can build a clean one.
                    amazon_url = raw_href
                    if asin and asin != "N/A":
                        # If we have an absolute URL, use its domain. If not, construct relative for app.py to fix.
                        if raw_href and raw_href.startswith('http'):
                            domain = "/".join(raw_href.split("/", 3)[:3])
                            amazon_url = f"{domain}/dp/{asin}"
                        else:
                            amazon_url = f"/dp/{asin}"
                    elif not raw_href:
                        amazon_url = "N/A"

                    # Reviews — extract the count (not the rating) from aria-label
                    # aria-label looks like: "4.7 out of 5 stars, 1,234 ratings"
                    reviews_count = 0
                    if reviews_el:
                        aria = await reviews_el.get_attribute('aria-label') or ""
                        if not aria:
                            aria = clean_text(await reviews_el.inner_text())
                        # Try to grab the number AFTER "stars," or just the last standalone number
                        m = re.search(r'stars?,?\s*([\d,]+)\s*ratings?', aria, re.IGNORECASE)
                        if m:
                            reviews_count = int(m.group(1).replace(',', ''))
                        else:
                            # Fallback: grab any number that looks like a review count (> 10)
                            nums = re.findall(r'[\d,]+', aria)
                            counts = [int(n.replace(',', '')) for n in nums if int(n.replace(',', '')) > 10]
                            if counts:
                                reviews_count = max(counts)

                    results.append({
                        "Rank":              rank_text,
                        "Book Title":        raw_title,
                        "Author Name":       author_name,
                        "Rating":            clean_numeric(await rating_el.inner_text()) if rating_el else 0,
                        "Number of Reviews": reviews_count,
                        "Price":             "N/A",  # Will be enriched from product page
                        "Amazon URL":        amazon_url
                    })

                # Deduplicate by Book Title (handles wrapper vs card on same product)
                seen_titles = set()
                unique_results = []
                for r in results:
                    t = r["Book Title"]
                    if t not in seen_titles:
                        seen_titles.add(t)
                        unique_results.append(r)

                print(f"Raw: {len(results)} items | After dedup: {len(unique_results)} unique books")
                return unique_results[:limit]

            finally:
                await browser.close()

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
            ]:
                el = await page.query_selector(sel)
                if el:
                    text = clean_text(await el.inner_text())
                    if (text and len(text) > 1
                        and not re.match(r'^[\d\.\$,]+$', text)
                        and 'out of' not in text.lower()
                        and 'stars' not in text.lower()):
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
            # Extract all format prices from the product page's format switcher (#tmmSwatches)
            # Output format: "Kindle - INR 92.13\nHardcover - INR 1,674.20\nPaperback - INR 850.00"
            price_lines = []
            seen_formats = set()
            try:
                # Use ONLY .a-button-inner (innermost) — NOT li which wraps it and causes duplicates
                format_items = await page.query_selector_all(
                    '#tmmSwatches .a-button-inner, '
                    '[id*="tmm-grid-swatch"] .a-button-inner'
                )
                for fi in format_items:
                    text = clean_text(await fi.inner_text())
                    if not text:
                        continue
                    # Text looks like "Kindle\nINR 92.13" or "Kindle Edition\n₹92.13"
                    parts = [p.strip() for p in text.split('\n') if p.strip()]
                    if len(parts) >= 2:
                        format_name = parts[0]
                        # Skip if we already have this format (dedup)
                        if format_name.lower() in seen_formats:
                            continue
                        seen_formats.add(format_name.lower())
                        # Find the price part (contains digits and currency)
                        price_part = next(
                            (p for p in parts[1:] if re.search(r'[\d,\.]+', p) and
                             re.search(r'[\u20b9\$\£\€]|INR|USD|GBP|EUR|Rs\.?', p, re.IGNORECASE)),
                            parts[-1]
                        )
                        price_clean = re.sub(r'\s+', ' ', price_part).strip()
                        if format_name and price_clean:
                            price_lines.append(f"{format_name} - {price_clean}")
            except Exception as e:
                print(f"Price extraction error: {e}")

            # Fallback: if tmmSwatches gave nothing, try JS scan
            if not price_lines:
                try:
                    js_prices = await page.evaluate("""() => {
                        const results = [];
                        const seen = new Set();
                        const swatches = document.querySelectorAll(
                            '#tmmSwatches .a-button-inner, .swatchElement .a-button-inner'
                        );
                        for (const sw of swatches) {
                            const txt = (sw.innerText || '').trim();
                            if (!txt) continue;
                            const lines = txt.split('\\n').map(l => l.trim()).filter(Boolean);
                            if (lines.length >= 2) {
                                const fmt = lines[0];
                                if (seen.has(fmt.toLowerCase())) continue;
                                seen.add(fmt.toLowerCase());
                                const price = lines.find(l => /[\\d,\\.]+/.test(l) && l !== fmt);
                                if (fmt && price) results.push(fmt + ' - ' + price);
                            }
                        }
                        return results;
                    }""")
                    if js_prices:
                        price_lines = js_prices
                except Exception:
                    pass

            price_str = "\n".join(price_lines) if price_lines else "N/A"

            # ====== NEW: SERIES, PAGES, INNER RANK EXTRACTION ======
            series_name = "N/A"
            book_number = "N/A"
            total_books_in_series = "N/A"
            pages = "N/A"
            inner_rank = "N/A"

            try:
                # 1. Series info (e.g., "Book 1 of 3: ...")
                series_el = await page.query_selector('#seriesBulletWidget_feature_div, #bookSeries_feature_div, .series-link')
                if series_el:
                    series_text = clean_text(await series_el.inner_text())
                    m = re.search(r'Book\s+(\d+)\s+of\s+(\d+)\s*:\s*(.+)', series_text, re.IGNORECASE)
                    if m:
                        book_number = m.group(1).strip()
                        total_books_in_series = m.group(2).strip()
                        series_name = m.group(3).strip()
                    else:
                        m2 = re.search(r'Part\s+of\s*:\s*(.+)', series_text, re.IGNORECASE)
                        if m2: series_name = m2.group(1).strip()

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
                "Author Name": "N/A", "Price": "N/A", "Series Name": "N/A",
                "Book Number": "N/A", "Total Books": "N/A", "Pages": "N/A", "Inner Rank": "N/A"
            }
        finally:
            await page.close()


class GoodreadsScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def scrape_goodreads_data(self, context, title, author, isbn10="N/A", isbn13="N/A", asin="N/A"):
        if not title or title == "N/A":
            return {}

        page = await context.new_page()
        try:
            book_url = None
            
            # --- TIER 1: Direct ID Strategy (Most Reliable) ---
            potential_ids = [isbn13, isbn10, asin]
            for pid in potential_ids:
                if pid and pid != "N/A":
                    print(f"  Goodreads: Discovery Tier 1 (Direct ID {pid})...")
                    direct_url = f"https://www.goodreads.com/book/isbn/{pid}"
                    try:
                        await page.goto(direct_url, wait_until="domcontentloaded", timeout=20000)
                        if "goodreads.com/book/show/" in page.url or "goodreads.com/work/" in page.url:
                            book_url = page.url
                            print(f"  Goodreads: Successful direct access: {book_url}")
                            break
                    except Exception:
                        continue
            
            # --- TIER 2: Internal Search (Safe since we are logged in) ---
            if not book_url:
                print(f"  Goodreads: Discovery Tier 2 (Internal Search)...")
                clean_title = normalize_title_for_search(title)
                search_query = f"{clean_title} {author}"
                search_url = f"https://www.goodreads.com/search?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                    
                    # Get all search results and filter for the best match
                    result_rows = await page.query_selector_all('tr[itemtype="http://schema.org/Book"]')
                    for row in result_rows:
                        row_text = (await row.inner_text()).lower()
                        # Skip summaries/guides
                        if any(x in row_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        
                        # Verify author if possible
                        author_link = await row.query_selector('.authorNameRes')
                        if author_link:
                            res_author = (await author_link.inner_text()).lower()
                            if author.lower() in res_author or any(part in res_author for part in author.lower().split()):
                                book_link = await row.query_selector('a.bookTitle')
                                if book_link:
                                    book_url = await book_link.evaluate("el => el.href")
                                    print(f"  Goodreads: Found via Internal Search (Author Match): {book_url}")
                                    break
                    
                    # Fallback to first non-summary if no author match
                    if not book_url and result_rows:
                        first_book = await page.query_selector('a.bookTitle, [data-testid="bookTitle"] a')
                        if first_book:
                            book_url = await first_book.evaluate("el => el.href")
                            print(f"  Goodreads: Found via Internal Search (First Result): {book_url}")
                except Exception as ie:
                    print(f"  Goodreads: Internal search failed: {ie}")

            # --- TIER 3: Brave Discovery (Broad Fallback) ---
            if not book_url:
                print(f"  Goodreads: Discovery Tier 3 (Brave Fallback)...")
                search_query = f"{normalize_title_for_search(title)} {author} goodreads"
                brave_url = f"https://search.brave.com/search?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(brave_url, wait_until="domcontentloaded", timeout=30000)
                    links = await page.query_selector_all('a[href*="goodreads.com/book/show/"]')
                    for link in links:
                        link_text = (await link.inner_text()).lower()
                        href = await link.evaluate("el => el.href")
                        # Skip summaries/guides
                        if any(x in link_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        book_url = href
                        print(f"  Goodreads: Found via Brave: {book_url}")
                        break
                except Exception:
                    pass

            # --- TIER 4: DuckDuckGo HTML Fallback ---
            if not book_url:
                print(f"  Goodreads: Discovery Tier 4 (DuckDuckGo)...")
                search_query = f"{normalize_title_for_search(title)} {author} in goodreads"
                ddg_url = f"https://html.duckduckgo.com/html/?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(ddg_url, wait_until="domcontentloaded", timeout=30000)
                    links = await page.query_selector_all('a[href*="goodreads.com/book/show/"]')
                    for link in links:
                        link_text = (await link.inner_text()).lower()
                        href = await link.evaluate("el => el.href")
                        if any(x in link_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        book_url = href
                        print(f"  Goodreads: Found via DuckDuckGo: {book_url}")
                        break
                except Exception:
                    pass

            # --- TIER 5: Broadened Discovery (Normalized Title) ---
            if not book_url:
                clean_q = normalize_title_for_search(title)
                print(f"  Goodreads: Discovery Tier 5 (Broad) for: '{clean_q}'")
                search_query = f"{clean_q} {author} genre subgenre goodreads"
                brave_url = f"https://search.brave.com/search?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(brave_url, wait_until="domcontentloaded", timeout=30000)
                    links = await page.query_selector_all('a[href*="goodreads.com/book/show/"]')
                    for link in links:
                        link_text = (await link.inner_text()).lower()
                        href = await link.evaluate("el => el.href")
                        if any(x in link_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        book_url = href
                        print(f"  Goodreads: Found via Broad Search: {book_url}")
                        break
                except Exception:
                    pass

            if not book_url:
                print(f"  Goodreads: No results found for '{title[:20]}'")
                return {}
            
            # Step 2: Final navigation to book page
            if page.url != book_url:
                await page.goto(book_url, wait_until="domcontentloaded", timeout=60000)
            
            await asyncio.sleep(4) # Extended wait for React hydration of genres

            # Extract Genres
            genres = []
            try:
                # Primary selector for new layout
                genre_els = await page.query_selector_all('[data-testid="genresList"] .Button__labelItem, .BookPageMetadataSection__genre a')
                if not genre_els:
                    # Fallback to general link scan
                    genre_els = await page.query_selector_all('a[href*="/genres/"]')
                
                for gel in genre_els:
                    txt = clean_text(await gel.inner_text())
                    if txt and txt not in genres and len(txt) < 30:
                        genres.append(txt)
            except Exception:
                pass
            
            # Specific Romantasy Check
            is_romantasy = "Yes" if any("romantasy" in g.lower() for g in genres) else "No"
            genre_main = genres[0] if genres else "N/A"
            genre_sub = genres[1] if len(genres) > 1 else "N/A"

            # Step 2: Extract Book Details (Ratings, Series URL)
            # Try JSON-LD first (most stable)
            ld_json = {}
            try:
                ld_element = await page.query_selector('script[type="application/ld+json"]')
                if ld_element:
                    import json
                    ld_json = json.loads(await ld_element.inner_text())
            except Exception:
                pass

            avg_rating = ld_json.get('aggregateRating', {}).get('ratingValue', "N/A")
            rating_count = ld_json.get('aggregateRating', {}).get('ratingCount', "N/A")

            # Series link
            series_url = "N/A"
            series_name = "N/A"
            # Look for link in title header or near book description
            series_el = await page.query_selector('h3.Text__title3 a[href*="/series/"], [data-testid="bookTitle"] + .Text + a[href*="/series/"]')
            if not series_el or str(series_el) == "JSHandle@undefined":
                # Fallback JS scan
                series_el = await page.evaluate_handle("""() => {
                    const links = Array.from(document.querySelectorAll('a[href*="/series/"]'));
                    return links.find(l => {
                        const txt = l.innerText.toLowerCase();
                        const pTxt = l.parentElement ? l.parentElement.innerText.toLowerCase() : "";
                        return txt.includes('series') || pTxt.includes('series');
                    }) || null;
                }""")
            
            # Re-check handle
            is_valid = await series_el.evaluate("el => el instanceof HTMLAnchorElement") if series_el else False
            
            if is_valid:
                series_url = await series_el.evaluate("el => el.href")
                series_name = clean_text(await series_el.inner_text())

            # Step 3: If Series URL exists, visit it
            series_data = {
                "Num_Primary_Books": "N/A",
                "Total_Pages_Primary_Books": 0,
                "Book1_Rating": "N/A",
                "Book1_Num_Ratings": "N/A"
            }

            if series_url and series_url != "N/A":
                try:
                    await page.goto(series_url, wait_until="domcontentloaded", timeout=60000)
                    
                    # 1. Primary books count (from header)
                    content = await page.content()
                    primary_match = re.search(r'(\d+)\s+primary\s+works', content, re.IGNORECASE)
                    if primary_match:
                        series_data["Num_Primary_Books"] = primary_match.group(1)

                    # 2. Extract details accurately from book rows
                    book_rows = await page.query_selector_all('.listWithDividers__item, .seriesWork, div.u-paddingBottomMedium')
                    total_pages = 0
                    found_book1 = False
                    
                    for i, row in enumerate(book_rows):
                        row_text = (await row.inner_text()).lower()
                        
                        # Only sum pages for primary works (avoiding .5, .6, 2.5)
                        # Primary works usually have a index like "book 1", "book 2" (integers)
                        is_primary = False
                        idx_match = re.search(r'book\s+(\d+)$', row_text.split('\n')[0], re.IGNORECASE) or \
                                     re.search(r'^\s*(\d+)\s*$', row_text.split('\n')[0])
                        
                        # Simpler check: if it doesn't contain a decimal in the first few words of the title row
                        page_match = re.search(r'(\d+)\s+pages', row_text)
                        
                        # For page summing, we try to be inclusive but prioritize likely primary works
                        if page_match:
                            total_pages += int(page_match.group(1))
                        
                        # 3. Targeted "Book 1" Extraction
                        # We want the FIRST book or the one explicitly labeled "book 1"
                        if not found_book1:
                            if "book 1" in row_text or (i == 0 and "book" not in row_text):
                                r_match = re.search(r'([\d.]+)\s+avg\s+rating\s+[—\-]\s+([\d,]+)\s+ratings', row_text, re.IGNORECASE)
                                if r_match:
                                    series_data["Book1_Rating"] = r_match.group(1)
                                    series_data["Book1_Num_Ratings"] = r_match.group(2).replace(',', '')
                                    found_book1 = True
                    
                    series_data["Total_Pages_Primary_Books"] = total_pages
                except Exception as se:
                    print(f"  Goodreads: Series page error: {se}")

            return {
                "GoodReads_Series_URL": series_url,
                "GoodReads_Rating": avg_rating,
                "GoodReads_Rating_Count": rating_count,
                "Genre": genre_main,
                "Sub_Genre": genre_sub,
                "Romantasy_Subgenre": is_romantasy,
                **series_data
            }
        except Exception as e:
            print(f"  Goodreads: Error for '{title[:20]}': {e}")
            return {}
        finally:
            await page.close()


class AuthorScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def find_author_details(self, context, author_name):
        if not author_name or author_name == "N/A":
            return {}

        page = await context.new_page()
        details = {
            "Author_Email": "N/A",
            "Agent_Email": "N/A",
            "Facebook": "N/A",
            "Twitter": "N/A",
            "Instagram": "N/A",
            "Website": "N/A",
            "Other_Contact": "N/A"
        }

        try:
            # Step 1: Find Official Website
            print(f"  Author: Searching for '{author_name}' official website...")
            search_query = f"{author_name} official website contact"
            brave_url = f"https://search.brave.com/search?q={search_query.replace(' ', '+')}"
            
            website_url = None
            try:
                await page.goto(brave_url, wait_until="domcontentloaded", timeout=30000)
                # Look for results that likely point to a personal homepage
                links = await page.query_selector_all('main a')
                for link in links:
                    href = await link.evaluate("el => el.href")
                    if any(x in href for x in ['facebook.com', 'twitter.com', 'instagram.com', 'wikipedia.org', 'goodreads.com', 'amazon.com']):
                        continue
                    if 'brave.com' in href:
                        continue
                    
                    # Heuristic: the first reasonable outside link is often the official site
                    website_url = href
                    print(f"  Author: Potential website found: {website_url}")
                    break
            except Exception:
                pass

            if not website_url:
                # Try DuckDuckGo fallback
                print(f"  Author: DDG Fallback for website...")
                ddg_url = f"https://html.duckduckgo.com/html/?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(ddg_url, wait_until="domcontentloaded", timeout=20000)
                    top_link = await page.query_selector('.result__a')
                    if top_link:
                        website_url = await top_link.evaluate("el => el.href")
                except Exception:
                    pass

            if website_url:
                details["Website"] = website_url
                # Step 2: Scrape the website for socials and contact
                await page.goto(website_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2) # JS Render
                
                content = await page.content()
                
                # Emails (Basic Regex)
                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', content)
                if emails:
                    # Heuristic: if 'agent' is near the email, it's an agent email
                    unique_emails = list(set(emails))
                    for email in unique_emails:
                        if 'agent' in email.lower() or 'literary' in content.lower()[content.lower().find(email.lower())-50:content.lower().find(email.lower())+50]:
                            details["Agent_Email"] = email
                        elif details["Author_Email"] == "N/A":
                            details["Author_Email"] = email

                # Social Links
                links = await page.query_selector_all('a[href]')
                for link in links:
                    href = await link.evaluate("el => el.href")
                    if 'facebook.com' in href and details["Facebook"] == "N/A":
                        details["Facebook"] = href
                    elif ('twitter.com' in href or 'x.com' in href) and details["Twitter"] == "N/A":
                        details["Twitter"] = href
                    elif 'instagram.com' in href and details["Instagram"] == "N/A":
                        details["Instagram"] = href
                
                # Check for "Contact" page specifically
                contact_link = await page.query_selector('a:has-text("Contact"), a:has-text("About")')
                if contact_link:
                    contact_url = await contact_link.evaluate("el => el.href")
                    await page.goto(contact_url, wait_until="domcontentloaded", timeout=30000)
                    contact_content = await page.content()
                    
                    # Re-scan for emails on contact page
                    c_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', contact_content)
                    if c_emails:
                        for email in set(c_emails):
                            if 'agent' in email.lower() or 'press' in email.lower():
                                details["Agent_Email"] = email
                            elif details["Author_Email"] == "N/A":
                                details["Author_Email"] = email

            return details
        except Exception as e:
            print(f"  Author: Error for '{author_name}': {e}")
            return details
        finally:
            await page.close()


