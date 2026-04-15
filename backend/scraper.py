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
                    title_el = await item.query_selector(
                        '.p13n-sc-untruncated-desktop-title, '
                        '._cDE_gridItem_truncate-title, '
                        '.zg-grid-general-faceout .a-size-base, '
                        '[class*="title"]'
                    )

                    # Skip immediately if no title -- this is a wrapper div, not a book card
                    raw_title = clean_text(await title_el.inner_text()) if title_el else ""
                    if not raw_title:
                        continue
                    # Reject if title looks like a price (e.g. "INR 1,394.08" or "$24.99")
                    if re.match(r'^(INR|USD|\$|£|€|₹|Rs\.?)\s*[\d,\.]+', raw_title, re.IGNORECASE) or re.match(r'^[\d,\.]+$', raw_title):
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

                    # Strategy 5: extract author name from the Amazon URL slug
                    # URLs look like /Author-Name-Book-Title/dp/BXXXXX
                    amazon_url = await link_el.get_attribute('href') if link_el else ""
                    if (not author_name or author_name == "N/A") and amazon_url:
                        url_slug = amazon_url.split('/dp/')[0] if '/dp/' in amazon_url else ""
                        url_slug = url_slug.rsplit('/', 1)[-1] if url_slug else ""
                        if url_slug:
                            # URL slugs use hyphens: Author-Name-Book-Title
                            # We'll store this as a fallback hint for the product page
                            pass  # Will be resolved by product-page extraction in Phase 2

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
            return {"Description": "N/A", "Publisher": "N/A", "Publication Date": "N/A", "Author Name": "N/A", "Price": "N/A"}
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
                "Price":            price_str
            }
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return {"Description": "N/A", "Publisher": "N/A", "Publication Date": "N/A", "Author Name": "N/A", "Price": "N/A"}
        finally:
            await page.close()


