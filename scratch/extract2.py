def scrape_product_details_tab(self, context, url, base_url="https://www.amazon.com"):
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
            
            while True:
                try:
                    if await page.query_selector('form[action="/errors/validateCaptcha"], input#captchacharacters'):
                        print("🚨 CAPTCHA DETECTED on details page! Please solve it in the browser...", flush=True)
                        await asyncio.sleep(5)
                    else:
                        break
                except Exception:
                    break
                    
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
                '#series-page-description',
                '.series-description',
                '[data-a-expander-name="series_description_expander"]',
                '#series-description-expander'
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
                byline_el = await page.query_selector('#bylineInfo, #byline, .author-bottom')
                if byline_el:
                    byline_text = clean_text(await byline_el.inner_text())
                    # Check for multi-author pattern: "by Sarina Bowen (Author), Rebecca Yarros (Author)"
                    if " (Author)" in byline_text:
                        # Extract all names before " (Author)"
                        m_authors = re.findall(r'([A-Z][A-Za-z\s\.\-\']+?)\s*\(Author\)', byline_text)
                        if m_authors:
                            author = ", ".join([a.strip() for a in m_authors if 'by ' not in a.lower() or a.lower().startswith('by ')])
                            author = re.sub(r'(?i)^by\s+', '', author) # remove 'by ' prefix if it got captured in the first author
                    
                    if author == "N/A" or not author:
                        m = re.search(r'\bby\s+([A-Z][A-Za-z\s\.\-\']+?)(?:\s*[\(,;|]|$)', byline_text, re.IGNORECASE)
                        if m:
                            author = m.group(1).strip()
            
            if author == "N/A":
                # Super fallback: look for any link containing author-related hrefs
                try:
                    author_fallback = await page.evaluate('''() => {
                        let links = Array.from(document.querySelectorAll('a'));
                        for(let a of links) {
                            let text = a.innerText.trim();
                            if(text.length > 1 && text.length < 50 && (a.href.includes('search-type=ss') || a.href.includes('/author/') || a.href.includes('contributor'))) {
                                if(!text.toLowerCase().includes('visit') && !text.toLowerCase().includes('search results')) {
                                    return text;
                                }
                            }
                        }
                        return "N/A";
                    }''')
                    if author_fallback and author_fallback != "N/A":
                        author = author_fallback
                except:
                    pass

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
                    
                    if publisher == "N/A":
                        m_sold = re.search(r'Sold by:\s*([A-Za-z0-9\s,&.-]+)', full_text, re.IGNORECASE)
                        if m_sold: publisher = m_sold.group(1).strip()
                
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
                        
                # 8. Author Name Fallback (Series layout)
                if author == "N/A":
                    # Looks for "(Author)" anywhere in the text and grabs the preceding words
                    m = re.search(r'(?:by\s+)?([A-Za-z\s\.,&]+?)\s*(?:\(Author\)|\(Contributor\))', full_text, re.IGNORECASE)
                    if m:
                        author_val = m.group(1).strip()
                        # Clean up if it grabbed too much
                        author_val = author_val.split('\n')[-1].strip()
                        if len(author_val) < 50:
                            author = author_val
                        
                # 9. Logline / Description Fallback
                desc = locals().get('description', "N/A")
                if desc == "N/A" or len(desc) < 20:
                    # Extremely loose "About this series" matcher
                    m_series = re.search(r'About[\s\S]{1,30}?series\s*\n([\s\S]{100,2000}?)(?:\n\n|\n[A-Z][a-z]+(?:\s[A-Z][a-z]+)?\n|Read less|Read more|See included|Customer reviews|$)', full_text, re.IGNORECASE)
                    if m_series:
                        desc = m_series.group(1).strip()
                    else:
                        # Grab any large paragraph block
                        m = re.search(r'\n([A-Z][\s\S]{200,2000}?)(?:\n\n|\n[A-Z][a-z]+(?:\s[A-Z][a-z]+)?\n|Read less|Read more|Customer reviews|$)', full_text)
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
    