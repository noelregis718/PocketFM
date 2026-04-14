import asyncio
import unicodedata
import re
from playwright.async_api import async_playwright

def clean_text(text):
    if not text:
        return ""
    # Unicode normalization (NFKD)
    text = unicodedata.normalize('NFKD', text)
    # Remove zero-width characters and extra whitespace
    text = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_numeric(text):
    if not text:
        return 0
    # Extract only numbers and dots
    match = re.search(r'[\d,.]+', text)
    if match:
        clean_val = match.group(0).replace(',', '')
        try:
            # Handle edge case where only a dot is matched
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
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                
                # Infinite scroll to load items
                await page.evaluate("""async () => {
                    await new Promise((resolve) => {
                        let totalHeight = 0;
                        let distance = 100;
                        let timer = setInterval(() => {
                            let scrollHeight = document.body.scrollHeight;
                            window.scrollBy(0, distance);
                            totalHeight += distance;
                            if(totalHeight >= scrollHeight){
                                clearInterval(timer);
                                resolve();
                            }
                        }, 100);
                    });
                }""")

                # Extract list items
                items = await page.query_selector_all('#gridItemRoot, [data-asin]')
                results = []
                
                for item in items[:limit]:
                    # More robust selector logic using generic classes and structure
                    rank_el = await item.query_selector('.zg-bdg-text')
                    title_el = await item.query_selector('.p13n-sc-untruncated-desktop-title, ._cDE_gridItem_truncate-title, [class*="title"]')
                    author_el = await item.query_selector('.a-link-child, .a-size-small.a-color-base, [class*="author"]')
                    rating_el = await item.query_selector('.a-icon-star-small .a-icon-alt, [class*="star"]')
                    reviews_el = await item.query_selector('.a-size-small, [class*="review"]')
                    price_el = await item.query_selector('.p13n-sc-price, [class*="price"]')
                    link_el = await item.query_selector('a.a-link-normal[href*="/dp/"], a.a-link-normal')

                    results.append({
                        "Rank": clean_text(await rank_el.inner_text()) if rank_el else "N/A",
                        "Book Title": clean_text(await title_el.inner_text()) if title_el else "N/A",
                        "Author Name": clean_text(await author_el.inner_text()) if author_el else "N/A",
                        "Rating": clean_numeric(await rating_el.inner_text()) if rating_el else 0,
                        "Number of Reviews": clean_numeric(await reviews_el.inner_text()) if reviews_el else 0,
                        "Price": clean_text(await price_el.inner_text()) if price_el else "N/A",
                        "Amazon URL": await link_el.get_attribute('href') if link_el else ""
                    })

                return results
            finally:
                await browser.close()

    async def scrape_product_details_tab(self, context, url):
        if not url: return {"Description": "N/A", "Publisher": "N/A", "Publication Date": "N/A"}
        if not url.startswith('http'): url = 'https://www.amazon.com' + url
            
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            desc_el = await page.query_selector('#bookDescription_feature_div')
            description = clean_text(await desc_el.inner_text()) if desc_el else "N/A"
            
            # Bullet points for Publisher and Date
            bullets = await page.query_selector_all('#detailBullets_feature_div li')
            details = {"Publisher": "N/A", "Publication Date": "N/A"}
            
            for bullet in bullets:
                text = clean_text(await bullet.inner_text())
                if "Publisher" in text:
                    details["Publisher"] = text.split(":")[-1].strip()
                if "Publication date" in text:
                    details["Publication Date"] = text.split(":")[-1].strip()
            
            return {
                "Description": description,
                "Publisher": details["Publisher"],
                "Publication Date": details["Publication Date"]
            }
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return {"Description": "N/A", "Publisher": "N/A", "Publication Date": "N/A"}
        finally:
            await page.close()
