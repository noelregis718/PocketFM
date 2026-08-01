import asyncio
from playwright.async_api import async_playwright
import re

async def test_selectors():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Test Book Page (A Court of Thorns and Roses)
        print("Testing Book Page...")
        await page.goto("https://www.goodreads.com/book/show/50659467-a-court-of-thorns-and-roses", wait_until="domcontentloaded")
        
        # Genres
        try:
            # Goodreads often uses different selectors for genres
            genres1 = await page.locator("a.BookPageMetadataSection__genreButton").all_inner_texts()
            genres2 = await page.locator('[data-testid="genresList"] a').all_inner_texts()
            genres3 = await page.locator('.BookPageMetadataSection__genres a').all_inner_texts()
            
            print(f"Genres (Selector 1): {genres1}")
            print(f"Genres (Selector 2): {genres2}")
            print(f"Genres (Selector 3): {genres3}")
            
            genres = genres1 or genres2 or genres3
            print(f"Final Genres: {genres}")
        except Exception as e:
            print(f"Error genres: {e}")
            
        # Reviews
        try:
            print("\nExtracting Reviews...")
            # Click "Show more reviews" if needed or just grab what's there
            review_locs = await page.locator('article.ReviewCard').all()
            print(f"Found {len(review_locs)} reviews.")
            for i, rev in enumerate(review_locs[:3]):
                text = await rev.locator('.ReviewText__content').inner_text()
                print(f"Review {i+1}: {text[:100]}...")
        except Exception as e:
            print(f"Error reviews: {e}")
            
        # Series Link
        try:
            print("\nChecking Series...")
            series_link = page.locator("h3.Text__title3 a").first
            if await series_link.is_visible():
                series_url = await series_link.get_attribute("href")
                print(f"Series URL: {series_url}")
                
                # Go to series page
                if series_url.startswith("/"):
                    series_url = "https://www.goodreads.com" + series_url
                await page.goto(series_url, wait_until="domcontentloaded")
                
                # Count primary works and pages
                works = await page.locator("div.listWithDividers__item").all()
                primary_count = 0
                total_pages = 0
                for work in works:
                    text = await work.inner_text()
                    text = text.replace('\n', ' ')
                    if "Book" in text or "Primary Work" in text:
                        primary_count += 1
                        page_match = re.search(r'(\d+)\s+pages', text)
                        if page_match:
                            total_pages += int(page_match.group(1))
                
                print(f"Primary count: {primary_count}, Total Pages: {total_pages}")
        except Exception as e:
            print(f"Error series: {e}")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_selectors())
