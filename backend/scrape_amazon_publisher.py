"""
Scrapes Publisher, Publication Date, and Logline for rows
where Publisher is missing in Merged_Romance_Keywords.xlsx.

Flow (exactly as user described):
1. Open the Amazon URL (series or book page)
2. Scroll down slowly to trigger lazy-loading
3. Find "Books in this series" → click the first book
4. On the book page: grab Logline (below ratings)
5. Click "See all details" → scrape Publisher + Publication Date
6. Save to Excel after every row
"""

import asyncio
import pandas as pd
from playwright.async_api import async_playwright
import re

EXCEL_FILE = r"E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx"
SAVE_EVERY = 5  # save to Excel every N rows


async def slow_scroll(page, steps=10, delay=0.5):
    """Scroll down in steps to trigger lazy-loading."""
    for _ in range(steps):
        await page.evaluate("window.scrollBy(0, 600)")
        await asyncio.sleep(delay)


async def click_continue_shopping(page) -> bool:
    """Click the 'Continue shopping' button if Amazon shows an interstitial page."""
    selectors = [
        'a:has-text("Continue shopping")',
        'input[value="Continue shopping"]',
        'button:has-text("Continue shopping")',
        '.a-button-input[aria-labelledby*="continue-shopping"]'
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=2000):
                await btn.click()
                await page.wait_for_load_state("domcontentloaded")
                await asyncio.sleep(2)
                return True
        except Exception:
            pass
    return False


async def extract_logline(page) -> str:
    """Extract the book description/logline from below the ratings."""
    selectors = [
        '#bookDescription_feature_div .a-expander-content',
        '#bookDescription_feature_div span:not(.a-expander-prompt)',
        '#productDescription',
        '#bookDescription_feature_div',
    ]
    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                text = (await el.inner_text()).strip()
                if len(text) > 20:
                    return text
        except Exception:
            pass
    return "N/A"


async def click_see_all_details(page) -> bool:
    """Click the 'See all details' link/button to reveal publisher info."""
    selectors = [
        'a:has-text("See all details")',
        'span:has-text("See all details")',
        'button:has-text("See all details")',
        '#seeAllDetailsBtn',
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            await btn.wait_for(state="visible", timeout=5000)
            await btn.click()
            await asyncio.sleep(3)
            print("  -> Clicked 'See all details'")
            return True
        except Exception:
            pass
    print("  -> 'See all details' button not found, continuing...")
    return False


async def extract_publisher_details(page) -> dict:
    """Extract Publisher and Publication Date from the product details section."""
    result = {"Publisher": "N/A", "Publication Date": "N/A"}

    # Try RPI (Rich Product Information) table first — most reliable
    try:
        rpi_pub = await page.query_selector(
            '#rpi-attribute-book_details-publisher .rpi-attribute-value'
        )
        if rpi_pub:
            val = (await rpi_pub.inner_text()).strip()
            if val:
                result["Publisher"] = val

        rpi_date = await page.query_selector(
            '#rpi-attribute-book_details-publication_date .rpi-attribute-value'
        )
        if rpi_date:
            val = (await rpi_date.inner_text()).strip()
            if val:
                result["Publication Date"] = val
    except Exception:
        pass

    # Fallback: scan visible text line by line
    if result["Publisher"] == "N/A" or result["Publication Date"] == "N/A":
        try:
            text = await page.evaluate("() => document.body.innerText")
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            for i, line in enumerate(lines):
                if result["Publisher"] == "N/A":
                    m = re.search(r"publisher\s*[:\u200e\u200f]\s*(.+)", line, re.IGNORECASE)
                    if m:
                        result["Publisher"] = re.sub(r"\s*\(\d+.*?\)\s*$", "", m.group(1)).strip()
                    elif line.lower() == "publisher":
                        for j in range(1, 4):
                            if i + j < len(lines) and len(lines[i + j]) > 1:
                                result["Publisher"] = lines[i + j]
                                break

                if result["Publication Date"] == "N/A":
                    m = re.search(r"publication\s*date\s*[:\u200e\u200f]\s*(.+)", line, re.IGNORECASE)
                    if m:
                        result["Publication Date"] = m.group(1).strip()
                    elif line.lower() == "publication date":
                        for j in range(1, 4):
                            if i + j < len(lines) and len(lines[i + j]) > 1:
                                result["Publication Date"] = lines[i + j]
                                break
        except Exception:
            pass

    return result


async def process_row(idx, row, context, sem, lock, df, EXCEL_FILE):
    async with sem:
        url = row["Amazon URL"]
        print(f"\n[Processing] Row {idx}: {url[:60]}...")
        
        page = await context.new_page()
        try:
            await asyncio.sleep(idx * 0.5) # Stagger tab opening slightly
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(3)
            
            # Check for CAPTCHA
            page_text = await page.evaluate("document.body.innerText")
            if "Type the characters you see in this image" in page_text or "Enter the characters you see below" in page_text:
                print(f"  [{idx}] -> AMAZON CAPTCHA DETECTED! Pausing for 30 seconds so you can solve it...")
                await asyncio.sleep(30)

            # Click "Continue shopping" if it appears
            if await click_continue_shopping(page):
                print(f"  [{idx}] -> Clicked 'Continue shopping' interstitial.")
                
            # Verify we are actually on the book page
            current_url = page.url
            if "/dp/" not in current_url and "/gp/product/" not in current_url:
                print(f"  [{idx}] -> WARNING: URL does not look like a book page: {current_url}")

            # Scroll to lazy-load elements
            await slow_scroll(page, steps=10, delay=0.5)

            # Extract Logline
            logline = await extract_logline(page)
            
            # Click See all details then extract Publisher
            await click_see_all_details(page)
            details = await extract_publisher_details(page)

            # Safely write to DataFrame and print logs
            async with lock:
                print(f"\n--- SCRAPED DETAILS (Row {idx}) ---")
                if logline != "N/A":
                    df.at[idx, "Logline"] = logline
                    print(f"  Logline: {logline[:80]}...")
                else:
                    print("  Logline: NOT FOUND")
                
                if details["Publisher"] != "N/A":
                    df.at[idx, "Publisher"] = details["Publisher"]
                    print(f"  Publisher: {details['Publisher']}")
                else:
                    print("  Publisher: NOT FOUND")

                if details["Publication Date"] != "N/A":
                    df.at[idx, "Publication Date"] = details["Publication Date"]
                    print(f"  Publication Date: {details['Publication Date']}")
                else:
                    print("  Publication Date: NOT FOUND")
                print("-----------------------------------")
        except Exception as e:
            print(f"  [{idx}] -> ERROR: {e}")
        finally:
            await page.close()


async def run():
    print("Loading Excel...")
    df = pd.read_excel(EXCEL_FILE)

    # Identify rows where Publisher is missing
    missing = []
    for idx, row in df.iterrows():
        url = row.get("Amazon URL")
        if pd.isna(url) or not str(url).startswith("http"):
            continue
        pub = row.get("Publisher")
        if pd.isna(pub) or str(pub).strip() in ("", "N/A", "nan"):
            missing.append(idx)

    # Limit to the first 10 rows as requested
    missing = missing[:10]
    print(f"Found {len(missing)} rows with missing Publisher. Processing 10 concurrently...")

    import os
    user_data_dir = os.path.join(os.getcwd(), 'playwright_goodreads_profile')
    
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        
        sem = asyncio.Semaphore(10)
        lock = asyncio.Lock()
        
        tasks = []
        for idx in missing:
            row = df.iloc[idx]
            tasks.append(process_row(idx, row, context, sem, lock, df, EXCEL_FILE))
            
        await asyncio.gather(*tasks)
        await context.close()

    print("\nFinal save...")
    df.to_excel(EXCEL_FILE, index=False)
    print("All done!")


if __name__ == "__main__":
    asyncio.run(run())
