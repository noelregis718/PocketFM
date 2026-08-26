import os
import re
import sys
import urllib.parse
from playwright.sync_api import sync_playwright

def download_book(book_name, series_name, base_url="https://annas-archive.gl"):
    # Setup download directory
    downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads', series_name)
    os.makedirs(downloads_path, exist_ok=True)
    print(f"Target download directory: {downloads_path}")

    # Ensure no trailing slash
    base_url = base_url.rstrip("/")
    search_url = f"{base_url}/search?q={urllib.parse.quote(book_name)}"

    with sync_playwright() as p:
        # Launch browser with anti-bot evasion flags
        # Use the real installed Google Chrome instead of Chromium to bypass TLS fingerprinting
        browser = p.chromium.launch(
            headless=False, 
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            accept_downloads=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        mirrors = [
            "https://annas-archive.gl",
            "https://annas-archive.li",
            "https://annas-archive.org",
            "https://annas-archive.se"
        ]
        
        if base_url in mirrors:
            mirrors.remove(base_url)
        mirrors.insert(0, base_url)

        working_base_url = ""
        search_success = False
        
        for mirror in mirrors:
            search_url = f"{mirror}/search?q={urllib.parse.quote(book_name)}"
            print(f"\nSearching for: {book_name} on {mirror}")
            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                search_success = True
                working_base_url = mirror
                break
            except Exception as e:
                print(f"Network error or timeout on {mirror}.")
                print("Trying next mirror...")
                
        if not search_success:
            print("All mirrors failed or are blocking your IP. Please use a VPN or try again later.")
            browser.close()
            return

        # Wait for search results to load
        try:
            print("Waiting for results (you have 60 seconds to pass any DDoS-Guard/CAPTCHA if it appears on screen)...")
            # Loop to check if we got redirected away by an ad, and navigate back if so
            for _ in range(30):
                if base_url not in page.url and "hcaptcha.com" not in page.url:
                    print(f"Redirected by an ad to {page.url}! Navigating back...")
                    page.goto(search_url, wait_until="domcontentloaded")
                
                # Check if results loaded
                if page.locator("a[href^='/md5/']").count() > 0:
                    break
                page.wait_for_timeout(2000) # wait 2 seconds
            
            page.wait_for_selector("a[href^='/md5/']", timeout=10000)
        except Exception:
            print("Could not find search results or timed out. Saving screenshot to debug...")
            try:
                page.screenshot(path="error_screenshot.png")
                with open("error_html.txt", "w", encoding="utf-8") as f:
                    f.write(page.content())
                print("Saved error_screenshot.png and error_html.txt for debugging.")
            except:
                pass
            browser.close()
            return

        # Fetch results
        links_data = page.evaluate("""
            () => {
                const links = Array.from(document.querySelectorAll("a[href^='/md5/']"));
                return links.map(a => {
                    // Try to get the containing card for full metadata (size, format)
                    let card = a.closest('div.flex') || a.parentElement.parentElement;
                    return {
                        href: a.getAttribute('href'),
                        text: card ? card.innerText : a.innerText
                    };
                });
            }
        """)

        if not links_data:
            print("No books found.")
            browser.close()
            return

        target_md5_urls = []
        
        print("\n--- Search Results Found ---")
        for item in links_data:
            text = item['text']
            # Clean up newlines for printing
            clean_text = text.replace('\n', ' | ')
            print(f"Result: {clean_text.encode('ascii', 'ignore').decode('ascii')}")
            
            # Extract format and size
            is_valid_format = "epub" in text.lower() or "pdf" in text.lower()
            size_match = re.search(r'([\d.]+)\s*(MB|KB)', text, re.IGNORECASE)
            is_valid_size = False
            
            if size_match:
                size_val = float(size_match.group(1))
                unit = size_match.group(2).upper()
                if unit == 'MB' and size_val < 2.0:
                    is_valid_size = True
                elif unit == 'KB':
                    is_valid_size = True # Always < 2MB
            
            if is_valid_format and is_valid_size:
                href = item['href']
                url = href if href.startswith('http') else working_base_url + href
                target_md5_urls.append(url)
                print(f"-> MATCHED CRITERIA: {clean_text.encode('ascii', 'ignore').decode('ascii')}")
                
        print("----------------------------\n")

        if not target_md5_urls:
            print("No results matched the criteria (EPUB/PDF and < 2MB).")
            browser.close()
            return

        download_success = False
        
        for book_url in target_md5_urls:
            if download_success:
                break
                
            # Navigate to the book's details page
            print(f"\nNavigating to detail page: {book_url}")
            page.goto(book_url, wait_until="domcontentloaded")
    
            # Collect all "Slow Partner Server" links for this book
            print("Looking for Slow Partner Server links...")
            slow_links_loc = page.locator("a:has-text('Slow Partner Server')")
            count = slow_links_loc.count()
            
            if count == 0:
                print("Could not find any Slow Partner Server links for this book. Trying next book...")
                continue
                
            mirror_urls = []
            for i in range(count):
                href = slow_links_loc.nth(i).get_attribute("href")
                if not href.startswith("http"):
                    href = working_base_url + href
                mirror_urls.append(href)
                
            for mirror_url in mirror_urls:
                print(f"\nNavigating to slow mirror page: {mirror_url}")
                page.goto(mirror_url, wait_until="domcontentloaded")
                
                # Wait for the "Download now" button to appear
                print("Looking for the 'Download now' button...")
                
                try:
                    with page.expect_download(timeout=45000) as download_info:
                        # Find the Download now link and wait for it to become visible
                        download_btn = page.locator("a:has-text('Download now')").first
                        download_btn.wait_for(state="visible", timeout=45000)
                        
                        # Extract the direct file URL to bypass any invisible ad overlays
                        file_url = download_btn.get_attribute("href")
                        if file_url and not file_url.startswith("http"):
                            file_url = working_base_url + file_url
                        
                        print(f"Found 'Download now' button! Direct URL: {file_url}")
                        print("Navigating to file URL to trigger download...")
                        
                        # Use evaluate to set location to avoid page.goto hanging
                        page.evaluate(f"window.location.href = '{file_url}'")
        
                    download = download_info.value
                    file_name = download.suggested_filename
                    final_path = os.path.join(downloads_path, file_name)
                    print(f"Downloading as: {file_name}")
                    download.save_as(final_path)
                    print(f"Successfully downloaded to {final_path}!")
                    download_success = True
                    break  # Success! Break out of the mirror loop
                    
                except Exception as e:
                    print(f"Download failed on this mirror (it may be expired or overloaded). Error: {e}")
                    print("Trying the next slow server option...")
                    
        if not download_success:
            print("\nFailed to download the book from all available slow servers and book options.")
            
        browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python anna_scraper.py \"Book Name\" \"Series Name\" [Base URL]")
        sys.exit(1)
        
    book_to_search = sys.argv[1]
    series_name = sys.argv[2]
    base_url = sys.argv[3] if len(sys.argv) > 3 else "https://annas-archive.gl"
    
    download_book(book_to_search, series_name, base_url)
