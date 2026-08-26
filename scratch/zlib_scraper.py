import sys
import os
import urllib.parse
import re
from playwright.sync_api import sync_playwright

def download_book_zlib(book_name, series_name, email, password):
    print(f"[Z-Library] Starting search for: {book_name}")
    downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads', series_name)
    os.makedirs(downloads_path, exist_ok=True)
    
    with sync_playwright() as p:
        # Use real Chrome
        browser = p.chromium.launch(
            headless=False,
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        # Block popups
        page.on("popup", lambda popup: popup.close())
        
        print("[Z-Library] Navigating to login page...")
        # Z-Library website often has a login portal or redirects.
        # It's safer to go to singlelogin if we can, but we'll try the provided URL first
        login_url = "https://z-library.website/"
        page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
        
        # Give it a moment to load or redirect
        page.wait_for_timeout(5000)
        
        # Try to find login fields
        # Common selectors for z-lib login
        email_input = page.locator("input[type='email'], input[name='email']").first
        password_input = page.locator("input[type='password'], input[name='password']").first
        
        if email_input.count() > 0:
            print("[Z-Library] Logging in...")
            email_input.fill(email)
            password_input.fill(password)
            page.keyboard.press("Enter")
            page.wait_for_timeout(5000)
        else:
            # Maybe we need to click a "Log in" link first
            login_link = page.locator("a:has-text('Log In'), a:has-text('Sign In'), a:has-text('Login')").first
            if login_link.count() > 0:
                login_link.click()
                page.wait_for_timeout(3000)
                email_input.fill(email)
                password_input.fill(password)
                page.keyboard.press("Enter")
                page.wait_for_timeout(5000)
            else:
                print("[Z-Library] Could not find login fields. Proceeding as guest (may fail).")
                
        print(f"[Z-Library] Searching for {book_name}...")
        # Z-lib search URL usually looks like /s/?q=...
        search_url = f"https://z-library.website/s/{urllib.parse.quote(book_name)}?e=1&extensions[]=epub&extensions[]=pdf"
        page.goto(search_url, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)
        
        print("[Z-Library] Parsing search results...")
        # Get all book title links. Z-Lib usually uses h3 a or just a with specific classes.
        # We can extract all links and filter by href containing "/book/"
        links_data = page.evaluate("""
            () => {
                const links = Array.from(document.querySelectorAll("a"));
                return links.map(a => ({
                    href: a.href,
                    text: a.innerText.trim()
                }));
            }
        """)
        
        book_links = []
        words = book_name.lower().split()
        for item in links_data:
            href = item['href']
            text = item['text']
            # Zlib book URLs usually contain "/book/"
            if href and "/book/" in href and len(text) > 2:
                # Check match
                match_score = sum(1 for w in words if w in text.lower())
                if match_score > 0 and href not in [l['href'] for l in book_links]:
                    book_links.append({'href': href, 'text': text})
                    
        if not book_links:
            print("[Z-Library] No matching books found.")
            browser.close()
            return False
            
        target_url = book_links[0]['href']
        print(f"[Z-Library] Navigating to book page: {target_url}")
        page.goto(target_url, wait_until="domcontentloaded")
        
        print("[Z-Library] Looking for download button...")
        # Z-lib usually has an "a.addDownloadedBook" or a link containing "Download ("
        download_btn = page.locator("a:has-text('Download ('), button:has-text('Download (')").first
        
        if download_btn.count() == 0:
            print("[Z-Library] Download button not found. Maybe daily limit reached?")
            # Check for limit message
            if page.locator("text='Daily limit reached'").count() > 0:
                print("[Z-Library] ERROR: Daily download limit reached!")
            browser.close()
            return False
            
        print("[Z-Library] Found download button! Clicking...")
        try:
            with page.expect_download(timeout=60000) as download_info:
                download_btn.click()
                
            download = download_info.value
            file_name = download.suggested_filename
            final_path = os.path.join(downloads_path, file_name)
            download.save_as(final_path)
            print(f"[Z-Library] Successfully downloaded to {final_path}")
            browser.close()
            return True
        except Exception as e:
            print(f"[Z-Library] Download failed or limit reached: {e}")
            browser.close()
            return False

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python zlib_scraper.py 'Book Name' 'Series Name' 'Email' 'Password'")
        sys.exit(1)
    download_book_zlib(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
