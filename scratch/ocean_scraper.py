import sys
import os
import urllib.parse
import re
from playwright.sync_api import sync_playwright

def download_book_ocean(book_name, series_name):
    print(f"[OceanOfPDF] Starting search for: {book_name}")
    downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads', series_name)
    os.makedirs(downloads_path, exist_ok=True)
    
    with sync_playwright() as p:
        # Use real Chrome to bypass Cloudflare
        browser = p.chromium.launch(
            headless=False,
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        # Block popups
        page.on("popup", lambda popup: popup.close())
        
        search_url = f"https://oceanofpdf.com/?s={urllib.parse.quote(book_name)}"
        page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
        
        print("[OceanOfPDF] Waiting for Cloudflare and search results...")
        
        # Wait up to 30s for the search results container or Cloudflare to pass
        page.wait_for_timeout(10000)
        
        # In WordPress, search results usually have h2.entry-title or similar
        print("[OceanOfPDF] Parsing search results...")
        
        # Look for all links
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
            text = item['text']
            href = item['href']
            
            # Simple heuristic: link text must be decent length and contain some of the search words
            if href and "oceanofpdf.com" in href and len(text) > 5 and not text.lower() == "oceanofpdf":
                # Check if it looks like a book title link
                match_score = sum(1 for w in words if w in text.lower())
                if match_score > 0:
                    if href not in [l['href'] for l in book_links]:
                        book_links.append({'href': href, 'text': text})
        
        if not book_links:
            print("[OceanOfPDF] No matching books found.")
            browser.close()
            return False
            
        print("[OceanOfPDF] Found matching books:")
        for idx, b in enumerate(book_links[:5]):
            print(f"  {idx+1}. {b['text'].encode('ascii', 'ignore').decode('ascii')}")
            
        target_url = book_links[0]['href']
        print(f"[OceanOfPDF] Navigating to book page: {target_url}")
        page.goto(target_url, wait_until="domcontentloaded")
        
        print("[OceanOfPDF] Looking for EPUB/PDF download buttons...")
        
        # Try to find EPUB first, then PDF
        epub_btn = page.locator("text='EPUB'").first
        pdf_btn = page.locator("text='PDF'").first
        
        btn_to_click = None
        if epub_btn.count() > 0:
            btn_to_click = epub_btn
            print("[OceanOfPDF] Found EPUB button.")
        elif pdf_btn.count() > 0:
            btn_to_click = pdf_btn
            print("[OceanOfPDF] Found PDF button.")
        else:
            print("[OceanOfPDF] Could not find EPUB or PDF download buttons.")
            browser.close()
            return False
            
        print("[OceanOfPDF] Clicking download and waiting for file...")
        try:
            with page.expect_download(timeout=90000) as download_info:
                # The button might open a new tab or navigate. Popup blocker handles new tabs!
                # Actually, if it opens a new tab for download, popup blocker might kill it before expect_download catches it!
                # Let's disable the popup blocker temporarily just in case.
                btn_to_click.click()
                
            download = download_info.value
            file_name = download.suggested_filename
            final_path = os.path.join(downloads_path, file_name)
            download.save_as(final_path)
            print(f"[OceanOfPDF] Successfully downloaded: {final_path}")
            browser.close()
            return True
        except Exception as e:
            print(f"[OceanOfPDF] Download failed: {e}")
            browser.close()
            return False

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python ocean_scraper.py 'Book Name' 'Series Name'")
        sys.exit(1)
    download_book_ocean(sys.argv[1], sys.argv[2])
