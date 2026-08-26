import os
import urllib.parse
from typing import List
from playwright.sync_api import sync_playwright
from backend.models.book_task import BookDownloadTask
import time
from concurrent.futures import ThreadPoolExecutor

def sanitize_filename(name: str) -> str:
    """Removes invalid characters for Windows filenames"""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, '')
    return name.strip()

def download_book_sync(task: BookDownloadTask, download_dir: str):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled"]
            )
            context = browser.new_context(
                accept_downloads=True
            )
            page = context.new_page()
            
            # Block popups
            page.on("popup", lambda popup: popup.close())
            
            query = urllib.parse.quote(task.title)
            search_url = f"https://oceanofpdf.com/?s={query}"
            
            print(f"[OceanOfPDF] [{task.title}] Searching: {search_url}")
            page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            
            page.wait_for_timeout(10000) # Give cloudflare a chance
            
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
            words = task.title.lower().split()
            for item in links_data:
                text = item['text']
                href = item['href']
                if href and "oceanofpdf.com" in href and len(text) > 5 and not text.lower() == "oceanofpdf":
                    match_score = sum(1 for w in words if w in text.lower())
                    if match_score > 0:
                        if href not in [l['href'] for l in book_links]:
                            book_links.append({'href': href, 'text': text})
                            
            if len(book_links) == 0:
                task.status = "failed"
                task.error_message = "No search results found."
                print(f"[OceanOfPDF] [{task.title}] No search results found.")
                browser.close()
                return
                
            # Click the first result
            target_url = book_links[0]['href']
            print(f"[OceanOfPDF] [{task.title}] Navigating to {target_url}")
            page.goto(target_url, wait_until="domcontentloaded")
            page.wait_for_load_state("domcontentloaded")
            
            
            # Find the PDF download form
            pdf_form = page.locator("form[action*='Fetching_Resource.php']").filter(has=page.locator("input[name='filename'][value$='.pdf']")).first
            
            if pdf_form.count() == 0:
                task.status = "failed"
                task.error_message = "Could not find PDF download form."
                print(f"[OceanOfPDF] [{task.title}] Could not find PDF download form.")
                browser.close()
                return
                
            print(f"[OceanOfPDF] [{task.title}] Found PDF download form, clicking...")
            try:
                # Remove target="_blank" so download happens in this page context
                pdf_form.evaluate("form => form.removeAttribute('target')")
                with page.expect_download(timeout=120000) as download_info:
                    pdf_form.evaluate("form => form.submit()")
                        
                download = download_info.value
                safe_title = sanitize_filename(task.title)
                file_name = f"{task.number}_{safe_title}.pdf"
                pdf_path = os.path.join(download_dir, file_name)
                download.save_as(pdf_path)
                
                task.pdf_path = pdf_path
                task.status = "downloaded"
                print(f"[OceanOfPDF] [{task.title}] Successfully downloaded to {pdf_path}")
            except Exception as e:
                task.status = "failed"
                task.error_message = f"Failed to trigger or save download: {str(e)}"
                print(f"[OceanOfPDF] [{task.title}] Download failed: {e}")
                
            browser.close()
    except Exception as e:
        task.status = "failed"
        task.error_message = str(e)
        print(f"[OceanOfPDF] [{task.title}] Exception: {e}")

def process_ocean_downloads(books: List[BookDownloadTask], download_dir: str):
    """
    Downloads books from OceanOfPDF with a concurrency limit.
    """
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        
    pending = [b for b in books if b.status == "pending"]
    if not pending:
        return
        
    # Use ThreadPoolExecutor for concurrent sync Playwright sessions
    # (Each thread gets its own Playwright instance)
    with ThreadPoolExecutor(max_workers=3) as executor:
        for book in pending:
            executor.submit(download_book_sync, book, download_dir)
            time.sleep(2) # stagger launches

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        title = sys.argv[1]
    else:
        title = "The Sex Club"
    b = BookDownloadTask(title=title, number=1)
    process_ocean_downloads([b], "e:\\Internship\\PocketFM\\downloads\\test_series")
    print(f"Result: {b}")
