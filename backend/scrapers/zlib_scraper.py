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

def download_book_sync_zlib(task: BookDownloadTask, download_dir: str, email: str, password: str):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled"]
            )
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            
            # Block popups
            page.on("popup", lambda popup: popup.close())
            
            print(f"[Z-Library] [{task.title}] Navigating to login page...")
            login_url = "https://z-library.website/"
            page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
            
            page.wait_for_timeout(5000)
            
            email_input = page.locator("input[type='email'], input[name='email']").first
            password_input = page.locator("input[type='password'], input[name='password']").first
            
            if email_input.count() > 0:
                print(f"[Z-Library] [{task.title}] Logging in...")
                email_input.fill(email)
                password_input.fill(password)
                page.keyboard.press("Enter")
                page.wait_for_timeout(5000)
            else:
                login_link = page.locator("a:has-text('Log In'), a:has-text('Sign In'), a:has-text('Login')").first
                if login_link.count() > 0:
                    login_link.click()
                    page.wait_for_timeout(3000)
                    email_input.fill(email)
                    password_input.fill(password)
                    page.keyboard.press("Enter")
                    page.wait_for_timeout(5000)
                else:
                    print(f"[Z-Library] [{task.title}] Could not find login fields. Proceeding as guest.")
                    
            print(f"[Z-Library] [{task.title}] Searching...")
            search_query = task.title
            if getattr(task, 'author', None):
                search_query = f"{task.title} {task.author}"
            search_url = f"https://z-library.website/s/{urllib.parse.quote(search_query)}?e=1&extensions[]=epub&extensions[]=pdf"
            
            for search_attempt in range(10):
                print(f"[Z-Library] [{task.title}] Searching... (Attempt {search_attempt+1}/10)")
                page.goto(search_url, wait_until="domcontentloaded")
                page.wait_for_timeout(3000)
                
                links_data = page.evaluate("""
                    () => {
                        const aLinks = Array.from(document.querySelectorAll("a")).map(a => {
                            let container = a.closest('tr') || a.closest('.book-item') || a.parentElement.parentElement;
                            let textContext = container ? container.innerText : a.innerText;
                            return { href: a.href, text: textContext.replace(/\\n/g, ' ').trim() };
                        });
                        
                        const zCards = Array.from(document.querySelectorAll("z-bookcard")).map(card => {
                            let href = card.getAttribute("href");
                            if (href && !href.startsWith('http')) {
                                href = window.location.origin + href;
                            }
                            let titleDiv = card.querySelector('[slot="title"]');
                            let authorDiv = card.querySelector('[slot="author"]');
                            let textContext = '';
                            if (titleDiv) textContext += titleDiv.innerText + ' ';
                            if (authorDiv) textContext += authorDiv.innerText + ' ';
                            let pub = card.getAttribute('publisher');
                            if (pub) textContext += pub + ' ';
                            textContext += card.innerText;
                            
                            return { href: href, text: textContext.replace(/\\n/g, ' ').trim() };
                        });
                        
                        return [...aLinks, ...zCards];
                    }
                """)
                
                from backend.scrapers.ocean_downloader import score_match
                
                best_link = None
                best_score = 0
                
                for item in links_data:
                    href = item['href']
                    text = item['text']
                    if href and "/book/" in href and len(text) > 2:
                        score = score_match(text, task.title, getattr(task, 'author', ''))
                        if score > best_score:
                            best_score = score
                            best_link = {'href': href, 'text': text}
                            
                if not best_link:
                    if search_attempt < 9:
                        print(f"[Z-Library] [{task.title}] No matching books found. Retrying in 5 seconds...")
                        page.wait_for_timeout(5000)
                        continue
                    else:
                        task.status = "failed"
                        task.error_message = "No search results found on Z-Library or none matched closely enough."
                        print(f"[Z-Library] [{task.title}] Gave up after 10 failed search attempts.")
                        browser.close()
                        return
                    
                target_url = best_link['href']
                print(f"[Z-Library] [{task.title}] Navigating to book page: {target_url}")
                page.goto(target_url, wait_until="domcontentloaded")
                
                # Check the actual format provided on the page
                page.wait_for_timeout(2000)
                format_text = page.evaluate("() => document.body.innerText")
                is_epub = "EPUB" in format_text or ".epub" in format_text.lower()
                is_pdf = "PDF" in format_text or ".pdf" in format_text.lower()
                
                download_btn = page.locator(".addDownloadedBook, a:has-text('Download ('), button:has-text('Download (')").first
                
                if download_btn.count() == 0:
                    if search_attempt < 9:
                        print(f"[Z-Library] [{task.title}] Download button not found. Maybe daily limit reached? Retrying search...")
                        continue
                    else:
                        task.status = "failed"
                        task.error_message = "Download button not found. Limit reached or captcha required."
                        print(f"[Z-Library] [{task.title}] Download button not found after 10 attempts.")
                        browser.close()
                        return
                    
                print(f"[Z-Library] [{task.title}] Found download button! Clicking...")
                try:
                    with page.expect_download(timeout=120000) as download_info:
                        download_btn.click()
                        
                    download = download_info.value
                    ext = ".epub" if is_epub and not is_pdf else ".pdf"
                    
                    suggested_ext = os.path.splitext(download.suggested_filename)[1].lower()
                    if suggested_ext in [".pdf", ".epub"]:
                        ext = suggested_ext
                        
                    safe_title = sanitize_filename(task.title)
                    file_name = f"{task.number}_{safe_title}{ext}"
                    final_path = os.path.join(download_dir, file_name)
                    
                    download.save_as(final_path)
                    
                    if ext == ".epub":
                        task.epub_path = final_path
                    else:
                        task.pdf_path = final_path
                        
                    task.status = "downloaded"
                    task.source = "Z-Library"
                    print(f"[Z-Library] [{task.title}] Successfully downloaded to {final_path}")
                    break # Success! break out of the 10-attempt loop
                except Exception as e:
                    if search_attempt < 9:
                        print(f"[Z-Library] [{task.title}] Download failed: {e}. Retrying search...")
                        continue
                    else:
                        task.status = "failed"
                        task.error_message = f"Z-Library Download failed: {str(e)}"
                        print(f"[Z-Library] [{task.title}] Download failed after 10 attempts: {e}")
                        break
                    
            browser.close()
    except Exception as e:
        task.status = "failed"
        task.error_message = str(e)
        print(f"[Z-Library] [{task.title}] Exception: {e}")

def process_zlib_downloads(books: List[BookDownloadTask], download_dir: str):
    """
    Downloads books from Z-Library for any book that failed on OceanOfPDF.
    """
    email = os.environ.get("ZLIB_EMAIL")
    password = os.environ.get("ZLIB_PASSWORD")
    
    if not email or not password:
        print("[Z-Library] Missing credentials in environment variables. Skipping Z-Library fallback.")
        return

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        
    pending = [b for b in books if b.status == "failed"]
    if not pending:
        return
        
    print(f"\n[Z-Library] Attempting fallback downloads for {len(pending)} failed books...")
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        for book in pending:
            book.status = "pending"
            executor.submit(download_book_sync_zlib, book, download_dir, email, password)
            time.sleep(3)
