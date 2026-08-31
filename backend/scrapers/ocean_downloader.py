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

import re

def score_match(text, title, author):
    text_clean = set(re.sub(r'[^\w\s]', '', text.lower()).split())
    title_clean = re.sub(r'[^\w\s]', '', title.lower()).split()
    
    if not title_clean:
        return 0
        
    title_matches = sum(1 for w in title_clean if w in text_clean)
    title_ratio = title_matches / len(title_clean)
    
    # Must match at least 60% of the title words
    if title_ratio < 0.6:
        return 0
        
    score = title_ratio * 10
    
    if author:
        author_clean = set(re.sub(r'[^\w\s]', '', author.lower()).split())
        author_matches = sum(1 for w in author_clean if len(w) > 2 and w in text_clean)
        if author_matches > 0:
            score += 5
            
    # Penalize if result has way too many extra words (likely an omnibus/box set)
    if len(text_clean) > len(title_clean) + 15:
        score -= 5
        
    return score

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
            
            search_query = task.title
            if getattr(task, 'author', None):
                search_query = f"{task.title} by {task.author}"
            query = urllib.parse.quote(search_query)
            search_url = f"https://oceanofpdf.com/?s={query}"
            
            for search_attempt in range(10):
                print(f"[OceanOfPDF] [{task.title}] Searching: {search_url} (Attempt {search_attempt+1}/10)")
                page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                
                page.wait_for_timeout(5000) # Give cloudflare a chance
                
                # Look for all links
                links_data = page.evaluate("""
                    () => {
                        const links = Array.from(document.querySelectorAll("a"));
                        return links.map(a => {
                            let container = a.closest('article') || a.closest('li') || a.closest('.post') || a.parentElement.parentElement;
                            let textContext = container ? container.innerText : a.innerText;
                            return {
                                href: a.href,
                                text: textContext.replace(/\\n/g, ' ').trim()
                            };
                        });
                    }
                """)
                
                best_link = None
                best_score = 0
                
                for item in links_data:
                    text = item['text']
                    href = item['href']
                    if href and "oceanofpdf.com" in href and len(text) > 5 and not text.lower() == "oceanofpdf":
                        score = score_match(text, task.title, getattr(task, 'author', ''))
                        if score > best_score:
                            best_score = score
                            best_link = {'href': href, 'text': text}
                                
                if not best_link:
                    if search_attempt < 9:
                        print(f"[OceanOfPDF] [{task.title}] No valid results found. Retrying in 5 seconds...")
                        page.wait_for_timeout(5000)
                        continue
                    else:
                        task.status = "failed"
                        task.error_message = "No search results found after 10 attempts."
                        print(f"[OceanOfPDF] [{task.title}] Gave up after 10 failed search attempts.")
                        browser.close()
                        return
                    
                # Click the best matched result
                target_url = best_link['href']
                print(f"[OceanOfPDF] [{task.title}] Navigating to {target_url}")
                page.goto(target_url, wait_until="domcontentloaded")
                page.wait_for_load_state("domcontentloaded")
                
                
                # Find the PDF download form
                pdf_form = page.locator("form[action*='Fetching_Resource.php']").filter(has=page.locator("input[name='filename'][value$='.pdf']")).first
                
                if pdf_form.count() == 0:
                    if search_attempt < 9:
                        print(f"[OceanOfPDF] [{task.title}] Could not find PDF download form on page. Retrying search...")
                        continue
                    else:
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
                    task.source = "OceanOfPDF"
                    print(f"[OceanOfPDF] [{task.title}] Successfully downloaded to {pdf_path}")
                    break # Success! break out of the 10-attempt loop
                except Exception as e:
                    if search_attempt < 9:
                        print(f"[OceanOfPDF] [{task.title}] Download attempt failed: {e}. Retrying search...")
                        continue
                    else:
                        task.status = "failed"
                        task.error_message = f"Failed to trigger or save download: {str(e)}"
                        print(f"[OceanOfPDF] [{task.title}] Download failed: {e}")
                        break
                    
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
