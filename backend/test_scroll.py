from playwright.sync_api import sync_playwright
import time

def test_scroll():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0")
        try:
            page.goto("https://app.thestorygraph.com/browse?search_term=Dark%20Academia", wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
            
            # Count initial
            elements = page.query_selector_all(".book-title-author-and-series")
            print(f"Initial count: {len(elements)}")
            
            # Scroll down
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(3000)
            
            # Count after scroll
            elements2 = page.query_selector_all(".book-title-author-and-series")
            print(f"Count after scroll: {len(elements2)}")
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    test_scroll()
