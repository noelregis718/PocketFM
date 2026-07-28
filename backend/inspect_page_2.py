from playwright.sync_api import sync_playwright

def inspect_page_2():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0")
        try:
            page.goto("https://app.thestorygraph.com/browse?search_term=Dark%20Academia&page=2", wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            
            # Print text of the page to see if it says "no results"
            print("Page 2 Text:")
            print(page.inner_text("body")[:1000])
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    inspect_page_2()
