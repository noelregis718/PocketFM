from playwright.sync_api import sync_playwright

def get_html():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        try:
            page.goto("https://app.thestorygraph.com/browse?search_term=Dark%20Academia", wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000) # Wait a bit for JS to load results
            html = page.content()
            with open("storygraph_sample_pw.html", "w", encoding="utf-8") as f:
                f.write(html)
            print("Playwright fetch successful")
        except Exception as e:
            print(f"Error with Playwright: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    get_html()
