from playwright.sync_api import sync_playwright
import time

def test_page_2():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0")
        try:
            page.goto("https://app.thestorygraph.com/browse?search_term=Dark%20Academia&page=2", wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
            html = page.content()
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            results = soup.find_all("div", class_="book-title-author-and-series")
            print(f"Results on page 2: {len(results)}")
            if len(results) > 0:
                first_text = results[0].text.strip().replace("\n", " ")
                print(f"First result on page 2: {first_text}")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    test_page_2()
