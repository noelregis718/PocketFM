from playwright.sync_api import sync_playwright
import time
from bs4 import BeautifulSoup

def investigate():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
        try:
            page.goto("https://app.thestorygraph.com/browse?search_term=Dark%20Academia", wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            
            # Press PageDown several times to simulate natural scrolling
            for _ in range(5):
                page.keyboard.press("PageDown")
                page.wait_for_timeout(1000)
            
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            results = soup.find_all("div", class_="book-title-author-and-series")
            if not results:
                results = soup.find_all("div", class_="book-pane")
            print(f"Results after natural scrolling: {len(results)}")
            
            # Let's print all buttons to see if there is a 'load more' button
            buttons = soup.find_all("button")
            print("Buttons found on page:")
            for b in buttons:
                print(f"- '{b.text.strip()}'")
                
            # Print links at the bottom of the page
            links = soup.find_all("a")
            print("Last 10 links on page:")
            for l in links[-10:]:
                print(f"- '{l.text.strip()}'")
                
        except Exception as e:
            print(f"Error: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    investigate()
