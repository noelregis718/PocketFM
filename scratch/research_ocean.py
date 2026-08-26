import sys
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto("https://oceanofpdf.com/?s=Kill+Plan+eva+hudson", timeout=60000)
    page.wait_for_timeout(5000)
    with open("ocean_search.html", "w", encoding="utf-8") as f:
        f.write(page.content())
    print("Saved OceanOfPDF search page.")
    browser.close()
