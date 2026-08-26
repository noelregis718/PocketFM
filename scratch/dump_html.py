import sys
import urllib.parse
from playwright.sync_api import sync_playwright

base_url = "https://annas-archive.gl"
search_url = f"{base_url}/search?q=Kill+Plan+eva+hudson"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
    context = browser.new_context()
    page = context.new_page()
    page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_selector("a[href^='/md5/']", timeout=30000)
    with open("search_results.html", "w", encoding="utf-8") as f:
        f.write(page.content())
    browser.close()
    print("Saved HTML")
