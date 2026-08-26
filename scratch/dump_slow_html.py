import sys
from playwright.sync_api import sync_playwright

url = "https://annas-archive.gl/slow_download/b8758164c704e97cbab3c2db21b46f6c/0/0"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    
    print("Waiting 5 seconds...")
    page.wait_for_timeout(5000)
    
    with open("slow_download2.html", "w", encoding="utf-8") as f:
        f.write(page.content())
    print("Saved slow_download2.html")
    browser.close()
