from playwright.sync_api import sync_playwright
import urllib.parse
with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=False,
        channel="chrome",
        args=["--disable-blink-features=AutomationControlled"]
    )
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    q = urllib.parse.quote("Declared Hostile Jack Stewart")
    print("Navigating...")
    page.goto(f"https://z-library.website/s/{q}?e=1&extensions[]=epub&extensions[]=pdf", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(5000)
    
    html = page.evaluate("() => document.body.innerHTML")
    
    with open("zlib_dump.html", "w", encoding="utf-8") as f:
        f.write(html)
        
    print("HTML dumped to zlib_dump.html")
    browser.close()
