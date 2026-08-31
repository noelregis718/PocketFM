from playwright.sync_api import sync_playwright
import json
with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=False,
        channel="chrome",
        args=["--disable-blink-features=AutomationControlled"]
    )
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    page.goto('https://z-library.website/book/Wrq9EPe5rb/declared-hostile.html', wait_until='domcontentloaded', timeout=60000)
    page.wait_for_timeout(5000)
    
    html = page.evaluate('() => document.body.innerHTML')
    with open('zlib_book_dump.html', 'w', encoding='utf-8') as f:
        f.write(html)
        
    js = """
        () => {
            const buttons = Array.from(document.querySelectorAll('a, button, z-download-button')).filter(el => 
                (el.innerText && el.innerText.toLowerCase().includes('download')) || 
                (el.getAttribute('href') && el.getAttribute('href').includes('/dl/')) ||
                (el.className && typeof el.className === 'string' && el.className.includes('download')) ||
                el.tagName.toLowerCase().includes('download')
            );
            return buttons.map(b => ({
                tag: b.tagName,
                text: b.innerText,
                href: b.getAttribute('href'),
                className: b.className || ''
            }));
        }
    """
    data = page.evaluate(js)
    print("Found download-related elements:")
    print(json.dumps(data, indent=2))
    browser.close()
