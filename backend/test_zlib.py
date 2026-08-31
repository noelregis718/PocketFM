from playwright.sync_api import sync_playwright
import urllib.parse
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    
    # Test WITH by
    q1 = urllib.parse.quote('Declared Hostile by Jack Stewart')
    page.goto(f'https://z-library.website/s/{q1}?e=1&extensions[]=epub&extensions[]=pdf', wait_until='domcontentloaded')
    page.wait_for_timeout(3000)
    
    js = """
        () => {
            const links = Array.from(document.querySelectorAll('a'));
            return links.map(a => {
                let container = a.closest('tr') || a.closest('z-bookcard') || a.closest('.book-item') || a.parentElement.parentElement;
                let textContext = container ? container.innerText : a.innerText;
                return {
                    href: a.href,
                    text: textContext.replace(/\\n/g, ' ').trim()
                };
            });
        }
    """
    links_data = page.evaluate(js)
    print("=== WITH 'BY' ===")
    for item in links_data:
        if '/book/' in item['href']:
            print(item['text'][:100])
            
    # Test WITHOUT by
    q2 = urllib.parse.quote('Declared Hostile Jack Stewart')
    page.goto(f'https://z-library.website/s/{q2}?e=1&extensions[]=epub&extensions[]=pdf', wait_until='domcontentloaded')
    page.wait_for_timeout(3000)
    
    links_data = page.evaluate(js)
    print("\n=== WITHOUT 'BY' ===")
    for item in links_data:
        if '/book/' in item['href']:
            print(item['text'][:100])
    browser.close()
