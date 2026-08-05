import urllib.request
import urllib.parse
import ssl

def test_urllib():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    search_url = "https://www.goodreads.com/search?q=Promised+to+the+Beta"
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    req = urllib.request.Request(search_url, headers=headers)
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as response:
            html = response.read().decode("utf-8", errors="ignore")
            print(f"Status: {response.getcode()}")
            print(f"HTML Length: {len(html)}")
            if "bookTitle" in html:
                print("SUCCESS: Search Page contains book titles!")
    except Exception as e:
        print(f"Failed: {e}")
        
if __name__ == "__main__":
    test_urllib()
