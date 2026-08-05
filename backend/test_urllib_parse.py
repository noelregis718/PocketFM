import urllib.request
import ssl
from bs4 import BeautifulSoup
import json
import re

def test_parse():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    }
    
    book_url = "https://www.goodreads.com/book/show/15722271-promised-to-the-beta"
    search_url = "https://www.goodreads.com/search?q=Promised+to+the+Beta"
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    print("Testing Search Parsing...")
    req = urllib.request.Request(search_url, headers=headers)
    with urllib.request.urlopen(req, context=ctx, timeout=15) as response:
        html = response.read().decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.select('a.bookTitle')
        for link in links:
            print("Search Link:", link.get('href'), link.text.strip())
            
    print("\nTesting Book Parsing...")
    req2 = urllib.request.Request(book_url, headers=headers)
    with urllib.request.urlopen(req2, context=ctx, timeout=15) as response:
        html = response.read().decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html, 'html.parser')
        
        # Parse JSON-LD
        ld_script = soup.find('script', type='application/ld+json')
        if ld_script:
            data = json.loads(ld_script.string)
            if isinstance(data, list): data = data[0]
            print("JSON-LD Pages:", data.get('numberOfPages'))
            print("JSON-LD Rating:", data.get('aggregateRating', {}).get('ratingValue'))
            print("JSON-LD Count:", data.get('aggregateRating', {}).get('ratingCount'))
            
        # Parse Apollo State (which has the series info usually)
        apollo_script = soup.find('script', id='__NEXT_DATA__')
        if apollo_script:
            apollo_data = json.loads(apollo_script.string)
            print("Has __NEXT_DATA__")
            
        # Fallback series tag check
        series_tag = soup.select_one('h3.Text__title3 a[href*="/series/"]')
        print("Series Tag:", series_tag)

if __name__ == "__main__":
    test_parse()
