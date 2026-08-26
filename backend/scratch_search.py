import urllib.request
import urllib.parse
import re

def resolve_series(author, title):
    query = urllib.parse.quote_plus(f"{author} {title}")
    url = f"https://www.goodreads.com/search?q={query}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        html = urllib.request.urlopen(req).read().decode('utf-8')
        m = re.search(r'href="(/book/show/[^"]+)"', html)
        if m:
            book_url = "https://www.goodreads.com" + m.group(1)
            req2 = urllib.request.Request(book_url, headers=headers)
            html2 = urllib.request.urlopen(req2).read().decode('utf-8')
            s = re.search(r'href="(https://www\.goodreads\.com/series/[^"]+)"', html2)
            if s:
                return s.group(1)
            return "No series link on book page"
        return "Book not found"
    except Exception as e:
        return str(e)

print('220:', resolve_series('Brandt Legg', 'Chasing Rain'))
print('221:', resolve_series('Joy Ellis', 'Crime on the Fens'))
print('222:', resolve_series('Melinda Leigh', 'Cross Her Heart'))
print('223:', resolve_series('Jen J. Danna', 'Dead Without a Stone to Tell It'))
print('224:', resolve_series('J.N. Chaney', 'Deadland Drifter'))
