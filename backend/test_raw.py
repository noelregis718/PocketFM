import urllib.request
from bs4 import BeautifulSoup

url = "https://www.goodreads.com/book/show/25579440-love-on-the-record"

proxy = "http://scraperapi:085655bf3504b2ddf5dfd29619c73cb6@proxy-server.scraperapi.com:8001"
proxy_support = urllib.request.ProxyHandler({'http': proxy, 'https': proxy})
opener = urllib.request.build_opener(proxy_support)
urllib.request.install_opener(opener)

req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
import ssl
context = ssl._create_unverified_context()

try:
    with urllib.request.urlopen(req, context=context) as response:
        print("Status:", response.status)
        html = response.read().decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        print("Title:", soup.title.string if soup.title else "None")
        print("Length:", len(html))
except Exception as e:
    print("Error:", e)
