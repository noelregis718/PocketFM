import urllib.request
from bs4 import BeautifulSoup

url = "https://app.thestorygraph.com/browse?search_term=Dark%20Academia"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

try:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response:
        html = response.read()
        soup = BeautifulSoup(html, "html.parser")
        with open("e:/Internship/PocketFM/backend/storygraph_sample.html", "w", encoding="utf-8") as f:
            f.write(soup.prettify())
        print("Saved HTML to storygraph_sample.html")
except Exception as e:
    print(f"Error fetching URL: {e}")
