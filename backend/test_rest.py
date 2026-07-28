import requests
from bs4 import BeautifulSoup

url = "https://www.goodreads.com/book/show/25579440-love-on-the-record"
payload = {
    'api_key': '085655bf3504b2ddf5dfd29619c73cb6',
    'url': url,
}

print("Fetching via ScraperAPI REST...")
r = requests.get('https://api.scraperapi.com/', params=payload)
print("Status:", r.status_code)
html = r.text
soup = BeautifulSoup(html, "html.parser")
print("Title:", soup.title.string if soup.title else "None")
print("Length:", len(html))

pages_format = soup.find(attrs={"data-testid": "pagesFormat"})
if pages_format:
    print("Found pages using pagesFormat:", pages_format.text)
else:
    print("Could not find pagesFormat")
