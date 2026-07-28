from bs4 import BeautifulSoup

with open("e:/Internship/PocketFM/backend/storygraph_sample_pw.html", "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f, "html.parser")

# Find the search results container
results = soup.find_all("div", class_="book-title-author-and-series")
if not results:
    results = soup.find_all("div", class_="book-pane")

for res in results[:5]:
    title_element = res.find("a", href=lambda href: href and "/books/" in href)
    author_element = res.find("a", href=lambda href: href and "/authors/" in href)
    series_element = res.find("a", href=lambda href: href and "/series/" in href)
    
    title = title_element.text.strip() if title_element else "N/A"
    author = author_element.text.strip() if author_element else "N/A"
    series = series_element.text.strip() if series_element else "N/A"
    
    print(f"Title: {title} | Author: {author} | Series: {series}")

print(f"Total results found on page: {len(results)}")

# Let's also find pagination links
pagination = soup.find("nav", {"aria-label": "Pagination"})
if pagination:
    links = pagination.find_all("a")
    print(f"Pagination links found: {[l.text for l in links]}")
else:
    print("No pagination found.")
