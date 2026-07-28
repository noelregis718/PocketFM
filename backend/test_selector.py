import urllib.request
from bs4 import BeautifulSoup
html = urllib.request.urlopen("https://www.goodreads.com/series/313590").read().decode("utf-8")
soup = BeautifulSoup(html, "html.parser")
h3 = soup.find("h3")
print("Parent classes of first h3:")
for parent in h3.parents:
    if parent.name == "div":
        print(parent.get("class"))
