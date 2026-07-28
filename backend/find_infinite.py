from bs4 import BeautifulSoup
import re

with open("e:/Internship/PocketFM/backend/storygraph_sample_pw.html", "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f, "html.parser")

infinite = soup.find_all(attrs={"data-controller": re.compile("infinite")})
for el in infinite:
    print(f"Infinite element: {el.attrs}")
    
urls = soup.find_all("a", href=re.compile("page="))
for u in urls:
    print(f"Page link: {u.get('href')}")
