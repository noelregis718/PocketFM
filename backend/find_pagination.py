from bs4 import BeautifulSoup
import re

with open("e:/Internship/PocketFM/backend/storygraph_sample_pw.html", "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f, "html.parser")

next_links = soup.find_all("a", text=re.compile("Next", re.I))
print(f"Next links: {next_links}")

pagination = soup.find_all(class_=re.compile("pagination", re.I))
print(f"Pagination classes: {pagination}")
