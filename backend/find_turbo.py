from bs4 import BeautifulSoup
import re

with open("e:/Internship/PocketFM/backend/storygraph_sample_pw.html", "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f, "html.parser")

frames = soup.find_all(re.compile("turbo-frame", re.I))
print(f"Turbo frames: {len(frames)}")
for frame in frames:
    print(f"Frame ID: {frame.get('id')}, src: {frame.get('src')}")
