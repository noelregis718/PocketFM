from bs4 import BeautifulSoup
with open(r'E:\Internship\PocketFM\amazon_series.html', 'r', encoding='utf-8') as f:
    soup = BeautifulSoup(f.read(), 'html.parser')
print('Found series description divs:')
for div in soup.find_all(class_=lambda c: c and 'description' in ' '.join(c).lower()):
    text = div.get_text(strip=True)
    if len(text) > 50:
        print(f'Class {div.get("class")}: {text[:200]}...')
