import re
with open(r'E:\Internship\PocketFM\backend\scraper.py', 'r', encoding='utf-8') as f:
    text = f.read()
match = re.search(r'def\s+scrape_product_details_tab[\s\S]*?(?=async def |def |$)', text)
if match:
    with open(r'E:\Internship\PocketFM\scratch\out.txt', 'w', encoding='utf-8') as out:
        out.write(match.group(0))
