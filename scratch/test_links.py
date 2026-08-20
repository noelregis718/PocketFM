import re
with open(r'E:\Internship\PocketFM\amazon_series.html', 'r', encoding='utf-8') as f:
    html = f.read()

links = re.findall(r'<a[^>]+href="([^"]+/dp/[^"]+)"[^>]*>', html)
print('Found dp links:', len(links))
if links: print(links[0])
