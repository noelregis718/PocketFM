import re
html = open('scratch_amazon_html.html', encoding='utf-8').read()
asins = re.findall(r'"asin":"([A-Z0-9]{10})"', html)
print("JSON ASINs:", set(asins))

links = re.findall(r'href="/([^/]+/[dp|product]+/[A-Z0-9]{10}[^"]*)"', html)
print("Found links with /dp/ or /product/ :")
for l in set(links):
    print(l)
