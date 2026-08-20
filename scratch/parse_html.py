from bs4 import BeautifulSoup

with open('scratch/search_fallback.html', 'r', encoding='utf-8') as f:
    soup = BeautifulSoup(f, 'html.parser')
    
results = soup.select('.BookListItem')
print('Found .BookListItem:', len(results))

books = soup.select('[data-testid="bookTitle"]')
if books:
    print('Found [data-testid=bookTitle]:', len(books))
    for b in books:
        if b.name == 'a':
            print('  Title:', b.text, 'href:', b.get('href'))
        else:
            print('  Title (div):', b.text)
