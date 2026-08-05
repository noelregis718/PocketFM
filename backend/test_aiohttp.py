import aiohttp
import asyncio

async def test_fetch():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    url = "https://www.goodreads.com/book/show/15722271-promised-to-the-beta"
    search_url = "https://www.goodreads.com/search?q=Promised+to+the+Beta"
    
    async with aiohttp.ClientSession(headers=headers) as session:
        print("Fetching Book URL...")
        async with session.get(url) as response:
            print(f"Book Status: {response.status}")
            html = await response.text()
            print(f"Book HTML Length: {len(html)}")
            if "RatingStatistics__rating" in html or "ApolloState" in html:
                print("Book Page contains React data!")
                
        print("\nFetching Search URL...")
        async with session.get(search_url) as response:
            print(f"Search Status: {response.status}")
            html = await response.text()
            print(f"Search HTML Length: {len(html)}")
            if "bookTitle" in html:
                print("Search Page contains book titles!")

if __name__ == "__main__":
    asyncio.run(test_fetch())
