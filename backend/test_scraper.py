import asyncio
from scraper import AmazonScraper

async def test():
    scraper = AmazonScraper(headless=True)
    print("Testing Part 1...")
    results = await scraper.scrape_bestseller_list("https://www.amazon.com/best-sellers-books-Amazon/zgbs/books/", limit=2)
    print(f"Part 1 found {len(results)} books.")
    for r in results:
        print(f" - {r['Book Title']}")

if __name__ == "__main__":
    asyncio.run(test())
