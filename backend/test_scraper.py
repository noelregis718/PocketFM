import asyncio
import os
import sys

# Add the backend directory to the path so we can import from app
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app import run_scrape_process

async def test():
    url = "https://www.amazon.com/best-sellers-books-Amazon/zgbs/books/"
    print(f"Testing full scrape process for: {url}")
    
    # run_scrape_process handles list extraction AND deep enrichment AND mapping
    results = await run_scrape_process(url, limit=2)
    
    print(f"\nFound {len(results)} books in total.")
    for i, r in enumerate(results):
        print(f"\n--- Book {i+1} ---")
        for col, val in r.items():
            if val and val != "N/A":
                print(f"{col}: {val}")

if __name__ == "__main__":
    asyncio.run(test())
