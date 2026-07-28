import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

async def fetch_html(session, url):
    async with session.get(url, headers=HEADERS, timeout=30) as response:
        print(f"Status: {response.status}")
        return await response.text()
        return None

async def test():
    async with aiohttp.ClientSession() as session:
        url = "https://www.goodreads.com/book/show/61291084-almost-strangers"
        html = await fetch_html(session, url)
        if not html:
            print("Failed to fetch")
            return
            
        soup = BeautifulSoup(html, "html.parser")
        pages_format = soup.find(attrs={"data-testid": "pagesFormat"})
        if pages_format:
            print("Found pages using pagesFormat:", pages_format.text)
        else:
            print("Could not find pagesFormat. Title:", soup.title.string if soup.title else "None")
        
asyncio.run(test())
