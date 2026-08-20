import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

async def main():
    url = 'https://www.amazon.com/dp/B0F8L7LLLM?binding=kindle_edition'
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        # Handle captcha if any
        try:
            continue_btn = await page.query_selector('text="Continue shopping"')
            if continue_btn:
                print("Clicking continue shopping")
                await continue_btn.click()
                await asyncio.sleep(3)
        except:
            pass
            
        await asyncio.sleep(3)
        
        # Get full HTML
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        # Look for "About this series"
        about_header = soup.find(lambda tag: tag.name in ['h2', 'h3', 'span', 'div'] and 'About this series' in tag.text)
        if about_header:
            print(f"Found 'About this series' header: <{about_header.name} class='{about_header.get('class')}'>")
            # The description is usually the next sibling or inside a parent container
            parent = about_header.parent
            print("Parent classes:", parent.get('class'))
            print("Parent ID:", parent.get('id'))
            print("Content:", parent.text[:200])
        else:
            print("Could not find 'About this series' text.")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
