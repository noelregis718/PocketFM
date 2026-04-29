import asyncio
import pandas as pd
import os
import re
from playwright.async_api import async_playwright

# Configuration
TARGET_FILE = r"E:\Internship\PocketFM\Jacob_Peppers_to_End_Enrichment.xlsx"
CONCURRENCY = 3 # Slow and steady for the final ones

async def intensive_scrape(idx, author_name, context, sem):
    async with sem:
        print(f"  [Intensive Scrape] Hunting for: {author_name}...")
        
        # High-intensity queries
        queries = [
            f'"{author_name}" author contact email',
            f'"{author_name}" literary agent contact email',
            f'site:facebook.com "{author_name}" email',
            f'site:instagram.com "{author_name}" email'
        ]
        
        details = {
            "Author_Email": "N/A",
            "Agent_Email": "N/A",
            "Contact_Website": "N/A"
        }
        
        for query in queries:
            if details["Author_Email"] != "N/A" and details["Agent_Email"] != "N/A":
                break
                
            page = await context.new_page()
            try:
                # Use DuckDuckGo for initial discovery of the official site
                search_url = f"https://html.duckduckgo.com/html/?q={query.replace(' ', '+')}"
                await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                
                links = await page.query_selector_all('.result__a')
                target_urls = []
                for link in links:
                    href = await link.evaluate("el => el.href")
                    if href and not any(x in href.lower() for x in ['google.com', 'amazon.com', 'goodreads.com', 'sentry.io']):
                        target_urls.append(href)
                    if len(target_urls) >= 4: break
                
                for url in target_urls:
                    try:
                        print(f"    [Scanning] {url[:50]}...")
                        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        await asyncio.sleep(2)
                        content = await page.content()
                        
                        email_pattern = r'[a-zA-Z0-9._%+-]+(?:@|\[at\]|at)[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                        found = re.findall(email_pattern, content)
                        
                        for email in found:
                            clean = email.replace('[at]', '@').replace('at', '@').replace(' ', '')
                            if '@' not in clean or '.' not in clean or len(clean) > 50: continue
                            if any(x in clean.lower() for x in ['google.com', 'sentry.io', 'wixpress.com', 'example.com', '.png', '.jpg', 'bootstrap']): continue
                            
                            lower_content = content.lower()
                            email_idx = lower_content.find(email.lower())
                            snippet = lower_content[max(0, email_idx-100):email_idx+100]
                            
                            if any(kw in snippet for kw in ['agent', 'rights', 'rep', 'agency', 'literary', 'press']):
                                if details["Agent_Email"] == "N/A": details["Agent_Email"] = clean
                            else:
                                if details["Author_Email"] == "N/A": details["Author_Email"] = clean
                        
                        if details["Contact_Website"] == "N/A": details["Contact_Website"] = url
                    except: continue
            except: continue
            finally: await page.close()
            
        return idx, details

async def run_final_mission():
    df = pd.read_excel(TARGET_FILE)
    # Target the last 30 rows
    last_30_indices = df.index[-30:].tolist()
    print(f"Locked onto the final {len(last_30_indices)} authors for intensive scraping.")
    
    sem = asyncio.Semaphore(CONCURRENCY)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        tasks = [intensive_scrape(idx, df.loc[idx, 'Author Name'], context, sem) for idx in last_30_indices]
        results = await asyncio.gather(*tasks)
        
        for idx, details in results:
            df.at[idx, 'Author Email ID'] = details["Author_Email"]
            df.at[idx, 'Agency Email ID'] = details["Agent_Email"]
            df.at[idx, 'Author Contact Form - Website'] = details["Contact_Website"]
            
        df.to_excel(TARGET_FILE, index=False)
        await browser.close()
        print("\nFinal 30 repair mission complete.")

if __name__ == "__main__":
    asyncio.run(run_final_mission())
