import asyncio
import pandas as pd
import os
import re
from playwright.async_api import async_playwright

# Configuration
MASTER_RESULTS_FILE = r"E:\Internship\PocketFM\Master_Author_Enrichment.xlsx"
CONCURRENCY = 10 
BATCH_SIZE = 50

async def deep_scan_author(idx, author_name, context, sem):
    async with sem:
        print(f"  [Deep Scan] Starting mission for: {author_name}")
        
        queries = [
            f'"{author_name}" official contact email',
            f'"{author_name}" literary agent contact',
            f'"{author_name}" press rights email'
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
                search_url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
                await page.goto(search_url, timeout=45000)
                
                # CAPTCHA DETECTION
                try:
                    await page.wait_for_selector('h3, a > h3, #search', timeout=10000)
                except:
                    print(f"    [!] Action Required: Please solve captcha in tab for {author_name}.")
                    solved = False
                    for _ in range(60):
                        await asyncio.sleep(2)
                        if await page.query_selector('h3, a > h3, #search'):
                            solved = True
                            break
                    if not solved: continue

                links = await page.query_selector_all('a:has(h3), div.g a, a[data-clearsrc]')
                target_urls = []
                for link in links:
                    href = await link.evaluate("el => el.href")
                    if href and not any(x in href.lower() for x in ['google.com', 'amazon.com', 'goodreads.com', 'facebook.com', 'twitter.com', 'instagram.com']):
                        target_urls.append(href)
                    if len(target_urls) >= 3: break
                
                for url in target_urls:
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                        content = await page.content()
                        email_pattern = r'[a-zA-Z0-9_.+-]+(?:@|\[at\]|at)[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
                        found = re.findall(email_pattern, content)
                        for email in found:
                            clean_email = email.replace('[at]', '@').replace('at', '@').replace(' ', '')
                            if '@' not in clean_email or '.' not in clean_email or len(clean_email) > 50: continue
                            if any(x in clean_email.lower() for x in ['google.com', 'sentry.io']): continue
                            
                            lower_content = content.lower()
                            email_idx = lower_content.find(email.lower())
                            snippet = lower_content[max(0, email_idx-100):email_idx+100]
                            if any(kw in snippet for kw in ['agent', 'rights', 'rep', 'agency', 'literary', 'press', 'media']):
                                if details["Agent_Email"] == "N/A": details["Agent_Email"] = clean_email
                            else:
                                if details["Author_Email"] == "N/A": details["Author_Email"] = clean_email
                        if details["Contact_Website"] == "N/A": details["Contact_Website"] = url
                    except: continue
            except: continue
            finally: await page.close()
            
        return idx, details

async def run_deep_scan_mission():
    if not os.path.exists(MASTER_RESULTS_FILE): return
    df = pd.read_excel(MASTER_RESULTS_FILE)
    mask = (df['Author Email ID'] == "N/A") | (df['Agency Email ID'] == "N/A") | (df['Author Email ID'].isna())
    missing_indices = df.index[mask].tolist()
    print(f"Found {len(missing_indices)} authors requiring deep-scan.")
    
    sem = asyncio.Semaphore(CONCURRENCY)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        
        for i in range(0, len(missing_indices), BATCH_SIZE):
            batch = missing_indices[i : i + BATCH_SIZE]
            print(f"\n>>> Processing Batch {i//BATCH_SIZE + 1} ({len(batch)} authors)...")
            
            tasks = [deep_scan_author(idx, df.loc[idx, 'Author Name'], context, sem) for idx in batch]
            results = await asyncio.gather(*tasks)
            
            for idx, details in results:
                if details["Author_Email"] != "N/A": df.at[idx, 'Author Email ID'] = details["Author_Email"]
                if details["Agent_Email"] != "N/A": df.at[idx, 'Agency Email ID'] = details["Agent_Email"]
                if details["Contact_Website"] != "N/A" and (pd.isna(df.loc[idx, 'Author Contact Form - Website']) or df.loc[idx, 'Author Contact Form - Website'] == "N/A"):
                    df.at[idx, 'Author Contact Form - Website'] = details["Contact_Website"]
                    
            df.to_excel(MASTER_RESULTS_FILE, index=False)
            print(f"Batch {i//BATCH_SIZE + 1} complete and saved.")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_deep_scan_mission())
