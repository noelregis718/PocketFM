import asyncio
import pandas as pd
import os
import re
from playwright.async_api import async_playwright

# Configuration
TARGET_FILE = r"E:\Internship\PocketFM\Jacob_Peppers_to_End_Enrichment.xlsx"
CONCURRENCY = 5 
BATCH_SIZE = 10

async def deep_scan_author(idx, author_name, context, sem):
    async with sem:
        print(f"  [Deep Repair] Investigating: {author_name}")
        
        # Broader queries to find more accurate results
        queries = [
            f'"{author_name}" author contact email',
            f'"{author_name}" literary agent contact',
            f'"{author_name}" official website'
        ]
        
        found_details = {
            "Author_Email": "N/A",
            "Agent_Email": "N/A",
            "Contact_Website": "N/A"
        }
        
        for query in queries:
            # If we found everything, we can stop for this author
            if found_details["Author_Email"] != "N/A" and found_details["Agent_Email"] != "N/A":
                break
                
            page = await context.new_page()
            try:
                # Use Google search via duckduckgo or direct if possible
                search_url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
                await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                
                # Check for results
                links = await page.query_selector_all('div.g a')
                target_urls = []
                for link in links:
                    href = await link.evaluate("el => el.href")
                    if href and not any(x in href.lower() for x in ['google.com', 'amazon.com', 'goodreads.com', 'facebook.com', 'twitter.com', 'instagram.com']):
                        target_urls.append(href)
                    if len(target_urls) >= 3: break # Top 3 results
                
                for url in target_urls:
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        content = await page.content()
                        
                        # Email regex
                        email_pattern = r'[a-zA-Z0-9_.+-]+(?:@|\[at\]|at)[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
                        found_emails = re.findall(email_pattern, content)
                        
                        for email in found_emails:
                            clean_email = email.replace('[at]', '@').replace('at', '@').replace(' ', '')
                            if '@' not in clean_email or '.' not in clean_email or len(clean_email) > 50: continue
                            
                            lower_content = content.lower()
                            email_idx = lower_content.find(email.lower())
                            snippet = lower_content[max(0, email_idx-150):email_idx+150]
                            
                            if any(kw in snippet for kw in ['agent', 'rights', 'rep', 'agency', 'literary', 'press', 'media']):
                                if found_details["Agent_Email"] == "N/A": found_details["Agent_Email"] = clean_email
                            else:
                                if found_details["Author_Email"] == "N/A": found_details["Author_Email"] = clean_email
                        
                        if found_details["Contact_Website"] == "N/A": found_details["Contact_Website"] = url
                    except: continue
            except: continue
            finally: await page.close()
            
        return idx, found_details

async def run_repair_mission():
    if not os.path.exists(TARGET_FILE):
        print("Error: File not found.")
        return
        
    df = pd.read_excel(TARGET_FILE)
    print(f"Loaded {len(df)} authors for repair.")
    
    sem = asyncio.Semaphore(CONCURRENCY)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        for i in range(0, len(df), BATCH_SIZE):
            batch_indices = df.index[i : i + BATCH_SIZE].tolist()
            print(f"\n>>> Repairing Batch {i//BATCH_SIZE + 1}...")
            
            tasks = [deep_scan_author(idx, df.loc[idx, 'Author Name'], context, sem) for idx in batch_indices]
            results = await asyncio.gather(*tasks)
            
            for idx, details in results:
                # Update only if new data was found
                if details["Author_Email"] != "N/A":
                    df.at[idx, 'Author Email ID'] = details["Author_Email"]
                if details["Agent_Email"] != "N/A":
                    df.at[idx, 'Agency Email ID'] = details["Agent_Email"]
                if details["Contact_Website"] != "N/A":
                    df.at[idx, 'Author Contact Form - Website'] = details["Contact_Website"]
            
            # Save progress periodically
            df.to_excel(TARGET_FILE, index=False)
            print(f"  [Progress] Batch {i//BATCH_SIZE + 1} saved.")
            
        await browser.close()
        print("\nRepair mission complete.")

if __name__ == "__main__":
    asyncio.run(run_repair_mission())
