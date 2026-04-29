import pandas as pd
import os
import asyncio
from playwright.async_api import async_playwright
import sys

# Add backend to path for scraper
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd())))

from scraper import AuthorScraper
from excel_utility import save_to_excel

# Configuration
MASTER_FILE = r"E:\Internship\PocketFM\Romantasy _ Self Publication Master (1).xlsx"
ENRICHED_FILE = r"E:\Internship\PocketFM\Master_Author_Enrichment.xlsx"
OUTPUT_FILE = r"E:\Internship\PocketFM\Jacob_Peppers_to_End_Enrichment.xlsx"
SHEET_NAME = "Picks for Licensing"
HEADER_ROW = 2
START_ROW = 713 # Jacob Peppers

async def run_targeted_mission():
    print(f"Loading Master File from Row {START_ROW}...")
    df_master = pd.read_excel(MASTER_FILE, sheet_name=SHEET_NAME, header=HEADER_ROW-1)
    
    # Filter for rows from Jacob Peppers (index 710) onwards
    df_targeted = df_master.iloc[START_ROW-3:].copy()
    print(f"Targeted {len(df_targeted)} authors.")

    # Load existing enrichment if available
    enriched_data = {}
    if os.path.exists(ENRICHED_FILE):
        df_enriched = pd.read_excel(ENRICHED_FILE)
        for _, row in df_enriched.iterrows():
            enriched_data[str(row['Author Name']).strip()] = row.to_dict()

    results = []
    missing_authors = []
    
    for _, row in df_targeted.iterrows():
        name = str(row['Author Name']).strip()
        if name in enriched_data:
            # Use existing data if it's not all N/A
            data = enriched_data[name]
            if data.get('Author Email ID') != "N/A" or data.get('Agency Email ID') != "N/A":
                results.append(data)
                continue
        missing_authors.append(name)

    if missing_authors:
        print(f"Scraping {len(missing_authors)} authors who were missed or have no data...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            author_scraper = AuthorScraper()
            
            for name in missing_authors:
                print(f"  [Scrape] {name}...")
                try:
                    details = await author_scraper.find_author_details(context, name)
                    res = {
                        "Author Name": name,
                        "Author Email ID": details.get("Author_Email", "N/A"),
                        "Author Contact Form - Website": details.get("Contact_Website", "N/A"),
                        "Agency Email ID": details.get("Agent_Email", "N/A")
                    }
                    results.append(res)
                except Exception as e:
                    print(f"    Error: {e}")
                    results.append({"Author Name": name, "Author Email ID": "N/A", "Author Contact Form - Website": "N/A", "Agency Email ID": "N/A"})
            
            await browser.close()
    
    # Save to targeted file
    df_final = pd.DataFrame(results)
    df_final.to_excel(OUTPUT_FILE, index=False)
    print(f"Targeted enrichment saved to {OUTPUT_FILE}")
    
    if os.name == 'nt':
        os.startfile(OUTPUT_FILE)

if __name__ == "__main__":
    asyncio.run(run_targeted_mission())
