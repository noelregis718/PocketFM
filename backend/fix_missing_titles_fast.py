import pandas as pd
import urllib.request
import re
import gzip
import time

EXCEL_FILE = r"E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx"

def fetch_amazon_details(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.5'
    }
    
    req = urllib.request.Request(url, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        if resp.info().get('Content-Encoding') == 'gzip':
            html = gzip.decompress(resp.read()).decode('utf-8', errors='ignore')
        else:
            html = resp.read().decode('utf-8', errors='ignore')
            
        title = None
        author = None
        
        # 1. Try to get title from <title> tag
        title_match = re.search(r'<title>(.*?)</title>', html)
        if title_match:
            raw_title = title_match.group(1).strip()
            # Clean up title
            raw_title = raw_title.replace('Amazon.com:', '').strip()
            
            # Series page check: "Book Title (5 book series) Kindle Edition"
            if '(book series)' in raw_title or ' book series)' in raw_title or 'Kindle Edition' in raw_title:
                title = raw_title.replace('Kindle Edition', '').strip()
                # Often authors are not in the series page title
            elif '- Kindle edition by' in raw_title:
                parts = raw_title.split('- Kindle edition by')
                title = parts[0].strip()
                author = parts[1].split('.')[0].strip()
            elif 'eBook :' in raw_title:
                parts = raw_title.split('eBook :')
                title = parts[0].strip()
                if ':' in parts[1]:
                    author = parts[1].split(':')[0].strip()
            else:
                title = raw_title
                
        # 2. Try to grab author from HTML if we didn't get it from title
        if not author:
            # Common author classes: "author", "contributorNameID", "bylineInfo"
            author_match = re.search(r'class="a-link-normal contributorNameID".*?>(.*?)<', html)
            if not author_match:
                author_match = re.search(r'<span class="author notFaded.*?<a.*?>(.*?)<', html, re.DOTALL)
            # Series page specific author tags
            if not author_match:
                author_match = re.search(r'<span class="a-truncate-full">([^<]+)</span>', html)
            if not author_match:
                author_match = re.search(r'<img alt="([^"]+)" src="[^"]*amzn-author-media-prod', html)
                
            if author_match:
                author = author_match.group(1).strip()
                
        return title, author
        
    except Exception as e:
        return None, None

def process_row(idx, url, df):
    print(f"[{idx}] Fetching {url} ...")
    title, author = fetch_amazon_details(url)
    return idx, url, title, author

def main():
    print(f"Loading {EXCEL_FILE}...")
    df = pd.read_excel(EXCEL_FILE)
    
    missing_indices = df[df['Book Title'].isna() | (df['Book Title'].astype(str).str.strip() == '') | df['Author Name'].isna() | (df['Author Name'].astype(str).str.strip() == '')].index
    print(f"Found {len(missing_indices)} rows to fix.")
    
    if len(missing_indices) == 0:
        print("Nothing to fix.")
        return
        
    fixed_count = 0
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    urls_to_process = []
    for idx in missing_indices:
        url = df.at[idx, 'Amazon URL']
        if pd.isna(url) or not str(url).startswith('http'):
            continue
        urls_to_process.append((idx, url))
        
    print(f"Processing {len(urls_to_process)} URLs with 8 concurrent requests...")
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_row, idx, url, df): idx for idx, url in urls_to_process}
        
        for future in as_completed(futures):
            idx, url, title, author = future.result()
            
            updated = False
            if title and (pd.isna(df.at[idx, 'Book Title']) or str(df.at[idx, 'Book Title']).strip() == ''):
                df.at[idx, 'Book Title'] = title
                print(f"  -> Title for [{idx}]: {title}")
                updated = True
            if author and (pd.isna(df.at[idx, 'Author Name']) or str(df.at[idx, 'Author Name']).strip() == ''):
                df.at[idx, 'Author Name'] = author
                print(f"  -> Author for [{idx}]: {author}")
                updated = True
                
            if updated:
                fixed_count += 1
                
    if fixed_count > 0:
        print(f"Saving changes to {EXCEL_FILE}...")
        df.to_excel(EXCEL_FILE, index=False)
        print("Done!")
    else:
        print("Could not extract any new details.")

if __name__ == "__main__":
    main()
