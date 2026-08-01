import pandas as pd
import glob
import os

def main():
    base_dir = r"e:\Internship\PocketFM"
    mega_sheet_path = os.path.join(base_dir, "Mega_Sheet.xlsx")
    
    print("Loading Mega_Sheet...")
    try:
        mega_df = pd.read_excel(mega_sheet_path)
    except Exception as e:
        print(f"Error loading Mega_Sheet: {e}")
        return

    # Dictionary to map (book_title, author_name) -> Amazon URL
    url_map = {}
    
    # Find all Amazon_Scraping_*.xlsx files
    scraping_files = glob.glob(os.path.join(base_dir, "Amazon_Scraping_*.xlsx"))
    
    for file in scraping_files:
        filename = os.path.basename(file)
        print(f"Processing '{filename}' for Amazon URLs...")
        
        try:
            df = pd.read_excel(file)
            if 'Book Title' not in df.columns or 'Author Name' not in df.columns or 'Amazon URL' not in df.columns:
                print(f"  Warning: Missing required columns in {filename}")
                continue
                
            # Iterate through rows and add to mapping
            for _, row in df.iterrows():
                title = str(row['Book Title']).strip().lower() if pd.notna(row['Book Title']) else ""
                author = str(row['Author Name']).strip().lower() if pd.notna(row['Author Name']) else ""
                url = str(row['Amazon URL']).strip() if pd.notna(row['Amazon URL']) else ""
                
                if not title or not url:
                    continue
                    
                key = (title, author)
                if key not in url_map:
                    url_map[key] = url
                
        except Exception as e:
            print(f"  Error reading {filename}: {e}")
            
    print(f"Found Amazon URLs for {len(url_map)} unique books.")
    
    # Update Mega_Sheet
    print("Updating Mega_Sheet...")
    updates_made = 0
    
    def get_amazon_url(row):
        nonlocal updates_made
        title = str(row['Book Title']).strip().lower() if pd.notna(row['Book Title']) else ""
        author = str(row['Author Name']).strip().lower() if pd.notna(row['Author Name']) else ""
        key = (title, author)
        
        if key in url_map:
            current_url = str(row['Amazon URL']).strip() if 'Amazon URL' in row and pd.notna(row.get('Amazon URL')) else ""
            if not current_url:  # Only update if it doesn't already have one, or if column is new
                updates_made += 1
                return url_map[key]
        return row.get('Amazon URL', None)
        
    if 'Amazon URL' not in mega_df.columns:
        mega_df['Amazon URL'] = None
        
    mega_df['Amazon URL'] = mega_df.apply(get_amazon_url, axis=1)
    
    print(f"Added/Updated Amazon URLs for {updates_made} rows in Mega_Sheet.")
    
    # Save the updated sheet
    print("Saving updated Mega_Sheet.xlsx...")
    mega_df.to_excel(mega_sheet_path, index=False)
    print("Done!")

if __name__ == "__main__":
    main()
