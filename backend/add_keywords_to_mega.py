import pandas as pd
import glob
import os
import re

def main():
    base_dir = r"e:\Internship\PocketFM"
    mega_sheet_path = os.path.join(base_dir, "Mega_Sheet.xlsx")
    
    print("Loading Mega_Sheet...")
    try:
        mega_df = pd.read_excel(mega_sheet_path)
    except Exception as e:
        print(f"Error loading Mega_Sheet: {e}")
        return

    # Dictionary to map (book_title, author_name) -> set of keywords
    keyword_map = {}
    
    # Find all Amazon_Scraping_*.xlsx files
    scraping_files = glob.glob(os.path.join(base_dir, "Amazon_Scraping_*.xlsx"))
    
    for file in scraping_files:
        filename = os.path.basename(file)
        # Extract keyword from filename, e.g., Amazon_Scraping_Beauty_Beast.xlsx -> Beauty Beast
        keyword = filename.replace("Amazon_Scraping_", "").replace(".xlsx", "").replace("_", " ")
        print(f"Processing '{filename}' for keyword '{keyword}'...")
        
        try:
            df = pd.read_excel(file)
            if 'Book Title' not in df.columns or 'Author Name' not in df.columns:
                print(f"  Warning: Missing required columns in {filename}")
                continue
                
            # Iterate through rows and add to mapping
            for _, row in df.iterrows():
                title = str(row['Book Title']).strip().lower() if pd.notna(row['Book Title']) else ""
                author = str(row['Author Name']).strip().lower() if pd.notna(row['Author Name']) else ""
                
                if not title:
                    continue
                    
                key = (title, author)
                if key not in keyword_map:
                    keyword_map[key] = set()
                keyword_map[key].add(keyword)
                
        except Exception as e:
            print(f"  Error reading {filename}: {e}")
            
    print(f"Found keyword mappings for {len(keyword_map)} unique books.")
    
    # Update Mega_Sheet
    print("Updating Mega_Sheet...")
    updates_made = 0
    
    def get_new_keyword(row):
        nonlocal updates_made
        title = str(row['Book Title']).strip().lower() if pd.notna(row['Book Title']) else ""
        author = str(row['Author Name']).strip().lower() if pd.notna(row['Author Name']) else ""
        key = (title, author)
        
        if key in keyword_map:
            # Combine new keywords with any existing ones (if any)
            existing = str(row['Keyword']) if pd.notna(row['Keyword']) else ""
            existing_set = {k.strip() for k in existing.split(',') if k.strip()}
            
            new_set = existing_set.union(keyword_map[key])
            if len(new_set) > len(existing_set) or not existing_set:
                updates_made += 1
                return ", ".join(sorted(new_set))
        return row['Keyword']
        
    if 'Keyword' not in mega_df.columns:
        mega_df['Keyword'] = None
        
    mega_df['Keyword'] = mega_df.apply(get_new_keyword, axis=1)
    
    print(f"Updated keywords for {updates_made} rows in Mega_Sheet.")
    
    # Save the updated sheet
    print("Saving updated Mega_Sheet.xlsx...")
    mega_df.to_excel(mega_sheet_path, index=False)
    print("Done!")

if __name__ == "__main__":
    main()
