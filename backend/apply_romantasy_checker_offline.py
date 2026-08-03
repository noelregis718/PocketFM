import pandas as pd
import os

def _apply_romantasy_checker(df, index, row):
    classification = "Fail"
    all_tags = []
    
    def add_tags(val):
        if pd.isna(val) or not val: return
        parts = [p.strip() for p in str(val).split(',')]
        for p in parts:
            if p and p not in all_tags: all_tags.append(p)
            
    # Add genre from amazon
    add_tags(row.get('Genre'))
    
    # Add Goodreads Genre Tags
    add_tags(row.get('Genre Tags'))
    
    # Add Amazon Keyword
    add_tags(row.get('Keyword'))
    
    fantasy_kws = ['fantasy', 'paranormal', 'supernatural', 'magic', 'fae', 'witch', 'vampire', 'dragon', 'mythology', 'fairy tale', 'monster', 'isekai', 'reincarnation', 'sci-fi', 'beast']
    romance_kws = ['romance', 'romantasy', 'romantic', 'love', 'mate', 'heart', 'beauty']
    
    idx_f, idx_r = -1, -1
    for i, tag in enumerate(all_tags):
        tag_lower = tag.lower()
        if idx_f == -1 and any(kw in tag_lower for kw in fantasy_kws): idx_f = i
        if idx_r == -1 and any(kw in tag_lower for kw in romance_kws): idx_r = i
    for i, tag in enumerate(all_tags):
        if 'romantasy' in tag.lower():
            if idx_f == -1 or i < idx_f: idx_f = i
            if idx_r == -1 or i < idx_r: idx_r = i
            
    if idx_f != -1 and idx_r != -1:
        rank = max(idx_f, idx_r)
        if rank < 5: classification = "Strong Match"
        elif rank < 9: classification = "Confirmed Match"
        else: classification = "Weak Match"
        
    num_books = row.get("Num_Primary_Books_in_Series")
    if pd.notna(num_books) and str(num_books).strip():
        try:
            if float(num_books) < 3:
                classification = "Weak Match"
        except: pass
        
    df.at[index, "Romantasy Checker"] = classification

def process_file(filepath):
    print(f"Processing {filepath}...")
    df = pd.read_excel(filepath)
    
    if "Romantasy Checker" not in df.columns:
        df["Romantasy Checker"] = None
        
    for index, row in df.iterrows():
        _apply_romantasy_checker(df, index, row)
        
    df.to_excel(filepath, index=False)
    
    counts = df['Romantasy Checker'].value_counts()
    print(f"Done! Results for {filepath}:")
    print(counts)
    print("------------------------------------------")

if __name__ == "__main__":
    import sys
    sys.path.append(r"e:\Internship\PocketFM\backend")
    import format_excel
    
    base_dir = r"e:\Internship\PocketFM"
    files = ["noel_part1.xlsx", "noel_part2.xlsx"]
    
    for file in files:
        path = os.path.join(base_dir, file)
        if os.path.exists(path):
            process_file(path)
            format_excel.apply_styling(path)
        else:
            print(f"File not found: {path}")
