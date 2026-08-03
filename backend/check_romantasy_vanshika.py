import pandas as pd
import os
import re
import math

def is_fantasy(tag):
    tag = tag.lower()
    fantasy_keywords = ['fantasy', 'paranormal', 'supernatural', 'magic', 'fae', 'witch', 'vampire', 'dragon', 'mythology', 'fairy tale', 'monster', 'isekai', 'reincarnation', 'sci-fi', 'beast']
    return any(kw in tag for kw in fantasy_keywords)

def is_romance(tag):
    tag = tag.lower()
    romance_keywords = ['romance', 'romantasy', 'romantic', 'love', 'mate', 'heart', 'beauty']
    return any(kw in tag for kw in romance_keywords)

def classify_romantasy(row):
    # Combine tags from 'Genre', 'Genre Tags', 'Keyword'
    tags = []
    
    def add_tags(val):
        if pd.isna(val): return
        val = str(val)
        # Split by comma
        parts = [p.strip() for p in val.split(',')]
        for p in parts:
            if p and p not in tags:
                tags.append(p)
                
    add_tags(row.get('Genre'))
    add_tags(row.get('Genre Tags'))
    add_tags(row.get('Keyword'))
    
    idx_f = -1
    idx_r = -1
    
    for i, tag in enumerate(tags):
        if idx_f == -1 and is_fantasy(tag):
            idx_f = i
        if idx_r == -1 and is_romance(tag):
            idx_r = i
            
    # "romantasy" counts as both
    for i, tag in enumerate(tags):
        if 'romantasy' in tag.lower():
            if idx_f == -1 or i < idx_f: idx_f = i
            if idx_r == -1 or i < idx_r: idx_r = i

    if idx_f == -1 or idx_r == -1:
        return 'Fail'
        
    rank = max(idx_f, idx_r)
    
    if rank < 5:
        verdict = 'Strong Match'
    elif rank < 9:
        verdict = 'Confirmed Match'
    else:
        verdict = 'Weak Match'
        
    num_books = row.get('Num_Primary_Books_in_Series')
    if pd.notna(num_books):
        try:
            num = float(num_books)
            if num < 3:
                verdict = 'Weak Match'
        except:
            pass
    else:
        # If no info about series length, do not arbitrarily downgrade
        pass
        
    return verdict

def main():
    file_path = r'E:\Internship\PocketFM\vanshika_part1.xlsx'
    print(f"Reading {file_path}...")
    df = pd.read_excel(file_path)
    
    # Process
    verdicts = []
    for _, row in df.iterrows():
        verdicts.append(classify_romantasy(row))
        
    df['Romantasy Checker'] = verdicts
    
    print("Saving updated file...")
    df.to_excel(file_path, index=False)
    
    print("Done. Sample results:")
    print(df[['Book Title', 'Num_Primary_Books_in_Series', 'Romantasy Checker']].head(15))

if __name__ == '__main__':
    main()
