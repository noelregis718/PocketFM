import pandas as pd
import re

files = [
    r"e:\Internship\PocketFM\noel_part2.xlsx"
]

def clean_genres(text):
    if pd.isna(text):
        return text
    text = str(text)
    
    # Remove common trailing Goodreads button texts
    text = re.sub(r',?\s*\.\.\.show all\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r',?\s*\.\.\.more\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r',?\s*show all\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r',?\s*more\b', '', text, flags=re.IGNORECASE)
    
    # Clean up any weird trailing commas or spaces
    return text.strip(', ')

for file in files:
    try:
        df = pd.read_excel(file)
        if 'Genre Tags' in df.columns:
            print(f"Cleaning 'Genre Tags' in {file}...")
            df['Genre Tags'] = df['Genre Tags'].apply(clean_genres)
            df.to_excel(file, index=False)
            print(f"Successfully saved {file}!")
        else:
            print(f"Column 'Genre Tags' not found in {file}")
    except Exception as e:
        print(f"Error processing {file}: {e}")
