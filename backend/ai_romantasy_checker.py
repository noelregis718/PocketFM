import pandas as pd
import os
import torch
import re
from sentence_transformers import SentenceTransformer, util

print("Loading Lightning-Fast Hybrid Model...")
# all-MiniLM-L6-v2 is extremely fast and accurate for semantic similarity
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
print("Model loaded successfully!")

# 1. EXACT KEYWORDS (Guarantees perfect accuracy instantly for obvious books)
exact_romantasy_kws = re.compile(
    r'\b(romantasy|fantasy romance|paranormal romance|shifter romance|vampire romance)\b', 
    re.IGNORECASE
)

# 2. NUANCED AI ANCHORS (Catches the hidden Romantasy books that don't use the exact keywords)
positive_concepts = [
    "High fantasy romance, magic, fairies, royal court, magical academy, enemies to lovers",
    "Werewolf and shifter pack romance, fated mates, omega, alpha",
    "Vampire, demon, or monster dark paranormal romance",
    "Mythological retelling, gods and mortals in love",
    "Cozy magical fantasy cottagecore romance"
]
positive_embeddings = model.encode(positive_concepts, convert_to_tensor=True)

# 3. NEGATIVE ANCHORS (Prevents false positives)
negative_concepts = [
    "Contemporary romance, normal humans, modern day real world, billionaire, office romance",
    "Hard science fiction, space opera, aliens, futuristic technology",
    "Thriller, horror, murder mystery, true crime without magic",
    "Historical fiction, World War, non-magical past"
]
negative_embeddings = model.encode(negative_concepts, convert_to_tensor=True)


def process_file(filepath):
    print(f"Processing {filepath} with Hybrid AI (Keywords + Embeddings)...")
    df = pd.read_excel(filepath)
    
    total_rows = len(df)
    print(f"Running on all {total_rows} rows...")
    
    texts_to_classify = []
    
    for index, row in df.iterrows():
        text_parts = []
        title = row.get('Name of Series') or row.get('Title')
        if pd.notna(title) and title: text_parts.append(f"Title: {title}")
        synopsis = row.get('Synopsis (if available)') or row.get('Logline')
        if pd.notna(synopsis) and synopsis: text_parts.append(f"Synopsis: {synopsis}")
        tags = [str(row.get(col)) for col in ['Keyword', 'Genre', 'Genre Tags'] if pd.notna(row.get(col)) and row.get(col)]
        if tags: text_parts.append(f"Tags: {', '.join(tags)}")
            
        text_to_classify = " | ".join(text_parts)
        if len(text_to_classify.strip()) < 10:
            text_to_classify = "Empty"
            
        texts_to_classify.append(text_to_classify[:1500]) # Truncate to save memory
        
    print("Computing embeddings for all books at once...")
    book_embeddings = model.encode(texts_to_classify, convert_to_tensor=True, show_progress_bar=True)
    
    print("Calculating similarity scores and applying rules...")
    for index in range(total_rows):
        book_text = texts_to_classify[index]
        
        if book_text == "Empty":
            df.at[index, "AI Romantasy Score"] = "No Text"
            df.at[index, "AI Classification Status"] = "No"
            continue
            
        # STEP 1: INSTANT KEYWORD MATCH
        if exact_romantasy_kws.search(book_text):
            df.at[index, "AI Romantasy Score"] = "Keyword Match"
            df.at[index, "AI Classification Status"] = "Yes"
            continue
            
        # STEP 2: SEMANTIC AI MATCH
        book_emb = book_embeddings[index]
        pos_sims = util.cos_sim(book_emb, positive_embeddings)
        max_pos_sim = torch.max(pos_sims).item()
        
        neg_sims = util.cos_sim(book_emb, negative_embeddings)
        max_neg_sim = torch.max(neg_sims).item()
        
        # If it's more similar to a Romantasy concept than any negative concept, and meets a safe threshold
        if max_pos_sim > max_neg_sim and max_pos_sim > 0.25:
            classification = "Yes"
        else:
            classification = "No"
            
        df.at[index, "AI Romantasy Score"] = f"AI Match (Pos: {max_pos_sim:.2f} | Neg: {max_neg_sim:.2f})"
        df.at[index, "AI Classification Status"] = classification
        
    print(f"Saving updated Excel to {filepath}...")
    df.to_excel(filepath, index=False)
    
    counts = df['AI Classification Status'].value_counts()
    print(f"Done! Updated {filepath}")
    print(counts)
    print("------------------------------------------")

if __name__ == "__main__":
    file_path = r"E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx"
    if os.path.exists(file_path):
        process_file(file_path)
    else:
        print(f"File not found: {file_path}")
