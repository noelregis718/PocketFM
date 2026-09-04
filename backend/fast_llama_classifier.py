import pandas as pd
import json
import requests
import os
import concurrent.futures

# Ollama settings
OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL = "qwen2.5:1.5b" # Extremely fast and highly accurate tiny model
MAX_WORKERS = 10 # Sends 10 requests to Llama at the exact same time
SAVE_INTERVAL = 50 # Saves the Excel file every 50 rows so you don't lose data

EXCEL_FILE = r"E:\Internship\PocketFM\Amazon A-Z Crawl List.xlsx"
SHEET_NAME = "Sheet1"

TAXONOMY = [
    "Fantasy",
    "Romantasy",
    "Romance Drama"
]

def query_ollama(prompt):
    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.0
        }
    }
    try:
        response = requests.post(OLLAMA_API_URL, json=payload, timeout=300)
        if response.status_code == 200:
            data = response.json()
            try:
                return json.loads(data['response'])
            except json.JSONDecodeError:
                return None
        else:
            return None
    except Exception:
        return None

def build_prompt(row):
    taxonomy_str = ", ".join(f'"{t}"' for t in TAXONOMY)
    prompt = f"""You are an expert book classifier. Classify the following book into exactly one of these taxonomy genres:
[{taxonomy_str}]

You must respond with ONLY a raw JSON object containing exactly these two keys:
"classified_genre": The exact string from the taxonomy list above that best fits the book.
"reasoning": A 1-2 sentence explanation of why this genre fits based on the book data.

BOOK DATA:
Title: {row.get('Book Title', '')}
Series: {row.get('Series', '')} (Books in Series: {row.get('Books in Series', '')})
Author: {row.get('Author', '')}
Publisher: {row.get('Publisher', '')}
Categories: {row.get('Genre', '')} > {row.get('Sub-Genre', '')} > {row.get('Sub-Sub-Genre', '')}
Browse Category: {row.get('Browse Category', '')}
Amazon Rating: {row.get('Star Rating', '')} stars from {row.get('Ratings Count', '')} reviews
ASIN/URL: {row.get('ASIN', '')} - {row.get('Amazon URL', '')}
Description: {row.get('Product Description', '')}
"""
    return prompt

def process_row(idx, row):
    print(f"Processing row {idx + 1}: {row.get('Book Title')}...")
    prompt = build_prompt(row)
    res = query_ollama(prompt)
    
    if res:
        return idx, res.get("classified_genre", "Error"), res.get("reasoning", "")
    else:
        return idx, "Error", "Ollama API Failure"

def main():
    print(f"Loading {EXCEL_FILE}...")
    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)
    except Exception as e:
        print(f"Failed to load excel: {e}")
        return

    # Prepare columns
    if "Detailed Genre (AI)" not in df.columns:
        df["Detailed Genre (AI)"] = ""
    if "AI Reasoning" not in df.columns:
        df["AI Reasoning"] = ""

    # Find rows to process
    rows_to_process = []
    for idx, row in df.iterrows():
        # Only process if it hasn't been assigned a valid genre yet, or if it errored out previously
        genre_val = str(row.get("Detailed Genre (AI)", "")).strip()
        if pd.isna(row.get("Detailed Genre (AI)")) or genre_val == "" or genre_val == "Error":
            rows_to_process.append((idx, row))
            
    print(f"Found {len(rows_to_process)} rows to process.")
    
    rows_to_process = rows_to_process[:600]
    print(f"Limiting to first {len(rows_to_process)} rows for this run...")
    
    processed_count = 0
    
    # Process concurrently!
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_row, idx, row): idx for idx, row in rows_to_process}
        
        for future in concurrent.futures.as_completed(futures):
            idx, genre, reasoning = future.result()
            df.at[idx, "Detailed Genre (AI)"] = genre
            df.at[idx, "AI Reasoning"] = reasoning
            
            processed_count += 1
            if processed_count % SAVE_INTERVAL == 0:
                print(f"--- Processed {processed_count} rows. Saving checkpoint... ---")
                try:
                    df.to_excel(EXCEL_FILE, index=False)
                except Exception as e:
                    print(f"Could not save checkpoint: {e}")
                
    # Final save
    print("Saving final results...")
    try:
        df.to_excel(EXCEL_FILE, index=False)
        print("ALL DONE!")
    except Exception as e:
        print(f"Failed to save final results: {e}")

if __name__ == "__main__":
    main()
