import pandas as pd
import json
import requests
import os

# Try to use standard API endpoint for Ollama
OLLAMA_API_URL = "http://localhost:11434/api/generate"

EXCEL_FILE = r"E:\Internship\PocketFM\Amazon A-Z Crawl List.xlsx"
SHEET_NAME = "Romance + Fantasy - Cut 1 (Uniq"

# Taxonomy list based on previous scripts
TAXONOMY = [
    "High Fantasy Court Adventure",
    "Gothic Dark Romantasy",
    "Dark Academia Romantasy",
    "Monster Romance (Non-Shifter)",
    "Werewolf / Shifter Romance",
    "High-Stakes Games & Deadly Trials",
    "Mythology, Legend & Fairy Tale Retelling",
    "War College / Military Academy",
    "Korean Romance Fantasy / Isekai",
    "Paranormal Romance",
    "Cozy / Cottagecore",
    "Urban / Contemporary Fantasy Romance"
]

def query_ollama(model, prompt):
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json"
    }
    try:
        response = requests.post(OLLAMA_API_URL, json=payload, timeout=300)
        if response.status_code == 200:
            data = response.json()
            try:
                # The response from Ollama should be a JSON string inside data['response']
                return json.loads(data['response'])
            except json.JSONDecodeError:
                print(f"Failed to parse JSON from {model}: {data['response']}")
                return None
        else:
            print(f"Error from Ollama {model}: {response.text}")
            return None
    except Exception as e:
        print(f"Exception querying Ollama {model}: {e}")
        return None

def build_prompt(row):
    taxonomy_str = ", ".join(f'"{t}"' for t in TAXONOMY)
    prompt = f"""You are an expert book classifier. Classify the following book into exactly one of these taxonomy genres:
[{taxonomy_str}]

You must respond with ONLY a raw JSON object containing exactly these three keys:
"classified_genre": The exact string from the taxonomy list above that best fits the book.
"reasoning": A 1-2 sentence explanation of why this genre fits based on the book data.
"confidence_score": An integer between 1 and 100 representing your conviction.

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

def main():
    print(f"Loading {EXCEL_FILE}...")
    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)
    except Exception as e:
        print(f"Failed to load excel: {e}")
        return

    # Add output column if not exists
    if "Detailed Genre (Ensemble)" not in df.columns:
        df["Detailed Genre (Ensemble)"] = ""
    if "Ensemble Reasoning" not in df.columns:
        df["Ensemble Reasoning"] = ""

    # Test on just a few rows first!
    test_rows = 10
    print(f"Processing first {test_rows} rows for testing...")

    for idx, row in df.head(test_rows).iterrows():
        # Skip if already classified
        if str(row.get("Detailed Genre (Ensemble)", "")).strip() != "" and str(row.get("Detailed Genre (Ensemble)", "")).strip() != "nan":
            continue

        print(f"\nProcessing row {idx + 1}: {row.get('Book Title')}")
        prompt = build_prompt(row)

        print("  Querying Llama 3.1...")
        res_llama = query_ollama("llama3.1", prompt)
        
        print("  Querying Gemma 2...")
        res_gemma = query_ollama("gemma2", prompt)

        final_genre = "Error"
        reasoning = ""

        if res_llama and res_gemma:
            genre_llama = res_llama.get("classified_genre")
            genre_gemma = res_gemma.get("classified_genre")
            
            # Safe parsing for confidence score
            try:
                conf_llama = int(res_llama.get("confidence_score", 0))
            except:
                conf_llama = 0
                
            try:
                conf_gemma = int(res_gemma.get("confidence_score", 0))
            except:
                conf_gemma = 0

            if genre_llama == genre_gemma:
                final_genre = genre_llama
                reasoning = f"Agreement ({conf_llama}% & {conf_gemma}%). Reason: {res_llama.get('reasoning')}"
            else:
                # Tie-breaker: choose the one with higher confidence
                if conf_llama >= conf_gemma:
                    final_genre = genre_llama
                    reasoning = f"Tie-break (Llama won {conf_llama}% vs {conf_gemma}%). Gemma suggested {genre_gemma}. Reason: {res_llama.get('reasoning')}"
                else:
                    final_genre = genre_gemma
                    reasoning = f"Tie-break (Gemma won {conf_gemma}% vs {conf_llama}%). Llama suggested {genre_llama}. Reason: {res_gemma.get('reasoning')}"
        elif res_llama:
            final_genre = res_llama.get("classified_genre")
            reasoning = f"Llama only ({res_llama.get('confidence_score', 0)}%). Reason: {res_llama.get('reasoning')}"
        elif res_gemma:
            final_genre = res_gemma.get("classified_genre")
            reasoning = f"Gemma only ({res_gemma.get('confidence_score', 0)}%). Reason: {res_gemma.get('reasoning')}"
        
        print(f"  Result: {final_genre}")
        df.at[idx, "Detailed Genre (Ensemble)"] = final_genre
        df.at[idx, "Ensemble Reasoning"] = reasoning

    print("Saving test results...")
    # Because all other sheets were deleted, we can safely overwrite the whole file with this sheet
    df.to_excel(EXCEL_FILE, index=False)
    print("Done!")

if __name__ == "__main__":
    main()
