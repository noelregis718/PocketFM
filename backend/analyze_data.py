import pandas as pd
import numpy as np

FILE_PATH = r'e:\Internship\scraped_data_dark_romance.xlsx'

def analyze():
    try:
        df = pd.read_excel(FILE_PATH)
        print(f"Total Rows: {len(df)}")
        
        cols = ['Book Title', 'Author Name', 'Amazon Stars', 'GoodReads_Series_URL', 'Price_Tier']
        print("\n--- Missing Data (NaN or 'N/A') ---")
        for col in cols:
            missing = df[col].isna().sum() + (df[col] == 'N/A').sum()
            print(f"{col}: {missing}")
            
        print("\n--- Currency Check ---")
        inr_count = df['Price_Tier'].astype(str).str.contains('₹|INR', regex=True).sum()
        print(f"Rows with INR prices: {inr_count}")
        
        print("\n--- Duplicate Check ---")
        dupes = df.duplicated(subset=['Book Title']).sum()
        print(f"Duplicate titles: {dupes}")
        
        print("\n--- Last 5 Titles ---")
        print(df['Book Title'].tail(5).to_list())

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze()
