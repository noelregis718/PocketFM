import pandas as pd
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf8')

def inspect_csv(file_path):
    print(f"--- Inspecting {file_path} ---")
    try:
        # Read only a chunk or just the first few rows for huge files
        df = pd.read_csv(file_path, nrows=5)
        print("Columns:", list(df.columns))
        print("Head:")
        print(df)
        
        # Get shape (this requires reading the whole file, which is fine for 89MB in pandas, takes 1-2 seconds)
        # Or we can just read column names and shape
        df_full = pd.read_csv(file_path, usecols=[0]) # just to get length efficiently
        print(f"Total Rows: {len(df_full)}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    print("\n")

if __name__ == "__main__":
    inspect_csv("consolidated_books_scraped.csv")
    inspect_csv("Remaining_Romance_Fantasy_Categories.csv")
