import pandas as pd
import os

def main():
    base_dir = r"e:\Internship\PocketFM"
    mega_sheet_path = os.path.join(base_dir, "Mega_Sheet.xlsx")
    
    print("Loading Mega_Sheet...")
    df = pd.read_excel(mega_sheet_path)
    
    cols = df.columns.tolist()
    if 'Amazon URL' in cols and 'Keyword' in cols:
        cols.remove('Amazon URL')
        
        # Insert Amazon URL right before 'Keyword'
        keyword_index = cols.index('Keyword')
        cols.insert(keyword_index, 'Amazon URL')
        
        # Reorder dataframe
        df = df[cols]
        print(f"Reordered columns. Amazon URL is now at index {keyword_index}.")
        
        # Save
        print("Saving updated Mega_Sheet.xlsx...")
        df.to_excel(mega_sheet_path, index=False)
        print("Done!")
    else:
        print("Columns missing, cannot reorder.")

if __name__ == "__main__":
    main()
