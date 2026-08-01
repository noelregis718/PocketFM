import pandas as pd
import glob
import os

target_columns = [
    'Book Title', 'Author Name', 'Series Name', 'Genre', 'Logline', 'Keyword', 
    'GoodReads_Series_URL', 'Num_Primary_Books_in_Series', 'Total_Page_Count_of_Primary_Books', 
    'Book1_Rating', 'Book1_Num_Ratings', 'Genre Tags', 'Romantasy Checker'
]

# Get the 8 excel sheets in the root directory
files = glob.glob('Amazon_Scraping_*.xlsx')

all_data = []

for file in files:
    print(f"Processing {file}...")
    try:
        df = pd.read_excel(file)
        
        # We need to extract the target columns, filling with NaN if they don't exist
        extracted_df = pd.DataFrame()
        for col in target_columns:
            if col in df.columns:
                extracted_df[col] = df[col]
            else:
                extracted_df[col] = pd.NA
                
        all_data.append(extracted_df)
    except Exception as e:
        print(f"Error processing {file}: {e}")

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_excel('Mega_Sheet.xlsx', index=False)
    print(f"Successfully merged {len(files)} files into Mega_Sheet.xlsx with {len(final_df)} total rows.")
else:
    print("No data processed.")
