import pandas as pd
import os
import format_excel

def combine_files():
    # The 4 specific files requested
    files_to_combine = [
        "vanshika_part1.xlsx",
        "vanshika_part2.xlsx",
        "noel_part1.xlsx",
        "noel_part2.xlsx"
    ]
    
    base_dir = r"e:\Internship\PocketFM"
    output_file = os.path.join(base_dir, "Combined_Scraped_Data.xlsx")
    
    print("Reading and combining files...")
    dataframes = []
    
    for filename in files_to_combine:
        filepath = os.path.join(base_dir, filename)
        if os.path.exists(filepath):
            print(f"Reading {filename}...")
            df = pd.read_excel(filepath)
            dataframes.append(df)
        else:
            print(f"ERROR: {filename} not found!")
            
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        print(f"Combined sheet has {len(combined_df)} rows.")
        print(f"Saving to {output_file}...")
        combined_df.to_excel(output_file, index=False)
        print("Applying styling...")
        format_excel.apply_styling(output_file)
        print("Done!")
    else:
        print("No files were found to combine.")

if __name__ == "__main__":
    combine_files()
