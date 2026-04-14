import pandas as pd
import os
from openpyxl.styles import Alignment, Font

def save_to_excel(data, filename="scraped_books.xlsx"):
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Save to Excel using OpenPyXL engine
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='PocketFM Scraped Books')
        
        # Access the worksheet
        worksheet = writer.sheets['PocketFM Scraped Books']
        
        # Formatting: Auto-sizing and Text Wrapping
        for idx, col in enumerate(df.columns):
            max_len = 0
            # Calculate max length in column
            for val in df[col]:
                if val:
                    # Factor in text wrap for description
                    val_str = str(val)
                    if col == 'Description':
                        max_len = 50 # Cap width for description
                    else:
                        max_len = max(max_len, len(val_str))
            
            # Set column width (add padding)
            adjusted_width = min(max_len + 2, 60) # Cap at 60
            worksheet.column_dimensions[chr(65 + idx)].width = adjusted_width
            
            # Apply wrapping and alignment
            for cell in worksheet[chr(65 + idx)]:
                cell.alignment = Alignment(wrap_text=True, vertical='top')
                if cell.row == 1:
                    cell.font = Font(bold=True)
                    
    return os.path.abspath(filename)
