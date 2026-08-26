# Excel Styling and Text Wrapping Guide

This guide explains how to properly format output Excel sheets in this project by applying thin rows and text wrapping. This ensures the output is readable, clean, and ready to be shared with the team.

## Overview

When generating large Excel files (e.g., `Merged_Romance_Keywords.xlsx`), the default formatting often makes the text bleed across cells or the rows look unorganized. 

To fix this, we use the `apply_style.py` script which relies on the `openpyxl` library to perform two main styling changes:
1. **Row Height:** Sets the row height to a thin `15` points.
2. **Text Wrapping:** Wraps text within its cell and aligns it to the `top`.

## How to Apply Styles

To format an Excel sheet, you can use the existing `apply_style.py` script located in the root directory.

### 1. The Script (`apply_style.py`)

If you need to adapt the styling for a new file, here is the standard template we use:

```python
import openpyxl

# 1. Set the path to your target Excel file
file_path = r'E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx'

try:
    print(f'Applying thin rows (15) and wrap text styling to {file_path}...')
    wb = openpyxl.load_workbook(file_path)
    ws = wb.active
    
    # 2. Iterate through all rows to set the height
    for row in range(1, ws.max_row + 1):
        ws.row_dimensions[row].height = 15
        
        # 3. Iterate through every cell to apply text wrapping
        for col in range(1, ws.max_column + 1):
            cell = ws.cell(row=row, column=col)
            
            # If the cell already has alignment properties, retain them but add wrapping
            if cell.alignment:
                cell.alignment = openpyxl.styles.Alignment(
                    horizontal=cell.alignment.horizontal,
                    vertical=cell.alignment.vertical,
                    text_rotation=cell.alignment.text_rotation,
                    wrap_text=True,
                    shrink_to_fit=cell.alignment.shrink_to_fit,
                    indent=cell.alignment.indent
                )
            else:
                # Default alignment: Top-aligned and wrapped
                cell.alignment = openpyxl.styles.Alignment(wrap_text=True, vertical='top')
                
    # 4. Save the formatted workbook
    wb.save(file_path)
    print('Styling applied successfully.')
    
except Exception as e:
    print(f'Failed to apply styling: {e}')
```

### 2. Running the Script

To apply the styling, open your terminal and ensure you are in the project root folder. Then, execute the script:

```bash
cd E:\Internship\PocketFM
python apply_style.py
```

## Performance Note
Because `openpyxl` individually targets and formats every single cell, applying this script to massive files (e.g., 10,000+ rows / 100,000+ cells) can take a minute or two to finish. 
* Do not cancel the process if it seems paused—it is actively working in the background. 
* The script will exit silently or print a success message once the file has been successfully written to the disk.
