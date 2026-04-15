import pandas as pd
import os
from datetime import datetime
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

def save_to_excel(data, filename="scraped_data.xlsx"):
    # Create DataFrame
    df = pd.DataFrame(data)

    def _write(path):
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Amazon Scraped Books')

            worksheet = writer.sheets['Amazon Scraped Books']

            # Per-column width rules
            col_widths = {
                'Rank':             6,
                'Book Title':       45,
                'Author Name':      25,
                'Rating':           8,
                'Number of Reviews':18,
                'Price':            35,   # Wide enough for multi-line pricing
                'Amazon URL':       50,
                'Description':      55,
                'Publisher':        28,
                'Publication Date': 20,
            }

            for idx, col in enumerate(df.columns):
                col_letter = get_column_letter(idx + 1)
                width = col_widths.get(col, 20)
                worksheet.column_dimensions[col_letter].width = width

                for cell in worksheet[col_letter]:
                    cell.alignment = Alignment(wrap_text=True, vertical='top')
                    if cell.row == 1:
                        cell.font = Font(bold=True)

            # Dynamic row height: auto-fit based on content length
            for row_num in range(2, worksheet.max_row + 1):
                max_lines = 1
                for col_idx, col_name in enumerate(df.columns):
                    col_letter = get_column_letter(col_idx + 1)
                    cell = worksheet[f"{col_letter}{row_num}"]
                    val = str(cell.value) if cell.value else ""
                    col_width = col_widths.get(col_name, 20)

                    # Count explicit newlines (e.g. Price has \n between formats)
                    newline_count = val.count('\n') + 1

                    # Estimate wrapped lines: chars per line ≈ column width * 1.2
                    chars_per_line = max(int(col_width * 1.2), 10)
                    for line in val.split('\n'):
                        wrapped = max(1, -(-len(line) // chars_per_line))  # ceil division
                        newline_count += wrapped - 1

                    max_lines = max(max_lines, newline_count)

                # 15px per line, minimum 20px
                worksheet.row_dimensions[row_num].height = max(max_lines * 15, 20)

    # Primary attempt: write to the requested filename
    try:
        _write(filename)
        print(f"Excel saved: {filename}")
        return os.path.abspath(filename)
    except PermissionError:
        # File is open in Excel — use a timestamped fallback filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base, ext = os.path.splitext(filename)
        fallback = f"{base}_{timestamp}{ext}"
        print(f"WARNING: '{filename}' is open in Excel. Saving to '{fallback}' instead.")
        _write(fallback)
        return os.path.abspath(fallback)

