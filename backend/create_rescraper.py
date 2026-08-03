with open('mega_goodreads_scraper.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Modify the fast-forward check to look for Num_Primary_Books_in_Series == 0
old_batch_check = '''        # Fast-forward check: skip launching browser if the whole batch is already done
        needs_processing = False
        for i in range(batch_start, batch_end):
            existing_val = str(df.iloc[i].get("GoodReads_Series_URL", "")).strip()
            if not (existing_val and existing_val.lower() != 'nan' and existing_val != 'none'):
                needs_processing = True
                break'''

new_batch_check = '''        # Rescrape Zeros Check
        needs_processing = False
        for i in range(batch_start, batch_end):
            num = df.iloc[i].get("Num_Primary_Books_in_Series")
            if str(num) == '0' or str(num) == '0.0':
                needs_processing = True
                break'''

content = content.replace(old_batch_check, new_batch_check)

old_row_skip = '''        existing_val = str(row.get("GoodReads_Series_URL", "")).strip()
        if existing_val and existing_val.lower() != 'nan' and existing_val != 'none':
            print(f"[{index}] Goodreads URL exists. Skipping.")
            return'''

new_row_skip = '''        num = row.get("Num_Primary_Books_in_Series")
        if str(num) != '0' and str(num) != '0.0':
            return'''

content = content.replace(old_row_skip, new_row_skip)

content = content.replace('TARGET_ROWS = 210', 'TARGET_ROWS = 2500')

with open('rescrape_zeros.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('rescrape_zeros.py created successfully!')
