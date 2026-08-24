import sys

with open('backend/goodreads_scraper.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
in_search = False

for i, line in enumerate(lines):
    if 'if not book_url:' in line and 'try:' in lines[i+1] and 'print(f"    [Goodreads] Searching:' in lines[i+2]:
        # Remove the if not book_url: line
        continue
    
    if 'try:' in line and 'print(f"    [Goodreads] Searching:' in lines[i+1]:
        in_search = True
        new_lines.append(line) # keep try:
        new_lines.append(line.replace('try:', '    if not book_url:')) # add if not book_url: INSIDE try
        continue
        
    if in_search:
        if 'except Exception as e:' in line and 'Search error' in lines[i+1]:
            in_search = False
            new_lines.append(line)
            continue
            
        if line.strip() != '':
            new_lines.append('    ' + line)
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)

with open('backend/goodreads_scraper.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
