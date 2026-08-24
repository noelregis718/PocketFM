import sys

with open('backend/goodreads_scraper.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
in_search_block = False

for i, line in enumerate(lines):
    if 'search_url = f"https://www.goodreads.com/search' in line:
        in_search_block = True
        
    if in_search_block:
        if 'except Exception as e:' in line and i+1 < len(lines) and 'Search error' in lines[i+1]:
            # end of search block
            in_search_block = False
            new_lines.append('    ' + line)
            new_lines.append('    ' + lines[i+1])
            # skip the next line since we just appended it
            continue
        if 'Search error' in line and 'except Exception as e:' in lines[i-1]:
            continue
            
        new_lines.append('    ' + line)
    else:
        new_lines.append(line)

with open('backend/goodreads_scraper.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
