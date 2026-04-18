import os

path = os.path.join('backend', 'scraper.py')
with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Indent logic for _execute_discovery
# We know it starts around line 84
# We fixed lines 86-143 in a previous turn (already correct)
# We need to fix lines from 'items = await page.query_selector_all' onwards.

fixed_lines = []
for i, line in enumerate(lines):
    # Find the discovery scan line that was missed
    if "items = await page.query_selector_all('[data-asin]')" in line and i > 140 and i < 200:
        # Check if it has 24 spaces (the broken indentation)
        if line.startswith(' ' * 24):
            # This is the line we need to start out-denting
            pass
    
    # Actually, a simpler way is to just look for lines after 143 that start with 24 spaces
    if i >= 144 and i <= 260:
        if line.startswith(' ' * 24):
            line = line[4:] # Remove 4 spaces
        elif line.startswith(' ' * 28):
            line = line[4:]
        elif line.startswith(' ' * 20):
            # If it's already less than 24, it might have been partially fixed or is a lower level
            pass
            
    fixed_lines.append(line)

with open(path, 'w', encoding='utf-8') as f:
    f.writelines(fixed_lines)

print("Scraper indentation fixed.")
