import sys

with open('backend/goodreads_scraper.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    if 'try:' in line and 'print(f"    [Goodreads] Searching: {query}...")' in lines[i+1]:
        # Inject our logic here
        indent = line[:line.find('try:')]
        new_lines.append(indent + "import asyncio\n")
        new_lines.append(indent + "book_url = await asyncio.to_thread(get_autocomplete_book_url, query)\n")
        new_lines.append(indent + "if book_url:\n")
        new_lines.append(indent + "    print(f\"    [Goodreads] Autocomplete API Success! URL: {book_url}\")\n")
        new_lines.append(indent + "if not book_url:\n")
        new_lines.append(line) # append the try:
    else:
        new_lines.append(line)

with open('backend/goodreads_scraper.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print("Patch successful!")
