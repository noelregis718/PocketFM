import pandas as pd
import shutil

shutil.copy2('vanshika_part2.xlsx', 'vanshika_part2_temp.xlsx')
try:
    df = pd.read_excel('vanshika_part2_temp.xlsx')
    print('\n--- Checking row indices 737 to 740 ---')
    for i, row in df.iloc[737:741].iterrows():
        title = row.get('Book Title', row.get('Book Name', ''))
        url = row.get('GoodReads_Series_URL', '')
        books = row.get('Num_Primary_Books_in_Series', '')
        pages = row.get('Total_Page_Count_of_Primary_Books', '')
        genres = str(row.get('Genre Tags', ''))[:50]
        synopsis = str(row.get('Synopsis', ''))[:50]
        print(f"\nIndex {i}:")
        print(f"Title: {title}")
        print(f"URL: {url}")
        print(f"Books: {books}")
        print(f"Pages: {pages}")
        print(f"Genres: {genres}...")
        print(f"Synopsis: {synopsis}...")
except Exception as e:
    print("Error:", e)
