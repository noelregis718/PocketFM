import pandas as pd

df = pd.read_excel(r'E:\Internship\PocketFM\Merged_Romance_Keywords.xlsx', nrows=15)

for idx, row in df.head(10).iterrows():
    print(f'--- Excel Row {idx+2} ---')
    print(f"Title: {row.get('Book Title', '')} / {row.get('Series Name', '')}")
    print(f"Link: {row.get('Goodreads Link')}")
    print(f"Publisher: {row.get('Publisher')}")
    print(f"Pub Date: {row.get('Publication Date')}")
    print(f"Genres: {row.get('Genre Tags')}")
    print(f"Primary Books: {row.get('Goodreads Primary Books Number')} | Pages: {row.get('Goodreads Primary Books Page Count')}")
    
    synopsis = str(row.get('Synopsis'))
    if synopsis != 'nan' and synopsis:
        print(f"Synopsis Length: {len(synopsis)} chars")
    else:
        print("Synopsis: None")
        
    print('-------------------')
