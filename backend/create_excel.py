import pandas as pd

columns = [
    "Series Name",
    "Author Name",
    "GR Links",
    "Publisher",
    "Goodreads Series URL",
    "Book 1 Ratings (stars)",
    "No. of Goodreads Ratings",
    "No. of Primary Books",
    "Book 1 page count",
    "Total Page Count (of primary books only)",
    "Genre",
    "Genre Tags",
    "Sub Genre"
]

df = pd.DataFrame(columns=columns)
df.to_excel(r"e:\Internship\PocketFM\Scraping_Sheet.xlsx", index=False)
print("Excel sheet 'Scraping_Sheet.xlsx' created successfully.")
