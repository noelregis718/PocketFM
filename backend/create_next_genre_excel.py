import pandas as pd

def create_excel(filename):
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
    df.to_excel(filename, index=False)
    print(f"Created empty excel sheet: {filename}")

if __name__ == "__main__":
    create_excel("../Scraping_Sheet_Genre_5.xlsx")
