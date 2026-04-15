from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import asyncio
from scraper import AmazonScraper
from excel_utility import save_to_excel
import os
from playwright.async_api import async_playwright

app = Flask(__name__)
CORS(app)

scraper = AmazonScraper(headless=False)

async def run_scrape_process(url, limit):
    # Part 1: List extraction
    book_list = await scraper.scrape_bestseller_list(url, limit)

    if not book_list:
        return []

    # Extract base domain from the input URL (e.g. https://www.amazon.in)
    from urllib.parse import urlparse
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    print(f"Base URL for product pages: {base_url}")

    # Part 2: Deep extraction (CONCURRENT TABS)
    print(f"Part 2: Visiting {len(book_list)} pages in a single browser with 5 tabs...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        sem = asyncio.Semaphore(5)

        async def limited_tab_scrape(book):
            async with sem:
                try:
                    title = book.get('Book Title', 'N/A')
                    print(f"Opening tab for: {title.encode('ascii', 'ignore').decode('ascii')}")
                    details = await scraper.scrape_product_details_tab(
                        context, book.get('Amazon URL', ''), base_url=base_url
                    )

                    # Prefer product-page author (more reliable), fall back to list-page author
                    list_author = book.get('Author Name', 'N/A')
                    detail_author = details.get('Author Name', 'N/A')

                    merged = {**book, **details}

                    if detail_author and detail_author != 'N/A':
                        merged['Author Name'] = detail_author
                    elif list_author and list_author != 'N/A':
                        merged['Author Name'] = list_author
                    else:
                        merged['Author Name'] = 'N/A'

                    return merged
                except Exception as e:
                    title = book.get('Book Title', 'N/A')
                    print(f"Tab error for {title.encode('ascii', 'ignore').decode('ascii')}: {e}")
                    return {**book, "Description": "Error", "Publisher": "Error", "Publication Date": "Error"}
        
        tasks = [limited_tab_scrape(book) for book in book_list]
        final_results = await asyncio.gather(*tasks)
        await browser.close()
        return final_results

@app.route('/api/scrape-bestsellers', methods=['POST'])
def scrape():
    data = request.json
    url = data.get('url')
    limit = data.get('limit', 50)

    if not url:
        return jsonify({"error": "URL is required"}), 400

    print(f"Starting scrape for category: {url}")
    
    try:
        # Use asyncio.run for a clean isolated loop
        results = asyncio.run(run_scrape_process(url, limit))
        
        # Save to Excel
        excel_path = save_to_excel(results, "scraped_data.xlsx")
        
        # Automatically open the file on the user's machine (Windows specific)
        if os.name == 'nt': # Only try on Windows
            try:
                os.startfile(excel_path)
                print(f"Opening Excel file: {excel_path}")
            except Exception as se:
                print(f"Could not open file automatically: {se}")
        
        return jsonify({
            "results": results,
            "excel_path": excel_path
        })
    except Exception as e:
        import traceback
        print("Scrape failed with traceback:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/download', methods=['GET'])
def download():
    path = "scraped_data.xlsx"
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    return jsonify({"error": "File not found"}), 404

if __name__ == '__main__':
    app.run(port=5000, debug=True)
