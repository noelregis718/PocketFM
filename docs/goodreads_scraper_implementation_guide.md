# PocketFM Goodreads Scraper - Implementation & Execution Guide

This document outlines the architecture and execution instructions for the high-concurrency Goodreads Scraper. Share this guide (and the `backend` folder) with your colleagues so they can run the scraper locally.

## 1. Core Architecture
The script `mega_goodreads_scraper.py` uses a **hybrid scraping architecture** to bypass strict anti-bot protections:
- **urllib WAF Bypass**: We first hit the Goodreads autocomplete API using raw HTTP requests (spoofing headers) to securely acquire the exact Book URL without triggering the AWS WAF firewall.
- **Playwright React Renderer**: Once we have the URL, we launch a Chromium browser via Playwright to fully render the page. This allows us to execute JavaScript, expand hidden genre tags (by clicking "...more"), and scrape dynamic React components.

## 2. 8-Tab Concurrency System
The script is heavily optimized for speed using an asynchronous `asyncio.Semaphore(8)` lock:
- It launches exactly 8 Playwright browser tabs simultaneously in the background.
- It dynamically manages memory by tearing down browser tabs via `await page.close()` the moment a row finishes processing.
- **Warning**: Running this will cause a temporary spike in CPU and RAM usage due to 8 headless Chromium instances rendering React simultaneously.

## 3. The Logic Pipeline (Romantasy Weightage)
For each book, the scraper performs the following:
1. **Extraction**: Pulls the Genre Tags, Average Rating, Total Ratings, and Synopsis.
2. **Classification (Step 1)**: Evaluates the top 5 and top 10 genres to classify the book as `A - Strong Match`, `B - Confirmed Match`, `C - Weak Match`, or `DF-0 - Fail (No Credible Match)`.
3. **Evidence Gate (Step 4)**: If the book has a rating `< 3.60` and total ratings `< 100`, it immediately overwrites the classification to `DF-3 - Low Evidence & Rating`.
4. **Primary Books Extraction**: Navigates to the Series Page (if it exists) and counts ONLY strict integer primary books (filtering out novellas like `.5` and bundles like `1-3`). It explicitly extracts the true Book 1 rating and review count.

## 4. Setup & Execution Instructions

### Prerequisites
Ensure you have the required Python libraries installed:
```powershell
pip install pandas openpyxl playwright asyncio
playwright install chromium
```

### File Structure
Ensure the following files are in your directory:
- `backend/mega_goodreads_scraper.py` (The main concurrent scraper)
- `backend/format_excel.py` (The auto-styler for Excel)
- `noel_part1.xlsx` (The target dataset)

### Running the Scraper
Simply execute the scraper from the terminal:
```powershell
python backend/mega_goodreads_scraper.py
```
*Note: The script automatically runs `format_excel.py` upon completion to lock row heights to 20, enable text wrapping, and apply thin borders to the final dataset.*
