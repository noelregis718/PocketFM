# Mega Goodreads Scraper: Implementation & Architecture Plan

This document outlines the architecture, logic, and setup requirements for the `mega_goodreads_scraper.py` script. It is designed to be shared with team members so they can run and understand the script on any device.

## Overview
The Mega Goodreads Scraper is a hybrid Playwright/HTTP scraping tool that processes books from an Excel file. It bypasses Web Application Firewalls (WAF) to find a book's Goodreads URL, aggressively extracts series data (including individual primary book page counts), and calculates a **Romantasy Checker** match score based on extracted genres and internal keywords.

---

## 1. Environment & Setup Requirements

For someone to run this scraper on a new device, they must have the following installed:

### Prerequisites
- **Python 3.9+**
- **Pandas**: `pip install pandas openpyxl`
- **Playwright**: `pip install playwright`
- **Playwright Browsers**: After installing Playwright, run `playwright install chromium`

### Required Input Files
- The script targets an Excel file (currently set to `vanshika_part1.xlsx`).
- The Excel file must contain at minimum the following columns:
  - `Book Title` (or `Book Name`, `Book 1 Title`)
  - `Author Name`
  - `Genre`
  - `Keyword`

---

## 2. Core Scraping Architecture

### A. WAF Bypass (HTTP Autocomplete)
To avoid triggering Goodreads' anti-bot protections (CAPTCHAs/403 Forbidden), the script first queries the hidden Goodreads Autocomplete API using raw `urllib` HTTP requests. 
- **Fallback**: If the autocomplete API fails, it spins up a Playwright browser to aggressively search Goodreads UI.

### B. Aggressive Page Count Extraction
Goodreads series pages are unreliable for page counts. The scraper now visits **every individual primary book page** in a series to extract the exact page count.

**Extraction Hierarchy (4-Tier Fallback System):**
1. **JSON-LD**: Looks for hidden `application/ld+json` script tags in the DOM and parses the `numberOfPages` attribute.
2. **Data-TestID**: Looks for the visible `[data-testid="pagesFormat"]` element.
3. **Button Click**: Clicks the "Book details & editions" expand button to force the DOM to render the page count, then tries the data-testid again.
4. **Regex Fallback**: Scans the entire raw HTML string for the pattern `(\d+) pages`.

**Retry Mechanism:** 
> [!IMPORTANT]
> If a book page returns `0 pages` after all 4 extraction methods, the script automatically reloads the page, waits 2 seconds, and retries the entire 4-tier extraction process again. (Note: Audiobooks will correctly return 0).

---

## 3. The Romantasy Subgenre Categorization Engine (V4)

The scraper features an embedded rules engine that evaluates if a book is a true "Romantasy" match, filters out junk/spam, and assigns it to one of 12 highly specific subgenres.

### A. Pre-Flight Checks
1. **The Negative Dealbreaker:** Scans the text for toxic genres (e.g., `"science fiction"`, `"childrens"`, `"middle grade"`). If found, the book is flagged as **`Fail (Dealbreaker)`** and skipped.
2. **The Spam Filter:** Checks `Book1_Num_Ratings`. If a book scores a Strong Match but has fewer than 10 reviews, it is flagged as **`Strong Match (Low Confidence)`** to warn against amateur keyword-stuffing.
3. **The Golden Override:** Checks `Author Name` against a hardcoded list of Romantasy Titans (e.g., Sarah J. Maas, Rebecca Yarros). If matched, it bypasses Dealbreakers and guarantees a **`Strong Match`**.

### B. NLP Weighted Multipliers
The script uses NLP (Natural Language Processing) suffix-expansion (e.g., capturing "wolves" from "wolf") to search for 278 keywords across 7 columns. Points are awarded based on location:
*   **x5 Points (Critical):** `Keyword`, `Genre` (The original Amazon intent)
*   **x3 Points (Very High):** `Book Title`, `Genre Tags` (Explicit categorization)
*   **x2 Points (High):** `Series Name`, `Logline` (Marketing hooks)
*   **x1 Point (Normal):** `Synopsis` (General context)

### C. Smart Tie-Breaking & Output
*   The script totals the points across all 12 Subgenres.
*   If two subgenres tie, it parses the `Book Title` to break the tie.
*   Outputs directly to the sheet: `Strong Match (Subgenre)` or `Weak Match (Subgenre)`.

## 4. Execution Flow

To execute the scraper, modify the following global variables at the top of `mega_goodreads_scraper.py` as needed:
```python
EXCEL_FILE = r"path\to\your\excel_file.xlsx"
START_ROW = 0
TARGET_ROWS = 10 # Number of rows to process
CONCURRENCY = 5  # Number of browser tabs to run simultaneously
BATCH_SIZE = 50
```

Then, run the script via terminal:
```bash
python mega_goodreads_scraper.py
```

The scraper will securely save the updated data directly back into the Excel file after every batch completes.
