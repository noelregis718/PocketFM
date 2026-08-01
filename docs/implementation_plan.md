# Book Series Metadata Scraper & Dataset Creation Plan

This plan outlines the design and implementation steps for building a comprehensive database of all book series represented by **Biagi Rights** and published under **Penzler Publishers** (and its imprints). 

---

## Proposed Strategy

We will follow a multi-stage approach to discover candidates, fetch Goodreads metadata, deduplicate records, and verify information.

### Stage 1: Candidate Discovery & Extraction
We will crawl the target websites and catalogs to construct a master list of candidate books, authors, and series:
1. **Penzler Publishers Category Scraping**: Crawl WooCommerce category pages for:
   - *The Mysterious Press* (`/product-category/mysterious-press/`)
   - *American Mystery Classics* (`/product-category/american-mystery-classics/`)
   - *Scarlet* (`/product-category/scarlet/`)
   - *Crime Ink* (`/product-category/crime-ink/`)
   Since the list pages do not display author names, we will perform a quick request to each product page to extract the **Author Name**, **Book Title**, and **Imprint**.
2. **MysteriousPress.com Author Directory**: Crawl all 12 pages of authors at `https://mysteriouspress.com/authors/` to extract a list of all ~240 authors associated with the imprint.
3. **Biagi Rights Catalog PDF Scanning**: Extract text from the 18 downloaded PDF catalogs (including `2026 Client Deck Draft final.pdf`, `PDFs_pegasus_spring25.pdf`, `PDFs_lee_low_spring25.pdf`, etc.) to find book titles and authors represented by the agency.
4. **Author/Series Listing Compile**: Create a unified deduplicated list of candidates (Author, Title, Publisher/Imprint).

### Stage 2: Goodreads Lookup & Series Identification
For each candidate book/author:
1. Search Goodreads using `https://www.goodreads.com/search?q={title}+{author}`.
2. Parse the search results page to find the matching book profile using fuzzy matching (`rapidfuzz`).
3. Visit the book page, extract the **Series URL** (if the book belongs to a series).
4. Cache search results and skip searching for authors or books where the series has already been resolved to minimize network requests.

### Stage 3: Goodreads Series Detail Scraping
For each unique Goodreads series URL discovered:
1. Navigate to the Goodreads Series page.
2. Extract:
   - **Series Name**
   - **No. of Primary Books in Series** (matching the "primary works" count on the series page).
3. Navigate to the page for **Book 1** in the series (the first primary entry) to extract:
   - **Goodreads Rating - Book 1**
   - **No. of Ratings Book 1** (total user ratings)
   - **No. of Pages in Book 1** (page count of Book 1)
4. Populate:
   - **Genre** & **Sub-Genre** (inferred from publisher catalogs/tags or Goodreads popular shelf tags).
   - **Author**
   - **Imprint / Publisher - Book** (verifying the specific publisher/imprint from our discovery cache).

### Stage 4: Deduplication & Sheet Compilation
1. Cross-reference results and remove duplicate series (e.g., if a series is represented by Biagi Rights and also published under Penzler).
2. Clean up blank fields where data is unavailable (following the rule to leave fields blank rather than guess).
3. Generate the final output file as a CSV (`final_series_dataset.csv`) formatted for import into Excel/Google Sheets, containing exactly the 10 requested columns.

---

## User Review Required

> [!IMPORTANT]
> **Aesthetic and Quality Focus**
> Since this is a data extraction project rather than a client-facing web application, we will produce a highly detailed diagnostic progress log and save the final dataset as a standard CSV/Excel sheet. If a web dashboard or visual interface is required to explore this dataset, please specify, and we will build a premium, responsive search interface.

> [!WARNING]
> **Goodreads Rate Limiting and WAF**
> Goodreads has strict rate limits and Web Application Firewalls (WAF) that can trigger security checks. We will utilize our established scraping engine architecture (single browser session, concurrency limit of 5, resource interception for images/media, and randomized delays between 2.0 and 4.0 seconds) to ensure reliable throughput without getting blocked.

---

## Open Questions

> [!IMPORTANT]
> 1. **Biagi Rights Scope**: Biagi Rights represents foreign/translation licensing for 24 client publishers (such as *Barrons*, *Kaplan*, *Lee & Low*, *Pegasus*, *She Writes Press*, *Ulysses Press*, etc.). Do we want to include **every book series** published by all 24 client publishers, or should we focus specifically on the literary/fiction/crime imprints (like *Pegasus Crime*, *She Writes Press*, *Lee & Low*, and *Penzler*)? 
>    *Note: Non-fiction publishers like Kaplan and Barrons primarily publish test prep/study guides, which rarely have "book series" in the traditional sense, but some may exist.*
>
> 2. **Imprint Attribution**: If a series is published under multiple imprints/publishers over its lifespan (e.g., originally by *Mysterious Press*, but reissued by *Open Road Media* or *Grove*), should we list the primary imprint associated with our discovery sources, or list both?

---

## Verification Plan

### Automated Verification
- Verify that every Goodreads Series URL is a valid, live URL.
- Validate that the first book ("Book 1") page count, rating, and number of ratings match Goodreads.
- Run validation checks to ensure no duplicate series exist.

### Manual Verification
- We will output a verification log listing any ambiguous matches or books where no series was found for your manual review.
