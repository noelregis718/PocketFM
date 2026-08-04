# Romantasy Subgenre Checker: Architecture & Logic Reference

This document outlines the architecture of the **Romantasy Subgenre Checker**, detailing the progression of its categorization engine up through **Version 3** (the current active, fully accepted, and executed logic).

## Core Architecture
The script processes a dataset of 9,560 scraped books. It categorizes each book into one of 12 highly specific Romantasy subgenres by scanning 5 columns of text and calculating a weighted point score.

### The 12 Subgenres:
1. High Fantasy Court Adventure
2. Gothic Dark Romantasy
3. Dark Academia Romantasy
4. Monster Romance (Non-Shifter)
5. Werewolf / Shifter Romance
6. High-Stakes Games & Deadly Trials
7. Mythology, Legend & Fairy Tale Retelling
8. War College / Military Academy
9. Korean Romance Fantasy / Isekai
10. Paranormal Romance
11. Cozy / Cottagecore Romantasy
12. Urban / Contemporary Fantasy Romance

---

## Logic Progression

### Version 1: The Foundation
*   **The Keyword Dictionaries:** Every subgenre has a dictionary of *Confirmed Matches* (worth 3 points) and *Master Keywords* (worth 1 point).
*   **The Scan:** The script combines the Book Title, Genre Tags, Series Name, Logline, and Synopsis into a single block of text and searches for exact keyword matches.
*   **Scoring:** It totals the points. If the book scores `>= 3` points, it is marked as a **Strong Match**. If it scores `1` or `2` points, it is a **Weak Match**.

### Version 2: Multipliers & Spam Filters
V2 radically changed the point calculation by separating the columns and applying mathematical multipliers based on the column's marketing importance.

*   **Column Weighting System:**
    *   **x3 Multiplier (Critical):** `Book Title`, `Genre Tags`
    *   **x2 Multiplier (High):** `Series Name`, `Logline`
    *   **x1 Multiplier (Normal):** `Synopsis`
*   **The Negative Dealbreaker:** A list of toxic genres (e.g., `"science fiction"`, `"childrens"`, `"middle grade"`) was added. If any of these are found anywhere in the text, the book is instantly marked as **`Fail (Dealbreaker)`** and skipped.

### Version 3: NLP Expansion & Golden Overrides (Current State)
V3 focused on rescuing false-negatives (books that failed due to slight spelling differences or vague synopses).

*   **NLP Suffix Expansion:** The script uses a native Regex NLP expansion when searching for keywords. It automatically catches plural and past-tense suffixes (`-s`, `-es`, `-ed`, `-ing`, `-ly`, `-al`). For example, a search for `"curse"` perfectly matches `"curses"` and `"cursed"`.
*   **The Gold Standard Author Override:** The script cross-references the `Author Name` column against a list of 13 legendary Romantasy authors (e.g., *Sarah J. Maas, Rebecca Yarros, Ali Hazelwood*). 
    *   If matched, the book completely bypasses the Negative Dealbreakers.
    *   It is mathematically guaranteed to output as a **`Strong Match`**, overriding any bad/vague synopsis data.

---

## Output Format
The script overwrites the `Romantasy Checker` column in the master Excel sheet directly, outputting strings like:
*   `Strong Match (Gothic Dark Romantasy)`
*   `Weak Match (Mythology, Legend & Fairy Tale Retelling)`
*   `Fail (Dealbreaker)`
*   `Fail`
