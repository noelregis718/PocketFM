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

## The Categorization Engine (Step-by-Step Logic)

The script processes every single book through a rigorous 6-step engine to ensure the final subgenre assignment is highly accurate and immune to false positives.

### Step 1: Text Normalization
The engine extracts data from the 5 relevant text columns (`Book Title`, `Genre Tags`, `Series Name`, `Logline`, `Synopsis`) and the `Author Name` column. 
* All text is converted to lowercase.
* All special characters and punctuation are stripped (replaced with spaces) to ensure clean matching.

### Step 2: The Negative Dealbreaker Screen
Before any points are calculated, the engine combines all text into a single block and searches it against the **Negative Dealbreakers** list. 
* If a toxic keyword (e.g., "science fiction" or "billionaire romance") is found, the book is instantly flagged.
* **The Exception:** If the book's author matches the **Gold Standard Authors** list, the Dealbreaker screen is completely bypassed. 
* Books that fail the screen without a Gold Author override are immediately marked as `Fail (Dealbreaker)` and skipped.

### Step 3: Column-Weighted Point Calculation
The engine iterates through the 12 subgenres. For each subgenre, it scans the text columns for keywords using an **NLP Regex Suffix Expansion** (which automatically catches plurals and past-tense words like `-s`, `-es`, `-ed`, `-ing`, `-ly`, `-al`). 

Points are awarded dynamically based on *where* the keyword was found:
*   **x3 Multiplier (Critical Columns):** `Book Title` and `Genre Tags`.
    *   *Confirmed* keyword = 9 points (3 pts x 3). 
    *   *Master* keyword = 3 points (1 pt x 3).
*   **x2 Multiplier (High Columns):** `Series Name` and `Logline`.
    *   *Confirmed* keyword = 6 points (3 pts x 2). 
    *   *Master* keyword = 2 points (1 pt x 2).
*   **x1 Multiplier (Normal Column):** `Synopsis`.
    *   *Confirmed* keyword = 3 points (3 pts x 1). 
    *   *Master* keyword = 1 point (1 pt x 1).

### Step 4: Sorting & Tie-Breaking
Once all 12 subgenres have been evaluated, the engine drops any subgenre that scored 0 points. It then sorts the remaining subgenres to determine the winner using a strict tie-breaking hierarchy:
1.  **Total Points:** Highest score wins.
2.  **Confirmed Keyword Count:** If points are tied, the subgenre with the most *Confirmed* keywords wins.
3.  **Master Keyword Count:** If still tied, the subgenre with the most *Master* keywords wins.

### Step 5: Final Verdict & Thresholds
The winning subgenre is evaluated against the confidence threshold (3 points):
*   **Strong Match:** The book scored `>= 3` points (e.g., just one Confirmed keyword in the synopsis, or one Master keyword in the Title).
*   **Weak Match:** The book scored exactly `1` or `2` points (e.g., a Master keyword found in the synopsis).

### Step 6: The Gold Author Rescue
If a book scores exactly `0` points across all 12 subgenres, it is normally marked as a generic `Fail`. 
* However, if the `Author Name` is on the **Gold Standard Authors** list, the engine rescues the book and forces an override. 
* Since it's guaranteed to be a Romantasy book, it defaults the output to `Strong Match (High Fantasy Court Adventure)` to ensure it is not discarded during publisher filtering.

---

## Output Format
The script overwrites the `Romantasy Checker` column in the master Excel sheet directly, outputting strings like:
*   `Strong Match (Gothic Dark Romantasy)`
*   `Weak Match (Mythology, Legend & Fairy Tale Retelling)`
*   `Fail (Dealbreaker)`
*   `Fail`

---

## Implementation Plan: The Keyword Dictionaries (Version 3)

The engine relies on three core dictionaries: **Negative Dealbreakers**, **Gold Standard Authors**, and the **Subgenre Keywords** (split into Confirmed and Master lists).

### 1. Negative Dealbreakers
If any of these terms are found, the book is instantly marked as a `Fail (Dealbreaker)`:
*   "science fiction", "sci-fi", "childrens", "middle grade", "historical non-fiction", "biography", "office romance", "billionaire romance"

### 2. Gold Standard Authors
If any of these authors are detected, the book bypasses all dealbreakers and is guaranteed a `Strong Match`:
*   Sarah J. Maas, Rebecca Yarros, Jennifer L. Armentrout, Laura Thalassa, Raven Kennedy, Carissa Broadbent, Scarlett St. Clair, Lexi Ryan, Elise Kova, Ali Hazelwood, Kresley Cole, S.J. Maas, Jennifer Armentrout

### 3. The 12 Subgenre Keyword Lists

#### 1. High Fantasy Court Adventure
*   **Confirmed (+3 points):** fae court romance, royal fantasy romance, fantasy court romance, kingdom romance, royal heir romance, noble fantasy romance, unseelie court, seelie court
*   **Master (+1 point):** royal court fantasy, court intrigue, political fantasy, kingdom politics, noble houses, royal succession, crown politics, throne war, castle court, imperial court, court mage, royal heir, kingdom alliance, noble romance, palace fantasy, throne of, crown of, king and queen, royal romance, fae king, prince romance, princess romance, royal intrigue, court secrets, fae prince

#### 2. Gothic Dark Romantasy
*   **Confirmed (+3 points):** gothic romance, dark fantasy romance, cursed castle romance, gothic fantasy romance, gothic thriller
*   **Master (+1 point):** gothic romance, haunted castle, haunted manor, dark curse, cursed castle, victorian fantasy, grim romance, gothic horror fantasy, dark cathedral, shadow manor, tragic love fantasy, macabre romance, gothic manor, dark estate, gothic supernatural, death romance, forbidden desire, undead romance, haunted house romance, dark lord, forbidden love, dark obsession, monstrous lover, dark magic, shadow magic, blood magic, grimdark romance, villain romance

#### 3. Dark Academia Romantasy
*   **Confirmed (+3 points):** dark academia romance, occult academy romance, forbidden magic academy, secret society romance, cursed university romance
*   **Master (+1 point):** dark academia, secret society, forbidden knowledge, occult studies, elite academy, cursed library, forbidden ritual, ancient grimoire, magic university, arcane college, sinister campus, forbidden spellwork, hidden magic society, dangerous knowledge, magical conspiracy, ink magic, tarot academy, clandestine order, ivy league magic, magical underground, magical school, dark school, ancient magic, secret magic, deadly academy, magic scholars, scholar romance

#### 4. Monster Romance (Non-Shifter)
*   **Confirmed (+3 points):** monster romance, demon romance, monster boyfriend, alien monster romance, non-human romance, orc romance, gargoyle romance, naga romance
*   **Master (+1 point):** monster romance, demon romance, monster boyfriend, vampire romance, fae romance, mer romance, kraken romance, minotaur romance, alien romance, creature romance, non-human love interest, monstrous partner, immortal captor, beast romance, alien mate, spider romance, monster lover, creature feature romance, human and monster, symbiote romance

#### 5. Werewolf / Shifter Romance
*   **Confirmed (+3 points):** werewolf romance, shifter romance, fated mates, wolf shifter romance, rejected mate romance, shifter mate
*   **Master (+1 point):** werewolf romance, shifter romance, fated mates, rejected mate, alpha romance, lycan romance, wolf pack romance, bear shifter romance, dragon shifter romance, omegaverse romance, pack romance, mate bond, true mate, shifter pack, animal shifter, werebear, werelion, werepanther, werecat, alpha male, luna, pack dynamics, wolf pack

#### 6. High-Stakes Games & Deadly Trials
*   **Confirmed (+3 points):** fantasy tournament romance, progression fantasy romance, dungeon trials romance, deadly trials romance, death game romance, magic tournament, deadly games
*   **Master (+1 point):** deadly trials, magical tournament, battle tournament, survival trials, death competition, trial by combat, fantasy arena, magic competition, elimination tournament, dungeon trials, labyrinth challenge, champion tournament, fantasy contest, survival arena, final trial, death game, magical contest, forced competition, win or die, chosen champion, trial of, survival romance, blood tournament, combat trials, gladiator romance

#### 7. Mythology, Legend & Fairy Tale Retelling
*   **Confirmed (+3 points):** beauty and the beast retelling, hades and persephone, greek mythology romance, fairy tale retelling romance, myth retelling romance
*   **Master (+1 point):** greek myth retelling, norse myth retelling, celtic legend, arthurian legend, fairy tale retelling, hades and persephone, cinderella retelling, sleeping beauty retelling, red riding hood retelling, snow white retelling, rapunzel retelling, medusa retelling, persephone retelling, mythic retelling, chinese mythology romance, folklore retelling, legend reimagined, fairy tale romance, myth romance, goddess romance, greek myth, roman myth, norse myth, egyptian myth, celtic myth, arthurian romance, camelot romance, robin hood retelling, peter pan retelling, alice in wonderland retelling, beauty and the beast, rumplestiltskin retelling

#### 8. War College / Military Academy
*   **Confirmed (+3 points):** magic academy romance, military fantasy romance, war college romance, dragon rider academy romance, combat academy romance
*   **Master (+1 point):** war college, military academy, officer academy, cadet academy, battle school, combat academy, warrior academy, military fantasy, officer training, academy cadets, battle training, tactical academy, war strategy, military campaign, combat training, rider academy, dragon rider, flight academy, wingleader, cadet romance, dragon school, battle academy, warrior school, magic soldier, warrior romance

#### 9. Korean Romance Fantasy / Isekai
*   **Confirmed (+3 points):** villainess romance, isekai romance, litrpg romance, transmigration romance, reincarnation fantasy romance
*   **Master (+1 point):** otome isekai, villainess, reincarnated heroine, transmigration, regression fantasy, reborn noblewoman, korean web novel, manhwa romance, duke of the north, tyrant romance, second chance fantasy, possession fantasy, system fantasy romance, korean fantasy novel, isekai romance, portal fantasy romance, game world romance, reincarnated into novel, manhwa adaptation, web novel romance, transmigrated, otome game, webtoon romance, manhwa, regression, time travel romance, reborn as

#### 10. Paranormal Romance
*   **Confirmed (+3 points):** vampire romance, witch romance, angel romance, ghost romance, psychic romance, warlock romance
*   **Master (+1 point):** vampire romance, witch romance, demon romance, angel romance, ghost romance, psychic romance, necromancer romance, fallen angel romance, supernatural romance, immortal romance, soul bond, witch coven romance, vampire mate, dark angel, haunting romance, vampire hunter, witch coven, demon hunter, angel and demon, nephilim, shifter and witch

#### 11. Cozy / Cottagecore Romantasy
*   **Confirmed (+3 points):** cozy fantasy romance, cottagecore romance, cozy witch romance, magical small town romance, cozy romantasy, cozy magic, slice of life fantasy, witchy cozy
*   **Master (+1 point):** cozy fantasy, cottagecore, small town magic, cozy witch, magical bakery, enchanted garden, herbalist romance, cottage magic, whimsical romance, gentle magic, warm magic, magical community, village witch, low-stakes magic, healing magic, nature magic romance, magical shop, bookshop magic, woodland cottage, cozy magical world, low stakes fantasy, magical baking, magical tea shop, cozy mystery with magic

#### 12. Urban / Contemporary Fantasy Romance
*   **Confirmed (+3 points):** urban fantasy romance, hidden magic romance, urban coven romance, secret magic world, contemporary fantasy romance
*   **Master (+1 point):** urban fantasy, contemporary fantasy, modern magic, hidden magic society, magical city, supernatural detective, paranormal investigation, secret magic world, modern sorcerer, magical crime, city witch, urban coven, magical apartment, urban spellcaster, metropolitan fantasy, hidden world, magic underground, supernatural organisation, modern witch, city fae, magic in the city, masquerade, supernatural city, modern day magic, detective witch, paranormal PI
