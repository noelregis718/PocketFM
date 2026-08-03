---
name: romantasy-weightage
version: final
description: >
  Goodreads Romantasy quality triage for content commissioning. Use whenever the user provides a book title, Goodreads URL, or a list of titles and wants a scored YES / Subjective Review / Reject verdict based on Goodreads evidence. Trigger on phrases like "romantasy weightage", "run the romantasy model", "score this romantasy title", "romantasy quality check", or any request to evaluate a romantasy title's reader reception. Works with single titles, batches, pasted Goodreads data, or Goodreads URLs. Produces a full evaluation report and populates an Excel output for batch runs. This skill scores romance centrality, fantasy relevance, and reader reception only — it does not reclassify subgenre. Supersedes all prior romantasy-weightage versions.
---

# Romantasy Weightage Model — Final

Answers three questions in sequence:
1. **Is this book genuinely romantasy?**
2. **Does it trigger any direct-fail condition?**
3. **Is its reader reception strong enough to proceed?**

**Core principle:** Romance is the leading eligibility and scoring signal. Fantasy establishes the category framework. Reader-review evidence determines whether execution is sufficiently healthy. The model filters out only clearly weak titles — it should pass books with credible central romance, genuine fantasy relevance, and no corroborated serious reader failure.

---

## Input contract

Accept any of:
- A Goodreads URL (e.g. `https://www.goodreads.com/book/show/...`)
- A title + author (fetch the Goodreads page)
- A batch in `.xlsx` / `.csv` with title and/or Goodreads URL columns

**Collect before evaluation:**
- Goodreads genre tags and reader shelves (with approximate rank positions)
- Official or Goodreads synopsis
- Average rating, total ratings, total written reviews
- Up to 5 usable popular reviews (star rating + written text)
- First 15 meaningful written reviews
- Star ratings and engagement (likes/comments) on popular reviews

If data is unavailable: mark `⏭️ No data`. Never invent genre tags, review content, star ratings, engagement numbers, or synopsis details.

**Review definitions:**
- **Meaningful review** — contains an actual opinion about romance, fantasy, characters, pacing, plot, world-building, or overall experience. Excludes empty reviews, textless ratings, GIF-only reactions, plot summaries without evaluation, promotional blurbs, duplicates.
- **Usable popular review** — a meaningful review with ≥ 4 combined likes, comments, or responses.
- **Standard route** — analyse up to 5 usable popular reviews.
- **Expanded route** — analyse first 15 meaningful reviews. Use when: fewer than 4 usable popular reviews exist but rating ≥ 3.60; standard sample is too limited; a direct-fail threshold requires broader confirmation.

---

## Decision sequence

Run all steps in order. Stop immediately when a direct fail is confirmed. Do not score a title that fails any gate.

```
STEP 1  Genre Check      → DF-0: fail if no credible romantasy genre match
STEP 2  Fantasy Gate     → DF-1: fail if fantasy is absent, decorative, or removable
STEP 3  Romance Gate     → DF-2: fail if romance is incidental or removable
STEP 4  Evidence Gate    → DF-3: fail if usable reviews < 4 AND avg rating < 3.60
STEP 5  Sentiment Gate   → DF-4: fail if ≥ 3 reviews are both low-star AND negative
STEP 6  Romance Promise  → DF-5: fail if ≥ 3 reviews say romance is absent/negligible
STEP 7  Score /100       → classify and produce full report output
```

---

## STEP 1 — Genre Check

### Qualifying romance tags (examples)
Romance · Romantasy · Fantasy Romance · Romantic Fantasy · Paranormal Romance · Urban Fantasy Romance · Monster Romance · Historical Fantasy Romance · Young Adult Romance · Dark Romance (when paired with genuine fantasy) · Slow Burn Romance (when paired with genuine fantasy)

### Qualifying fantasy tags (examples)
Fantasy · High Fantasy · Epic Fantasy · Urban Fantasy · Paranormal · Supernatural · Magic · Fae · Witches · Vampires · Dragons · Mythology · Fairy Tales · Monsters · Isekai · Reincarnation · Science Fantasy · Time Travel (when structurally speculative)

### Genre match classification

First, check if both a **Fantasy** and **Romance** element exist. If no fantasy/romance element is present, classify as **Fail**. Then, check the priority of the tags/keywords:

- **Strong Match:** Both fantasy and romance genre tags / internal keywords appear in the **Top 5** priority positions.
- **Confirmed Match:** Both fantasy and romance genre tags / internal keywords appear within the **next 4** positions (ranks 6 through 9).
- **Weak Match:** Both fantasy and romance genre tags / internal keywords appear in **rank 10 or beyond** ("Any other keyword").
- **Downgrade Rule:** Regardless of tag rank, if the number of books in the series (`Num_Primary_Books_in_Series`) is **less than 3**, automatically classify as a **Weak Match**.

### DF-0 — No Credible Romantasy Genre Match

**Fail if:** no meaningful romance-family tag found; no meaningful fantasy-family tag found; and synopsis + reviews fail to establish both elements. Also fail if one side is missing and secondary evidence does not credibly confirm it.

> Do not send a clearly non-romantasy title to Subjective Review merely because one isolated reader placed it on a Romance or Fantasy shelf.

---

## STEP 2 — Fantasy Gate

**Fantasy structural test — ask:** *Could the fantasy element be removed and the same story told as a normal contemporary or historical romance with only minor changes?*
- Yes → Fantasy is probably incidental.
- No → Fantasy is structurally meaningful.

Fantasy is meaningful when it shapes ≥ 2 of: setting/world · central conflict · character identity · character abilities or limitations · political/social order · main plot progression · relationship obstacles · resolution.

### DF-1 — No Meaningful Fantasy

**Fail if** fantasy or supernatural content is: absent · decorative · metaphorical · confined to one minor object or scene · unrelated to the main conflict · easily removable · too weak to place the book within any recognisable romantasy subgenre.

**Subgenre calibration — judge against subgenre promise, not epic-fantasy standards:**

| Subgenre | Appropriate fantasy requirement |
|---|---|
| High Fantasy Court Adventure | Secondary world, magical politics, courts, conflict |
| Paranormal Romance | Supernatural character, species, or hidden world |
| Monster Romance | Non-human romantic partner as the defining element |
| Urban Fantasy Romance | Magic or supernatural conflict in a modern setting |
| Cozy Fantasy Romance | Genuine but intentionally low-stakes magic |
| Isekai / Reincarnation Romance | Transmigration or reincarnation as the structural hook |

Ambiguous but potentially genuine fantasy → Subjective Review (SR-F), not auto-fail.

---

## STEP 3 — Romance Gate

**Romance structural test — ask:** *If the romantic relationship were removed, would the main plot, emotional journey, and major character decisions remain largely unchanged?*
- Yes → Romance is probably incidental.
- No → Romance is structurally important.

Romance is substantial when ≥ 2 of: relationship is one of the protagonist's main goals/conflicts · materially affects important decisions · influences the main plot's outcome · love interest is indispensable to the protagonist's journey · relationship develops throughout a meaningful portion of the story · romantic stakes create significant emotional consequences · removing the relationship would substantially alter the narrative.

**Marketing phrases alone are not sufficient:** "fated mates" · "forbidden attraction" · "dangerous prince" · "irresistible stranger" · "enemies with chemistry". The synopsis must demonstrate what the relationship actually changes.

### DF-2 — Romance Is Incidental or Removable

**Fail if:** romance is only a minor subplot · protagonist has no substantial romantic arc · relationship appears briefly without sustained development · love interest is narratively replaceable · removing the romance would not materially alter the story · romance exists mainly through marketing language · relationship has no meaningful emotional or plot consequence.

A quest, war, mystery, or political conflict may remain the external plot — but romance must still be central, sustained, and consequential.

---

## STEP 4 — Evidence Gate

### DF-3 — Low Evidence + Low Rating

**Fail if** both are true simultaneously:
1. Usable popular reviews < 4; AND
2. Average Goodreads rating < 3.60

**Exception:** if usable reviews < 4 but rating ≥ 3.60 → do not fail; switch to expanded route (first 15 meaningful reviews). Limited evidence lowers confidence — it does not independently prove poor quality.

---

## STEP 5 — Sentiment Gate

### DF-4 — Three Low-Star Negative Reviews

**Fail if** ≥ 3 analysed reviews are **both**: rated 1★ or 2★ AND clearly negative in written content. Both conditions must exist in the same review.

Do not count: a low-star review with neutral/unclear text · a negative review whose star rating is unavailable · a 3★ mixed review · a review criticising only one personal-preference issue.

Analyse ≥ 5 meaningful reviews before applying. Expand to first 15 when necessary.

---

## STEP 6 — Romance Promise Gate

### DF-5 — Repeated Failure of the Romantic Promise

**Fail if** ≥ 3 independent meaningful reviews state romance is: virtually absent · negligible · introduced too late · completely lacking chemistry · forced or fundamentally unconvincing · falsely marketed as central · missing relationship development · without emotional payoff · functionally non-existent.

**These alone do NOT trigger DF-5:** "I wanted more romance" · "the romance was slow" · "it was more fantasy than romance" · "the couple needed more page time" · "the slow burn was too slow for me."

---

## Logline

Write a logline for every title that survives DF-0. For Direct Fail at DF-0, write "N/A — title does not qualify as romantasy."

**Format:** one to two sentences, leading with the fantasy premise, followed by the romantic hook, ending with the central stakes or obstacle.

**Structure guide:**
```
[Protagonist description] in [fantasy world / supernatural situation],
who must [central conflict or goal] — while [romantic complication or tension].
```

**Tone:** match the book's register — dark and urgent for dark romance, playful for cozy fantasy, epic for high fantasy court adventure. Do not default to generic marketing language.

**Rules:**
- Draw only from confirmed synopsis and review evidence. Do not invent events.
- Do not use trope labels as substitutes for story specifics ("forbidden love" alone is not sufficient — explain what makes it forbidden in this world).
- The logline must give a creative team an immediate sense of the world, the protagonist, and the romantic stakes in one reading.
- For Direct Fail titles (DF-1 through DF-5): still write the logline based on available synopsis evidence, then note which gate failed.

**Examples of strong loglines:**
- *"An ancient Witch Hunter awakened after 150 years of sleep must protect the arrogant young vampire she'd rather leave to die — as the 2,000-year blood feud she was created to end resurfaces with new and deadlier players."*
- *"A wolf shifter princess bound by a blood-curse that kills at twenty-one is forced into a fated-mate bond with four guardians she cannot trust — and must choose between the kingdom's survival and the love she never expected."*
- *"A crossdressing girl sold to a vampire as his thrall in a boys' dormitory must hide her identity from the one supernatural being who grows closer to her every night."*

---

## Scoring components

Score only titles that survive all direct-fail gates.

| # | Component | Max pts |
|---|---|---|
| 1 | Romance Centrality & Strength | 30 |
| 2 | Negative Review Health | 30 |
| 3 | Fantasy Relevance & Strength | 15 |
| 4 | Positive Reader Support | 10 |
| 5 | Popular Low-Star Profile | 5 |
| 6 | Evidence Confidence | 5 |
| 7 | Goodreads Reach & Engagement | 3 |
| 8 | Average Goodreads Rating | 2 |
| | **Total** | **100** |

Components 1–3 = **Core Romantasy Quality Score (75 pts)**.

---

## Component 1 — Romance Centrality & Strength (30 pts)

**Evidence weighting:** Synopsis/official description 50% · Goodreads genres and shelves 30% · Review confirmation 20%

| Romance profile | Pts |
|---|---|
| Romance unmistakably drives the emotional narrative and materially shapes the main plot | 30 |
| Romance is highly central, sustained, and clearly consequential | 27 |
| Romance is a major co-primary plot alongside fantasy/adventure | 24 |
| Romance is substantial but somewhat secondary to the external plot | 20 |
| Romance is clearly present but receives limited synopsis emphasis | 15 |
| Romance appears secondary or subplot-like | 8 |
| Romance is incidental, negligible, or absent | Direct Fail |

> Do not award 24–30 solely because GR contains a Romance tag. Synopsis and reviews must support structural centrality.

---

## Component 2 — Negative Review Health (30 pts)

Count each complaint category once per review. Select the single row that best represents the overall pattern. Do not add multiple deductions. A complaint in only 1–2 of 15 reviews is isolated — do not present it as consensus.

**Complaint taxonomy:**

| Severity | Complaints |
|---|---|
| Mild | romance develops slowly · uneven pacing · predictable in places · confusing fantasy terminology · world-building could be deeper · side characters need development · ending slightly rushed · wanted more romance · wanted more fantasy · repetitive internal monologue |
| Serious — romance | no chemistry · romance feels forced · relationship unconvincing · romance barely present · instalove without development · repetitive romantic conflict · no emotional payoff · love interest completely flat · relationship progression makes no sense |
| Serious — fantasy | fantasy world feels empty/decorative · world-building incoherent · magic system makes no sense · fantasy element barely present · excessive unexplained terminology · major internal inconsistencies |
| Serious — general | boring · painfully slow · repetitive/filler · DNF · flat protagonist · incoherent plot · major plot holes · terrible ending · book feels unfinished · impossible to care about characters |

**Standard route (5 popular reviews):**

| Pattern | Pts |
|---|---|
| No repeated negative category | 30 |
| 1 mild category in 2 reviews | 28 |
| 1 mild category in 3+ reviews | 25 |
| 2 mild categories repeatedly present | 22 |
| 1 serious category in 2 reviews | 20 |
| 1 serious category in 3 reviews | 14 |
| 1 serious category in 4–5 reviews | 9 |
| 2 serious categories in ≥ 2 reviews each | 7 |
| 3+ serious categories repeatedly present | 2–5 |

**Expanded route (15 meaningful reviews):**

| Pattern | Pts |
|---|---|
| No meaningful recurring negative category | 30 |
| 1 mild category in 3 reviews | 28 |
| 1 mild category in 4–5 reviews | 25 |
| 2 mild categories in ≥ 3 reviews each | 22 |
| 1 serious category in 3 reviews | 20 |
| 1 serious category in 4 reviews | 17 |
| 1 serious category in 5 reviews | 14 |
| 1 serious category in 6+ reviews | 9 |
| 2 serious categories in ≥ 4 reviews each | 7 |
| 3+ serious categories repeatedly present | 2–5 |

---

## Component 3 — Fantasy Relevance & Strength (15 pts)

**Evidence weighting:** Goodreads genres and shelves 40% · Synopsis/official description 40% · Review confirmation 20%

| Fantasy profile | Pts |
|---|---|
| Fantasy is foundational to setting, conflict, characters, and plot | 15 |
| Strong fantasy identity appropriate to the subgenre | 13 |
| Clear and meaningful fantasy framework | 11 |
| Moderate fantasy presence supporting the romance | 8 |
| Limited but genuine fantasy element | 5 |
| Fantasy is incidental, decorative, or absent | Direct Fail |

Judge fantasy depth against the subgenre promise, not against epic-fantasy standards.

---

## Component 4 — Positive Reader Support (10 pts)

Classify first 15 meaningful reviews as Positive / Mixed / Negative / Neutral. Mixed ≠ Positive. Exclude Neutral from denominator.

```
Positive Rate = Positive ÷ (Positive + Mixed + Negative) × 100
```

**Positive signals:** strong chemistry · emotional investment · addictive romance · compelling tension · immersive world · memorable leads · satisfying emotional payoff · eagerness for sequel

| Positive rate | Count / 15 | Pts |
|---|---|---|
| ≥ 85 % | 13–15 | 10 |
| 70–84 % | 11–12 | 9 |
| 60–69 % | 9–10 | 8 |
| 45–59 % | 7–8 | 6 |
| 30–44 % | 5–6 | 4 |
| 15–29 % | 3–4 | 2 |
| < 15 % | 0–2 | 0 |

A zero score here does not independently cause rejection.

---

## Component 5 — Popular Low-Star Profile (5 pts)

| Low-star profile | Pts |
|---|---|
| None | 5 |
| One 2★ | 4 |
| One 1★ | 3 |
| Two 2★ | 2 |
| One 1★ + one 2★ | 1 |
| Two 1★ | 0 |
| ≥ 3 both low-star AND negative | Direct Fail (→ DF-4) |

---

## Component 6 — Evidence Confidence (5 pts)

| Evidence condition | Pts |
|---|---|
| ≥ 8 usable reviews among first 15 | 5 |
| 6–7 usable reviews | 4 |
| 4–5 usable reviews | 3 |
| < 4 usable reviews, but 8–15 meaningful reviews analysed | 2 |
| < 4 usable reviews, but 4–7 meaningful reviews analysed | 1 |
| No meaningful written reviews | Subjective Review |
| < 4 usable reviews AND avg rating < 3.60 | Direct Fail (→ DF-3) |

---

## Component 7 — Goodreads Reach & Engagement (3 pts)

```
Review-to-rating ratio = (written reviews ÷ total ratings) × 100
```

| Total ratings | Base pts |
|---|---|
| ≥ 10,000 | 2 |
| 1,000–9,999 | 1.5 |
| 250–999 | 1 |
| < 250 | 0.5 |

Add 1 pt (capped at 3) if review-to-rating ratio ≥ 3%.

---

## Component 8 — Average Goodreads Rating (2 pts)

Apply only after the title survives DF-3.

| Avg rating | Pts |
|---|---|
| ≥ 4.20 | 2 |
| 3.90–4.19 | 1.5 |
| 3.60–3.89 | 1 |
| < 3.60 | 0 |

---

## Scoring calculation

```
TOTAL = C1 + C2 + C3 + C4 + C5 + C6 + C7 + C8

  C1 = Romance Centrality & Strength    (0–30)
  C2 = Negative Review Health           (0–30)
  C3 = Fantasy Relevance & Strength     (0–15)
  C4 = Positive Reader Support          (0–10)
  C5 = Popular Low-Star Profile         (0–5)
  C6 = Evidence Confidence              (0–5)
  C7 = Goodreads Reach & Engagement     (0–3)
  C8 = Average Goodreads Rating         (0–2)
  ──────────────────────────────────────────
  MAX = 100
```

Always show individual component scores, the summed total, and the derived verdict. Never state a verdict without first showing the arithmetic.

---

## Final classification

### A — YES (Standard Pass)

Return **YES** when: total score 75–100 AND no direct fail applies.

### B — YES (Core Evidence Pass)

A title scoring **65–74** may still receive **YES** when ALL of the following are met:
- Genre result is Strong Match, Confirmed Match, or Confirmed by Secondary Evidence
- Romance Centrality ≥ 24/30
- Fantasy Relevance ≥ 11/15
- Negative Review Health ≥ 20/30
- Positive Reader Support ≥ 6/10
- Combined C1+C2+C3 ≥ 58/75
- No direct fail applies
- No serious complaint pattern approaches a rejection threshold
- The lower total is mainly caused by reach, rating, or evidence-volume components

### C — SUBJECTIVE REVIEW

Return **Subjective Review** when:
- Score 65–74 but Core Evidence Pass conditions are not met
- Score 55–64 and no direct fail or rejection safeguard applies
- Romance or fantasy is genuinely ambiguous but not clearly absent
- GR tags, synopsis, and review evidence materially conflict
- Meaningful review evidence is insufficient
- A serious concern approaches but does not reach a direct-fail threshold
- The book possesses both romance and fantasy but execution cannot confidently be passed or rejected

**Subjective Review is not a complete verdict.** It must always identify the exact Primary Pain Point.

### D — REJECT

Return **Reject** when: any direct-fail condition applies; OR total score < 55 AND at least one rejection safeguard is met.

**Standard-route rejection safeguard** (at least one must be present):
- (a) 1 serious complaint in ≥ 3 of 5 reviews
- (b) 2 serious categories each in ≥ 2 of 5 reviews
- (c) Repeated DNF behaviour
- (d) ≥ 3 reviewers say romance is absent, negligible, or fundamentally unconvincing

**Expanded-route rejection safeguard** (at least one must be present):
- (a) 1 serious complaint in ≥ 5 of 15 reviews
- (b) 2 serious categories each in ≥ 4 of 15 reviews

A score < 55 without a safeguard met → **Subjective Review**, never Reject.

---

## Subjective Review diagnostic framework

Every Subjective Review must include one **Primary Pain Point** and, where relevant, one **Secondary Pain Point**.

### Pain-point codes

| Code | Category | Subcodes |
|---|---|---|
| SR-R | Romance concern | R1: appears secondary to fantasy · R2: underdeveloped · R3: centrality unclear from synopsis · R4: reviews question chemistry/development · R5: weak GR romance tagging |
| SR-F | Fantasy concern | F1: lightly developed · F2: supports plot but not foundational · F3: weak GR fantasy tagging · F4: synopsis doesn't establish structural importance · F5: reviews question world-building |
| SR-N | Negative review concern | N1: repeated romance criticism · N2: repeated pacing criticism · N3: repeated chemistry criticism · N4: repeated world-building criticism · N5: repeated character criticism · N6: DNF below threshold · N7: polarised reviews |
| SR-P | Weak positive support | P1: low proportion of positive reviews · P2: predominantly mixed response · P3: limited romance enthusiasm · P4: limited fantasy enthusiasm · P5: premise praised, execution questioned |
| SR-E | Insufficient evidence | E1: too few meaningful reviews · E2: too few usable popular reviews · E3: GR tags unavailable · E4: star ratings unavailable · E5: synopsis too vague · E6: insufficient evidence for fantasy relevance |
| SR-C | Conflicting evidence | C1: tags suggest romance but synopsis doesn't · C2: synopsis suggests central romance but reviews describe it as minor · C3: GR suggests fantasy but appears decorative · C4: sharply polarised reviews · C5: marketing promise conflicts with reader experience · C6: different editions produce inconsistent evidence |
| SR-Q | Execution concern | Q1: uneven pacing · Q2: repetitive plot/monologue · Q3: flat protagonist or love interest · Q4: incoherent plot development · Q5: weak ending/payoff · Q6: excessive unexplained terminology · Q7: several mild concerns combine into broader execution risk |

### Pain-point priority order

1. **Genre eligibility** — if romance or fantasy is insufficiently established, the weaker element is primary (SR-R or SR-F)
2. **Near-direct-fail concern** — a concern approaching a DF threshold is primary
3. **Repeated serious popular-review complaint** — exact complaint is primary (SR-N)
4. **Largest core-score weakness** against healthy benchmarks:

| Component | Healthy benchmark |
|---|---|
| Romance Centrality | 24/30 |
| Negative Review Health | 20/30 |
| Fantasy Relevance | 11/15 |
| Positive Reader Support | 6/10 |
| Evidence Confidence | 3/5 |

### Pain-point severity

- **Mild** — lowers confidence slightly; does not threaten core romantasy suitability
- **Moderate** — materially prevents YES but does not establish failure
- **Severe but Unconfirmed** — nearly reaches a direct-fail or rejection threshold; should normally be the Primary Pain Point

### Required Subjective Review conclusion

> **The title remains in Subjective Review primarily because [primary pain point]. The secondary concern is [secondary pain point, if applicable]. It does not qualify for rejection because [counter-evidence or unmet rejection threshold], but it does not receive YES because [specific unresolved weakness].**

---

## Output — single title report

```
Title:            [title]
Author:           [author]
Goodreads URL:    [url]
Goodreads data:   Avg rating: __  |  Ratings: __  |  Written reviews: __

VERDICT:  YES (__ / 100)  /  SUBJECTIVE REVIEW (__ / 100)  /  REJECT — [reason]  /  DIRECT FAIL — [reason]

LOGLINE
[One to two sentences. Lead with the fantasy premise, then the romantic hook, then the central stakes.
Draw only from confirmed synopsis and review evidence.]


GENRE CHECK

Genre match:  [Strong Match / Confirmed Match / Confirmed by Secondary Evidence / Insufficient Evidence / Does Not Qualify]
Genre:        [Romantasy / Not Romantasy]
Subgenre:     [one of the 12 valid subgenres]

Romance tags on Goodreads:  [tags + approximate positions]
Fantasy tags on Goodreads:  [tags + approximate positions]


SYNOPSIS ASSESSMENT

Romance role:  [central / co-primary / secondary / incidental]
[One sentence explaining the structural role and whether the story would change without the romance]

Fantasy role:  [foundational / clear / moderate / limited / decorative]
[One sentence explaining the structural role and whether the story could be retold without the fantasy]


ELIGIBILITY GATES

No Credible Romantasy Genre Match:       Pass / Fail
No Meaningful Fantasy:                   Pass / Fail
Romance Is Incidental:                   Pass / Fail
Low Evidence and Low Rating:             Pass / Fail
Three Low-Star Negative Reviews:         Pass / Fail
Romantic Promise Failure:                Pass / Fail

[When any gate fails: stop and explain the exact evidence in plain English]


REVIEW ASSESSMENT

Route:                          Standard (5 popular reviews) / Expanded (15 meaningful reviews)
Meaningful reviews analysed:    __
Usable popular reviews:         __
Positive reviews:  __   Mixed:  __   Negative:  __   Neutral:  __
Low-star negative reviews:      __

Recurring mild complaints:      [list or None]
Recurring serious complaints:   [list or None]


SCORE BREAKDOWN

Romance Centrality & Strength:   __ / 30   [one-line justification]
Negative Review Health:          __ / 30   [one-line justification]
Fantasy Relevance & Strength:    __ / 15   [one-line justification]
Positive Reader Support:         __ / 10   [one-line justification]
Low-Star Review Profile:         __ / 5    [one-line justification]
Evidence Confidence:             __ / 5    [one-line justification]
Goodreads Reach & Engagement:    __ / 3    [one-line justification]
Average Goodreads Rating:        __ / 2    [one-line justification]
──────────────────────────────────────────
TOTAL:                           __ / 100

[If score 65–74: state whether Core Evidence Pass conditions were met and which condition passed or failed]


PRIMARY CONCERN  [Subjective Review only]

[Plain English description of the main weakness]
Severity: [Mild / Moderate / Severe but unconfirmed]
Evidence: [exact counts and quotes]
Why it does not cause outright rejection: [one sentence]
Why it prevents a YES verdict: [one sentence]


SECONDARY CONCERN  [Subjective Review only, if applicable]

[Plain English description]


WHAT PREVENTED REJECTION  [Subjective Review only]

[One sentence of positive counter-evidence]


FINAL REASONING

[Evidence-based conclusion in plain English covering: genre eligibility · synopsis structure ·
review quality · recurring complaints · positive support · score · exact reason for the verdict]

[For Subjective Review, end with:
"This title remains in Subjective Review primarily because [primary concern in plain English].
The secondary concern is [secondary concern]. It does not qualify for rejection because
[counter-evidence]. It does not receive a YES because [unresolved weakness]."]
```

---

## Batch output — Excel report

For every batch run using an input `.xlsx` file, distribute the output across **5 columns** written to the right of the existing data. Locate the existing columns (or append new ones) using exactly these header names:

---

### Column 1 — "Final Verdict & Score"

Write only:
```
YES (__ / 100)
SUBJECTIVE REVIEW (__ / 100)
REJECT — [name of the gate or safeguard that triggered, in plain English]
DIRECT FAIL — [plain English reason, e.g. "No romance — romance is a minor subplot in a cozy mystery"]
⏭️ No data — [reason, e.g. "Only 43 Goodreads ratings; insufficient evidence"]
```

This column is the first thing anyone reads. Keep it to one line. No codes, no abbreviations.

---

### Column 2 — "Logline & Genre Check"

Write:
```
LOGLINE: [one to two sentences as per logline rules]

Genre match: [Strong Match / Confirmed Match / Confirmed by Secondary Evidence / Insufficient Evidence / Does Not Qualify]
Genre: [Romantasy / Not Romantasy]
Subgenre: [one of the 12 valid subgenres]
Romance tags on Goodreads: [tags + approximate positions, e.g. "Paranormal Romance (top 3) · Romance (top 5)"]
Fantasy tags on Goodreads: [tags + approximate positions, e.g. "Fantasy (top 1) · Fae (top 2) · Werewolves (top 4)"]
```

---

### Column 3 — "Synopsis & Review Assessment"

Write:
```
Romance role: [central / co-primary / secondary / incidental]
[One sentence explaining the role and whether removing the romance would change the story]

Fantasy role: [foundational / clear / moderate / limited / decorative]
[One sentence explaining the role and whether the story could be retold without the fantasy]

Eligibility gates:
  No Credible Romantasy Genre Match:       Pass / Fail
  No Meaningful Fantasy:                   Pass / Fail
  Romance Is Incidental:                   Pass / Fail
  Low Evidence and Low Rating:             Pass / Fail
  Three Low-Star Negative Reviews:         Pass / Fail
  Romantic Promise Failure:                Pass / Fail
[If any gate fails: explain the exact evidence in plain English]

Review analysis route: Standard (5 popular reviews) / Expanded (15 meaningful reviews)
Total meaningful reviews analysed: __
Usable popular reviews: __
Positive reviews: __ · Mixed: __ · Negative: __ · Neutral: __
Low-star negative reviews: __
Recurring mild complaints: [list or None]
Recurring serious complaints: [list or None]
```

---

### Column 4 — "Score Breakdown"

Write:
```
Romance Centrality & Strength:   __ / 30  [one-line justification]
Negative Review Health:          __ / 30  [one-line justification]
Fantasy Relevance & Strength:    __ / 15  [one-line justification]
Positive Reader Support:         __ / 10  [one-line justification]
Low-Star Review Profile:         __ / 5   [one-line justification]
Evidence Confidence:             __ / 5   [one-line justification]
Goodreads Reach & Engagement:    __ / 3   [one-line justification]
Average Goodreads Rating:        __ / 2   [one-line justification]
─────────────────────────────────────────
TOTAL:                           __ / 100

[If score 65–74: state whether the Core Evidence Pass conditions were met or which one failed]
```

---

### Column 5 — "Pain Points & Reasoning"

Write:
```
[For YES or REJECT: write only the Final Reasoning paragraph in plain English]

[For Subjective Review:]
Primary concern: [plain English description of the main weakness] — [Mild / Moderate / Severe but unconfirmed]
Evidence: [exact counts and quotes that support the concern]
Why it does not cause outright rejection: [one sentence]
Why it prevents a YES verdict: [one sentence]

Secondary concern: [plain English description, if applicable]

What prevented rejection: [one sentence of positive counter-evidence]

Final Reasoning: [evidence-based conclusion in plain English]
"This title remains in Subjective Review primarily because [primary concern in plain English].
The secondary concern is [secondary concern]. It does not qualify for rejection because
[counter-evidence]. It does not receive a YES because [unresolved weakness]."
```

---

**Batch rollup — print in chat after file is produced:**
- Total titles evaluated
- Counts by verdict: YES / Subjective Review / Reject / Direct Fail / No data
- Average score among scored titles
- Most common primary concern categories among Subjective Review titles

---

## Hard rules

- Begin with Goodreads romance and fantasy genre tags before reading the synopsis.
- Write the logline from confirmed synopsis and review evidence only — never invent events or relationships. Trope labels alone are not sufficient.
- Verify both romance and fantasy structurally before scoring.
- Do not qualify a title solely from its cover, title, trope language, or one reader shelf.
- Run all eligibility gates in order. Stop at the first confirmed failure.
- Do not invent missing evidence. Mark unavailable data as ⏭️ No data.
- Distinguish isolated complaints from recurring patterns. "Wanted more romance" does not mean romance is absent. A slow burn does not mean romance has failed. Low-stakes fantasy does not mean decorative fantasy.
- Judge fantasy depth against the subgenre promise, not against epic-fantasy standards.
- Do not let popularity or average rating override strong core evidence.
- Do not treat personal dislike of steam level, prose style, tropes, or pacing as objective failure.
- Every verdict must explain genre evidence, synopsis structure, and review quality in plain English.
- Every Subjective Review must name one primary concern with severity and exact evidence counts. No codes or abbreviations.
- Use a secondary concern only when it materially affects the verdict.
- "Mixed reviews", "borderline score", "unclear quality", and "needs human review" are not sufficient explanations.
- State exact evidence counts wherever possible.
- Explain why a concern did not reach a rejection threshold.
- A Subjective Review verdict must be actionable and specific, not merely uncertain.
- Every Reject must name in plain English the exact gate or safeguard condition that was met.
- Always show the full score arithmetic before stating the verdict.
- For batch runs: distribute the output across 5 columns — "Final Verdict & Score", "Logline & Genre Check", "Synopsis & Review Assessment", "Score Breakdown", "Pain Points & Reasoning". Never put the entire report in a single cell. Never use codes or abbreviations in any cell.
- This skill scores reception quality only — subgenre mapping is a separate task.
