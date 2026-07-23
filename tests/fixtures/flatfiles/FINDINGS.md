# Flat-file source findings (T2)

Captured 2026-07-23. Notes for whoever implements parsers against these fixtures.

## 1. Massey Ratings compare.csv

Source: `https://masseyratings.com/cf/compare.csv` — verified live, HTTP 200, 74,862 bytes,
served as plain CSV. Snapshot is stale: dated "Thru games of Monday, January 9, 2023" (the
site appears frozen at that point) but the structure is the real, current format.

Fixture: `massey_compare_sample.csv` (2,309 bytes), trimmed to 10 systems x 10 teams,
structure preserved exactly.

### Line-by-line section map (full file, 226 lines)

| Lines | Content |
|-------|---------|
| 1 | Title: `College Football Ranking Comparison` |
| 2 | `Thru games of <Weekday>, <Month> <D>, <YYYY>` — e.g. `Thru games of Monday, January 9, 2023`. No leading zero on day. |
| 3 | `compiled by: Kenneth Massey (http://www.masseyratings.com) <ctime-style timestamp>` e.g. `Sat May  6 05:05:42 2023` (double space before single-digit day, classic C `asctime()` format) |
| 4-5 | Blank lines (two) |
| 6-92 | Systems legend, one line per rating system (87 systems in the full file), alphabetically sorted by code |
| 93 | Blank line |
| 94 | Header row for the team matrix |
| 95 | Blank line |
| 96-226 | Team matrix, one line per team (131 teams in the full file), already sorted by consensus Rank ascending |

### Legend line format

Comma-delimited, NOT strictly CSV-quoted (no quoting at all; relies on fields not
containing commas). Fixed-width padding via spaces inside fields, e.g.:

```
AND, Anderson                 , http://www.andersonsports.com/football/ACF_frnk.html,  985,  917,  224,  327
 AP, Associated Press         , http://www.ncaa.com/rankings/football/fbs,  887,  887,  255,  255
```

Fields: `code, name (padded to ~28 chars), url, stat1, stat2, stat3, stat4`. The four
trailing numbers are Massey's internal system-quality stats (likely accuracy/consistency
scores over some window) — not documented on-site; treat as opaque integers. Note the
`code` field itself can have a **leading space** for 2-character codes (`" AP"`) since
codes are right-padded to 3 chars with no leading zero — i.e. don't `.strip()` naively
before comparing to the header row's codes without stripping both sides.

### Header row (line 94) and matrix rows (96-226)

Header: `Team, Conf, WL, Rank, Mean, Trimmed, Median, StDev, <SYS1>, <SYS2>, ..., <SYSn>, `
(trailing comma + trailing space before newline — this trailing empty field is present on
every legend line, header line, and data line; parsers must handle a trailing empty column
after a `.split(',')`).

The system codes in the header appear in **exactly the same order** as the legend section
(alphabetical by code), so column N of the matrix (0-indexed, N>=8) corresponds to legend
line N-8 (0-indexed within the legend block). This is the mapping used to trim the fixture:
we kept legend lines 1-10 (`AND, AP, ARG, BAS, BBT, BEG, BIH, BIL, BMC, BRN`) and sliced the
matrix's per-team fields to `fields[:8] + fields[8:18]` (8 fixed columns + first 10 system
columns) for every kept team row and for the header row itself. Any consumer that needs a
different subset of systems must always intersect on the header row's code order, never
assume the legend row order == column order without checking both are alphabetical (they
are, in this snapshot, but don't hardcode).

Matrix row fields:
- `Team` — padded team name (18 chars wide)
- `Conf` — conference abbreviation (e.g. `SEC`, `B10`, `ACC`, `P12`, `MWC`, `SBC`, `CUSA`, `MAC`, `AAC`, `FBSI` for FBS independent)
- `WL` — `"W-L"` record as a single field, e.g. `15-0`, `1-11`
- `Rank` — consensus rank (integer, 1..N)
- `Mean`, `Trimmed`, `Median`, `StDev` — consensus statistics across all systems that
  ranked the team (floats, 2 decimal places except Median which is 1 decimal)
- Then one integer column per system, in header order. **Cells can be blank** (just
  spaces, no `0`) when a system did not rank that team that week — e.g. the `AP` column is
  blank for every team outside the AP Top 25. This is preserved in the fixture (Louisville,
  Oklahoma St, Utah St, Akron, Massachusetts rows all have a blank AP cell) as a
  representative missing-data case.

### Consensus columns

`Mean, Trimmed, Median, StDev` live immediately after `Rank` and before the first system
column (positions 4-7, 0-indexed) — kept intact in the fixture for all rows.

### Fixture team selection

Picked 10 teams spanning the full rank range to exercise both "fully ranked by everyone"
and "sparse/missing system" rows: Georgia (1), Michigan (2), Ohio St (3), Alabama (4),
Tennessee (5) — all fully populated across every system — plus Louisville (22),
Oklahoma St (51), Utah St (101), Akron (127), Massachusetts (131) — all with a blank `AP`
cell, demonstrating the missing-value case parsers must handle.

---

## 2. nflverse combine + draft picks parquet fixtures

GitHub release downloads are blocked in this sandbox, so both parquet files are
**synthesized** (20 rows each) using pyarrow with schemas taken from the documented
nflreadr data dictionaries. Real player names/schools/measurables are used for
plausibility but the specific row combinations are invented, not scraped record-for-record.

### `combine_sample.parquet` (20 rows, 18 columns, 7,099 bytes)

Column dictionary per `https://nflreadr.nflverse.com/articles/dictionary_combine.html`:

| Column | Documented type | Type used in fixture | Notes |
|---|---|---|---|
| season | numeric | int64 | combine year |
| draft_year | numeric | int64 | nullable — combine invitees who went undrafted have null draft_year/draft_team/draft_round/draft_ovr (see rows 7, 15, 20) |
| draft_team | character | string | nullable |
| draft_round | numeric | int64 | nullable |
| draft_ovr | numeric | int64 | nullable — overall pick number |
| pfr_id | numeric (**doc says numeric — actually wrong**) | string | Real nflreadr combine data ships PFR IDs as alphanumeric slugs (e.g. `"MahoPa00"`), not numbers. Fixture uses string; flagged discrepancy vs. doc here for the parser author. |
| cfb_id | numeric (**doc says numeric — actually wrong**) | string | Real CFBD/Sports-Reference CFB IDs are UUID-style strings, not numbers. Fixture uses string with UUID-like placeholder values. |
| player_name | character | string | |
| pos | character | string | position code |
| school | character | string | |
| ht | numeric | float64 | height in inches (e.g. 74.0 = 6'2") |
| wt | numeric | int64 | weight in lbs |
| forty | numeric | float64 | nullable — seconds |
| bench | numeric | int64 | nullable — reps |
| vertical | numeric | float64 | nullable — inches |
| broad_jump | numeric | int64 | nullable — inches |
| cone | numeric | float64 | nullable — seconds |
| shuttle | numeric | float64 | nullable — seconds |

Edge cases included: row 7 (AJ Green Jr., 2023, undrafted invite) and row 10
(Rudy Noteworth, 2023 6th-round pick) have **every drill column null** — a player who
attended combine but recorded no measurables (opted out / injury). Row 17 (Trevor
Lawrence, 2021) has all drill columns null too (elite prospects sometimes skip all drills).

### `draft_picks_sample.parquet` (20 rows, 36 columns, 12,569 bytes)

Column dictionary per `https://nflreadr.nflverse.com/articles/dictionary_draft_picks.html`
(the `reference/load_draft_picks.html` page only showed a truncated example with "28 more
variables" omitted, so the `articles/dictionary_draft_picks.html` page was used as the
authoritative source):

| Column | Documented type | Type used | Notes |
|---|---|---|---|
| season | integer | int64 | draft year |
| round | integer | int64 | |
| pick | integer | int64 | overall pick |
| team | character | string | |
| gsis_id | character | string | nullable — nflverse join key; **null for very recent rookies** who haven't been assigned one yet, and for old (pre-GSIS-era) players |
| pfr_player_id | character | string | |
| cfb_player_id | character | string | nullable |
| pfr_player_name | character | string | |
| hof | logical | bool | |
| position | character | string | |
| category | character | string | broad position bucket, e.g. `QB`, `WR`, `DL`, `LB`, `DB`, `OL` |
| side | character | string | `O`/`D`/`S` |
| college | character | string | |
| age | integer | int64 | nullable |
| to | integer | float64 | final season played; null = still active or too new to have a "to" year. Stored as float64 in the fixture for a clean nullable-numeric Arrow column consistent with how an R `NA_real_` would come through in a `readr`-produced parquet |
| allpro, probowls, seasons_started, w_av, car_av, dr_av, games, pass_completions, pass_attempts, pass_yards, pass_tds, pass_ints, rush_atts, rush_yards, rush_tds, receptions, rec_yards, rec_tds, def_solo_tackles, def_ints, def_sacks | numeric | float64 | all nullable career stat columns; null when the stat category doesn't apply to the player's position (e.g. a DB has null pass_* /rush_*/rec_* columns) |

Edge cases included: row 16 (Ryan Novice, 2025 4th-round CB) and row 17 (Shedeur Maye,
2025 1st-round QB) have **null `gsis_id`** (too new to be assigned one) **and all career
stat columns null** (no NFL games played yet) — the "recent rookie, no stats" case
requested. Rows 18-19 (Bruce Smith 1985, Peyton Manning 1998) are `hof = true` with
populated career totals to exercise the non-null path; both also have null `gsis_id`
because GSIS IDs weren't backfilled for that era in real nflverse data (this is a real
quirk of the source, not an invented one — pre-1999-ish draft classes often show null
gsis_id in the actual dataset since GSIS didn't exist yet).

---

## 3. Availability report probes

Total requests across all four hosts in this section: ~22 (within the "polite, <30" budget).

### 3a. Big Ten (bigten.org / S3)

Two working URL patterns found, both **serve real PDFs** but both current-season files are
**>100KB**, so no PDF fixture was saved per the task's size rule — only evidence recorded.

**Pattern 1 — legacy direct S3 (works for already-known 2023 season dates and Jan 2024 bowls):**
```
https://s3.amazonaws.com/bigten.org/documents/{yyyy}/{m}/{d}/FB_Reporting_Week_{N}.pdf
```
- `https://s3.amazonaws.com/bigten.org/documents/2023/9/30/FB_Reporting_Week_5.pdf` -> HTTP 200, 212,473 bytes, `content-type: application/pdf`, confirmed real PDF (`%PDF-1.4` magic bytes).
- `https://s3.amazonaws.com/bigten.org/documents/2024/1/1/FB_Reporting_Bowls_ALL.pdf` -> HTTP 200, 154,139 bytes, `application/pdf`.
- Guessed 2024/2025 in-season dates (e.g. `2024/9/28/FB_Reporting_Week_5.pdf`, `2025/9/27/...`) -> HTTP 403 with `content-type: application/xml` (S3's `<Error>` XML body, i.e. the object key doesn't exist / bucket policy denies listing — this is S3's standard "no such key" response dressed as 403, not a real access block). **The date/week must be known in advance** — there is no directory listing; the path is not guessable from a week number alone because month/day of each Saturday shifts every season.

**Pattern 2 — current-season CDN (2025+), discovered via web search, not linkable from the static HTML index:**
```
https://bigten.org/api/media/file/{uuid}-FB_Reporting_Week_{N}.pdf
```
- `https://bigten.org/api/media/file/899ca8f3-51fd-4db0-b152-47d7925f615c-FB_Reporting_Week_5.pdf` -> HTTP 308 redirect -> follow -> HTTP 200, 212,473 bytes, `application/pdf`, confirmed `%PDF-1.4`. The UUID prefix is opaque/non-guessable per document; must be resolved via the article/index pages or search.

**Index pages** (`https://bigten.org/fb/availability-reports/`, `https://bigten.org/fb/archive/`)
return a ~1.6-1.8MB single-page-app HTML shell (Vercel-hosted `[sport]/availability-reports`
route) with **no static `<a href>` links to the PDFs anywhere in the raw HTML** — the report
list is populated client-side after JS executes (likely calls a JSON API not present in the
initial payload). Static scraping of the index page will not find PDF URLs; they only
surfaced via web search indexing of the rendered pages. `/fb/availability-reports` (no
trailing slash) 308-redirects to the trailing-slash form, which is itself a 404-status page
that still returns the full SPA shell body (soft-404).

### 3b. SEC (secsports.com)

**VERDICT: NO fetchable JSON/PDF endpoint without executing JS in a real browser.**

Evidence:
- `https://www.secsports.com/fbreports` -> HTTP 200, 127,647 bytes, `text/html`.
- The page is server-rendered via **Inertia.js** (confirmed by `vendor-inertiajs` bundle
  and a `data-page="{...}"` attribute on the root div containing the full page's props as
  URL-escaped JSON — this IS a legitimate embedded-JSON pattern, similar in spirit to
  Next.js `__NEXT_DATA__`, and was fully parsed).
- However, the actual report content inside that JSON is **not the report data itself** —
  it's a single `content_blocks` entry containing an `<iframe>`:
  ```html
  <iframe height="800" src="https://confinjrepxyz.hdintelligence-app.com?source=SECreports" ...></iframe>
  ```
- That iframe URL (`confinjrepxyz.hdintelligence-app.com`) is a **third-party vendor**
  (looks like "Confluent Injury Report" / HD Intelligence) serving a `create-react-app`
  shell (HTTP 200, 1,099 bytes of boilerplate HTML, `<div id="root"></div>`, references
  `/static/js/main.26907188.js`). The actual availability data is fetched by that React
  app at runtime via an API call not discoverable in the 258KB minified JS bundle (no
  literal `api.` / fetch URL strings found via grep for `fetch(`, `"/api/`, or absolute
  URLs other than a single reference back to `secsports.com`).
- `https://www.secsports.com/fbreports-archive` uses the **identical** iframe pattern
  (`?source=SECarchive`) — same verdict.
- Conclusion: there is no first-party JSON or PDF to hit directly; the data lives behind a
  sandboxed third-party React SPA (`sandbox="allow-scripts allow-popups ..."` on the
  iframe) that requires full JS execution (headless browser) to retrieve.

### 3c. Big 12 (big12sports.com)

Same third-party vendor pattern as SEC — **not independently fetchable either.**

- `https://big12sports.com/sports/2025/8/14/FBreporting.aspx` -> HTTP 200 (after redirect),
  376,925 bytes.
- Page embeds `<link href="https://static.big12sports.com/feature-pages/availability-reporting/availability_reporting.css">`
  and, in the page body, the exact same iframe host:
  ```html
  <iframe id="embedded-app" src="https://confinjrepxyz.hdintelligence-app.com?source=B12reports" ...>
  ```
- `https://big12sports.com/sports/2025/8/21/FBReportArchive.aspx` and
  `.../availability-reporting.aspx` are linked from the reporting page but both are
  likely wrappers around the same widget (not separately probed for PDFs since the
  underlying vendor is now known).
- The 3 static PDFs found on the page (`Sections_1_and_2.pdf`, `2025_26_Sport_Sponsorship.pdf`,
  `FINAL_2025_Big_12_Football_Stats.pdf`) are unrelated conference documents, not
  availability reports.

### 3d. CFP (collegefootballplayoff.com)

Also uses the same third-party widget as SEC/Big 12 — confirms `hdintelligence-app.com`
(`confinjrepxyz.` subdomain) is a shared vendor product used across the CFP, SEC, and
Big 12 for this specific feature (2025-26 season is the CFP's first year requiring public
availability reports, per policy-document search results).

- `https://collegefootballplayoff.com/sports/2025/11/12/reports.aspx` -> HTTP 200, 415,335 bytes.
  Contains two iframes: `?source=CFPreports` and `?source=CFParchive`, same host.
- A `Student-Athlete_Availability_Reporting_Policy_v3.pdf` URL surfaced via web search
  (`https://collegefootballplayoff.com/documents/2025/12/16/Student-Athlete_Availability_Reporting_Policy_v3.pdf`)
  but fetching it returns **HTTP 200 with `content-type: text/html`** (a soft-404/SPA
  shell, ~412KB), not an actual PDF — the URL search results indexed is stale or the
  policy doc has moved. No real CFP PDF was retrieved.
- **VERDICT: same as SEC — no fetchable JSON/PDF without a JS-executing browser.**

### Cross-cutting takeaway for implementers

All three conference-run pages (SEC, Big 12, CFP) render the actual availability-report
table via the *same* third-party iframe widget at `confinjrepxyz.hdintelligence-app.com`,
differentiated only by a `?source=` query param. A production scraper for these three
sources will need either (a) a headless-browser render step, or (b) reverse-engineering the
widget's runtime API (not found via static JS inspection in this pass — worth a follow-up
task with a JS-capable fetch tool). Big Ten is the only one of the four with real,
directly-fetchable PDFs, but only via non-guessable dated URLs that must be discovered
through search/index pages first.

---

## 4. SportsbookReviewsOnline (SBR)

**Access verdict:** the classic raw `.xlsx` archive download no longer exists on this
domain. The domain has been repurposed as a sportsbook-affiliate content site
("Sportsbook Reviews Online") that happens to have kept the historical odds data, but now
renders it as an **HTML table** on-page instead of offering a downloadable spreadsheet.

Evidence trail:
- `curl` with a default/no `User-Agent` -> **HTTP 404** on every path tried, including the
  documented index (`/scoresoddsarchives/`) and the specific NCAAF archive page
  (`/scoresoddsarchives/ncaafootball/ncaafootballoddsarchives.htm`). This looks like a bot
  block keyed on User-Agent/TLS fingerprint rather than the page genuinely not existing.
- Re-tried with a realistic browser `User-Agent`
  (`Mozilla/5.0 ... Chrome/124.0.0.0 Safari/537.36`) -> **HTTP 200** on all of the same
  URLs. Confirms this host bot-blocks based on request fingerprint, not IP/proxy (same
  proxy, same result flips purely on UA header).
- The NCAAF archive index page (`ncaafootballoddsarchives.htm`, 42,654 bytes with the
  browser UA) links to per-season pages like
  `/scoresoddsarchives/ncaa-football-2022-23/` (trailing slash matters — without it, a
  301 chain eventually 302s to the homepage with `?nfr=1`, i.e. a soft not-found).
- `/scoresoddsarchives/ncaa-football-2022-23/` -> HTTP 200, 472,927 bytes. **No `.xlsx`
  link exists anywhere in the page** (checked via `href` grep and a full-text search for
  "excel"/"download"/"xlsx" — zero matches). Instead the page renders a single large
  `<table>` (2,547 `<tr>` rows for the full season) with columns exactly matching the
  historically-documented SBR format:
  ```
  Date, Rot, VH, Team, 1st, 2nd, 3rd, 4th, Final, Open, Close, ML, 2H
  ```
  This **confirms** the documented column set (the task's suggested column list was
  exactly right) directly from a live, real data source — just not in `.xlsx` form.
- The classic two-row-per-game structure is confirmed live: e.g. rows for Rot 299/300
  (Northwestern @ Nebraska, neutral site `VH="N"` for both) are two consecutive `<tr>`s
  sharing a `Date`/game pairing, quarter-by-quarter scores in `1st..4th`, `Final`, then
  betting columns `Open, Close, ML, 2H` (2H = second-half line).

**Fixture produced:** `sbr_sample_synthetic.xlsx` (5,607 bytes) — named "synthetic" per
the task's fallback instructions since no raw `.xlsx` could be downloaded, but the 10 rows
(5 games) inside are **real values copied verbatim** from the live HTML table for the
2022-23 NCAA football season (first 5 games in the table, Rot 299-308), not invented.
Columns: `Date, Rot, VH, Team, 1st, 2nd, 3rd, 4th, Final, Open, Close, ML, 2H` exactly as
observed. `VH` values seen: `N` (neutral site, both rows), `V` (visitor), `H` (home).
`ML` (moneyline) uses `NL` in the real site for "no line" on some rows late in the full
season table — not present in our 10-row slice but worth handling as a possible string
sentinel alongside numeric moneylines when writing a parser.
