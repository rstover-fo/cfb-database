# PFF+ API: Research Findings and Integration Brainstorm

**Date:** 2026-09-01
**Status:** Brainstorm — research snapshot of a **pre-release** API. Nothing here
is committed work; the real API spec does not exist yet and every integration
option below is gated on open questions in section 3.
**Trigger:** PFF's "next generation of PFF products" announcement
(https://www.pff.com/news/introducing-the-next-generation-of-pff-products)
promises subscriber API access; we hold a fresh PFF+ subscription.

## 1. What actually exists today (verified 2026-09-01)

### The developer portal is live; the API is not

- **https://developer.pff.com** is up, with three sections: `guide/`
  (Restish CLI walkthrough), `reference/` (OpenAPI-rendered docs), and
  `featured/` (community projects).
- The published spec at `https://developer.pff.com/openapi.json` is titled
  **"PFF Developer API (SAMPLE)"**, version `0.0.0-sample`, and says so
  explicitly: *"This is a SAMPLE specification used to build the developer
  portal's reference UI before the real API ships. Endpoint shapes are
  illustrative — the real spec is published by the API team and replaces this
  file automatically at build time."*
- The declared base URL **`api.pff.com` does not resolve** (connection failure,
  not 403/404). There is nothing to call yet.

### What the sample spec sketches (illustrative, not contractual)

| Endpoint | Shape |
|----------|-------|
| `GET /v1/seasons/{season}/grades` | Player grade rows, filters: `position`, `team`, `week`; cursor pagination, `limit` ≤ 200 |
| `GET /v1/players/{playerId}/grades` | One player's season/week grades |
| `GET /v1/seasons/{season}/stats/{facet}` | Premium stats by facet — `stats` is an open `map<string, number>` (e.g. `attempts`, `yards`, `epa_per_play`) |
| `GET /v1/players`, `GET /v1/players/{playerId}` | Player search/lookup |
| `GET /health` | Unauthenticated health check |

Notable details even at sample stage:

- **Season minimum 2006** in the `Season` parameter schema.
- **Cursor pagination** (`next_cursor`) — unlike CFBD, which has none.
- **429 responses are modeled** — rate limits will exist; thresholds unpublished.
- **Zero mentions of NCAA/college anywhere** in the spec or portal. All
  examples are NFL (Patrick Mahomes, team `KC`). The flagship announcement's
  API blurb is also NFL-framed ("the numbers that all 32 NFL teams rely on").
- The stats facet response schema is deliberately loose (`additionalProperties:
  number`) — column contracts will have to be discovered empirically per facet,
  not read off the spec.

### Auth: browser-based v1, API keys "on the way"

The v1 flow is: install the third-party **Restish CLI** (brew/GitHub
releases/Docker), point it at `api.pff.com`, and **authenticate in the browser
with your PFF account** — "no API keys to copy or rotate." The portal says
*"API keys for scripts and CI are on the way — v1 authenticates through your
browser."*

Consequence: **v1 is not automatable in GitHub Actions.** A browser login flow
means a human-interactive token grant; until keys ship, anything scheduled
(daily-load style) is off the table, and even local automation means scraping
whatever token Restish caches — fragile and possibly ToS-hostile.

### Subscription entitlement is ambiguous

Three sources disagree, in age order:

1. **PFF support article** ("Does API access come with a subscription?",
   profootballfocussupport.zendesk.com): API access does **not** come with
   PFF+; organizations go through **b2b.pff.com**. Likely stale, pre-dates the
   announcement, but it is what support currently says.
2. **The announcement** frames API access as part of the new subscriber
   product generation (API, CLI, or CSV — "delivered straight into whatever
   you're building").
3. **developer.pff.com** repeatedly says a **"PFF Pro subscription"** is
   required for data endpoints (docs are public). "PFF Pro" is not obviously
   the same SKU as "PFF+" — it may be the rebranded premium tier, or a new
   higher tier. Unresolved.

### The college data question

PFF's CFB subscriptions (separate announcement,
pff.com/news/pff-launches-new-college-football-subscriptions...):

- **CFB Grades+** — $7.99/mo / $29.99/yr: player grade pages + preview magazine.
- **CFB Premium Stats+** — $27.99/mo / $119.99/yr: grades + **NCAA Premium
  Stats back to 2014** + NCAA Greenline betting dashboard.

NCAA Premium Stats carries "all of the same grades and advanced stats in NFL
Premium Stats" for 2014+. So the **data exists in their warehouse at exactly
the granularity we'd want**, and 2014+ aligns perfectly with our advanced-
metrics floor (SP+/PPA are 2014+ too). Whether it will be exposed through the
developer API — and under which subscription — is unknown.

### What is usable *today*: Premium Stats CSV export

Premium Stats reports (NFL and NCAA) have a per-report **CSV download button**,
and Premium Stats Pro advertises "cleaner CSV exports." This is auth-gated
browser download, not a stable URL — but it is real NCAA grades/stats data we
can touch right now.

## 2. Why we care (what PFF adds that CFBD cannot)

- **Player grades** are the single most famous input we lack: every-snap
  0–100 grading, offense/defense/facet splits, weekly grain, FBS-wide, 2014+.
  Nothing in CFBD approximates them.
- **Charted detail** CFBD doesn't have: pressures, pass-block/rush-defense
  win rates, coverage grades, aDOT against, drops, missed tackles (exact NCAA
  facet list TBD from the real spec or CSV exports).
- **Modeling upside:** team-aggregated position-group grades (returning OL
  grade, QB grade, secondary coverage grade) are natural `features.team_week`
  candidates and preseason inputs — with the usual §2.5 screen and leak-free
  as-of discipline (a week-W grade must only enter the substrate at week W+1).
- **Scouting upside:** cfb-scout reads `core.roster`; player-level grades are
  an obvious enrichment for its `scouting` schema.

## 3. Open questions (blockers, in dependency order)

1. **Does our PFF+ subscription entitle API access?** ("PFF Pro" wording vs
   the PFF+ SKU we bought; stale B2B-only support article.) → Check the
   account page for developer/API mentions; ask support directly.
2. **Will NCAA data be in the developer API at all**, or NFL-only at launch?
   Every current signal is NFL-framed. If NFL-only, the API is irrelevant to
   this repo (we have no NFL surface beyond draft outcomes) and the CSV path
   is the only one.
3. **What do the ToS say about warehousing?** We would be persisting PFF data
   into Supabase and exposing it to cfb-app/cfb-scout. Private personal-use
   warehousing is presumably the intent of "wire it into whatever you're
   building," but redistribution limits matter before anything PFF-derived
   lands in a `public`/`api` view that could ever be shared. Read the API
   terms when they publish.
4. **When do API keys ship?** Until then no CI automation.
5. **Rate limits / call budget?** Modeled 429s but no numbers. Determines
   whether we mirror the CFBD rate-limiter budget pattern.

## 4. Integration architecture options

### Option A — now: NCAA Premium Stats CSVs as a flat-file source family

The flat-file subsystem (registry + parsers + `meta.flat_file_loads` ledger +
team-name crosswalk) is exactly shaped for this, with one twist: PFF CSVs are
**auth-gated browser downloads with no stable fetch URL**, so `fetch_url`/
`url_template` don't apply. Two sub-options:

- **A1 — manual drop:** export CSVs by hand (weekly during season), drop them
  into a watched location (Dropbox folder or a local `data/pff/` path), and
  give `FlatFileSpec` a local-file fetch mode. Hash-skip ledger already makes
  re-drops idempotent. Low tech, human-in-the-loop, fine for a weekly cadence
  and for backfilling 2014–2025 once.
- **A2 — authenticated fetch:** replay the browser session (cookie/token) from
  a script to hit the report-export endpoint directly. Fragile, reverse-
  engineered, possibly ToS-violating. **Not recommended** — the vendor is
  literally building the sanctioned version of this right now.

A1 is the pragmatic start: it gets 2014+ NCAA grades into the warehouse this
season and forces us to solve the two hard problems (schema + player
crosswalk, §5) that the eventual API integration needs anyway. When the API
ships, the fetch layer swaps out and the parsers/tables/xwalk survive.

### Option B — when API keys ship: a proper dlt REST source

A `pff.py` source module following the repo's conventions: httpx client
(separate from the CFBD client — different auth, base URL, budget), cursor
pagination (finally, a source where dlt's paginator machinery applies),
`merge` disposition into `pff.*` tables, year iteration 2014+ (NCAA) bounded
by whatever the real spec's season floor is. Its own rate-limit budget entry
in `.dlt/config.toml` once limits are published. Runs in `daily-load` or a
weekly workflow depending on how often grades revise (PFF re-grades after
review — expect within-week mutations, so `merge` on (player, season, week)
grain, and grades are **not** `IMMUTABLE_ONCE_FINAL` until a season is well
past).

### Option C — bridge: Restish CLI + manual token, locally

Once `api.pff.com` is live but before keys: run Restish locally (browser
auth), pipe JSON to a load script. Same standing as A1 — human-triggered,
local only — but exercises the real API shapes early. Worth doing once purely
as reconnaissance on the NCAA question and the facet column contracts.

**Recommendation:** A1 now (start with one report family, e.g. season-level
NCAA player grades, to de-risk the crosswalk), C as soon as the API is live,
B as the end state. Skip A2 entirely.

## 5. The two hard problems (independent of transport)

### Player identity crosswalk

We resolve team names via the xwalk framework; PFF forces the **player**
version of that problem. PFF player IDs share nothing with CFBD athlete IDs.
Matching keys: name + team + position + season, with all the usual traps
(Jr./III suffixes, transfers mid-database, duplicate names, position
relabeling). Plan:

- New `ref.player_xwalk` (or `pff.player_xwalk`) built by deterministic match
  (normalized name + team-season + position group) with a manual-review queue
  for ambiguous rows — same fail-loud philosophy as `UnmappedNamesError`:
  measure the unmapped fraction, gate on it, never silently drop.
- Roster coverage is on our side: `core.roster` is complete for the overlap
  years, and FBS-only scope keeps cardinality at ~15k player-seasons/year.

### Schema placement and the downstream contract

- New **`pff` schema** (own domain, own licensing posture) rather than mixing
  into `stats`/`ratings`. Candidate tables: `pff.player_grades`
  (player-season-week grain, facet columns), `pff.player_stats_<facet>` or a
  long-format `pff.player_facet_stats`, `pff.team_grades` (aggregates as
  loaded, if published, else derived in a mart).
- Anything consumed by cfb-app/cfb-scout goes through the usual `api`-view
  gate and a `SCHEMA_CONTRACT.md` entry — **but hold PFF-derived columns out
  of the contract surface until the ToS question (§3.3) is settled.**
  Internal `features.team_week` columns are lower-risk than user-facing views.
- Loose facet maps in the API (`map<string, number>`) mean column contracts
  must be pinned by us at parse time (explicit allowlist per facet, fail loud
  on unknown keys we care about) — the vendor schema will not do it for us.

## 5a. Export validation (2026-09-01, real 2025 data)

A hand-exported **NCAA passing_summary CSV** (2025 season, By Position view)
settled the §5 unknowns in the right direction:

- **Stable IDs exist.** Every row carries `player_id` (PFF's player ID) and
  `franchise_id` (PFF's team ID). The crosswalk is a build-once mapping table,
  not a per-load name-matching problem.
- **The export is FBS-scoped**: 560 rows, exactly 136 teams (the 2025 FBS
  membership, Delaware and Missouri State included), even though the UI
  banner says "957 Teams".
- **Shape:** one row per player-season, 44 columns: identity
  (`player`, `player_id`, `position`, `team_name`, `franchise_id`,
  `player_game_count`), grades (`grades_offense`, `grades_pass`,
  `grades_run`, `grades_hands_fumble`), and ~34 facet stats (aDOT,
  time-to-throw, big-time throws, turnover-worthy plays, pressure-to-sack
  rate, EPA, drop rate, ...). **No season column** — the season is implied by
  the export filter and must be injected at load time (parser gets it from
  `ParseContext`, same as other seasoned sources).
- **Facet CSVs mix positions.** `passing_summary` includes every player with
  a dropback: 408 QBs plus WR/HB/P/K trick-play rows. Facet tables must not
  assume position purity; filter at query time, not load time.
- **Team-name mapping is solved.** PFF uses ALL-CAPS abbreviations
  (`BOWL GREEN`, `LA LAFAYET`, `NWESTERN`). All 136 resolve to CFBD school
  names — mapping committed alongside this doc as
  `2026-09-01-pff-team-name-map.json` (seed data for the future xwalk).
- **Player matching is easy at QB.** Trial-matched the 176 QBs with ≥100
  dropbacks against `api.roster_lookup` (current-season roster) on last name
  + first initial + exact team: **169/176 (96%) matched**. All 7 misses are
  name-shape artifacts, not identity problems: multi-token surnames
  (Van Buren, Del Rio-Wilson) and suffix players (Barnett III, Fox Jr.)
  where CFBD's `last_name` carries the suffix or particle. Suffix/particle
  normalization on both sides should push this to ~100% at QB; expect worse
  at deep-roster positions (OL, DB) where CFBD coverage and duplicate
  names bite harder — that's the next thing to measure with a
  defense/blocking export.
- **Caveat:** the match ran against the *current* roster view while the CSV
  is 2025; a same-season match can only do better.

### Defense exports (2026-09-04, three seasons of defense_summary)

Three hand-exported `defense_summary` files extended the validation to the
deep-roster case:

- **Column contract:** 55 columns, identical across all three seasons — six
  grade columns (`grades_defense`, `grades_coverage_defense`,
  `grades_pass_rush_defense`, `grades_run_defense`, `grades_tackle`,
  `grades_defense_penalty`), pressure/tackling counting stats, coverage
  stats (targets, receptions, QB rating against), and **per-alignment snap
  counts** (`snap_counts_box`, `_slot`, `_corner`, `_fs`, `_dl_a_gap`, ...)
  — the weighting substrate for team-level aggregation. ~5,300–5,700 rows
  per season, ~10 positions (core CB/S/LB/DI/ED plus offensive players with
  defensive snaps).
- **Season fingerprinting works and must be a load-time guard.** The files
  carry no season column and identical export filenames; season was
  recovered from FBS-membership changes (Kennesaw State ⇒ 2024+, Delaware /
  Missouri State ⇒ 2025+, 134 vs 136 teams). The manual-drop loader must
  take season from the season-tagged filename **and verify it** against the
  membership fingerprint, failing loud on mismatch — misfiled uploads are
  otherwise silent data corruption (this nearly happened on the very first
  batch: three files named `defense_summary*.csv` with nothing else to
  distinguish them).
- **Deep-roster matching holds up.** Stratified sample of 175 CB/S/LB/DI/ED
  players (regulars + 50–200-snap depth) from the 2025 file against the
  roster: **159/175 (91%)** on last name + first initial + team. 15 of the
  16 misses are one systematic bug — suffix players (Jr./II/III) where
  CFBD's `last_name` retains the suffix while PFF appends it to the display
  name — fixable by stripping suffixes on both sides before comparing.
  Expected post-fix rate ≈99%; the one residual miss is a nickname
  (PFF "Trey" vs a legal first name). Positions with 1,000+ graded players
  per season match as cleanly as QB did.
- `api.roster_lookup` spans seasons (one row per player-season), so
  any-season presence matching works without a season-aware view.

### Blocking exports (2026-09-04, three seasons of offense_blocking)

- **Column contract:** 31 columns, identical across 2023–2025:
  `grades_pass_block`, `grades_run_block`, `grades_offense`, `pbe`
  (pass-blocking efficiency), pressures/sacks/hits/hurries allowed, and
  per-slot snap counts (`snap_counts_lt/lg/ce/rg/rt/te`) — enough to build
  snap-weighted OL line grades and positional continuity features without
  any positional guesswork. ~5,600–6,000 rows/season; every position that
  ever blocks appears (1,400+ WRs), so OL aggregation filters on
  T/G/C + slot snap counts, not on the position label alone.
- **Filename ordering is unreliable — fingerprints are mandatory.** In this
  batch the *unnumbered* file was 2025 and `...3.csv` was 2023, the exact
  reverse of the defense batch. Browser download numbering reflects upload
  order, nothing else. Confirms the season-fingerprint verification is a
  hard requirement, not defensive nicety.

Implication for §5: the `pff` schema should mirror the export families —
one wide table per facet (`pff.passing_summary`, `pff.receiving_summary`,
`pff.blocking_summary`, ...), keyed `(player_id, season)` (plus `week` when
weekly exports are in play), with `player_id → athlete_id` resolved through a
`pff.player_xwalk` built from the ID-bearing rows. The CSV column names are
already snake_case and can be taken verbatim as the column contract.

## 6. Cheap monitoring until the API ships

The spec replaces itself in place when real ("replaces this file automatically
at build time"). Two probes, either manual or as a tiny weekly check alongside
`flat-files.yml`:

1. `GET https://developer.pff.com/openapi.json` → alert when `info.version` ≠
   `0.0.0-sample` (and grep the new spec for `ncaa`/`college`/`league`).
2. `GET https://api.pff.com/health` → alert on first non-connection-failure.

Not worth a workflow of its own yet; a manual re-check every couple of weeks
during the season is fine, or bolt a no-fail step onto an existing daily job.

## 7. Proposed sequencing

1. **Now (account/legal):** confirm what our PFF+ SKU actually includes;
   ask PFF support whether developer API access will cover PFF+ and NCAA
   data; read API ToS when published. (Human task — blocked on PFF.)
2. **Now (data):** hand-export one NCAA Premium Stats CSV (season player
   grades), inspect columns, and draft the `pff` schema + player-xwalk design
   from real data. This is the highest-information next step and costs one
   afternoon.
3. **Then:** A1 flat-file specs + parsers for the chosen report family;
   backfill 2014–2025; weekly manual export during season.
4. **On API-live:** Option C reconnaissance run; answer the NCAA question
   definitively; pin facet contracts.
5. **On API-keys:** Option B dlt source; retire the manual export; wire
   grade-derived candidates through the §2.5 feature screen.

## Sources

- https://www.pff.com/news/introducing-the-next-generation-of-pff-products
- https://developer.pff.com (+ `/guide/`, `/reference/`, `/openapi.json`)
- https://www.pff.com/news/pff-launches-new-college-football-subscriptions-loaded-with-fbs-grades-advanced-stats-and-more
- https://profootballfocussupport.zendesk.com/hc/en-us/articles/32094827302163 (API-with-subscription support article; likely stale)
- Direct probes of `api.pff.com` (no DNS/connect as of 2026-09-01)
