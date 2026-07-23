# Warehouse Extension: Data Sources Beyond CFBD

**Date:** 2026-07-23
**Status:** Research / brainstorm
**Method:** Five parallel research passes (free/open sources, betting & odds, commercial
providers, talent pipeline, enrichment domains), each evaluated against current CFBD
coverage. Endpoints marked "verified" were tested live during research.

The warehouse ingests all 61 CFBD endpoints. This doc catalogs what CFBD does *not*
provide — or provides poorly — and where to get it, free and paid.

---

## Executive Summary

The biggest genuine gaps in CFBD, in rough order of predictive/analytical value:

1. **Injuries & availability** — no CFBD endpoint at all; largest unmodeled game-level factor
2. **Player grades, snap counts, participation** — only accessible via PFF ($120/yr, CSV)
3. **Multi-book odds, props, line movement, sharp/closing lines** — CFBD lines are snapshotty, single-source-ish, no props
4. **Weather** — CFBD's game weather is patchy; hourly history is free (Open-Meteo)
5. **Portal/recruiting timing & NIL** — CFBD portal rows lack entry/commit timestamps; NIL absent
6. **External model benchmarks** — Massey composite (~86 systems), ESPN per-game FPI, prediction-market prices
7. **Program context** — coach salaries, athletic-department finances, coordinator changes

### Recommended tiers

**Tier 1 — free, low risk, do first:**
- Open-Meteo weather backfill + forecasts (hourly, 1940+, no key)
- Massey composite weekly CSV snapshot (`masseyratings.com/cf/compare.csv`, verified)
- Travel/venue computed features (zero external deps — haversine, tz-crossing, elevation from existing `ref.venues`)
- nflverse combine + draft-picks parquet → `draft` schema
- ESPN hidden core API: futures, odds w/ open lines, per-game FPI, recruits, rosters (verified live, no auth)
- Kalshi + Polymarket APIs (free, real traded prices on CFB games)
- **Start archiving conference availability-report PDFs now** — history only exists from 2023+, nobody archives it

**Tier 2 — cheap paid, high value:**
- PFF CFB Premium Stats+ — **$119.99/yr** — grades, snap counts, participation, 2014+ (CSV export, no API)
- The Odds API — **free tier → $59/mo in season, $119 for one backfill month** — multi-book NCAAF lines, props, historical 5-min snapshots back to 2020 *including Pinnacle* (public Pinnacle API died July 2025)
- Rolling Insights — **$100/mo, 30-day free trial** — only individual-priced true API with CFB injuries + depth charts

**Tier 3 — higher effort/risk, evaluate later:**
- On3 portal wire + NIL valuation scrapes (Playwright vs. Cloudflare; high ToS risk, internal-only use)
- SportsDataIO Discovery Lab (~$99–149/mo, delayed data)
- SIS DataHub Pro NCAA ($999/yr, charted advanced stats, CSV)
- Unabated API ($500+/mo) — only if CLV results justify sharper inputs

**Skip:** Sportradar / Genius Sports / Stats Perform / OpticOdds (enterprise-only, $10K+/mo class), MaxPreps (explicit ToS prohibition, CBS-owned, minors' data), X/Twitter sentiment (priced out), standalone Rivals pipeline (being absorbed into On3), Sports-Reference as a *pipeline* (20 req/min hard limit, bot-hunting — one-time throttled backfill only), officiating crew data (no public source exists).

---

## 1. Free & Open Sources

### Open-Meteo (weather) — top free pick
- Historical API: hourly ERA5 reanalysis back to **1940** (temp, wind/gust, precip, snow, humidity, pressure), plus 16-day forecasts and archived historical forecasts (2017+).
- REST JSON, **no API key**; free tier 10,000 call-equivalents/day — full backfill of ~250 venues fits in days.
- CC-BY data, non-commercial free tier. Near-zero risk.
- **Integration:** new `weather` schema; backfill kickoff-hour ±3h conditions per game via `ref.venues` lat/lon → `weather.game_conditions`; in-season forecast snapshots in daily load; wind >15 mph and sub-freezing temps are proven totals/passing signals for fitted_v1. Exclude domes (flag via Wikidata/venues).
- Secondary: NWS API (free, authoritative forecasts, no archive), Meteostat (station observations, validation source).

### ESPN unofficial APIs (`site.api.espn.com`, `sports.core.api.espn.com`)
Verified live, free, no auth. Provides things CFBD lacks:
- **Betting futures** (championship/conference odds) — CFBD has none; snapshot them, ESPN doesn't archive
- **Odds with open vs. current lines** + per-side prices + line-movement history sub-resource
- **Per-game FPI projections** (win prob + predicted margin per matchup)
- **Per-play ESPN win probability** — independent second opinion for calibrating `live.wp_params`
- **Recruits** (5,497 for 2026, verified): ESPN grades, commitment status, HS name/ID, measurables
- **Rosters**: jersey, class standing, birthplace, headshots, ht/wt — weekly snapshots build development history
- NOT available for CFB: depth charts (500s), ATS records
- Risk: medium (unofficial, but same endpoints have survived ~10 years; keep volume ~1 req/s; used by entire sportsdataverse ecosystem)

### cfbfastR-data (sportsdataverse)
- Parquet releases: PBP **2002+** (two seasons earlier than CFBD's 2004), pre-computed EPA columns, ID crosswalks (ESPN ↔ CFBD).
- Ingest via HTTPS parquet download + dlt filesystem source — no R needed. MIT, actively maintained (cfbfastR 2.0, May 2026).
- Use: one-time 2002–03 PBP backfill; EPA cross-validation mart.

### henrygd/ncaa-api (FCS/D2/D3)
- Only free structured source below FBS: scoreboards, box scores, PBP, standings, FCS playoff brackets. Verified live.
- Self-host via Docker (public demo limited to 5 req/s). MIT. Risk medium (proxies ncaa.com).
- Use: `ncaa` schema for sub-FBS games — FCS-vs-FBS context, transfer-origin scouting for cfb-scout.

### Wikidata (SPARQL) — CC0
- Stadium enrichment (roof/surface/elevation/capacity history — needed for the weather dome exclusion), coach biographical data. Quarterly job.

### Sports-Reference CFB
- Deepest history anywhere (1869+, coach records, polls to 1936) but **high ToS risk**: 20 req/min hard limit, 24-hour jail, active bot-hunting. One-time hand-throttled backfill (coach tenures, pre-1936 games) only — never a pipeline. sportsipy library is dead; roll your own.

---

## 2. Betting & Odds

### The Odds API — best value paid
- NCAAF ML/spread/totals/alt/period markets + player props; 40+ books incl. **Pinnacle (eu region)**; live in-play odds.
- Pricing: free 500 credits/mo; $30/20K; **$59/100K**; $119/5M; $249/15M. Credit cost = markets × regions per call.
- **Historical snapshots to June 2020** (5-min from Sept 2022; props from May 2023), paid tiers only, 10× credit cost.
- Why it matters: **Pinnacle's public API closed July 2025** — this is the cheapest legitimate Pinnacle NCAAF closing-line series for CLV work.
- Integration: daily pre-game snapshot → `betting.odds_api_snapshots` (last-before-kickoff = closing line); Saturday live poller alongside `live-scoreboard.yml`; one $119 month for the 2020–26 backfill.

### Kalshi + Polymarket — best free
- Kalshi (CFTC-regulated): CFB single-game winner/spread/total + futures since 2025; **free API** (REST/WS, generous limits) with orderbook, trades, candlesticks — traded-price history + volume, which sportsbook APIs don't give. Yes-price = vig-free implied probability.
- Polymarket: free read-only APIs (Gamma/CLOB), CFB games + props; liquidity thin outside marquee games/futures.
- Integration: `betting.prediction_markets` polling daily (hourly Saturdays) → second market consensus for `compute_predictions.py`, plus sportsbook-vs-market divergence signal.

### Free historical backfill
- **sportsbookreviewsonline.com**: free Excel archives of NCAAF open/close spreads/totals/MLs, ~2007+ (FCS 2019+). Community-grade quality; needs team-name crosswalk. Roughly doubles backtest history for `check_backtest.py`.

### Public betting percentages
- Action Network: free bet% pages; PRO ($30/mo) adds money%; Action Labs ($249/mo) has 2003+ betting DB (ex-Sports Insights). No API, enterprise anti-bot — best-effort scrape only, never a core dependency.
- Covers consensus is contest-picks, not real money — weak proxy.

### Premium tier
- SportsGameOdds: $99/mo Rookie (77 books incl. Pinnacle, 3-min updates, object-based billing favors all-books-per-event pulls); decent Odds API alternative.
- Unabated: screen $199/mo; API $500–3,000/mo — market-making books (Circa, Bookmaker, 3et, Pinnacle proxy) + vig-free Unabated Line. The pragmatic premium buy if CLV work matures.
- OpticOdds (~$5K/mo enterprise, 200+ books, sub-second SSE): operator-scale only. Vendor-risk flag: pending Swish Analytics lawsuit.
- Betfair exchange: not available to US residents; thin CFB liquidity — skip.

---

## 3. Commercial Providers

| Provider | Unique data vs. CFBD | Cost | Verdict |
|---|---|---|---|
| PFF CFB Premium Stats+ | **Grades, snap counts, participation, alignment, premium stats 2014+, Greenline** | **$119.99/yr** | Best unique-data-per-dollar; CSV export only (no API with consumer subs); keep out of public `api`/`marts` surfaces per ToS |
| Rolling Insights | **CFB injuries + depth charts via real API** | $100/mo pre-game tier; 30-day free trial, no contract | Small shop — audit data quality in trial (roster match rate vs `core.roster`) |
| SIS DataHub Pro NCAA | Charted advanced stats (Total Points, boom/bust), projections | $149.99/mo or $999.99/yr; CSV export | Good but duplicates house EPA work |
| SportsDataIO Discovery Lab | Injuries/depth charts/props/news (delayed, personal-use) | Free last-season tier; ~$99–149/mo | Useful free schema-mapping exercise; delayed data adds little |
| SportsDataIO commercial | Full catalog, real-time | ~$16.5K/yr median | Not realistic |
| Sportradar | Official-grade live PBP, push feeds; **no CFB injuries/depth charts** | $10K+/mo class | Not realistic |
| Genius Sports / Stats Perform | Official NCAA feed (exclusive through 2032) | Enterprise | Not realistic |
| TruMedia / Telemetry (Teamworks) | Team-grade charting/video | "Coach salary" tier | Team/media only |
| RotoWire syndication | Injury/news/depth-chart editorial feed (XML/JSON) | Custom, est. low-$K/yr | Worth an email if Rolling Insights disappoints |
| API-Sports (RapidAPI) | Thin NCAA coverage, has injuries endpoint | Free–$39/mo | Redundancy only |

**Participation/tracking data:** nobody sells college snap counts at individual prices via API. PFF CSV export is the only accessible answer.

---

## 4. Talent Pipeline (recruiting / portal / NIL)

### Landscape shift
On3's ownership group **acquired Rivals (mid-2025)** and merged databases — the industry is now 247Sports vs. On3/Rivals. On3's NIL valuation switched to **deal-based (July 1, 2026)**, making it the closest public proxy for per-player pay (including post-House rev-share era where real numbers are non-public).

### ESPN recruits + rosters (free, verified, do first)
- Second independent rating (ESPN grades), commitment status, HS name/ID (future entity-resolution key), measurables; rosters give class standing, jersey, birthplace, ht/wt history via weekly snapshots.
- Fits dlt REST pattern exactly. → `recruiting.espn_recruits`, `core.roster_espn_snapshot`.

### nflverse combine + draft picks (free, zero risk)
- `combine.parquet` (2000+, all measurables), `draft_picks.parquet` (1980+). CFBD draft picks carry `collegeAthleteId`, which does the college→NFL join.
- Completes the recruit → college → NFL chain: stars-to-draft conversion, measurables percentiles (compute MockDraftable-style percentiles with one window function).
- Supplement: array-carpenter/nfl-draft-data (hand-charted combine + pro-day 2007–2026).

### On3 (portal wire + NIL) — high value, high risk
- Portal wire: entry/commit/withdraw/enroll **timestamps**, portal rankings, NIL valuations attached — CFBD's `/player/portal` has only an ambiguous single `transferDate` and no status lifecycle.
- Access: Cloudflare 403s plain HTTP (confirmed) — needs Playwright parsing `__NEXT_DATA__`, or Apify actors ($10–50/mo). High ToS risk; internal-only use keeps practical exposure modest.
- Integration: `recruiting.transfer_portal_events` (event feed merged onto CFBD portal spine); weekly `recruiting.nil_valuations` snapshots — the snapshot *history* becomes a proprietary asset.

### 247Sports Crystal Ball
- Commitment predictions with predictor/confidence — valuable trending signal, but high ToS risk (CBS), growing VIP-gating, partially redundant with On3 RPM. Pursue only if commit-prediction becomes core to cfb-scout; never scrape VIP content.

### NIL reality check
No public deal-level dataset exists. On3 valuations are the only automatable per-player signal. Aggregates for benchmarks: College Sports Commission quarterly reports ($355M cleared / 34,195 deals since June 2025), Opendorse annual report ($4.5B market est. 2026-27; P4 QB avg $1.5M). Hand-maintained `recruiting.nil_market_benchmarks` table, refreshed ~2×/yr.

### Skip
- **MaxPreps**: ToS explicitly prohibits all scraping and even non-profit reuse; CBS-owned; minors' data. Manual lookups only.
- **Rivals standalone**: get Rivals ratings via On3's Industry Comparison; anything Rivals-specific will break as consolidation continues.

---

## 5. Enrichment Domains

### Injuries & availability — biggest CFBD gap
- **Conference availability reports** (free, authoritative): Big Ten gameday PDFs on S3 (2023+), SEC Wed/Thu/Fri + T-90min reports at secsports.com/fbreports (2024+, JS-rendered), Big 12 + CFP (2025+). Fine-backed accuracy. **Archive now** — shallow history, nobody guarantees old PDFs persist; status *trajectory* (probable→out) is itself a signal.
- Structured commercial: SportsDataIO Discovery Lab or Rolling Insights (see §3) to get features this season without building 4 scrapers.
- Integration: `availability.reports` append-only snapshots (like `predictions.game_predictions`), fuzzy-joined to `core.roster`; derive `qb1_out`, `starters_out_count`, starter-availability index weighted by PPA share → `features.team_week`.

### Massey composite ratings — cheapest external benchmark
- `masseyratings.com/cf/compare.csv` (verified): consensus rank + mean/median/stdev across **~86 systems**, weekly in season. FCS version exists.
- **No retroactive archive — must snapshot weekly starting now.**
- Integration: `ratings.massey_composite`; uses: house-model-vs-consensus benchmark in `check_backtest.py`; consensus-deviation as edge predictor; `stdev` as team-uncertainty feature (high-disagreement teams get mispriced). ~1 day incl. team-name crosswalk.

### Travel/venue features — best value-to-effort, zero external deps
- All computable from `ref.venues` + `core.games`: haversine travel distance, tz-crossings, body-clock kickoff hour, elevation delta (Wyoming 7,220 ft), surface mismatch, rest-day differential.
- Literature: westward tz-crossing penalties documented (Coleman 2017); effects shrinking in charter era — fit, don't assume.
- Integration: `core.game_travel` computed in `load_season.py` → `features.team_week`.

### Program finance & coaching
- **Knight-Newhouse** (knightnewhousedata.org): category-level athletics finances, 230+ public D-I schools, 2005+, genuine data download. Publics only.
- **EADA** (ope.ed.gov): all institutions incl. privates, less granular. Combine into `program.finances`.
- **USA Today salary DBs**: head coaches (~2006+), assistants, strength coaches (2025+). JSON-backed tables, no export; private schools missing.
- **FootballScoop** coordinator-change trackers: weekly scrape in carousel season → `program.staff_changes`; first-year OC/DC flags shift early-season EPA — regress adjusted-EPA priors accordingly.
- Full assistant staffs: no structured source; coordinators-only via FootballScoop + Wikipedia is the realistic scope.

### Attendance, TV, realignment
- NCAA annual attendance PDFs (1978+ records) → capacity-utilization feature; per-game attendance already in CFBD.
- Sports Media Watch viewership (2012+) is chart-image-heavy — awkward extraction, modest value; 506sports has clean scheduled-broadcast HTML (2013+). Kickoff-window features derivable from CFBD media data already.
- Conference membership derivable in-house; JacobH140/century-of-college-football for 1924–2024 ground truth → `ref.conference_membership` + realignment-shock flag.

### Dead ends
- **Officiating**: no public CFB crew-assignment source; QwikRef is internal; RefMetrics doesn't cover CFB yet (re-check annually). Substitute: `analytics.penalty_profiles` by officiating-conference from own PBP.
- **Social sentiment**: X API priced out; pytrends dead (official Trends API alpha is gated); Reddit workable (100 QPM OAuth) but research shows sentiment adds little beyond market lines you already have. App-facing "buzz" widget at best.

---

## Suggested Roadmap

**Now (free, ~1–2 weeks of work total):**
1. Massey composite weekly snapshot (start immediately — history accrues only from first snapshot)
2. Conference availability-report PDF archiver (same urgency)
3. Travel/venue computed features migration
4. Open-Meteo `weather` schema: historical backfill + forecast snapshots in daily load
5. nflverse combine/draft parquet loads
6. ESPN dlt source: recruits, rosters, futures, game odds, per-game FPI

**August 2026 (one-time spend):**
7. The Odds API $119 month: 2020–26 historical closing-line backfill (incl. Pinnacle) → `betting.closing_lines`
8. SBR Excel archives → `betting.sbr_historical` (extends backtests to ~2007)

**In season (recurring):**
9. The Odds API $59/mo: daily snapshots + Saturday live odds polling
10. PFF CFB Premium Stats+ $119.99/yr: weekly CSV drop → staging loader (grades, snap counts)
11. Kalshi/Polymarket prediction-market poller
12. Rolling Insights 30-day trial → $100/mo if injury/depth-chart quality passes audit

**Later / conditional:**
13. On3 portal wire + NIL scrape (Playwright or Apify) — when cfb-scout needs portal timing/NIL
14. henrygd/ncaa-api self-hosted for FCS/D2/D3
15. Knight-Newhouse + EADA + USA Today annual program-context loads
16. Unabated API — only if CLV analysis proves out and justifies $500+/mo
