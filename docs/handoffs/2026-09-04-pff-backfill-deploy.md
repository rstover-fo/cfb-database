# Handoff: PFF Backfill Deploy (migration 061 + 15-file load + player xwalk)

**Branch:** `claude/pff-plus-api-1z1naq` (all code reviewed and pushed; tests green)
**Design/context:** `docs/brainstorms/2026-09-01-pff-plus-api.md`
**Task:** apply the pff migration, load the 15 hand-exported PFF CSVs, build the
player crosswalk. Everything below runs locally (needs `SUPABASE_DB_URL` /
`.dlt/secrets.toml`; the remote session that built this had no DB write access).

## Prerequisites

- The 15 CSVs, named `{family}_{season}.csv` (families: `passing_summary`,
  `receiving_summary`, `rushing_summary`, `offense_blocking`,
  `defense_summary`; seasons 2023–2025). The operator (Rob) has the originals;
  they should live in Dropbox, **never in this repo** (licensed data).
- Filenames are trusted only for convenience — the `--season` flag is what
  counts, and the parser independently verifies it against the file's
  FBS-membership fingerprint (`SeasonFingerprintError` on contradiction).
- `pip install -e ".[dev,compute,flatfiles]"` in the venv.

## Steps

```bash
# 1. Migration (idempotent; creates schema pff + 5 tables + seeded team_map + xwalk shell).
#    Applied to production 2026-09-04 as 059_pff_tables.sql (Deploy Schema run 33922048941),
#    renamed to 061 before merge because main already carried 059_rushing_grants_indexes.sql.
python scripts/run_migrations.py --file src/schemas/migrations/061_pff_tables.sql

# 2. Load all 15 files (order irrelevant; each is independent)
for season in 2023 2024 2025; do
  for fam in passing_summary receiving_summary rushing_summary offense_blocking defense_summary; do
    python scripts/load_flat_files.py --source pff_${fam} --season ${season} \
        --file /path/to/pff/${fam}_${season}.csv
  done
done

# 3. Player crosswalk (DB-only; no PFF files needed)
python scripts/build_pff_player_xwalk.py          # add --dry-run first if cautious
```

## Expected results

Row counts per (family, season) — from smoke-running the real files through the
full parse path; a large deviation means the wrong file was fed:

| family | 2023 | 2024 | 2025 |
|--------|------|------|------|
| passing_summary | 542 | 513 | 560 |
| receiving_summary | 2,277 | 2,301 | 2,371 |
| rushing_summary | 1,645 | 1,680 | 1,712 |
| offense_blocking | 5,647 | 5,737 | 5,988 |
| defense_summary | 5,334 | 5,442 | 5,717 |

- Re-running a load with an unchanged file → `skipped_hash` (ledger:
  `meta.flat_file_loads`, source key `pff_<family>:<season>`).
- Xwalk: **98.6% matched** on the 2023–2025 backfill (2026-09-04 rebuild:
  18,117 of 18,375 distinct `player_id`s; 121 ambiguous, 137 unmatched).
  The first run scored 88.3% because candidates spanned every roster
  season since 2004 — the matcher now scopes by PFF season and tiebreaks on
  first name (17,517 same-season, 548 first-name tiebreaks, 42 season±1,
  10 any-season). Unmatched + ambiguous players print to stdout — review,
  don't force; the residual is West Georgia (D-II, no CFBD roster), PFF
  "Unknown <team> <number>" placeholders, nicknames (PFF "Pickle"/"Trey"
  vs a legal first name), and true same-name-same-season collisions.

## Post-load verification (any SQL client)

```sql
SELECT 'passing' f, season, count(*) FROM pff.passing_summary GROUP BY season
UNION ALL SELECT 'defense', season, count(*) FROM pff.defense_summary GROUP BY season
ORDER BY 1, 2;                                   -- matches the table above
SELECT count(*) FROM pff.team_map;               -- 137 (136 FBS map + W GEORGIA, a D-II
                                                 -- team PFF graded in 2023)
SELECT count(*) FROM pff.player_xwalk;           -- ~18.1k after step 3
SELECT school, count(*) FROM pff.defense_summary WHERE season=2025
GROUP BY school ORDER BY 2 DESC LIMIT 5;         -- school column resolved, sane counts
```

## Failure modes (all fail-loud by design — do not work around them)

- `SeasonFingerprintError`: the file's FBS membership contradicts `--season`
  (e.g. a 2025 export claimed as 2023). Fix the season/file pairing; never
  bypass the guard — three upload batches had three different filename
  orderings, this guard is the only real defense.
- `UnmappedNamesError` (threshold 0.0): a team name not in `pff.team_map` —
  means realignment or a new PFF spelling; add the mapping to the migration
  seed (with the exact CFBD school string) rather than hand-editing the table.
- `ParserStructureError`: column drift — PFF changed an export format; update
  the allowlist in `flatfile_parsers/pff.py` deliberately, don't loosen it.

## Hard rules

- **No grants on the `pff` schema, no api/public views over it, no
  SCHEMA_CONTRACT.md entry.** Licensed data; posture stays service-role-only
  until the PFF ToS question (brainstorm §3.3) is resolved.
- Never commit PFF CSVs or row data to the repo. Test fixtures stay synthetic.

## After deploy

Next milestone (separate task, modeling-scientist territory): team-aggregated
grade features (snap-weighted QB/OL/coverage grades) through the §2.5
partial-correlation screen against fitted_v1, per the pre-registration
discipline — that verdict decides whether the 2014–2022 export backfill and
any in-season weekly collection are worth the manual effort.
