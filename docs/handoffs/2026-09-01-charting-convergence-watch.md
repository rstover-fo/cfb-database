# Handoff: passing/rushing-charting convergence watch — findings and follow-up

**From:** cfb-database
**Date:** 2026-09-01 (watch started 2026-08-31 ~04:15 UTC); rushing frontier
added 2026-09-03.
**Audience:** cfb-app (bot answer/caching policy) + future cfb-database
sessions running the 2025 re-pull cadence.
**Question under test:** does `parse_status='partial'` in
`stats.passing_plays` resolve upstream (CFBD re-charts and re-publishes), or
is it terminal?
**Answer:** split by season. In-season (2026+) partial is an **active
re-charting queue** resolving within about a day, with a ~0.6% residue that
may be terminal. For 2025, convergence is still being measured — re-pull
cadence below.

## 2025 frontier baseline (pulled 2026-08-30, backfill run 5)

- plays: 53,554 | air_yards charted: 23,810 | partial: 29,744
- week air-yards coverage: wk1-7 = 0.0%, wk8 = 2.7%, wk9-16 = 92-100%
- Re-pull #1 dispatched 2026-08-31 ~04:15 UTC (seasons 2025, sources
  `passing`, ref `main`; queued behind player_overview drain #3). Diff these
  numbers on completion.
- Decision rule: if the numbers moved, 2025 charting is still in progress
  upstream — keep a weekly re-pull cadence until plateau. If static across
  **2 consecutive re-pulls**, declare 2025 terminal-as-stated.

**Next action (owner: cfb-database):** next 2025 frontier re-pull due
**~2026-09-07** — dispatch the `backfill-sources` workflow on `main` with
seasons `2025`, sources `passing` (62 calls). Compare plays / air-yards
charted / partial against the baseline above. Two consecutive static reads
=> declare 2025 terminal-as-stated and end the watch.

## 2026 in-season tracers (week 1, games 2026-08-29/30)

Week 1 arrived near-complete: 510 plays, 502 air-yards charted, 8 partial
(1.6%). The 8 partial (game_id, play_id) tracers — flip-to-complete across
daily loads means in-season partial resolves; stuck means terminal residue:

| game_id | play_id | detail |
|---|---|---|
| 401856766 | 401856766505 | TCU, passer NULL, incompletion |
| 401856766 | 401856766693 | TCU, Jaden Craig, completion |
| 401862693 | 401862693399 | UNLV, Jackson Arnold, incompletion |
| 401864494 | 401864494122 | USC, Jayden Maiava, completion |
| 401864494 | 401864494251 | San José State, Luke Weaver, completion |
| 401864494 | 401864494300 | USC, passer NULL, incompletion |
| 401864494 | 401864494429 | USC, Jayden Maiava, completion |
| 401864577 | 401864577190 | North Dakota State, Nathan Hayes, completion |

Checked after the daily loads (10:00 UTC) on 08-31 / 09-01 / 09-02.

**VERDICT (checked 2026-09-01 11:10 UTC, after the 08-31 daily re-pull):**
5 of 8 flipped partial -> complete with air yards charted. Still partial:
401856766505 (passer NULL), 401862693399, 401864494300 (passer NULL) — the
low-information plays. In-season conclusion: partial is an **active
re-charting queue resolving within ~a day**; residue ~0.6% of week-1 plays
may be terminal. Bot policy: a **~2-day provisional window** for
current-season charting answers suffices (the earlier suggested 7 days was
conservative).

## Bot invalidation checksum (recommended to cfb-app)

Per season, persist alongside any charting-derived answer the tuple:

```
(plays, partial_count, ay_charted_count, sum(total air yards))
```

from `stats.passing_plays` joined to `core.games`. A changed tuple means
recompute. Do **NOT** use `max(_dlt_load_id)`: merge rewrites rows on every
re-pull even when values are identical, so load-id watermarks produce false
invalidations.

## Rushing 2025 frontier (baseline pending first read, 2026-09-03)

Rushing charting (Stage A of the 2026-09-03 rushing-charting unit) is now
live: `stats.rushing_plays` carries 63,234 rows across 2025+2026 combined
(from the backfill presence check, Deploy Schema run 33778100985) -- the
per-season split has **not yet been read**, so there is no rushing frontier
baseline number yet, only the combined total. The baseline for this watch
should be captured from the first Stage B validation run (or a standalone
query) once rushing views deploy, using the same tuple shape as the passing
baseline above:

```sql
SELECT
  season,
  count(*) AS plays,
  count(*) FILTER (WHERE parse_status = 'partial') AS partial_count,
  count(*) FILTER (WHERE parse_status = 'invalid') AS invalid_count,
  count(*) FILTER (WHERE rush_direction IS NOT NULL) AS direction_charted_count
FROM stats.rushing_plays
GROUP BY season
ORDER BY season;
```

`invalid` is rushing's own third `parse_status` bucket (passing only has
`complete`/`partial`) -- track it separately from `partial` in every
baseline and re-pull comparison; do not fold it into either "charted" or
"not yet charted."

A same-day live probe (earlier 2026-09-03, before the presence-check backfill
completed) saw 2025 week-5 plays all `parse_status='partial'` and 2026 week-1
`complete`, with season-grain row counts 1,622 player-seasons / 136
team-seasons for 2025 and 76 / 16 (partial season) for 2026 -- consistent
with passing's pattern of 2025 being an active in-progress re-charting queue
rather than terminal-as-published.

**Next action (owner: cfb-database):** fold rushing into the next dated
2025 frontier re-pull, due **~2026-09-07** alongside the passing re-pull
above -- dispatch `backfill-sources` on `main` with seasons `2025`, sources
`passing,rushing` (~124 calls combined). Record the rushing tuple per season
on that run as the first baseline, then apply the same rule as passing: two
consecutive static reads => declare 2025 rushing terminal-as-stated.

## Related context

- Charting coverage semantics (2025 partial-by-policy, permanent
  `*_attempts_available` denominators, finished-season skip implications):
  `.claude/skills/cfbd-api/SKILL.md`, "Passing charting coverage" and
  "Rushing charting coverage".
- Rushing charting shape, denominators, and the R10 non-reconciliation rule:
  `docs/handoffs/2026-09-03-rushing-charting-for-cfb-app.md`.
- Finished-season skip means 2025 charting improvements reach the warehouse
  **only** via explicit `--sources passing`/`--sources rushing` re-pulls —
  the daily path will never pick them up on its own.
