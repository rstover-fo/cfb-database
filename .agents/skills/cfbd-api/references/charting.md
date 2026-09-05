# Passing and rushing charting

Read this reference only for CFBD passing or rushing charting endpoints and the
warehouse views derived from them. Treat coverage fields as part of the data
contract rather than incidental quality metadata.

## Standing semantics

### Passing

- Charting begins in the 2025 data era. The matching
  `*_attempts_available` field is the denominator for each charted metric
  family. Carry it into rates and leaderboards, or enforce a documented
  coverage threshold, so the result does not rank charting coverage as player
  skill.
- `parse_status='partial'` can represent work that upstream charting later
  completes. Confirm the repository's current finished-season reload policy
  before assuming a daily run will pick up revisions.

### Rushing

- Charting begins in the 2025 data era. Preserve these distinct denominators:
  `rushing_yards_available` for yardage tiers;
  `direction_eligible_attempts` and `direction_available_attempts` for
  direction; and `touchdown_status_available` for team touchdown metrics.
- Direction `unknown` is the unresolved remainder of eligible attempts. Use
  `unknown / direction_eligible_attempts` as a coverage gap. Do not treat it as
  a fourth charted direction or divide it by `direction_available_attempts`.
- Keep `complete`, `partial`, and `invalid` parse statuses separate. `invalid`
  is neither charted nor another spelling of `partial`.
- Player rows include individually attributed rushes. Team rows also include
  sacks, kneels, team rushes, multi-carrier plays, and unresolved attribution.
  Player totals therefore do not sum to team totals by design. Do not add team
  counters to player totals or present the two grains as reconciling.
- Individually attributed sacks and kneels can be folded into a player's
  attempts while the corresponding player-level counters remain zero. Interpret
  counters in the context of row grain.

For both families, null charted metrics mean unavailable charting; zero is an
observed value. Preserve coverage denominators and row grain through API views
and consumer calculations.

## Dated observations

These measurements describe upstream state at a point in time. Re-measure them
before using them as current availability guarantees.

- On 2026-08-31, 2025 passing air-yards coverage was near zero in weeks 1-7
  and about 92-100% in weeks 9 and later. The early-season gap matched CFBD's
  partial-backfill note rather than establishing an ingestion defect.
- On 2026-08-31, 2026 passing coverage was about 98% for week 1. Most partial
  rows appeared to resolve within roughly a day, with a small unresolved
  residue.
- CFBD described 2025 rushing charting as partial and 2026 as mostly complete.
  A 2026-09-03 probe found 2025 week-5 sampled rows marked `partial` and 2026
  week-1 sampled rows marked `complete`.

Unexpected coverage remains a reason to inspect source availability, request
filters, parse status, loader policy, and warehouse counts. A historical
measurement alone does not prove either a current source gap or an ingest bug.
