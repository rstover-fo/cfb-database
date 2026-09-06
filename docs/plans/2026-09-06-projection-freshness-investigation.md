# Projection freshness investigation — September 6, 2026

## Finding

The September 6 daily load stopped before predictions and season projections.
This explains why the Campbell and Western Carolina outlooks still carry
September 5 snapshots. Refreshing only their schedule counts would misrepresent
the vintage of the remaining projection fields.

## Executed evidence

- [September 5 daily run](https://github.com/rstover-fo/cfb-database/actions/runs/33968074057):
  season simulation succeeded at 13:26:40 UTC, writing 716 team projections for
  2026. The later verification step failed on unexpected `stats.game_havoc`
  variant columns; that failure did not prevent those projections being written.
- [September 6 daily run](https://github.com/rstover-fo/cfb-database/actions/runs/34035735070):
  all 20 source loads reported success, including games. The mart refresh failed
  at 13:35:28 UTC on `idx_player_comparison_pk`, duplicate key
  `(player_id, team, season) = (5302440, Alabama State, 2026)`.
  The load command exited 1; all subsequent Elo, EPA, prediction, feature,
  training, scoring and projection steps were skipped.
- [PR 121 CI run](https://github.com/rstover-fo/cfb-database/actions/runs/34043232927):
  live query found both affected `fitted_v1` projections computed at
  `2026-09-05 13:26:39.236668+00`, with 12 stored scheduled games versus 13
  current regular-season games. Producer-filter regression passed independently.

Initial local investigation lacked warehouse credentials. On explicit user
approval, the existing Deploy Schema workflow ran read-only diagnostics with
its configured secret and logged the results; see the recovery evidence below.

## Source inspection and next diagnostics

`src/schemas/marts/020_player_comparison.sql` has three possible fanout paths:

1. The stats pivot groups by name and position in addition to the published
   player/team/season key. Different names or positions can create multiple rows.
2. The exact roster join is not deduplicated on player/team/year.
3. The PPA join uses player/season without team or a uniqueness reduction.

These are hypotheses for this incident, not confirmed live root causes. Run
the accompanying read-only SQL against the configured warehouse to inspect
the failing key and affected schedules before selecting a repair.

## Recovery evidence

- [Warehouse probe](https://github.com/rstover-fo/cfb-database/actions/runs/34047820046)
  confirmed Amarion Fuller has DE metadata on three source stat rows and DL on
  seven, with exactly one matching roster row and no PPA row. The stats pivot's
  metadata grouping is the demonstrated duplicate-producing path.
- The live dependency closure contains only the mart and `api.player_comparison`.
  Both are owned by postgres. The mart grants SELECT to anon/authenticated;
  the API additionally grants SELECT to analyst_ro. No relation options exist.
- The candidate uses one grouped aggregation at player/team/season and selects
  a coherent modal observed name/position pair, with deterministic tie-breaking.
  Migration 061 snapshots and restores privileges/owners/comments, uses no
  CASCADE, and rejects unexpected relation options or column metadata. Local
  PostgreSQL 16 executed it twice with exact ACL preservation and successful
  reads under anon, authenticated, and analyst_ro. Independent review passed.
- [CFBD probe](https://github.com/rstover-fo/cfb-database/actions/runs/34047986266)
  still lists both 401866625 (Saturday) and 401917058 (Sunday). The old event is
  marked completed by CFBD; that flag alone cannot establish a played game.
- [Western Carolina's official postponement release](https://catamountsports.com/news/2026/9/5/football-catamounts-camels-postponed-until-sunday.aspx)
  confirms one game moved to Sunday at 11 a.m. ET. ESPN's
  [original event](https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=401866625)
  reports STATUS_POSTPONED with no statistics/drives, while its
  [replacement event](https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=401917058)
  carries the live Sunday contest and statistics. Retain provider records;
  a reviewed replacement mapping must prevent counting both in projections.
- The prior havoc variant failure is already corrected in current main's
  `variant_twins.py` allow-list; no additional havoc arithmetic change is needed.

Production migration and downstream compute require the explicit approvals
requested after automatic approval review rejected the first migration dispatch.
Preparation and local execution do not establish production recovery.

## Recovery order

1. Verify the affected schedule rows and identify the duplicate-producing input.
2. Prepare a grain-preserving mart repair with an independent review and an
   executed regression for the observed input. Do not drop the unique index or
   silently discard conflicting player records to make the refresh pass.
3. On an authorized production target, apply the repair and refresh its required
   dependency closure. Investigate the separate havoc variant warning before
   expecting the entire daily workflow to finish successfully.
4. Run the appropriate downstream compute sequence through season simulation
   using current inputs; a full ingestion rerun is not required merely to update
   stored projections. Reuse the daily workflow's ordering and model gates.
5. Verify projection timestamps, scheduled counts, unscored games and finite
   outputs for both teams, and confirm no duplicate player keys remain.

This operational incident is separate from F03's premature season-closure
predicate. The investigation identifies the blocked refresh; it does not mark
the warehouse repaired or the projections fresh.
