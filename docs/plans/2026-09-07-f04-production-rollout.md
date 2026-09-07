# F04 production rollout — 2026-09-07

The user explicitly approved pausing writers, inspecting production dependencies,
applying migration/views, scoring upcoming games, verifying, and resuming work.
PR #125 merged as `ccddd8a`; the operational branch retains that implementation.

## Executed changes

Daily Season Load, Historical Refresh, Backfill Sources, and Flat File Load were
active before the cutover and temporarily disabled. No warehouse writer runs
were active; CI was allowed to continue. Live Scoreboard was unaffected.

| Operation | GitHub Actions run | Outcome |
|---|---|---|
| Read-only catalog/aggregate preflight | [34070176943](https://github.com/rstover-fo/cfb-database/actions/runs/34070176943) | Only expected thin API dependents; owners and ACLs matched |
| Atomic schema cutover | [34070330017](https://github.com/rstover-fo/cfb-database/actions/runs/34070330017) | Migration 063, both marts, and all four API views committed together |
| Upcoming Elo/blend scoring | [34070399045](https://github.com/rstover-fo/cfb-database/actions/runs/34070399045) | 6,494 rows across 3,247 games |
| Upcoming fitted scoring | [34070441061](https://github.com/rstover-fo/cfb-database/actions/runs/34070441061) | 3,247 rows; 100% scorer coverage; 2025 frozen fit for 2026 |
| Calibration cold-start check | [34070493815](https://github.com/rstover-fo/cfb-database/actions/runs/34070493815) | 0 eligible completed outcomes; explicitly wrote 0 outlook rows |
| Refresh and final verification | [34070518680](https://github.com/rstover-fo/cfb-database/actions/runs/34070518680) | See final results below |

The exact atomic bundle was executed twice against disposable PostgreSQL 16 and
independently reviewed before production. It embeds all six merged SQL files
with their source SHA256 hashes and applies in one `run_migrations.py --file`
transaction. A ledger lock, dependency guards, payload checks, and actual caller
queries prevent a partially verified schema commit. The bundle deliberately
cannot be replayed after new published rows exist; use the ordinary idempotent
manifest for any separately planned subsequent schema maintenance.

## Preservation evidence

- 364,959 original prediction rows retained their IDs and all original payload
  values. New fields remain NULL with `evaluation_mode='legacy_unknown'`.
  Ordered full-payload/ID digest: `596053a3eb85ac927166c7a6712bb581`.
- All 39,860 stored season-projection rows remained unchanged, including their
  timestamps. Full-row digest: `6c8f33295dac29003d056500bd8e5154`.
- The original marts were owned by `postgres` with no direct ACL. Their only
  view dependents were `api.scored_matchup_edges` and `api.prediction_accuracy`,
  owned by `postgres` with SELECT for `anon`, `authenticated`, and `analyst_ro`.
  Recreated API views retain owner-rights behavior and actual SELECTs succeeded
  under all three caller roles. Consumer DML remains denied.
- Old prediction dates were not promoted into publication timestamps; no
  historical backfill, model refit, or fabricated calibration fallback ran.

## Remaining boundaries

Published accuracy initially has no completed prospective outcomes. Simulation
requires at least 100 eligible outcomes and retains old outlook timestamps in
the meantime. A successful nightly cold-start skip does not establish fresh
season projections. Artifact/input references are verified; production SHA256
contents were not independently recomputed across every new row.

Downstream API type regeneration and deployed empty/stale UI validation in
cfb-app/cfb-scout remain consumer adoption work. The local cfb-app checkout uses
explicit selected columns and supports missing prediction/accuracy rows; that
read-only inspection does not verify the deployed app. cfb-scout was not present
in this workspace. No consumer repositories or app deployments were changed.

## Final results

The final verification passed. There are 9,741 published forecasts (3,247 per
model) with three immutable model artifacts and 374,700 total history rows.
Eligible fitted coverage is 3,225/3,247 (99.32%): 22 pending records were already
at/past kickoff when scoring ran and are correctly excluded by the prospective
cutoff. The 100% scoring gate therefore is not 100% pre-kickoff eligibility.
Published accuracy and calibration each have zero completed eligible games;
this is the expected initial condition, not missing migration data.

All four paused workflows were restored and confirmed `active`. Temporary local
PostgreSQL was removed. Production changes and all six Actions runs succeeded;
no rollback was required. Independent review cleared the atomic bundle and
post-scoring SQL; local verification executed the exact bundle twice and the
post-scoring SQL with equivalent fixture baseline constants.
