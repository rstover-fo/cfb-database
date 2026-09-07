# F05 production rollout — 2026-09-07

The user approved this production rollout after PR #127 merged as `40b3e9c`.
Operations use `codex/f05-production-rollout`; training recorded source revision
`19e50637c0296ba688080aaf8091663e7a9bb988`. Documentation-only commits do not
invalidate the content-based training freshness check.

## Execution evidence

| Operation | Actions run | Result |
|---|---|---|
| Read-only baseline | [34127237645](https://github.com/rstover-fo/cfb-database/actions/runs/34127237645) | Captured original payload hashes and IDs before schema changes |
| Migration 064, twice | [34127338910](https://github.com/rstover-fo/cfb-database/actions/runs/34127338910) | Both applications succeeded |
| Import legacy fits without promotion | [34127419197](https://github.com/rstover-fo/cfb-database/actions/runs/34127419197) | Nine immutable `legacy_unknown` candidates |
| Preservation, exact import and actual roles | [34127557531](https://github.com/rstover-fo/cfb-database/actions/runs/34127557531) | All assertions passed; no deployment pointers/history before training |
| Refit and explicit promotion | [34127733768](https://github.com/rstover-fo/cfb-database/actions/runs/34127733768) | Nine known-lineage fits, 2017–2025, promoted |
| Freshness no-op | [34127829401](https://github.com/rstover-fo/cfb-database/actions/runs/34127829401) | Explicitly reported all selected vintages through FINISHED season 2025 match code/inputs; nothing to refit |
| Complete ladder and coefficient comparison | [34127924333](https://github.com/rstover-fo/cfb-database/actions/runs/34127924333) | All nine selected manifests validated |
| Upcoming scoring and mart refresh | [34127978533](https://github.com/rstover-fo/cfb-database/actions/runs/34127978533) | 3,247 forecasts; all five Tier 2 marts refreshed |

Daily Season Load, Historical Refresh, Backfill Sources, and Flat File Load were
active before the rollout and paused with no active conflicting warehouse writers. CI and
Live Scoreboard remained active. Final restoration is recorded below.

## Preservation and access

The baseline and post-import assertions preserved all payload fields of nine
legacy metadata rows, 414 coefficient rows, 374,700 original predictions
(`prediction_id <= 545692`), and three original scoring artifacts. Baseline
checksums are embedded in `2026-09-07-f05-preservation.sql`. New rows are allowed;
old rows cannot be silently rewritten or relabeled by this verification.

Every imported parameter object matched legacy feature order, means, scaling,
coefficients and Platt parameters exactly, including NUMERIC/string semantics.
Actual `anon` and `authenticated` reads succeeded for all three new registry
relations, with effective INSERT/UPDATE/DELETE/TRUNCATE privileges absent. Actual `analyst_ro` API access succeeded
and raw reads of all three registry tables failed as intended.

## Training results

Production used Python **3.11.16**, NumPy **2.4.6**. The compute subprocess took
approximately 13.4 seconds, including production reads and nine fit writes;
peak memory was not instrumented. Training populations ranged from 4,638 games
(2017) to 26,455 (2025). Every manifest records positive row count, input digest,
closed training window and the unchanged `training_logits` Platt population.

| Train-through seasons | Maximum absolute coefficient difference from legacy |
|---|---:|
| 2017–2023 | 0.000000 |
| 2024 | 0.041681 |
| 2025 | 0.038205 |

These are coefficient differences, not forecast error or held-out metrics.
The current-input refit is not claimed to reproduce historical input snapshots;
legacy lineage is unknown. No algorithm or calibration-method change was made,
and no accuracy/calibration improvement is claimed. The 2025 selected training
fit is `d8ac3e47ef98690bc41f51d1da3a6326642047122f9eb6b960e3ac21a3b464c3`.

## Final verification

[Final assertion run 34128090369](https://github.com/rstover-fo/cfb-database/actions/runs/34128090369)
passed all preservation, full-ladder and post-scoring checks. All **3,247** new
forecasts have the selected immutable training ID and valid publication/input
references. Distinct-game scorer coverage is **3,247/3,247 (100%)** for season
2026; pre-kickoff eligibility is **3,225/3,247 (99.32%)**. The remaining 22 do
not qualify as prospective forecasts. Both per-season gates passed; no duplicate
publication can inflate these counts.

The new scoring artifact is
`9ed4c5535161fb8c565811b1b01e3b6cc180f23f1b186c96157458f8487212e0`, linked to the
2025 training fit above. All 374,700 original predictions, three original
artifacts, nine metadata rows and 414 coefficients retained their full payload
hashes. There are now 377,947 prediction history rows. Artifact linkage was
executed and verified; content hashes were not independently recomputed for
every new production artifact/input snapshot.

The refreshed published-accuracy API has **zero rows**; new pending forecasts
still need prospective outcomes. This is not evidence of improved accuracy.
All four paused workflows were restored and confirmed **active** after these
checks. All rollout Actions runs succeeded; no rollback was required.

Independent review checked the fit-ladder and post-scoring SQL. The post-scoring
check counts distinct games by pending season, validates selected training IDs
and strictly prior fit windows, and requires a non-NULL margin and probability within [0,1].
It distinguishes scorer coverage from the stricter pre-kickoff eligibility gate.
F04 downstream consumer adoption and prospective outcome accumulation remain
separate work; this rollout does not fabricate historical publication times.
