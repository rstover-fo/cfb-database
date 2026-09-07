# F04: prediction provenance and prospective evaluation

## Goal

Historical reconstruction and hindsight must never replace published forecasts
or change their measured accuracy or simulation calibration. Preserve existing
ambiguous history honestly and retain the consumed model/input artifacts for
new predictions. F05 still owns immutable training manifests/deployment pointers.

## Contract

- Modes: `legacy_unknown`, `published_forecast`,
  `walk_forward_reconstruction`, `hindsight_experiment`.
- Existing prediction IDs and payloads remain; new provenance is unknown/NULL
  for those rows. New rows append, with no daily-key upsert.
- New provenance columns: `evaluation_mode`, `created_at`, `published_at`,
  `simulated_as_of_at`, `experiment_label`, `fit_id`, `input_hash`,
  `input_snapshot`. `fit_id` references immutable
  `predictions.model_artifacts(fit_id,model_version,artifact,created_at)`.
- Published timestamps are database write instants. Reconstructed timestamps
  are nominal as-of times, never fabricated publication times. A missing
  historical kickoff cannot be replaced with January 1 for provenance.
- Canonical JSON hashing captures the exact consumed model/input data and an
  implementation fingerprint. Artifacts retain coefficients/normalization or
  closed-form parameters; source inputs retain features/Elo/EPA/market data.
- Existing forecast API columns stay, with additive provenance metadata.
  `api.game_predictions` is published-only; `api.prediction_history` exposes
  all retained modes. Prospective accuracy and calibration apply strict
  pre-kickoff eligibility before selecting the latest timestamp/ID.
- Blend reconstruction defaults to as-of-week EPA. Full-season hindsight is an
  explicit labeled experiment. Fitted historical S-1 scoring is reconstruction.

## Development and verification

Use disjoint writer, consumer, and schema ownership, integrate through a shared
provenance helper, then independent review. Test actual PostgreSQL migration
upgrade twice and actual API/mart/calibration SQL, not invented fixture columns.
Fixtures distinguish midday/at-kickoff/post-kickoff published rows from newer
reconstruction, hindsight, and unknown rows. Verify immutable IDs/artifacts,
legacy retention, correct API roles/grants, and invariant calibration/accuracy
when historical experiments are added. Run affected checks and repository CI.

## Rollout boundary

No F04 production writes are authorized by the earlier F03 recovery approvals.
Prepare a dependency-complete deployment manifest and operational checks.
Pause prediction writers for schema/index/consumer cutover, then deploy updated
writers together. Legacy forecasts are not automatically promoted. Upcoming
published rows must be regenerated in an authorized run. Accuracy may be empty;
simulation must fail closed until its existing minimum number of eligible
published outcomes exists. Preserve the last stored outlook rather than
silently deriving a new sigma from unknown history. Historical backtests remain
explicitly retrospective and isolated from production calibration.

## Coordinated deployment procedure (requires separate approval)

1. Pause/drain nightly, recovery, and ad-hoc prediction writers. Record ledger
   row count and IDs and inspect `pg_depend` for every dependent of
   `marts.scored_matchup_edges` and `marts.prediction_accuracy`. The checked-in
   closure is their two thin API views; preserve any additional production-only
   dependents/grants before using their existing `DROP ... CASCADE` definitions.
2. Apply `deploys/f04-prediction-provenance-manifest.json` from the reviewed
   revision. Files commit individually: keep writers paused throughout, and
   resume a failed apply at the failed step after correcting the cause. The
   migration and complete ordered manifest are repeatable. Never reapply old
   migration 024 after 063: its obsolete daily unique index is incompatible.
3. Confirm every pre-cutover row/ID remains `legacy_unknown` with NULL new
   provenance, both rebuilt marts and all four API views exist, and `anon`,
   `authenticated`, and the API-only `analyst_ro` can SELECT the intended views.
   Confirm consumer DML remains denied and update triggers are installed.
4. Deploy the new prediction writers and consumers together. Run authorized
   upcoming scoring, then refresh edges/accuracy. Verify new rows have artifact
   and input hashes, database publication timestamps, and complete pending-game
   coverage. No historical promotion or mass reconstruction is required.
5. Regenerate cfb-app/cfb-scout API types and verify empty prospective accuracy
   and stale retained season outlook are represented honestly. That cross-repo
   rollout is pending; the warehouse alone cannot verify deployed consumers.
6. Resume nightly work. `--allow-calibration-cold-start` is used only by the
   daily simulation step: it explicitly warns and writes no new projections
   until sufficient published outcomes exist, allowing mart refresh, backtest,
   and verification to continue. Default/manual simulation and recovery still
   fail on cold start. Invalid sigma or unrelated errors always fail. Retained
   outlooks keep their old timestamps; this is not evidence of fresh simulation.

The write role retains controlled archival/deletion authority for recovery;
normal prediction/model artifact writers only insert. Consumer roles cannot
mutate the ledger. An owner can disable triggers for approved maintenance;
this is not a tamper-proof audit store against a privileged administrator.

## Verification environments

`tests/test_prediction_provenance_sql.py` uses only explicit
`F04_TEST_DB_URL` pointing to an empty disposable local PostgreSQL database.
CI provisions PostgreSQL 16 separately from the live warehouse checks. It
executes the actual migration, views, marts and sigma query, including upgrade
and repeat application, timestamp filtering, provenance retention and caller
roles. It must never fall back to production credentials.

Before 063 is deployed, live `api.game_predictions` checks retain its previous
column contract; the new history view check reports an explicit deployment skip.
Once the base provenance column exists, all additive API columns and history
are required. This allows review before production deployment without claiming
that production has already adopted the new contract.

## Implementation evidence

- Independent Astra review completed with all material findings resolved:
  transitive scoring fingerprint coverage, explicit nightly cold-start handling,
  and disposable-test cleanup ownership guards.
- PostgreSQL 16 disposable database: **10 passed**, including migration upgrade
  twice, real writer/JSON/artifact round trips, publication-vs-transaction time,
  strict intraday eligibility, experimental-cohort invariance and caller roles.
- Main suite: **2,389 passed, 460 skipped**. Skips include live warehouse tests
  and the separately executed explicit-DSN PostgreSQL suite; they do not prove
  production rollout. MCP: **59 passed**.
- Ruff check/format and agent setup validation pass for repository/F04 files.
  An initial unrestricted Ruff run found nine preexisting errors in unrelated,
  untracked `scripts/film/update_storage_uri.py`; that user work was preserved
  and excluded from the subsequent lint scope and this PR.
- Production catalog closure, migration timing/performance, deployed consumer
  types, and real prospective sample accumulation remain rollout checks. No
  production migration, scoring, reconstruction, or rebuild was performed.
