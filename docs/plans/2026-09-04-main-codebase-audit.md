# Main codebase audit and remediation plan

Audit date: September 4, 2026. Reviewed branch: **main**, commit **4a798fde40480ac01d60a4d08c9c4eba42bed2c2**. This report assesses the local main checkout; it does not assert that the remote branch or production schema is identical.

PR preparation, September 5: the setup branch starts from **ec1c7a2c47dc6ab9d846867a6adb70042e169b38**, including PRs #113 and #117. Evidence links and audit measurements below remain pinned to the original reviewed commit. The accompanying [agent setup](../agent-setup.md) consolidates instructions and installation commands, and makes local database tests opt-in. This is partial progress on F27/F30; dependency locking, disposable-database CI (F26), and the application findings remain open. F01 implementation has not started.

## Assessment

The warehouse has a useful architecture and substantial domain knowledge, but its operational complexity has outgrown its orchestration and schema-management conventions. The largest risks are **silently incomplete ingestion, stale derived data, unreliable season boundaries, and ambiguous prediction provenance**. These deserve attention before cosmetic cleanup or a broad rewrite.

The best performance improvements are likely to come from fetching fewer unchanged games, rebuilding only affected seasons, and eliminating repeated full-history materialized-view work. Python micro-optimizations and additional indexes come after measurement.

The main source of “slop” is repeated policy and historical explanation: multiple runner registries, connection helpers, refresh lists, completion rules, and deployment paths. Many comments describe valuable production incidents, but those incidents have become substitutes for enforceable system-wide contracts. Fix the underlying contracts, then shorten the prose.

**Recommendation:** retain Python, dlt, Postgres, the raw/analytics/marts/API separation, and the current consumer interfaces. Introduce shared operational primitives and migrate responsibilities incrementally. A new orchestration platform, database replacement, or wholesale package rewrite is not justified by this audit.

## Scope and evidence

The inventory covers 538 tracked files, including 156 Python files, 203 SQL files, 107 Markdown files, nine workflows, and the separate MCP package. Approximately 58,900 lines of Python include tests; SQL accounts for approximately 19,900 lines. The review covered ingestion, HTTP and quota handling, flat-file parsing, database DDL and deployment, materialized views, public/API/scouting boundaries, modeling, live polling, CI, tests, and documentation. Primary execution paths received detailed source review; repository-wide scans checked definitions, duplication, dependencies, and hardcoded policies. This is not a claim that every line has been dynamically exercised.

Excluded existing untracked work: scripts/film/, migrations/060_tracking_video_assets.sql, and migrations/seed/team_aliases_seed_drive_film_log.sql. No application, pipeline, or schema changes were made for this audit.

### Verification performed

| Check | Result | Interpretation |
|---|---|---|
| Main pytest suite, with the project virtual environment on PATH | **2,113 passed; 447 skipped** | Strong local baseline; skipped database checks are an important coverage limit. |
| Separate MCP suite | **59 passed** | Run after installing the MCP package's declared development dependencies into the existing virtual environment. |
| Ruff on tracked Python/configuration files | **Passed** | Untracked film work was excluded from the assessment. |
| Ruff formatting on tracked Python files | **156 already formatted** | Formatting is not a remediation priority. |
| Mocked exhausted network failure in both box-score resources | **Both returned an empty result instead of raising** | Reproduces F01 without provider or database access. |
| Weekly EPA builder supplied only week-one plays | **No snapshot emitted** | Reproduces the missing next-week snapshot in F02. |
| Season-final predicate supplied 1,590 completed games out of 1,600 | **Returned true with ten games remaining** | Reproduces the premature completion decision in F03 with a mocked database result. |
| Materialized-view registry comparison | **All 50 marts found; no detected mart-to-mart ordering violation in the Python full-refresh list** | The problem is dependencies across workflows and compute stages, not a missing generic refresh list. |

The first main-suite invocation had six failures because CLI tests invoke a bare “python” executable; those six passed when the virtual environment was on PATH. That is a portability issue included in F27, not six application failures.

No database credentials were configured. No production writes, CFBD ingestion, live-role security probes, query-plan measurements, or production load tests were performed. Actual query latency, table/index sizes, data gaps, role memberships, and deployed migration state remain to be measured. Findings distinguish demonstrated code behavior from conditional runtime impact and optimization hypotheses.

### What should be preserved

- The raw-to-derived-to-API layering and the narrow MCP/PostgREST interface are sensible boundaries.
- Merge-based ingestion, explicit source keys, captured provider fixtures, and dlt variant-column handling encode real provider constraints.
- Several newer fan-out loaders already use bounded backlog selection and miss receipts. Extend those patterns instead of building another loader framework.
- The adjusted-EPA accumulator streams plays; simulation already uses NumPy effectively. Neither needs an automatic rewrite.
- Walk-forward modeling, coverage denominators, nullable unavailable metrics, fixed simulation seeds, and recap input hashes are useful foundations.
- The Python mart registry is complete for current marts. Existing schema manifests and validation SQL provide material for a safer release system.
- Owner-rights API views and private scouting schemas are intentional. Do not apply blanket security-invoker changes, blanket raw-table grants, or indiscriminate indexes.

## Priority and effort conventions

**P1:** fix before trusting affected output or expanding unattended operation. **P2:** address in the next hardening/optimization sequence. **P3:** maintainability work after correctness and release safety. No confirmed P0 production incident was established.

**Confirmed** means directly supported by executable source or a local reproduction; production occurrence may still depend on data. **Validate** means a material concern whose actual effect requires a representative database/runtime. **Opportunity** means a performance or organization improvement requiring a baseline.

Effort estimates describe focused engineering work: **S** = roughly half a day to one day; **M** = one to three days; **L** = three to seven days. Shared work overlaps across findings; do not add these mechanically.

## Findings: correctness and model integrity

### F01 — Loaders can turn exhausted failures into successful empty loads

**P1 · Confirmed and reproduced · M**

The team and player box-score resources re-raise the explicit rate-limit exceptions but catch every other exception and continue. Exhausted timeouts, server failures, authentication errors, malformed responses, and the local quota exception can therefore become missing data with a successful source result. Roster loading has a related broad catch, with logging but still no aggregate failure signal.

Evidence: [box-score loaders](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/game_stats.py#L89), [roster loader](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/rosters.py#L80), [request wrapper](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/base.py#L25).

**Change:** standardize outcomes as succeeded, expected-no-data, deferred, and failed. Only explicitly recognized provider no-data responses may skip successfully. Propagate exhausted transport/auth/quota failures, retain per-game/week receipts, and aggregate partial failure at the job boundary.

**Acceptance:** inject 401, 403, exhausted 429, 5xx, timeout, malformed payload, and local quota exhaustion. None may produce an unqualified successful load. Expected no-data fixtures remain nonfatal and visible.

**Implementation prepared — September 5, 2026:** the three affected resources now validate each response and propagate failures with endpoint/scope, original cause, and invocation-level request outcomes. The season summary and single/weekly/all-source CLI paths preserve failure status after earlier successes. Regression coverage includes the failure matrix, valid empty responses, request limits, client cleanup, and dlt wrapping; independent review also exercised actual offline `pipeline.extract`. Full offline validation passed (2,257 root tests, 447 skipped; 59 MCP tests). See [operational failure reporting](../warehouse-operations.md#box-score-and-roster-request-failures). Live rollout, historical gap detection, and corrective backfills remain unverified; this update does not claim those are complete.

### F02 — Weekly EPA omits the snapshot needed for upcoming games

**P1 · Confirmed and reproduced · M**

The boundary builder emits a snapshot for week W only after seeing a play from W. It emits nothing after the final observed week. Upcoming-game feature resolution reads those weekly snapshots; it cannot substitute the current full-season fit. With only week-one plays available, week-two predictions fall back to prior-season coefficients. After later completed weeks, predictions can similarly omit the most recently completed week's information until a new week has plays.

Evidence: [boundary builder](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/compute_adjusted_epa_week.py#L136), [feature resolution](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/build_features.py#L170).

**Change:** supply explicit target boundaries derived from schedule and as-of policy. Emit a snapshot for the next relevant scheduled week from all strictly earlier eligible plays, including transitions to postseason. Keep historical same-week exclusion intact.

**Acceptance:** week one complete/week two unplayed; midweek updates; bye weeks; sparse schedule weeks; regular-to-postseason transition; and late corrections all produce the intended snapshot without using target-week outcomes.

### F03 — A 99% completion ratio can freeze an unfinished season and train on it

**P1 · Completion predicate reproduced; downstream impact conditional · M**

Three paths define a finished season using at least 100 games and a 99% completion ratio. A 1,600-game schedule with ten games remaining passes. The unattended loader then skips games and other supposedly immutable sources; the refitter can treat that season as complete, and upcoming scoring selects the newest fit. Remaining games can receive a fit trained on earlier games in their own season, violating the stated strict seasonal walk-forward policy. Skipping the games source also prevents the unattended path from learning that the remaining games finished.

Evidence: [skip policy and completion test](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/load_season.py#L195), [training completion query](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/train_model.py#L702), [pipeline completion logic](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/run.py#L1065), [fit selection](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/score_fitted.py).

**Change:** centralize season lifecycle using known future games, actual completion, cancellations/postponements, calendar boundaries, and an explicit unresolved-data policy. Keep schedule reconciliation running. Independently enforce train-through-season < prediction-season at scoring time for the current model contract.

**Acceptance:** playoff tail, postponed final, cancelled game, incomplete provider schedule, and old unresolved rows cannot freeze a live season or admit an invalid fit. A completed historical season eventually closes without requiring every cancelled row to become completed.

### F04 — Historical reconstructions and published forecasts share ambiguous provenance

**P1 · Confirmed design defect · L**

The blend backfill defaults to full same-season EPA, while the optional as-of path is safer for historical evaluation. Both use the same model label and prediction table. Backfills assign kickoff dates as prediction dates. The accuracy mart selects the latest dated prediction without distinguishing reconstruction from an actually published forecast; residual-sigma estimation accepts dates at or before kickoff, which backdated rows satisfy. Even safe reconstructions need a distinct label because present-day corrected data and closing market lines were not necessarily available at the original prediction time.

Evidence: [backfill defaults and dating](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/compute_predictions.py#L645), [accuracy selection](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/marts/038_prediction_accuracy.sql#L108), [simulation residual selection](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/simulate_season.py#L767), [fitted historical scoring](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/score_fitted.py).

**Change:** record immutable prediction ID, fit ID, input generation/hash, actual creation/publication time, simulated as-of time, and evaluation mode. Separate published forecasts, walk-forward reconstructions, and hindsight experiments. Make as-of reconstruction the default; require explicit experimental labeling for hindsight. Backfills must not overwrite original published forecasts.

**Acceptance:** an experimental backfill cannot change published forecast accuracy or simulation calibration. Intraday post-kickoff rows are excluded from prospective evaluation. Existing ambiguous rows are retained and labeled unknown/reconstructed rather than assigned invented provenance.

### F05 — “Frozen” model versions are mutable and freshness checks are incomplete

**P2 · Confirmed · M/L; coordinate with F04**

Persisting a fit deletes and rewrites coefficients and metadata under the same model-version/train-through-season key. Automatic staleness checks compare feature-name sets, but do not capture changed formulas, data revisions, hyperparameters, or code. The maximum-existing-season rule also cannot repair arbitrary holes in the historical fit ladder. Reproducing an old prediction against today's same-named fit is therefore unreliable.

Evidence: [fit persistence](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/train_model.py#L531), [stale-season selection](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/train_model.py#L693), [feature contract check](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/train_model.py#L772).

**Change:** append immutable fits with a training manifest and content-derived identity. Include feature transformations, data cutoffs, parameters, code revision, and calibration method. Use explicit deployment pointers to promote a fit; find missing expected fits by set difference. Evaluate out-of-fold/held-out calibration against the current training-logit calibration before changing that methodology.

**Acceptance:** identical inputs reproduce the same fit identity; changed formulas invalidate a fit even if column names stay the same; historical predictions remain traceable after retraining; missing middle seasons are detected. Calibration changes must improve or preserve held-out reliability, not just training loss.

## Findings: schema, releases, and derived-data dependencies

### F06 — The documented migration/bootstrap path does not reconstruct the warehouse

**P1 · Confirmed · L**

The default migration list ends at root schema 018, while subsequent DDL lives under migrations/, api/, public/, functions/, and scouting/. There is no applied-migration/checksum ledger. The default runner replays files, including table-copy/swap operations. The README's migrations-before-ingestion sequence conflicts with core schema files that create indexes on dlt-created tables. Initial reference DDL also predates current normalized dlt shapes. Production state depends on accumulated ingestion and manual deployment history.

Evidence: [migration runner](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/run_migrations.py#L27), [core DDL](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/002_core.sql), [reference DDL](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/001_reference.sql), [partition migration](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/011_partition_plays.sql), [setup documentation](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/README.md).

**Change:** inventory the deployed catalog and establish a reviewed baseline matching representative dlt output. Add an applied-migration ledger, checksums, unique IDs, ordering, and a migration lock. Distinguish immutable data migrations from repeatable view/function definitions and explicit ingestion bootstrap. Retain old migrations as history rather than replaying them blindly.

**Acceptance:** one documented command builds a disposable database from zero, another upgrades a prior baseline, and a second invocation is a no-op. Changed applied migrations fail clearly. Roles, extensions, grants, seeds, nested tables, and consumer views are included.

### F07 — Generic mart deployment can remove consumer objects through CASCADE

**P1 · Confirmed · L; shares work with F06**

Many mart definition files drop materialized views with CASCADE. The generic runner applies sorted mart files with per-file commits and does not recreate dependent api/public objects. An individual --only deployment can remove a dependency closure beyond its requested file. Handwritten deployment manifests sometimes restore that closure, but the generic command does not enforce it.

Evidence: [mart runner](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/run_marts.py#L64), [play EPA DDL](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/marts/010_play_epa.sql#L5), [schema deployer](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/deploy_schema.py), [deployment manifests](https://github.com/rstover-fo/cfb-database/tree/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/deploys).

**Change:** make a dependency-aware release manifest the supported path. Preview objects removed/recreated, include consumer definitions and grants, and reject an incomplete closure. Use transactional changes where practical and staged replacements for long builds; account for Postgres object dependencies rather than assuming a rename repoints existing views.

**Acceptance:** change an upstream mart in a disposable database, inject a downstream failure, and verify the documented rollback/recovery leaves API contracts and grants intact. Partial deployment cannot silently strand consumers.

### F08 — Partition and historical-year configuration stops at 2026

**P1 before 2027 ingestion · Confirmed repository limit; validate live catalog · M**

The plays partition migration creates years 2004–2026. No automated future-partition lifecycle was found. Configured historical year ranges also end at 2026. The load verifier checks a partition's name after ingestion, rather than ensuring an attached partition with correct bounds exists before loading.

Evidence: [partition creation](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/011_partition_plays.sql#L54), [year configuration](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/config/years.py), [partition verification](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/verify_load.py#L93).

**Change:** create a bounded future horizon during a preflight/maintenance step, inspect attachment and bounds, and separate fixed coverage-start years from a rolling current/end year. Review how late historical records are routed.

**Acceptance:** a simulated year rollover loads the first 2027 game/play without an emergency migration. A correctly named but unattached or incorrectly bounded table fails preflight. Verify existing production partitions before creating anything.

### F09 — The plays table swap can leave expected indexes on the old table

**P1 to inspect; P2 remediation if affected · Confirmed migration-sequence flaw · M**

Initial core DDL creates named plays indexes. The partition migration renames the original table to plays_old, then creates indexes on the new table using the same names and IF NOT EXISTS. Existing index names stay associated with the old relation, so the later statements can skip creating the intended indexes. Subsequent same-name statements are not a repair. Production may already have been repaired manually; its catalog must decide that.

Evidence: [original indexes](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/002_core.sql#L48), [table swap and new indexes](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/011_partition_plays.sql#L86), [later index definitions](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/016_analytics_indexes.sql).

**Change:** verify index target, definition, validity, and partition coverage through catalog queries. Repair only missing useful indexes with safe builds, explicitly rename retained old indexes, and retire old storage after validation. Index-name existence alone is insufficient; PostgreSQL explicitly provides no definition-equivalence guarantee for IF NOT EXISTS. [PostgreSQL CREATE INDEX documentation](https://www.postgresql.org/docs/current/sql-createindex.html).

**Acceptance:** an upgrade fixture follows the old table swap and proves the final parent/partitions own the expected valid indexes. Production EXPLAIN results and write overhead justify each retained index.

### F10 — Workflow dependencies drift even though the full mart order is valid

**P1 · Confirmed · L**

Daily loading refreshes the full mart set before computing new model inputs, then refreshes a manually selected subset. That subset omits epa_crossvalidation, which depends on newly computed team-adjusted EPA. Flat-file loading runs on a separate time-based schedule with no completion dependency or downstream refresh, so external-rating consumers can lag. The coach-tenure backfill path explicitly skips refresh because its comment says it feeds no mart, while the new coach_tenures mart reads that table.

Evidence: [daily sequence](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/.github/workflows/daily-load.yml#L79), [flat-file workflow](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/.github/workflows/flat-files.yml), [backfill refresh exclusion](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/.github/workflows/backfill-sources.yml#L134), [coach-tenure mart](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/marts/048_coach_tenures.sql), [crossvalidation mart](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/marts/044_epa_crossvalidation.sql).

**Change:** declare dependencies from source resource through raw table, computed table, mart, and API contract. Plan work from changed assets/partitions and successful upstream generations. Workflow start times should not serve as dependency guarantees. Add the immediate missing refreshes before undertaking the larger orchestration change.

**Acceptance:** a coach-tenure backfill, external-rating update, or adjusted-EPA recompute automatically selects all affected descendants. A delayed upstream workflow cannot publish downstream success prematurely. The generated full-refresh order remains complete and acyclic.

### F11 — Refreshing after failures can publish mixed generations; full refreshes repeat history

**P1 for dependency gating; P2 for cost · Confirmed / Opportunity · L**

The loader can refresh marts after a source fails. The Python refresher continues after an upstream view fails, allowing descendants to refresh from old upstream contents. Per-view commits make mixed generations visible. The SQL refresh-all function uses a different, nonconcurrent transaction pattern. Separately, daily full refreshes repeatedly scan historical play data across several large marts.

Evidence: [load failure/refresh flow](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/load_season.py#L604), [refresh loop](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/refresh_marts.py#L235), [SQL refresher](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/functions/refresh_all_marts.sql), [game EPA aggregation](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/marts/002_game_epa_calc.sql).

**Change:** mark descendants blocked when required upstream assets fail, record generation IDs, and expose current/partial/stale state honestly. Align Python and RPC refresh semantics. Measure heavy scans, then materialize reusable game/season facts and update dirty seasons with validation before publication. CONCURRENTLY reduces reader blocking; it still replaces a materialized view's contents and is not incremental maintenance. [PostgreSQL REFRESH documentation](https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html).

**Acceptance:** injected upstream failure cannot yield a fresh-success label for affected descendants. An unchanged historical season incurs no recomputation in the optimized daily path. A correction rebuild agrees with a full reference rebuild within specified numeric tolerances. Readers retain the last valid generation during long work.

### F12 — Historical correction campaigns can finish before corrections reach player analytics

**P1 · Confirmed documented gap · L**

Campaign finalization refits adjusted EPA from core.plays and refreshes views, while the campaign updates stats.play_stats/advanced game statistics. Historical player EPA through 2025 lives in a separately staged build that requires a manual full rebuild. The code explains this limitation accurately, but the campaign's finalized watermark does not mean all affected products contain its corrections. The historical/live split is also hardcoded by year.

Evidence: [campaign finalization](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/backfill_refresh.py#L604), [historical build](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/migrations/022_player_epa_staged_build.sql), [historical/live union](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/marts/011_player_game_epa.sql).

**Change:** extract a resumable, per-season rebuild operation with input and output receipts. Feed affected seasons into it from the correction campaign. Publish campaign completion only after required descendant receipts succeed, or explicitly distinguish source completion from product completion. Manage the historical/live cutoff as recorded state.

**Acceptance:** correct a historical player play-stat fixture, run the campaign to completion, and observe the correction in player-game and player-season API output. Interruption resumes the missing season without replaying all twelve seasons in one transaction.

## Findings: ingestion cost and resource use

### F13 — “Incremental” ingestion often reloads the current season in full

**P2, high expected return · Confirmed / Opportunity · L**

For many sources, incremental means selecting the current year and merging it again. Play-stat loading fans out over all completed games on every run, while several weekly resources visit a fixed complete week range, including unchanged or unpublished weeks. This is substantially different from the bounded set-difference behavior already implemented for some newer fan-out sources. Repeated full reference loading adds smaller avoidable cost.

Evidence: [play-stat fan-out](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/stats.py#L418), [passing resources](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/passing.py), [rushing resources](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/rushing.py), [existing bounded fan-out patterns](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/run.py).

**Change:** plan work at resource/game/week grain. Fetch newly completed or changed units, retain a correction lookback, revisit partially parsed data, and run older corrections as explicit bounded campaigns. Give reference and seasonal assets different cadences. Reuse existing receipts and backlog mechanisms, with resource-level failure isolation.

**Acceptance:** a second run over unchanged fixtures makes only the documented reconciliation calls; a corrected game remains eligible; a backlog never exceeds its configured cap. Record actual calls, rows changed, and elapsed time before claiming a savings percentage.

### F14 — API-budget accounting measures successful logical requests, not actual attempts

**P2 · Confirmed · M**

The request wrapper records a call only after the client returns successfully. Retries and terminal failures consume attempts without equivalent accounting. Budget state is a local JSON file, so fresh workflow runners and separate jobs do not share one durable account-wide total. Source estimates also diverge from actual fan-out cost and cannot safely govern execution.

Evidence: [accounting boundary](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/base.py#L25), [local budget state](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/utils/rate_limiter.py), [transport retries](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/utils/api_client.py), [source estimates](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/load_season.py).

**Change:** record or reserve attempted calls at the transport boundary in durable account/month state shared by daily, backfill, and live jobs. Reconcile provider-reported usage when available. Derive estimates from the concrete selected work units, while retaining safety margin for retries. Make concurrent reservations atomic.

**Acceptance:** retry exhaustion counts every attempted call; overlapping jobs cannot exceed the shared reservation; month rollover works in a long-lived process; a new runner retains prior usage. Provider billing semantics are documented separately from local attempt counts.

### F15 — Week and season-type policies differ across resources

**P2 · Confirmed inconsistency; provider coverage impact to validate · M**

Plays requests regular weeks 1–15 and postseason week 1; box-score and weekly runner ranges differ from passing/rushing/success resources. Independently hardcoded ranges can miss provider weeks or make unnecessary calls as schedules change. The calendar resource merges by season/week without season type; whether that is a collision depends on the provider's calendar grain and needs a fixture check.

Evidence: [plays requests](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/plays.py), [box-score week ranges](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/game_stats.py), [calendar key](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/reference.py#L210), [pipeline week selection](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/run.py).

**Change:** derive eligible (season, season_type, week) units from schedule/calendar data with endpoint-specific supported-coverage rules. Keep those rules in one tested registry. Confirm calendar key uniqueness before changing its grain and migrating historical data.

**Acceptance:** fixtures with an additional regular week and multiple postseason rounds select every supported unit once. Future unpublished weeks are deferred. Calendar uniqueness and null handling are asserted against representative payloads.

### F16 — Flat-file ingestion holds several full-file representations in memory

**P2 · Confirmed architecture; peak RSS unmeasured · L**

Fetching returns all bytes in memory. Parquet parsers read complete Arrow tables and convert them to Python dictionaries; the coordinator materializes parser output and additional kept/main/child collections. Large play-level files can coexist as raw bytes, Arrow buffers, and multiple Python object collections.

Evidence: [file fetching](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/utils/file_fetcher.py#L28), [flat-file coordinator](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/flat_files.py#L659), [Parquet parsers](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/flatfile_parsers/ncaa.py), [ESPN parser](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/flatfile_parsers/espn.py).

**Change:** spool downloads to bounded temporary storage while hashing; use conditional requests where supported; iterate Parquet batches with required columns; normalize/load bounded batches into staging. Preserve whole-source validation before promotion, especially unmapped-team thresholds and parent/child consistency.

**Acceptance:** representative large files and a scaled fixture demonstrate bounded peak RSS, equivalent rows/keys, unchanged rejection behavior, and cleanup after cancellation. Streaming must not publish half a file that later fails validation.

### F17 — Hash-only load receipts prevent clean replay after parser or mapping fixes

**P2 · Confirmed · M**

An already-loaded decision uses only source and file SHA-256. Identical bytes remain skipped after a parser, schema, or team crosswalk changes. A previously accepted file with some dropped unmapped records is especially important: fixing the mapping alone does not make the source eligible again. The existing cadence override is not a complete content-reprocessing contract.

Evidence: [hash lookup](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/utils/load_ledger.py#L50), [flat-file skip decision](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/load_flat_files.py#L358), [flat-file resource construction](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/flat_files.py#L659).

**Change:** store parser/schema/mapping versions in receipts and add explicit reprocessing with retained lineage. Quarantine unmapped rows and associate them with their source receipt. Reprocessing should repair rows predictably without destructive blind replacement.

**Acceptance:** unchanged bytes plus a mapping/parser version change can reprocess; truly identical processing skips; previously unmapped records recover; old receipts remain auditable.

### F18 — Flat-file retry behavior is weaker than the CFBD client

**P2 · Confirmed · S**

The flat-file fetcher parses Retry-After with int(), sleeps without a cap, and sleeps again even on the terminal 429 attempt before falling through to an “unreachable” error. HTTP-date or malformed headers can fail during parsing. The CFBD client already contains more careful policy, so there are two drifting implementations of the same transport concern.

Evidence: [flat-file retry loop](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/utils/file_fetcher.py#L43), [CFBD retry handling](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/utils/api_client.py).

**Change:** share tested header parsing and bounded backoff primitives while retaining provider-specific statuses, budgets, and retry limits. Preserve the terminal HTTP error and avoid sleeping when no attempt remains.

**Acceptance:** numeric, HTTP-date, negative, malformed, and excessive Retry-After values behave predictably; terminal attempts do not sleep; total waiting is bounded and tests use a fake clock.

## Findings: freshness, live behavior, and API contracts

### F19 — Database maintenance timestamps are an unreliable freshness signal

**P1 for trustworthy monitoring · Confirmed · M/L**

The freshness mart infers loading from vacuum/analyze timestamps, with a server-start fallback. Maintenance can occur without fresh source data, and fresh ingestion need not immediately trigger maintenance. More fundamentally, days-since-activity and is_stale are stored in a materialized view: if the refresher stops, their values stop aging. The tracked-table list also trails the expanding source surface. Existing load verification does not consistently prove that outputs belong to the latest required generation.

Evidence: [freshness definition](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/marts/028_data_freshness.sql#L65), [cached age/status](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/marts/028_data_freshness.sql#L108), [load verification](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/verify_load.py).

**Change:** record actual extraction/load/compute success timestamps, source watermarks, expected coverage, row deltas, and run/generation IDs. Calculate staleness at query time. Keep pg_stat activity as maintenance telemetry, not proof of source freshness. Derive asset coverage from the same source/dependency registry.

**Acceptance:** stopping all jobs causes a healthy asset to become stale without refreshing the freshness view. ANALYZE cannot make old source data fresh. A successful zero-row/no-data result is distinguishable from an error or incomplete coverage. Prediction verification checks the expected game set and generation, not merely historical existence.

### F20 — The live polling guard can stop coverage at UTC midnight

**P1 · Confirmed conditional defect · S/M**

The workflow deliberately continues from Saturday into Sunday UTC, but its guard checks only games whose start_date falls on the current UTC date. If an ongoing Saturday-started game crosses midnight and no games start on Sunday UTC, the guard becomes false while that game is still active. Casting the stored timestamp to date also makes session timezone and index behavior part of the query.

Evidence: [schedule and guard](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/.github/workflows/live-scoreboard.yml#L24).

**Change:** query an explicit UTC start-time window that includes recent ongoing games, plus a reliable terminal-state rule and grace period. Use range predicates and record the last successful observation. Keep Saturday-only coverage explicit; expand to weekday games only if that is the desired product contract.

**Acceptance:** a game crossing midnight, a delayed late game, overtime, cancellation, and an empty slate all produce the expected polling decision. The continuation window does not depend on another game's kickoff date.

### F21 — Scoreboard deduplication ignores status-only transitions

**P1 · Confirmed conditional defect · S**

The snapshot hash includes period, clock, score, and possession but not status. A zero-clock in-progress state followed by final with the same hashed values is skipped. Other persisted fields can also change independently of the chosen hash. The issue is which fields define equality, not MD5 cryptographic strength.

Evidence: [snapshot hash](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/poll_scoreboard.py#L141), [poll/write flow](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/poll_scoreboard.py#L431).

**Change:** define a normalized snapshot contract including lifecycle status and all semantically relevant changing fields, or separate game-state and derived-probability updates. Track observed-at independently from state-changed-at.

**Acceptance:** identical ticks remain idempotent; final/delayed/cancelled status-only transitions are stored; corrected scores and changed probability inputs are handled explicitly.

### F22 — One recap database error can poison the remaining batch

**P2 · Confirmed conditional defect · S/M**

The batch catches per-game exceptions and continues on the same connection without rolling back. A SQL error leaves the transaction aborted, so subsequent games fail as well. Fact reads also keep a database transaction open while waiting for the external model response.

Evidence: [fact/model/write sequence](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/generate_recaps.py#L527), [batch exception handler](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/generate_recaps.py#L584).

**Change:** finish the fact-read transaction before the network call, use a bounded write transaction per game, and roll back failures. Retain prompt/input hashes and add an idempotent generation receipt if retrying failed writes would otherwise repeat paid model calls. Batch reusable fact queries where measurement supports it.

**Acceptance:** one injected SQL failure does not prevent the next game from succeeding; no transaction remains open during the model request; a retried write does not needlessly regenerate the same recap.

### F23 — API definitions duplicate business logic and can present inconsistent seasons

**P2 · Confirmed · M**

The matchup API independently recomputes history instead of sharing the existing matchup mart. Its completed-game filter lacks the mart's nonnull-score requirement, allowing missing-score rows to fall into tie counts. “Last ten years” windows use different fixed cutoff years. Team detail and matchup independently choose latest team-summary and latest EPA seasons, so a current-season label can accompany prior-season EPA without explicit metric-season labeling. Player search accepts an arbitrary SQL p_limit, including NULL, without a database-level cap.

Evidence: [matchup API](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/api/004_matchup.sql), [matchup mart](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/marts/007_matchup_history.sql), [team detail](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/api/001_team_detail.sql), [search limit](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/public/009_player_search_function.sql#L6).

**Change:** establish shared canonical matchup logic and an explicit as-of-season definition for rolling windows. Surface metric-specific seasons/coverage or align joins deliberately. Enforce bounds and null handling inside public RPCs; client validation alone does not cover direct callers. Coordinate response-shape changes with cfb-app/cfb-scout rather than silently changing contracts.

**Acceptance:** completed games with missing scores are excluded consistently; a rolling-window boundary moves with its declared as-of season; 2026 summary/2025 EPA is explicit; negative, NULL, and excessive limits cannot cause unbounded output.

### F24 — Connection and timeout policies are duplicated; MCP discards HTTP pooling

**P2 · Confirmed / Opportunity · M**

There are 18 get_db_url implementations with differing environment fallbacks and timeout behavior. Some rely on startup options, while the refresher documents that the deployed pooler ignores them and sets session options explicitly. Flat-file ledger helpers open separate connections for individual lookups/receipts. The MCP PostgREST client constructs a new AsyncClient for each request, preventing reuse across calls.

Evidence: [refresh connection policy](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/refresh_marts.py#L218), [ledger connections](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/utils/load_ledger.py#L50), [MCP client](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/mcp/src/cfb_mcp/postgrest.py#L133).

**Change:** introduce one DSN resolver and explicit connection profiles for reads, loading, computation, and migration, including connection/query/lock timeouts and application names. Prefer explicit environment overrides consistently. Pass a connection or unit-of-work into ledger operations and batch reads. Reuse an injected AsyncClient over the MCP server lifespan with bounded connection limits and clean shutdown. HTTPX recommends client reuse for pooling. [HTTPX async documentation](https://www.python-httpx.org/async/).

**Acceptance:** all callers resolve the same supplied configuration; actual session timeouts are verified through the intended pooler; a repeated MCP workload reuses connections; clients close on shutdown. Do not add a database pool to every short-lived script without a measured need.

### F25 — Free-form analyst SQL needs adversarial validation of its actual role boundary

**P1 validation; remediation depends on result · Validate · M/L**

The publicly granted RPC accepts arbitrary SELECT/WITH, switches locally to analyst_ro, enables read-only mode, and applies an outer 200-row cap. This is a thoughtful boundary, but its checks and current validation do not establish resistance to role-setting/session-changing functions, callable public functions, expensive aggregates, or large single-value results. A row cap does not bound computation or result bytes. PostgreSQL checks SET ROLE authority against session_user, even after a prior role change; consequently a local role switch is not automatically proof that a query cannot regain another role available to the connection's original identity. [PostgreSQL SET ROLE documentation](https://www.postgresql.org/docs/current/sql-set-role.html).

Evidence: [role grants and executor](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/public/012_run_analyst_query.sql#L64), [current validation](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/schemas/public/validation_run_analyst_query.sql).

**Change:** test the real PostgREST authentication/session-role arrangement in an isolated equivalent environment. Examine role and read-only setting changes, function execution grants, restricted-schema reads, resource exhaustion, effective top-level timeouts, and response-size caps. If containment fails, use a dedicated genuinely restricted database login in an isolated query service, or replace free-form access with allowlisted RPCs. A stricter text regex is not a sufficient security boundary.

**Acceptance:** adversarial queries cannot regain broader privileges, write, reach restricted schemas/functions, or evade configured time/size limits. Preserve harmless analytical SELECT capabilities intentionally. **This audit does not claim a demonstrated production privilege escalation.**

## Findings: tests, organization, and maintainability

### F26 — CI does not prove new SQL works on a clean or upgraded database

**P1 · Confirmed coverage gap · L**

CI passes the shared Supabase URL into the main suite. Database fixtures use that connection, while absent credentials cause many checks to skip. Those checks validate an already deployed schema, not necessarily the schema introduced by the PR. A large unit suite and SQL text assertions therefore coexist with untested bootstrap, migration-order, grant, and transactional behavior. The shared connection is not enforced read-only by the fixture itself.

Evidence: [CI test job](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/.github/workflows/ci.yml#L25), [database fixtures](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/tests/conftest.py#L52), [schema tests](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/tests/test_api_views.py), [deployment tests](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/tests/test_deploy_schema.py).

**Change:** separate unit tests, disposable database integration/upgrade tests, and explicit read-only deployed smoke tests. Build representative dlt-normalized fixture tables, apply the schema, execute real SQL, and validate grains, values, grants, dependency recovery, and transaction behavior. Keep production credentials out of ordinary PR test jobs. Required SQL integration checks must fail when their environment is unavailable rather than silently skip.

**Acceptance:** CI catches a missing nested column, invalid migration order, lost API grant, broken table swap, and incorrect aggregation on a small known fixture. Fork PRs can run meaningful database checks without production secrets. The existing fast unit suite remains available.

### F27 — Dependency resolution and a few CLI tests are not reproducible

**P2 · Confirmed · M**

Operational dependencies generally have broad minimum versions and no committed lockfile. CI installs current Ruff separately. An unchanged commit can therefore receive a new resolver result or lint policy. The MCP package has some compatibility bounds, but that is only partial protection. Six CLI tests also depend on a bare python executable instead of the interpreter running pytest.

Evidence: [warehouse package](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/pyproject.toml), [MCP package](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/mcp/pyproject.toml), [CI dependency installation](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/.github/workflows/ci.yml#L21), [CLI tests](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/tests/test_seed_team_xwalk.py).

**Change:** lock supported operational/dev extras and the separate MCP environment; install in frozen mode in CI and workflows. Update dependencies through explicit tested changes. Use sys.executable for subprocess tests and document the supported runtime/install command.

**Acceptance:** two clean installs of the same commit resolve identically; a dependency update has a reviewable diff; CLI tests work when only the virtual environment's interpreter is invoked by absolute path.

### F28 — Endpoint configuration is largely disconnected from execution

**P2 · Confirmed · M/L**

The substantial EndpointConfig registry is referenced by its tests but does not drive runtime source definitions or dispatch. Runtime policy is repeated across source decorators, runner maps, source order, call estimates, and workflow allowlists. The generic resource factory and config defaults still use replace, contrary to the project's merge-first convention. This leaves a tested configuration surface that can disagree with actual behavior.

Evidence: [endpoint registry](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/config/endpoints.py), [generic factory default](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/sources/base.py#L39), [runner dispatch](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/run.py), [season orchestration](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/load_season.py).

**Change:** either retire unused registry fields or make a compact authoritative resource specification drive source selection, expected grain, supported scope, cadence, cost planning, and dependency metadata. Retain custom extraction functions for endpoint quirks. Make destructive disposition explicit rather than a generic default.

**Acceptance:** adding one resource does not require editing several unrelated lists. Tests compare declared keys/tables/dependencies with actual dlt resources. No unused configuration remains presented as an enforcement mechanism.

### F29 — Large scripts mix domain logic, database access, and orchestration

**P3, enable incrementally during P1/P2 work · Confirmed organization debt · L**

The main pipeline runner is roughly 1,700 lines. Feature building, season loading, campaign coordination, training, and experimentation contain substantial domain and persistence code under scripts/, with scripts importing other scripts. MCP tools also live in a large single server module. Repeated connection/table-existence helpers and SQL definitions show that responsibilities are crossing module boundaries.

Evidence: [pipeline runner](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/src/pipelines/run.py), [feature builder](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/build_features.py), [campaign runner](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/backfill_refresh.py), [MCP server](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/mcp/src/cfb_mcp/server.py).

**Change:** move touched functionality into cohesive ingestion, database, orchestration, feature/model, and serving modules. Keep command-line scripts as thin adapters. Extract one shared concern at a time, starting with connection policy, season lifecycle, receipts, and dependency planning. Keep model math pure and persistence explicit. Avoid a large rename-only migration or abstraction for every endpoint.

**Acceptance:** production logic is importable without script-path manipulation; CLI flags and published interfaces remain compatible; duplicated policy is removed rather than merely relocated; tests exercise shared domain behavior directly.

### F30 — Documentation and comments contain competing truths and repetitive incident history

**P3 · Confirmed · M**

README inventory/setup descriptions lag the repository. CLAUDE.md and schema-contract documentation accumulate operational history alongside current rules. Many source headers retell multiple PR incidents; useful behavior is sometimes explained at greater length than it is enforced. The coach-tenure refresh comment in F10 is a concrete example of stale prose preserving an incorrect operational assumption. Numeric SQL naming also has collisions across independently managed directories, complicating discovery.

Evidence: [README](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/README.md), [project instructions](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/CLAUDE.md), [schema contract](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/docs/SCHEMA_CONTRACT.md), [season-loader policy commentary](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/load_season.py#L160), [backfill workflow commentary](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/.github/workflows/backfill-sources.yml#L134).

**Change:** keep one concise current architecture/setup guide and one generated asset/API inventory. Move durable decisions into short ADRs and historical incidents into dated records. Label completed plans as historical. Keep local comments for nonobvious invariants, provider caveats, null semantics, and rationale that tests cannot explain. Assign new migrations globally unique IDs without rewriting applied history.

**Acceptance:** a new contributor can install, build a fixture warehouse, run tests, and trace one source to its API from current documentation. Inventory generation detects drift. Remove repetition only after preserving its important constraints in code/tests or a linked decision record.

### F31 — Compute optimization should target measured allocations and repeated work

**P2 after I/O baselines · Opportunity · M per justified hotspot**

The streaming ridge accumulator allocates small NumPy arrays and an outer product for every play. Drive-chain modeling materializes era-wide plays as Python dictionaries. Feature construction and experimental screening perform repeated extraction/aggregation over overlapping data. These are credible CPU/RSS targets, but the existing streaming accumulator and vectorized simulation are sound starting points; no production profile was available to rank them above database or provider costs.

Evidence: [per-play accumulation](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/compute_adjusted_epa.py#L119), [drive-chain fetch](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/compute_drive_chain.py#L739), [feature extraction](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/build_features.py#L762), [screening](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/screen_preseason_features.py), [simulation](https://github.com/rstover-fo/cfb-database/blob/4a798fde40480ac01d60a4d08c9c4eba42bed2c2/scripts/simulate_season.py#L256).

**Change:** profile representative seasons/eras. If justified, batch sufficient-statistic updates with stable team indices, stream drive transitions, and cache immutable experiment inputs keyed by generation. Preserve exact update semantics and deterministic seeds. Tune BLAS/thread usage against the actual runner rather than adding parallelism blindly.

**Acceptance:** compare runtime and peak RSS against the same dataset, with numerical parity/tolerances, seed reproducibility, and output-coverage checks. Reject optimizations that trade away temporal correctness, explainability, or meaningful memory bounds for a small benchmark gain.

## Target architecture

Keep the existing warehouse layers, with explicit contracts between them:

1. **Resource specification:** endpoint/file source, natural grain, scope, coverage policy, cadence, retry policy, and estimated units of work. This is executable metadata, not a parallel descriptive catalog.
2. **Ingestion adapters:** provider-specific normalization and dlt loading. Adapters produce structured outcomes and source receipts; they do not decide global refresh order.
3. **Operational metadata:** reuse and extend the existing meta tables for runs, attempts, source watermarks, processing versions, dirty seasons/games, and published generations. Distinguish attempts, committed source data, and verified downstream publication.
4. **Feature/model computation:** pure computation separated from reads/writes, with explicit as-of inputs and immutable fit/prediction identities. Corrections create a new generation instead of silently changing an old forecast's meaning.
5. **Dependency planner:** one graph from resources to computed assets and marts. It selects affected work, enforces successful predecessors, checkpoints progress, and records blocked descendants. GitHub Actions initially remains the scheduler invoking this planner.
6. **Stable serving contracts:** api/public views and RPCs retain documented grains, bounds, null semantics, and access roles. MCP uses a reusable HTTP client and thin domain tools. Consumer-visible changes are versioned and coordinated.
7. **Release system:** baseline plus immutable migrations and repeatable object definitions, with a dependency closure, checksum ledger, catalog/grant validation, and disposable-database tests.

An early implementation can live within the existing src package. Suggested module responsibilities are db/configuration, ingestion/resources, orchestration/planning, metadata/receipts, compute/features, compute/models, and contracts. Move code into these boundaries when fixing it; do not begin with a mass directory reorganization.

## Implementation sequence

### Phase 0 — Establish a trustworthy baseline and validate conditional risks

**First one to two focused days, before performance tuning.**

- Capture read-only production catalog state: tables, partitions and bounds, index definitions/validity, views/functions, grants, role memberships, extensions, row estimates, sizes, and schema drift from main.
- Inspect whether the plays index/partition issues actually exist, and whether API definitions deployed today match these files.
- Run F25's adversarial query tests in an isolated environment reproducing the actual PostgREST session-role arrangement. Prioritize containment remediation immediately if a boundary failure is reproduced.
- Record one representative daily run's stage durations, attempted provider calls, rows loaded/changed, refresh duration, failure/deferred counts, and process peak RSS. Reuse logs where possible before triggering expensive work.
- Preserve current published predictions and model metadata before any retrospective repair. Separate unknown provenance from known prospective output.
- Add regression fixtures for the reproduced failure swallowing and weekly-boundary defects so the fixes have concrete targets.

**Exit:** known deployed baseline, a short list of confirmed live risks, representative performance measurements, and no destructive “cleanup” based only on file names or estimates.

### Phase 1 — Fix incorrect or misleading behavior

**First implementation tranche; roughly three to seven engineering days depending on model-provenance migration scope.**

- Correct F01 error outcomes and F22 rollback boundaries; standardize retry primitives for F18.
- Fix F02 weekly snapshots and F03 closure/fit eligibility using a shared season policy. Start F15 by replacing the demonstrated inconsistent ranges with tested schedule-driven selection.
- Correct F20 midnight polling and F21 lifecycle deduplication.
- Add the missing immediate refresh dependencies in F10 and block affected descendants after failures in F11.
- Prevent hindsight/reconstruction rows from being reported as published prospective accuracy while the full F04/F05 lineage migration is prepared.
- Make freshness age dynamically in F19, clearly labeling maintenance-based signals as provisional until receipts are available.

**Exit:** no silent loader success on real failures, no premature season closure, upcoming EPA includes all eligible prior weeks, live terminal states persist, and affected data cannot be labeled freshly complete after an upstream failure.

### Phase 2 — Make schema and publication changes reproducible

**Approximately one to two engineering weeks; can overlap small Phase 1 fixes once the baseline is captured.**

- Deliver F06/F26/F27: a fixture warehouse, real SQL integration/upgrade tests, a migration ledger, and locked environments.
- Repair F08/F09 using the actual catalog; add rollover and index-target assertions.
- Deliver F07's complete release closure and recovery checks. Deprecate generic unsafe apply paths rather than maintaining two supported release semantics.
- Migrate model/prediction provenance for F04/F05, preserving old rows and labeling ambiguity. Add scoring-time eligibility constraints.
- Implement the core shared configuration, resource definitions, and dependency planner for F24/F28/F10. Replace existing lists gradually and remove duplicates once parity is established.

**Exit:** a new environment builds from the repository; an upgrade is repeatable; dependent API objects/grants survive deployment; model output is reproducible; one plan explains what a run will read, change, and publish.

### Phase 3 — Reduce repeated work and close correction/freshness gaps

**Approximately one to two engineering weeks for the highest-value measured work.**

- Unify attempt/run/asset receipts and implement durable quota accounting (F14/F19).
- Apply incremental unit planning with correction windows (F13); turn historical player rebuilds into resumable affected-season operations (F12).
- Measure and replace the most expensive full-history refresh stages with dirty-season/game facts (F11). Keep a full rebuild path as the correctness reference.
- Stream the largest flat-file paths and make reprocessing version-aware (F16/F17).
- Reuse MCP HTTP connections and batch ledger access (F24); consolidate matchup semantics and RPC bounds (F23).

**Exit:** unchanged input does materially less work; corrections propagate end to end; asset freshness is based on source/publication receipts; memory stays within an agreed runner budget; before/after performance is documented.

### Phase 4 — Consolidate code and remove obsolete prose

**Approximately three to five engineering days, plus only justified compute optimizations.**

- Finish F29's module extractions around responsibilities already stabilized in earlier phases.
- Retire the unused or superseded EndpointConfig paths and duplicated maps/helpers (F28).
- Generate asset inventory/contract documentation, rewrite current setup instructions, and move incident history to dated decisions/records (F30).
- Address F31 hotspots only where profiles demonstrate meaningful benefit.

**Exit:** one source of truth for operational policy, thin CLI adapters, current reproducible setup documentation, and fewer independently editable definitions of the same rule.

**Planning range:** allow roughly **five to eight engineer-weeks** for the core hardening and a focused set of measured optimizations. This is an initial capacity estimate, not a delivery commitment. Large production data repairs, consumer migrations, and a major full-refresh redesign can extend it. Re-estimate after Phase 0; deliver the small correctness fixes well before the entire program is finished.

### Work packets and dependencies

Each packet may require several small PRs; these are ownership boundaries, not instructions to create enormous combined changes.

| Packet | Deliverable | Findings | Dependencies / rollout gate |
|---|---|---|---|
| 01 | Typed ingestion outcomes, shared retry helpers, per-game recap transaction recovery | F01, F18, F22 | Regression fixtures; can start immediately |
| 02 | Shared season/calendar policy, trailing weekly EPA snapshots, scoring eligibility | F02, F03, F15 | Provider/schedule fixtures; compare affected predictions |
| 03 | Live observation-window and status-change correctness | F20, F21 | Deterministic clock/status tests |
| 04 | Immutable fits and explicit forecast/reconstruction lineage | F04, F05 | Preserve existing outputs; schema path from 05; consumer contract review |
| 05 | Catalog baseline, migration ledger, fixture DB CI, frozen environments | F06, F26, F27 | Phase 0 catalog; established dlt fixture shapes |
| 06 | Partition preflight and index ownership repair | F08, F09 | Catalog inspection and upgrade tests from 05 |
| 07 | Dependency-complete schema releases and recovery | F07 | 05; validate API/grant closure before deployment |
| 08 | Authoritative resource metadata, dependency planner, failure propagation | F10, F11, F28 | Immediate dependency fixes first; shared configuration; expand by subsystem |
| 09 | Unified operational receipts, dynamic freshness, durable call reservations | F14, F19 | 05 and 08 contracts; distinguish attempt/load/publish success |
| 10 | Incremental work units and resumable historical correction propagation | F12, F13; cost portion of F11 | 08/09; parity against full rebuild and provider correction policy |
| 11 | Bounded flat-file loading and version-aware reprocessing | F16, F17 | 09 receipt contract; staging publication tests |
| 12 | Canonical API metrics/bounds and shared DB/HTTP clients | F23, F24 | Consumer contract fixtures; shared DB policy can start earlier |
| 13 | Analyst SQL boundary verification and any required containment change | F25 | Validation starts in Phase 0; deploy changes only from a concrete reproduced result |
| 14 | Cohesive modules, current documentation, measured compute refinements | F29, F30, F31 | Extract as earlier packets touch code; final cleanup follows stable contracts |

## Performance measurement and optimization plan

Measure the following on the same inputs, hardware class, and concurrency conditions before and after each relevant change. Record database cache conditions and query plans so a warm-cache run is not mistaken for an implementation improvement.

| Area | Baseline to collect | First intervention | Success criterion |
|---|---|---|---|
| Daily ingestion | Attempts by resource/game/week; retry count; rows changed; stage wall time | New/dirty-unit planning and correction lookback | Unchanged inputs avoid unnecessary fan-out while corrections still arrive |
| Largest marts | Per-view duration, buffers, temp spill, rows scanned, lock waits | Shared lower-grain facts and affected-season updates | Less historical scan work with full-rebuild parity and no freshness regression |
| API hot paths | Representative team, matchup, player, play-log, and search query latency; result bytes | Canonical preaggregation, bounded RPCs, justified indexes | Agreed p95 budget on representative fixtures/load, with identical semantics |
| Flat files | File size, row count, peak RSS, parsing/loading time | Spooling and bounded Arrow batches | Peak memory governed by batch size rather than total decoded file size |
| Model compute | Per-season/era time and RSS; time split across SQL, conversion, accumulation, solve | Batch repeated operations only where profiling points | Measurable improvement with stable outputs and temporal invariants |
| MCP | Request count, connection creation, handshake overhead, end-to-end latency | Lifespan HTTP client and safe batching | Connection reuse and lower repeated-call overhead without resource leaks |
| Operations | End-to-end publication delay; stale/partial duration; failed/deferred units | Dependency completion and receipts | Freshness and failure state correspond to actual committed generations |

For SQL, start with catalog inspection, available query statistics, and nonexecuting EXPLAIN. Run EXPLAIN (ANALYZE, BUFFERS) against a representative clone or deliberately bounded read workload, because ANALYZE actually executes the query. Inspect cardinality estimates, sorts/hashes and spills, join fan-out, partition pruning, index selectivity, and serialized function work. Do not extrapolate a sequential scan on a small table into an index requirement.

Select indexes from measured filters/joins/orderings and validate their ownership/validity. Include ingestion overhead and index size in the decision. In particular, verify the plays partition tree, nested dlt parent joins, search expressions, and high-volume season/game lookups. Avoid global work_mem increases or unlimited timeouts as a blanket performance remedy.

## Data repair and deployment safeguards

The fixes change data behavior as well as code. Each affected subsystem needs a scoped repair plan:

- **Ingestion gaps:** compare expected completed-game/week units against committed receipts and target-table keys, then replay only missing/failed units. Preserve expected-no-data outcomes.
- **Weekly EPA/features:** rebuild affected current-season boundaries and features; compare upcoming predictions and annotate the model-input change. Historical backtests must retain their as-of rules.
- **Season closure:** reopen prematurely finalized seasons if found, reconcile remaining schedule/results, and identify any fits or predictions that violated seasonal eligibility.
- **Prediction provenance:** retain existing records. Do not infer historical publication times from backdated dates. Mark ambiguous history and publish separate accuracy cohorts.
- **Historical corrections:** rebuild dirty seasons in staging with resumable progress, validate player/game/season totals, then publish the new generation.
- **Schema/index work:** compare catalog definitions before/after; validate row counts, keys, partitions, consumer views, and grants. Retire old tables only after an explicit retention/recovery decision.
- **Public API changes:** capture representative payload contracts and notify dependent repository work through the normal release process; preserve compatibility or supply a versioned migration path.

Long-running data builds should leave the last valid generation readable. A failed new generation should remain failed/blocked with its receipts intact, rather than being hidden by a fresh timestamp or a successful downstream refresh.

## Program completion criteria

The program is complete when all of the following are demonstrable:

1. Real provider/database failures cannot be reported as successful empty data, and every deferred or missing unit has an explainable state.
2. A season rollover, playoff tail, bye week, and late correction produce correct ingestion and model eligibility automatically.
3. Published forecasts can be reproduced from immutable fits and input provenance, and accuracy cohorts distinguish prospective output from reconstruction.
4. The repository builds and upgrades a disposable warehouse, including consumer views and grants, with required SQL checks in CI.
5. Every derived asset has explicit upstream dependencies and a verifiable published generation; stale flags continue aging when jobs stop.
6. Daily work is bounded by new/changed units and documented reconciliation windows, with an explicit full-rebuild reference path.
7. The largest ingestion/refresh/memory costs have measured before/after results meeting agreed operational budgets.
8. Shared season, retry, connection, source, and deployment policies each have one maintained implementation; current documentation matches the executable system.

The first implementation priority is **F01–F03**, followed closely by the refresh/freshness defects and live-state fixes. Start schema reproducibility and analyst-role validation alongside that work. Defer broad cleanup and speculative tuning until those foundations are trustworthy.
