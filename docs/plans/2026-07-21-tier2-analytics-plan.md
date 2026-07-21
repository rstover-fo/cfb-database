# cfb-database: Tier 2 Analytics — House Elo, Ridge-Adjusted EPA, Scored Edges, Predictions

## Context

Tier 1 (PR #13) and the Phase 0 contract views (PR #14) are merged and live; the warehouse loads itself daily and surfaces authoritative CFBD data. What it still cannot do is **generate its own opinions**: there is no house team-strength rating (CFBD's Elo starts 2015; `marts/016_matchup_edges` only styles-scores completed games with an unvalidated number), no opponent-adjusted EPA of our own (WEPA is CFBD's black box), no expected margins for upcoming games, and no place to store predictions so accuracy is auditable. Tier 2 closes that gap with **transparent math only — no fitted ML model** (user decision): house Elo from 157 years of `core.games`, ridge-regressed opponent-adjusted EPA over `marts.play_epa` (3.6M plays, 2004+), a blended expected margin compared against the market line, and an append-only `predictions` schema scored retroactively for MAE/ATS/Brier — benchmarked against CFBD's own `metrics.pregame_win_probability` and the closing line.

**User decisions:** all four capabilities in scope; predictions = ratings → spread + edge vs market (no ML fit, no backtested model training); execution follows the model-delegation ladder.

**Constraints:** sandbox cannot reach prod — all DB work runs via the push-triggered `deploy/**` + `deploy-manifest.json` mechanism; Supabase may be downsized from XL, so no giant single-statement SQL (per-season idempotent writes, thin marts); no new CFBD endpoints (budget untouched). Verified numbering: next mart **034**, next api **028**, next migration **024** (019+ apply via `run_migrations.py --file`).

## Architecture

Sequential Elo and ridge linear algebra don't belong in Postgres. Three **Python compute scripts** run on the Actions runner, write `analytics.*` staging tables idempotently (DELETE+INSERT per season / ON CONFLICT), and **thin marts** surface them (no UNION-ALL live arm needed — the staging tables ARE the materialization). One new deploy-manifest action **`compute`** (allowlisted scripts only) runs them from deploy branches; the daily workflow runs them incrementally.

## Design decisions (final; tunables flagged)

### House Elo — `scripts/compute_house_elo.py` (stdlib only, unit-testable without DB)
- Seed 1500; **K=20**; HFA **+65 Elo** (0 if neutral); expected margin = elo_diff/**25**; win prob = canonical 400-scale logistic.
- **MOV multiplier (538 form):** `ln(|margin|+1) × 2.2/(0.001·elo_diff_winner + 2.2)`.
- **Season carryover:** `1500 + (prev − 1500)·2/3`.
- **Pooling (era-safe, data-density):** teams with <4 games in a season collapse into a per-season `__FCS__` pooled opponent (resets to 1500 yearly). Full history from 1869, no floor; `low_confidence` flag = <4 games or season<1900.
- Tables: `analytics.house_elo_game` (grain game_id: pregame/postgame Elo both sides, win prob, expected vs actual margin, CFBD elo copies for validation) + `analytics.house_elo_current` (grain team: live rating snapshot). Modes `--full` (deploy) / `--incremental` (daily, current season only). Prints Pearson r vs CFBD Elo 2015+ (expect ≳0.9).

### Ridge-adjusted EPA — `scripts/compute_adjusted_epa.py` (numpy)
- Per season: `epa ~ mu + off[team] + def[team] + hfa·is_home_offense`, garbage time excluded, plays streamed via server-side cursor into accumulated `XᵀX`/`Xᵀy` (never a dense X); ridge **λ=200** on team columns only (≈200 pseudo-plays prior); solve ~520² normal equations with `np.linalg.solve`.
- Table: `analytics.adjusted_epa_build` (team, season, off_coef, def_coef, hfa_coef, mu, plays, lambda). Sign: off higher=better, def lower=better. Coverage 2004+. Daily mode refits current season only. Prints correlation vs `marts.team_wepa_season` as sanity check.

### Expected margin, win prob, edge — `scripts/compute_predictions.py` (stdlib)
- `elo_margin = elo_diff/25 + 2.6·(1−neutral)`; `epa_margin = (off_h + def_a − off_a − def_h)·68 + hfa_coef·68·(1−neutral)`; **blend 0.6·elo + 0.4·epa** (falls back to Elo-only pre-2004/thin data). Win prob = **Elo-only** logistic (calibrated, comparable to CFBD).
- Market-implied home margin = **−spread** (verified vs `api/003_game_detail.sql` cover logic). Upcoming: latest `betting.line_snapshots` per game (consensus preferred), fallback `betting.lines`. Past: `betting.lines` ≈ closing (documented limitation pre-2026-07-21).
- **`edge = expected_home_margin + spread`** (>0 = home undervalued); `edge_pick` home/away; report thresholds |edge| ≥ {3, 6, 10}.
- `predictions.game_predictions`: append-only, unique `(game_id, model_version, prediction_date)` with same-day ON CONFLICT DO UPDATE → one immutable snapshot per day, auditable line-vs-model evolution. Backfill mode writes retro predictions 2015–2025 from `house_elo_game` pregame ratings for scoring.

**Tunable ledger (revisit after backtest gate):** K=20, divisor=25, HFA=65, MOV 2.2/0.001, carryover 2/3, pooling ≥4, λ=200, PLAYS=68, blend 60/40, thresholds {3,6,10}.

## File inventory

| Kind | Files |
|---|---|
| Migrations (via `--file`) | `024_predictions_schema.sql` (CREATE SCHEMA predictions + game_predictions + grants), `025_tier2_analytics_staging.sql` (3 analytics tables; DDL also duplicated IF-NOT-EXISTS in consuming marts, mirroring 022↔011) |
| Scripts (new) | `compute_house_elo.py`, `compute_adjusted_epa.py`, `compute_predictions.py`, `check_backtest.py` (read-only gate report) |
| Scripts (edit) | `deploy_schema.py` (+`compute` action, `COMPUTE_SCRIPTS` allowlist, ComputeSpec, CLI mapping), `refresh_marts.py` (+Layer 6, +`--views` ordered-subset flag), `check_presence.py` (+Tier 2 gate rows) |
| Marts 034–038 | `house_elo` (team/season, empty-guard), `house_elo_game` (game grain, empty-guard), `team_adjusted_epa` (team/season + WEPA compare, empty-guard), `scored_matchup_edges` (UPCOMING games vs market, no guard — empty out of season), `prediction_accuracy` (season/model/threshold: MAE, RMSE, ATS records, Brier + CFBD-Brier + beat-closing-line rate; emits elo-only and blend rows) |
| API 028–032 | `team_elo`, `game_elo_history`, `scored_matchup_edges`, `prediction_accuracy`, `game_predictions` (DISTINCT ON latest per game) |
| Config | `pyproject.toml`: `[project.optional-dependencies] compute = ["numpy>=1.26"]`; **CI Tests job and both workflows install `.[dev,compute]`** (ridge unit test also `pytest.importorskip("numpy")` for numpy-less local envs); `daily-load.yml` + compute steps; `deploy-schema.yml` + compute inputs |
| Tests | new no-DB: `test_house_elo.py` (hand-computed deltas), `test_adjusted_epa.py` (synthetic-recovery, λ→∞ shrinkage), `test_predictions.py` (blend/logistic/edge signs); edits: `test_deploy_schema.py` (compute action + allowlist), `test_marts.py` + `test_api_views.py` inventories |
| Docs | `SCHEMA_CONTRACT.md` (predictions schema, 5 marts, 5 views, 016 marked style-only for prediction use), `CLAUDE.md` (schema row, counts 34→39 marts / 27→32 api), `docs/plans/2026-07-21-tier2-analytics-plan.md` |

**Untouched by design:** `marts/016_matchup_edges` (contracted; superseded for prediction use, not rewritten), `betting.lines` schema, `load_season.py` (stays ingestion-only, numpy-free).

Daily-load step order: load_season --weekly → elo --incremental → adjusted_epa --season current → predictions → `refresh_marts --views <5 tier2 marts>` → verify_load.

## Execution phases (each leaves prod consistent; deploy branch per phase)

0. **Infra + presence gate**: compute action + tests, numpy extra, `--views` flag, presence rows → `deploy/tier2-presence` (presence_check). Confirms play_epa depth, snapshots accruing, CFBD elo/WP present. Prod unchanged.
1. **Schema**: migrations 024+025 → `deploy/tier2-schema` (apply). Empty tables, nothing reads them.
2. **Elo historical build**: → `deploy/tier2-elo` (compute --full). Gate: r vs CFBD Elo ≳0.9.
3. **Ridge historical fit**: → `deploy/tier2-epa` (compute --from 2004). Gate: strong WEPA correlation.
4. **Marts + views**: 034–038 + 028–032 + Layer-6 registration → `deploy/tier2-marts` (apply marts_from=034, files=api, refresh).
5. **Backtest gate**: predictions --backfill 2015 2025 + `check_backtest.py` → `deploy/tier2-backtest`. **GATE:** Brier/MAE within documented band of CFBD pregame WP; ATS at |edge|≥6 > 50%. If off: tune K/λ/blend (main loop), re-run 2/3/5.
6. **Daily wiring + tests + docs + PR**: workflows, test inventories (only after marts live — inherited sequencing rule), contract/CLAUDE docs, final clean `deploy/tier2-apply`, PR to main; **verification = PR CI green against the populated live DB**, then merge.

## Delegation map (per the Tier 1 precedent)

| Task class | Model |
|---|---|
| Registry/inventory adds (refresh layers, test lists, presence rows), index SQL, CLAUDE.md row, manifest JSON, numpy-extra edit | **haiku** |
| Compute-script scaffolding (DB IO/CLI/idempotency), thin marts 034–036 + all api views, migrations DDL, all tests, workflow edits, `--views` flag, `check_backtest.py`, docs/contract | **sonnet** |
| Elo parameterization, ridge design/numerics, blend + backtest methodology (leakage, gates), `prediction_accuracy` + `scored_matchup_edges` SQL, deploy-sequencing review | **opus** |
| Orchestration, commits, deploy pushes, log reading, param tuning from backtest, final PR | **main loop (fable)** |

## Verification

- Unit gates locally every phase: `ruff check`, `ruff format --check`, `pytest -q` (compute math tested without DB).
- Live gates per phase from deploy-run logs: Elo–CFBD correlation (Phase 2), WEPA correlation (Phase 3), empty-guards (Phase 4), backtest metrics via `check_backtest.py` (Phase 5: MAE/ATS/Brier vs CFBD benchmark and closing line).
- End-to-end: PR CI (745+ tests incl. new inventories) against the live, populated database; then one daily-load run showing the compute steps + targeted refresh green.
