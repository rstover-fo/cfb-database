# cfb-database: Tier 3 Analytics — Walk-Forward Honesty, Fitted Models, Features, In-Game

## Context

Tier 2 (merged 2026-07-21) gave the warehouse its own opinions: house Elo (r=0.948 vs CFBD), ridge-adjusted EPA (r=0.97-0.99 vs WEPA), transparent blended predictions, and an append-only auditable ledger scored by `marts.prediction_accuracy`. Two honest weaknesses remain: the blend's backtest (56.1% ATS at |edge|≥6) is inflated by in-season EPA leakage (full-season fits scoring early-season games — the leak-free `elo_v1` sits at 50.3%), and no model is actually *trained*. Meanwhile the P3.4 spike (merged tonight) green-lit a live Saturday dashboard on direct CFBD polling, and `metrics.win_probability` turns out to be **effectively empty** — the existing loader calls `/metrics/wp?year=` but the endpoint is gameId-scoped, and the 400s were swallowed.

**User decisions:** four pillars — (A) walk-forward honesty via through-week EPA; (B) fitted + calibrated models plus a tuning grid over the Tier 2 ledger; (C) `features.team_week` as the modeling substrate, API-exposed; (D) in-game = Saturday `/scoreboard` snapshots + a transparent house live win-prob, calibrated against a one-time 2015+ `/metrics/wp` backfill (~9,600 calls). `/ppa/predicted`: skipped (adds nothing over per-play EPA already loaded). No sklearn/pandas — numpy closed-form only.

**Verified numbering:** migrations next **026**, marts **039**, api **033**. All DB work via `deploy/**` + manifest (sandbox can't reach prod). Registries per new mart: `refresh_marts.py`, `refresh_all_marts.sql`, `test_marts.py`, `test_api_views.py` (+ `EMPTY_OK` precedent for legitimately-empty).

## Pillar A — Walk-forward honesty

- **One-pass as-of EPA**: `RidgeAccumulator` is additive → stream each season's plays ordered by week; at each week boundary solve (state = plays `week < W`) and emit `(team, season, week)` coefficients = rating *entering* week W. One season-pass + ~15 tiny solves; 2004+ ≈ 82K rows, minutes total. Column layout fixed from the full-season team list (identity isn't leakage; only coefficients are as-of).
- Storage: `analytics.adjusted_epa_week_build` (migration 026, mirrors adjusted_epa_build + week; UNIQUE (team, season, week)). New `scripts/compute_adjusted_epa_week.py` (imports RidgeAccumulator; `--from/--season/--incremental`; empty-season no-op; prints last-week≈full-season validation, expect r ≳ 0.97).
- **The fix**: `compute_predictions.py` gains `--as-of-week` for backfill — EPA arm reads week coefficients per game's week, falling back to *previous-season* full fit for week 1/thin data, else Elo-only. Elo arm untouched (already walk-forward via house_elo_game pregame values — the leak is exactly one lookup in `run_backfill`).
- **Gate A**: re-backfill 2015-2025 → refresh 038 → `check_backtest`. Report the honest blend ATS≥6 (replaces 56.1%); `elo_v1` must be byte-identical (proves the Elo arm didn't move). Update 038's leakage caveat.

## Pillar B — Fitted + calibrated models (numpy only)

- **`fitted_v1`** = ridge linear margin model (normal equations, α≈5-10, intercept unpenalized) + logistic win-prob via IRLS (~8 Newton iters, ridge-stabilized Hessian) + **Platt scaling** for calibration. One model_version row per game in `predictions.game_predictions` → `marts/038` scores it automatically.
- **Features** (home-minus-away diffs from `features.team_week` + neutral_site): elo, adj net/off/def EPA, season-to-date epa/success/explosiveness, def allowed, tempo, havoc, returning production, preseason SP. **Market deliberately excluded** so `edge` stays meaningful (`fitted_market_v1` = documented follow-up).
- **Walk-forward protocol**: expanding window, min 3 train seasons; score S∈2018..2025 with model trained through S-1. All per-season fits stored in `features.model_coefficients` + `model_metadata` (migration 027) so backfill uses the right frozen model; daily upcoming mode uses the latest frozen fit.
- Scripts: `train_model.py` (fits + Platt, prints FITTED_GATE lines), `score_fitted.py` (`--backfill 2018 2025` / upcoming).
- **Gate B**: `fitted_v1` must beat `elo_v1` on walk-forward MAE AND Brier (target Brier ≲ 0.168 vs elo's 0.187; CFBD benchmark 0.159). Fails → stays advisory, not wired into daily.
- **Tuning grid** `tune_params.py`: in-memory, advisory, never auto-applies. Grid K{16,20,24,28} × divisor{22,25,28} × HFA{55,65,75}; λ{100,200,400}; blend{.5,.6,.7}. Objective: walk-forward MAE primary, ATS≥3/≥6 secondary. Elo rebuilt in-memory per combo (~5-10s each); EPA XtX built once/season, re-solved per λ. <15 min total; prints ranked TUNE_RESULT table. User decides ledger changes.

## Pillar C — features.team_week

- Compute-script-built (`build_features.py`, unit-testable leak rules; joins script-written week coefficients), NOT SQL. Grain (team, season, week); spine = ref.calendar × season schedule (upcoming weeks get rows). **As-of W = data through end of W-1**, per-column-family rules documented (Elo pregame = walk-forward by construction; season-to-date aggregates from marts.play_epa `week<W` non-garbage; adj EPA from week table w/ prior-season fallback; returning production + preseason SP = preseason-known constants). ~30 columns, backfill 2015+ (~21K rows), daily cadence in season. UNIQUE (season, week, team).
- Exposure: mart `039_team_week_features` + `api/033_team_week_features`; optional transparency pair `040/035_adjusted_epa_week`.

## Pillar D — In-game

- **`live` schema** (migration 028): `live.scoreboard_snapshots` (append-only: captured_at, game state, score, clock→seconds_remaining, possession, lines, cfbd live WP, `house_live_home_wp`, hash) + `live.wp_params` (σ, blend_weight, fit metadata — single row).
- **House live WP (closed-form, stdlib erf)**: `f = clamp(sec_remaining/3600, ε, 1)`; `projected = margin + pregame_expected_margin·f`; `wp = Φ(projected / (σ·√f))`. Boundary tests: f→0 ⇒ wp→{0,1} by margin sign; f=1 ⇒ ≈ pregame Elo WP; monotone in margin. σ seeded ≈16, then calibrated.
- **`/metrics/wp` fix + backfill**: new `win_probability_by_game_resource(game_ids)` in metrics.py (`?gameId=`, merge on play_id — replaces the broken year-param path); `backfill_ingame_wp.py` one-time 2015+ (~9,600 calls, resumable, budget-guarded) + forward weekly (~60-80/wk) in daily; indexes in migration 029 post-load.
- **Calibration**: `calibrate_live_wp.py` fits σ (grid) minimizing Brier vs outcomes over reconstructed in-game states; writes wp_params; prints decile calibration curve + house-vs-CFBD Brier (**Gate D**).
- **Polling**: `.github/workflows/live-scoreboard.yml` — cron `*/5 16-23 * * 6` + `*/5 0-7 * * 0` (Sat noon ET → Sun 3am ET), first step guards on games-today (no CFBD call otherwise); each tick = ONE `/scoreboard` call for all live games via `poll_scoreboard.py` (Elo from house_elo_current + carryover, σ from wp_params). ~150-200 calls/Saturday.
- **Surface**: `api/034_live_scoreboard.sql` — plain view (not matview), DISTINCT ON latest per game. Additive convenience for the dashboard, not a dependency (per the spike, cfb-app may keep direct polling).

## File inventory

Migrations 026-029 · marts 039(+040 optional) · api 033, 034(+035 optional) · 8 new scripts (`compute_adjusted_epa_week`, `build_features`, `train_model`, `score_fitted`, `tune_params`, `poll_scoreboard`, `backfill_ingame_wp`, `calibrate_live_wp`) · edits: `compute_predictions.py` (--as-of-week), `deploy_schema.py` (+8 allowlist names, TIER3_MART_VIEWS), `refresh_marts.py` (+Layer 7), `metrics.py` (gameId resource), `check_presence.py` (Tier 3 rows) · workflows: new `live-scoreboard.yml`, daily-load step inserts · 7 new no-DB test files + inventory/allowlist test edits · docs: sprint plan, SCHEMA_CONTRACT (`features` + `live` schemas, fitted_v1, blend-now-honest changelog), CLAUDE.md rows/counts.

## Execution phases (deploy branch + gate each; prod consistent throughout)

0. Infra+presence (`deploy/tier3-presence`) — allowlist, presence rows; confirms play_epa week coverage, wp table empty, /scoreboard reachable.
1. Schema (`tier3-schema`): migrations 026/027/028 — empty tables.
2. As-of EPA build (`tier3-epa-week`): --from 2004. Gate: coverage + last-week≈full-season r ≳ 0.97.
3. Features build (`tier3-features`): --from 2015 + marts/views apply. Gate: null-rates + leak spot-checks (week-1 = fallback, aggregates 0).
4. **Gate A** (`tier3-backtest-A`): leak-free re-backfill → honest blend number; elo_v1 unchanged.
5. **Gate B** (`tier3-fitted`): train + backfill-score fitted_v1 → must beat elo_v1 MAE+Brier; only then wire into daily.
6. Tuning grid (`tier3-tune`): advisory table; user decides ledger changes (re-run 2/4/5 if adopted).
7. **Gate D data** (`tier3-ingame-wp`): 9.6K-call wp backfill + σ calibration + curve report.
8. Live wiring (`tier3-live`): api 034 + Saturday workflow; manual-dispatch poll test (or clean off-season skip).
9. Daily wiring + inventories (post-live only) + docs + final `tier3-apply` + **PR to main; CI green vs populated DB**; one daily run + one poll green; merge.

## Delegation map (Tier 1/2 precedent)

| Model | Work |
|---|---|
| haiku | All registry/inventory/allowlist adds, EMPTY_OK entries, presence rows, cron literals, manifest JSONs, CLAUDE.md rows, count bumps, index SQL |
| sonnet | All 8 script scaffolds (IO/CLI/idempotency), migrations DDL, thin marts + api views, metrics.py resource, both workflow files, all tests, docs/contract |
| opus | As-of accumulation semantics, ridge/IRLS/Platt math + walk-forward protocol, leak-free feature column design, tuning methodology, live-WP formula + σ calibration, --as-of-week correctness review, gate interpretation |
| main loop (fable) | Orchestration, commits, deploys, log reading, gate go/no-go, ledger decisions, PR |

## Verification

- Every phase: local `ruff` + `pytest -q` (all model/formula math unit-tested without DB: synthetic ridge/IRLS recovery, Platt monotonicity, Φ boundary conditions, as-of week-boundary semantics, frozen-model selection).
- Live gates from deploy logs: r ≳ 0.97 (Phase 2), leak spot-checks (3), honest-blend + unchanged-elo_v1 (4), fitted beats elo_v1 (5), calibration curve + Brier (7), poll snapshot with house_live_home_wp populated (8).
- End-to-end: PR CI (DB-gated inventories incl. new surfaces) against populated prod; one green daily-load with the new steps; one green Saturday poll (or clean guard skip); merge.
