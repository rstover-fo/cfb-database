#!/usr/bin/env python3
"""Advisory in-memory tuning grid over Elo K/divisor/HFA, EPA lambda, and the
Elo/EPA blend weight (Tier 3 analytics, Pillar B tuning grid --
docs/plans/2026-07-21-tier3-analytics-plan.md).

CONTRACT (read before running or editing this script):
  * IN-MEMORY. Every combo is scored against data pulled into Python once at
    startup; nothing here re-queries per combo.
  * ADVISORY ONLY. This script NEVER writes to any analytics/predictions
    table, any mart, or the ledger. It only prints ranked results. Whether to
    change compute_house_elo.py's K/DIVISOR/HFA, compute_adjusted_epa.py's
    LAMBDA, or compute_predictions.py's BLEND_ELO/BLEND_EPA based on these
    numbers is a HUMAN decision -- see the plan's Pillar B paragraph and
    delegation map ("user decides ledger changes").
  * READ-ONLY on the DB: three SELECT fetches total (games, market spreads,
    plays), then everything is pure in-memory Python/numpy.

WHAT IT REPLAYS (mirrors, and in several places directly imports, the real
compute scripts so the grid can never silently drift from production logic):
  * Elo: scripts.compute_house_elo.EloEngine is pure (operates on plain
    dicts/floats) -- each grid combo gets a fresh EloEngine with K/DIVISOR/HFA
    overridden as INSTANCE attributes (SEED/CARRYOVER/POOL_THRESHOLD stay at
    the class's FINAL defaults; only K/divisor/HFA are swept, per the plan).
    Game fetch, per-season bucketing, and scheduled-game-count pooling logic
    (load_games_by_season, to_engine_game, fetch_scheduled_counts) are
    imported unchanged from compute_house_elo, not re-implemented.
  * EPA lambda: scripts.compute_adjusted_epa.RidgeAccumulator is imported
    unchanged. compute_week_boundaries_multi_lambda below mirrors
    compute_adjusted_epa_week.compute_week_boundaries's per-week-boundary
    walk, but solves ALL swept lambdas from the SAME accumulated XtX/Xty
    state at each boundary (XtX/Xty are lambda-independent -- only the
    penalized solve differs), so a season's plays are streamed ONCE
    regardless of how many lambdas are in the grid, per the plan's key
    efficiency note.
  * Blend: scripts.compute_predictions.epa_margin and .compute_edge are
    imported unchanged. blend_margin_weighted below generalizes
    compute_predictions.blend_margin's fixed 0.6/0.4 split to an arbitrary
    weight for the sweep.
  * Market: scripts.compute_predictions.fetch_market_from_lines is imported
    and used AS-IS -- the exact same betting.lines closing-consensus
    query/fallback the --backfill path uses (DISTINCT ON game_id, consensus
    provider preferred, else alphabetically-first provider).

SIMPLIFICATIONS vs the full production pipelines (documented, not hidden):
  1. Elo replay starts at ELO_REPLAY_FLOOR_SEASON=2005, not compute_house_elo
     --full's 1869. Ten warm-up seasons before the default 2015 scoring
     window is enough for ratings to separate from EloEngine.SEED; replaying
     156 years x 36 combos would blow the <15min budget for no scoring
     benefit (the scoring window never reaches back that far).
  2. EPA lambda evaluation uses ONLY within-season as-of coefficients
     (compute_adjusted_epa_week's week-boundary walk), thresholded on
     accumulated offensive plays >= MIN_EPA_PLAYS (150): the greatest
     boundary week_index <= the game's week_index with enough plays is used,
     or the game is SKIPPED for EPA/blend scoring if none qualifies. This is
     simpler than compute_predictions.py's real fallback ladder (which falls
     back to the previous season's FULL-season fit for thin/early data) --
     here there is no cross-season fallback at all. Each TUNE_RESULT
     stage=lambda line reports `coverage` = the fraction of scoring-window
     games that had a qualifying EPA rating for BOTH teams, so the tradeoff
     is visible rather than silently absorbed.
  3. Blend-stage games require BOTH arms to be available (no Elo-only
     fallback the way compute_predictions.blend_margin has) -- "games where
     both arms available" per the plan, to keep the blend comparison honest.

Usage:
    python scripts/tune_params.py
        Full grid (36 Elo combos x 3 lambdas x 3 weights), scoring window
        2015-2025.

    python scripts/tune_params.py --start-season 2018 --end-season 2024
        Same grid, different scoring window (the Elo replay / plays fetch
        windows widen automatically to cover any earlier --start-season).

    python scripts/tune_params.py --quick
        Shrunk grid (2x2x2 Elo combos, 2 lambdas, 2 weights) for
        smoke-testing the pipeline quickly.

Prints ranked tables as aligned machine-readable lines (one per combo):
    TUNE_RESULT stage=elo K={k} divisor={d} hfa={h} mae={m:.3f} ats3={a3:.4f} ats6={a6:.4f} n={n}
    TUNE_RESULT stage=lambda lambda={l} mae={m:.3f} coverage={c:.3f} n={n}
    TUNE_RESULT stage=blend K={k} divisor={d} hfa={h} lambda={l} weight={w} mae={m:.3f} \
        ats3={a3:.4f} ats6={a6:.4f} n={n}
and a final TUNE_BASELINE line for the CURRENT ledger config (K=20,
divisor=25, HFA=65, lambda=200, weight=0.6 -- EloEngine/compute_adjusted_epa/
compute_predictions' live defaults, imported not hardcoded) plus a
"current ledger rank: X of Y" summary against the swept blend combos.
"""

import argparse
import logging
import math
import sys
import time
from collections import defaultdict
from collections.abc import Iterable

from scripts.compute_adjusted_epa import (
    CURSOR_ITERSIZE,
    LAMBDA,
    RidgeAccumulator,
    get_season_teams,
)
from scripts.compute_adjusted_epa_week import PLAY_QUERY_WEEK
from scripts.compute_house_elo import (
    EloEngine,
    compute_team_game_counts,
    fetch_scheduled_counts,
    load_games_by_season,
    to_engine_game,
)
from scripts.compute_predictions import (
    BLEND_ELO,
    compute_edge,
    epa_margin,
    fetch_market_from_lines,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- Scoring window (CLI-overridable) -----------------------------------
DEFAULT_START_SEASON = 2015
DEFAULT_END_SEASON = 2025

# --- Fixed data-fetch floors (NOT CLI-configurable; see module docstring
# simplification #1). Widened automatically if --start-season is earlier. ---
ELO_REPLAY_FLOOR_SEASON = 2005
PLAYS_FLOOR_SEASON = 2014

# --- EPA as-of lookup: minimum accumulated offensive plays for a week
# boundary to be usable (simplification #2). ---
MIN_EPA_PLAYS = 150

ATS_THRESHOLDS = (3.0, 6.0)

# --- Current ledger ("production") config -- imported, not hardcoded, so
# TUNE_BASELINE can never silently drift from the real defaults. ---
BASELINE_K = EloEngine.K
BASELINE_DIVISOR = EloEngine.DIVISOR
BASELINE_HFA = EloEngine.HFA
BASELINE_LAMBDA = LAMBDA
BASELINE_WEIGHT = BLEND_ELO


# =============================================================================
# Grid construction -- pure, unit-tested directly.
# =============================================================================


def elo_grid(quick: bool = False) -> list[tuple[float, float, float]]:
    """36 (K, divisor, HFA) combos, or 8 (2x2x2) when `quick`."""
    ks = (16.0, 20.0) if quick else (16.0, 20.0, 24.0, 28.0)
    divisors = (22.0, 25.0) if quick else (22.0, 25.0, 28.0)
    hfas = (55.0, 65.0) if quick else (55.0, 65.0, 75.0)
    return [(k, d, h) for k in ks for d in divisors for h in hfas]


def lambda_grid(quick: bool = False) -> list[float]:
    """3 lambdas, or 2 when `quick`."""
    return [100.0, 200.0] if quick else [100.0, 200.0, 400.0]


def weight_grid(quick: bool = False) -> list[float]:
    """3 blend weights, or 2 when `quick`."""
    return [0.5, 0.6] if quick else [0.5, 0.6, 0.7]


# =============================================================================
# Elo replay -- pure, drives the real EloEngine, unit-tested directly.
# =============================================================================


def replay_elo(
    games_by_season: dict[int, list[dict]],
    scheduled_counts: dict[int, dict[str, int]],
    k: float,
    divisor: float,
    hfa: float,
) -> dict[int, dict]:
    """Replay EloEngine in memory for one (K, divisor, HFA) combo across every
    season in `games_by_season`, in season order (start_season, then
    process_game per game -- exactly compute_house_elo.run_full's loop).

    K/DIVISOR/HFA are overridden as INSTANCE attributes on a fresh EloEngine;
    SEED/CARRYOVER/POOL_THRESHOLD stay at the class's FINAL defaults (only
    K/divisor/HFA are swept, per the plan). Because process_game computes
    `expected_home_margin` from `self.DIVISOR`/`self.HFA`, the returned rows
    already carry this combo's walk-forward predicted margin -- no separate
    elo_margin() call is needed.

    Returns {game_id: row} (row = process_game's full dict).
    """
    engine = EloEngine()
    engine.K = k
    engine.DIVISOR = divisor
    engine.HFA = hfa
    rows_by_game_id: dict[int, dict] = {}
    for season in sorted(games_by_season):
        season_games = games_by_season[season]
        counts = scheduled_counts.get(season) or compute_team_game_counts(season_games)
        engine.start_season(season, counts)
        for g in season_games:
            row = engine.process_game(g)
            rows_by_game_id[row["game_id"]] = row
    return rows_by_game_id


# =============================================================================
# EPA week-boundary walk with multiple lambdas solved per boundary -- pure.
# =============================================================================


def _solve_boundary(
    accumulator: RidgeAccumulator, season: int | None, week_index: int, lam: float
) -> list[dict]:
    """Solve `accumulator`'s current (pre-this-week) state for one lambda.
    Same row shape as compute_adjusted_epa_week._boundary_rows."""
    mu, hfa, off_coef, def_coef, _n_plays = accumulator.solve(lam)
    return [
        {
            "team": team,
            "season": season,
            "week_index": week_index,
            "off_coef": off_coef[team],
            "def_coef": def_coef[team],
            "hfa_coef": hfa,
            "mu": mu,
            "plays": int(accumulator.off_play_counts[i]),
            "lambda": lam,
            "n_teams": accumulator.n_teams,
        }
        for i, team in enumerate(accumulator.teams)
    ]


def compute_week_boundaries_multi_lambda(
    plays: Iterable[tuple[str, str, bool, float, int]],
    teams: list[str],
    lambdas: list[float],
    season: int | None = None,
) -> dict[float, list[dict]]:
    """Like compute_adjusted_epa_week.compute_week_boundaries, but solves ALL
    `lambdas` from the SAME accumulated RidgeAccumulator state at each week
    boundary -- one season-pass total instead of one pass per lambda (the
    plan's tuning-grid efficiency note: XtX/Xty are lambda-independent, only
    the penalized solve differs). Returns {lambda: [boundary_row, ...]}.

    Boundary semantics identical to compute_week_boundaries: a boundary for
    week_index W is emitted the moment a play with that week_index is seen
    AND the accumulator already holds >= 1 play from strictly earlier
    week_index values. No boundary before the first week; none after the
    last play.
    """
    accumulator = RidgeAccumulator(teams)
    boundaries: dict[float, list[dict]] = {lam: [] for lam in lambdas}
    prev_week_index: int | None = None

    for off_team, def_team, is_home_offense, epa, week_index in plays:
        if (
            prev_week_index is not None
            and week_index != prev_week_index
            and accumulator.n_plays > 0
        ):
            for lam in lambdas:
                boundaries[lam].extend(_solve_boundary(accumulator, season, week_index, lam))

        accumulator.add_play(off_team, def_team, is_home_offense, epa)
        prev_week_index = week_index

    return boundaries


def build_epa_index(boundary_rows: list[dict]) -> dict[tuple[str, int], list[tuple[int, dict]]]:
    """{(team, season): [(week_index, row), ...]} sorted ascending by
    week_index, for the greatest-boundary-<=W as-of lookup below."""
    idx: dict[tuple[str, int], list[tuple[int, dict]]] = defaultdict(list)
    for r in boundary_rows:
        idx[(r["team"], r["season"])].append((r["week_index"], r))
    for entries in idx.values():
        entries.sort(key=lambda t: t[0])
    return dict(idx)


def lookup_epa_boundary(
    idx: dict[tuple[str, int], list[tuple[int, dict]]],
    team: str,
    season: int,
    week_index: int,
    min_plays: int = MIN_EPA_PLAYS,
) -> dict | None:
    """Greatest boundary week_index <= `week_index` for (team, season) with
    accumulated offensive plays >= `min_plays`, else None. This is the
    documented simplification vs the design doc's full prior-season-fallback
    ladder (module docstring simplification #2): thin/early-season games are
    SKIPPED for EPA/blend scoring rather than falling back to a previous
    season's fit.
    """
    entries = idx.get((team, season))
    if not entries:
        return None
    best = None
    for wi, row in entries:
        if wi > week_index:
            break
        if row["plays"] >= min_plays:
            best = row
    return best


def game_week_index(week: int, season_type: str) -> int:
    """week_index convention shared with compute_adjusted_epa_week.py: bowls
    (season_type='postseason') sort after every regular week."""
    return 100 + week if season_type == "postseason" else week


# =============================================================================
# Scoring: MAE + ATS hit rate -- pure, unit-tested directly.
# =============================================================================


def mean_abs_error(pairs: Iterable[tuple[float, float]]) -> tuple[float, int]:
    """(mae, n) over (expected, actual) pairs. nan/0 when empty."""
    diffs = [abs(expected - actual) for expected, actual in pairs]
    n = len(diffs)
    return (sum(diffs) / n if n else float("nan")), n


def ats_result(edge_pick: str, actual_home_margin: float, market_spread: float) -> str:
    """'win' | 'loss' | 'push' -- mirrors marts/038_prediction_accuracy.sql's
    cover convention: home covers when actual_home_margin + market_spread > 0;
    the model's pick wins exactly when it matches the covering side."""
    cover_margin = actual_home_margin + market_spread
    if cover_margin == 0:
        return "push"
    home_covers = cover_margin > 0
    win = (edge_pick == "home" and home_covers) or (edge_pick == "away" and not home_covers)
    return "win" if win else "loss"


def ats_hit_rate(graded: list[dict], threshold: float) -> tuple[float | None, int]:
    """`graded` rows: {edge, edge_pick, actual_home_margin, market_spread}.
    Hit rate at |edge| >= threshold, market required, pushes excluded from
    both wins/losses and the denominator (marts/038's edge_threshold>0
    semantics). Returns (None, 0) when no game qualifies."""
    wins = losses = 0
    for r in graded:
        edge = r["edge"]
        market_spread = r["market_spread"]
        if edge is None or market_spread is None or abs(edge) < threshold:
            continue
        result = ats_result(r["edge_pick"], r["actual_home_margin"], market_spread)
        if result == "win":
            wins += 1
        elif result == "loss":
            losses += 1
    n = wins + losses
    return (wins / n if n else None), n


def score_margins(rows: list[dict]) -> dict:
    """`rows`: {expected, actual, market_spread}. MAE over ALL rows
    (edge_threshold=0 semantics -- the whole scored population, market or
    not); ATS hit rate at |edge|>=3 and >=6 via compute_predictions.compute_edge
    (market required for edge to exist at all). Returns {mae, n, ats3,
    ats3_n, ats6, ats6_n}."""
    pairs = [(r["expected"], r["actual"]) for r in rows]
    mae, n = mean_abs_error(pairs)

    graded = []
    for r in rows:
        edge, pick = compute_edge(r["expected"], r["market_spread"])
        graded.append(
            {
                "edge": edge,
                "edge_pick": pick,
                "actual_home_margin": r["actual"],
                "market_spread": r["market_spread"],
            }
        )
    ats3, ats3_n = ats_hit_rate(graded, ATS_THRESHOLDS[0])
    ats6, ats6_n = ats_hit_rate(graded, ATS_THRESHOLDS[1])
    return {"mae": mae, "n": n, "ats3": ats3, "ats3_n": ats3_n, "ats6": ats6, "ats6_n": ats6_n}


def blend_margin_weighted(elo_m: float, epa_m: float, weight: float) -> float:
    """weight*elo + (1-weight)*epa -- generalizes compute_predictions.blend_margin's
    fixed 0.6/0.4 split to an arbitrary weight for the sweep. Unlike
    blend_margin, both arms are REQUIRED here (no None fallback): stage 3
    only scores games where both arms are available (module docstring
    simplification #3)."""
    return weight * elo_m + (1.0 - weight) * epa_m


# =============================================================================
# Ranking -- pure, unit-tested directly.
# =============================================================================


def rank_by_mae(results: list[dict]) -> list[dict]:
    """Stable ascending sort by 'mae' -- the grid's primary ranking
    objective. NaN MAE (no scored games) sorts last."""
    return sorted(results, key=lambda r: (math.isnan(r["mae"]), r["mae"]))


def rank_baseline(baseline_mae: float, other_maes: list[float]) -> tuple[int, int]:
    """1-based rank of `baseline_mae` as if inserted into `other_maes`
    ascending (rank 1 = best/lowest MAE). Returns (rank, total) with
    total = len(other_maes) + 1."""
    total = len(other_maes) + 1
    rank = 1 + sum(1 for m in other_maes if m < baseline_mae)
    return rank, total


def _fmt(x: float | None, spec: str = ".4f") -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "nan"
    return format(x, spec)


# =============================================================================
# --- I/O layer --- (three fetches total, then pure Python drives everything)
# =============================================================================


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Copied from scripts/compute_house_elo.py's get_db_url pattern (each
    compute_*.py / tuning script keeps its own copy rather than importing
    across scripts for this one utility).
    """
    import os

    import dlt

    url = None
    try:
        creds = dlt.secrets.get("destination.postgres.credentials")
        if creds:
            url = str(creds)
    except Exception:
        pass

    if not url:
        url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")

    if not url:
        raise RuntimeError(
            "No database URL found. Set destination.postgres.credentials in "
            ".dlt/secrets.toml or SUPABASE_DB_URL environment variable."
        )

    if "options=" not in url:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}options=-c%20statement_timeout%3D0"

    return url


def fetch_elo_inputs(
    conn, replay_start: int, end_season: int
) -> tuple[dict[int, list[dict]], dict[int, dict[str, int]]]:
    """Games + scheduled counts for the Elo replay grid, via
    compute_house_elo's OWN query/mapping (load_games_by_season,
    to_engine_game, fetch_scheduled_counts), imported and reused directly --
    never re-implemented -- so the grid can't drift from compute_house_elo.py's
    real game query."""
    raw_buckets = load_games_by_season(conn, replay_start, end_season)
    games_by_season = {
        season: [to_engine_game(row) for row in rows] for season, rows in raw_buckets.items()
    }
    scheduled_counts = fetch_scheduled_counts(conn, replay_start, end_season)
    return games_by_season, scheduled_counts


def build_scoring_games(
    games_by_season: dict[int, list[dict]], start_season: int, end_season: int
) -> list[dict]:
    """Games (engine-shaped dicts, from fetch_elo_inputs) restricted to the
    scoring window, each carrying actual_home_margin + week_index -- the
    common per-game frame all three stages score against."""
    games: list[dict] = []
    for season in range(start_season, end_season + 1):
        for g in games_by_season.get(season, []):
            games.append(
                {
                    "game_id": g["game_id"],
                    "season": season,
                    "home_team": g["home_team"],
                    "away_team": g["away_team"],
                    "neutral_site": bool(g.get("neutral_site")),
                    "actual_home_margin": g["home_points"] - g["away_points"],
                    "week_index": game_week_index(g["week"], g["season_type"]),
                }
            )
    return games


def fetch_market_spreads(conn, game_ids: list[int]) -> dict[int, float]:
    """betting.lines closing consensus -- the SAME query/fallback
    compute_predictions.py's --backfill mode uses (fetch_market_from_lines,
    imported directly and reused as-is, not reimplemented): DISTINCT ON
    game_id, provider='consensus' preferred else alphabetically-first
    provider."""
    market = fetch_market_from_lines(conn, game_ids)
    return {gid: m["spread"] for gid, m in market.items() if m.get("spread") is not None}


def fetch_plays_and_teams(
    conn, start_season: int, end_season: int
) -> dict[int, tuple[list[str], list[tuple]]]:
    """Per-season (teams, plays) for seasons start_season..end_season,
    mirroring compute_adjusted_epa_week.fit_season_weeks's fetch pattern
    exactly (same get_season_teams call + PLAY_QUERY_WEEK query, both
    imported and reused). Plays are buffered fully in memory per season
    (needed to solve multiple lambdas from one shared accumulator state)."""
    result: dict[int, tuple[list[str], list[tuple]]] = {}
    for season in range(start_season, end_season + 1):
        with conn.cursor() as cur:
            teams = get_season_teams(cur, season)
        if not teams:
            continue
        cursor_name = f"tune_params_plays_{season}"
        with conn.cursor(name=cursor_name) as cur:
            cur.itersize = CURSOR_ITERSIZE
            cur.execute(PLAY_QUERY_WEEK, (season,))
            plays = [
                (offense, defense, bool(is_home), float(epa), int(week_index))
                for offense, defense, epa, is_home, week_index in cur
            ]
        result[season] = (teams, plays)
    return result


def _blend_scoring_rows(
    scoring_games: list[dict],
    elo_rows_by_game_id: dict[int, dict],
    epa_idx: dict[tuple[str, int], list[tuple[int, dict]]],
    market_by_game_id: dict[int, float],
    weight: float,
) -> list[dict]:
    """Blended {expected, actual, market_spread} rows for one (elo combo,
    lambda, weight): only games where BOTH the elo row and both teams' EPA
    boundaries are available (module docstring simplification #3)."""
    rows = []
    for g in scoring_games:
        elo_row = elo_rows_by_game_id.get(g["game_id"])
        if elo_row is None:
            continue
        home_row = lookup_epa_boundary(epa_idx, g["home_team"], g["season"], g["week_index"])
        away_row = lookup_epa_boundary(epa_idx, g["away_team"], g["season"], g["week_index"])
        if home_row is None or away_row is None:
            continue
        epa_m = epa_margin(
            home_row["off_coef"],
            home_row["def_coef"],
            away_row["off_coef"],
            away_row["def_coef"],
            home_row["hfa_coef"],
            g["neutral_site"],
        )
        blended = blend_margin_weighted(elo_row["expected_home_margin"], epa_m, weight)
        rows.append(
            {
                "expected": blended,
                "actual": g["actual_home_margin"],
                "market_spread": market_by_game_id.get(g["game_id"]),
            }
        )
    return rows


def run_stage_elo(
    games_by_season: dict[int, list[dict]],
    scheduled_counts: dict[int, dict[str, int]],
    scoring_games: list[dict],
    market_by_game_id: dict[int, float],
    quick: bool,
) -> tuple[list[dict], dict[tuple[float, float, float], dict[int, dict]]]:
    combos = elo_grid(quick)
    logger.info(f"Stage 1 (elo): {len(combos)} combo(s)")
    results: list[dict] = []
    rows_cache: dict[tuple[float, float, float], dict[int, dict]] = {}
    for i, (k, d, h) in enumerate(combos, start=1):
        t0 = time.monotonic()
        rows_by_game_id = replay_elo(games_by_season, scheduled_counts, k, d, h)
        rows_cache[(k, d, h)] = rows_by_game_id
        scoring_rows = [
            {
                "expected": rows_by_game_id[g["game_id"]]["expected_home_margin"],
                "actual": g["actual_home_margin"],
                "market_spread": market_by_game_id.get(g["game_id"]),
            }
            for g in scoring_games
            if g["game_id"] in rows_by_game_id
        ]
        metrics = score_margins(scoring_rows)
        results.append({"k": k, "divisor": d, "hfa": h, **metrics})
        print(
            f"TUNE_RESULT stage=elo K={int(k)} divisor={int(d)} hfa={int(h)} "
            f"mae={_fmt(metrics['mae'], '.3f')} ats3={_fmt(metrics['ats3'])} "
            f"ats6={_fmt(metrics['ats6'])} n={metrics['n']}"
        )
        logger.info(
            f"[elo {i}/{len(combos)}] K={k} divisor={d} hfa={h} "
            f"elapsed={time.monotonic() - t0:.1f}s"
        )
    return results, rows_cache


def run_stage_lambda(
    plays_and_teams: dict[int, tuple[list[str], list[tuple]]],
    scoring_games: list[dict],
    quick: bool,
) -> tuple[list[dict], dict[float, dict]]:
    lambdas = lambda_grid(quick)
    logger.info(f"Stage 2 (lambda): {len(lambdas)} value(s)")
    boundary_rows_by_lambda: dict[float, list[dict]] = {lam: [] for lam in lambdas}
    for season in sorted(plays_and_teams):
        teams, plays = plays_and_teams[season]
        if not plays:
            continue
        t0 = time.monotonic()
        rows_by_lambda = compute_week_boundaries_multi_lambda(plays, teams, lambdas, season=season)
        for lam in lambdas:
            boundary_rows_by_lambda[lam].extend(rows_by_lambda[lam])
        logger.info(
            f"[lambda season={season}] {len(plays)} plays, {len(lambdas)} lambda(s) solved per "
            f"boundary, elapsed={time.monotonic() - t0:.1f}s"
        )

    epa_idx_by_lambda = {
        lam: build_epa_index(rows) for lam, rows in boundary_rows_by_lambda.items()
    }

    n_window_games = len(scoring_games)
    results: list[dict] = []
    for lam in lambdas:
        t0 = time.monotonic()
        idx = epa_idx_by_lambda[lam]
        pairs = []
        for g in scoring_games:
            home_row = lookup_epa_boundary(idx, g["home_team"], g["season"], g["week_index"])
            away_row = lookup_epa_boundary(idx, g["away_team"], g["season"], g["week_index"])
            if home_row is None or away_row is None:
                continue
            m = epa_margin(
                home_row["off_coef"],
                home_row["def_coef"],
                away_row["off_coef"],
                away_row["def_coef"],
                home_row["hfa_coef"],
                g["neutral_site"],
            )
            pairs.append((m, g["actual_home_margin"]))
        mae, n = mean_abs_error(pairs)
        coverage = n / n_window_games if n_window_games else float("nan")
        results.append({"lambda": lam, "mae": mae, "n": n, "coverage": coverage})
        print(
            f"TUNE_RESULT stage=lambda lambda={int(lam)} mae={_fmt(mae, '.3f')} "
            f"coverage={_fmt(coverage, '.3f')} n={n}"
        )
        logger.info(
            f"[lambda score={lam}] mae={_fmt(mae, '.3f')} coverage={_fmt(coverage, '.3f')} "
            f"elapsed={time.monotonic() - t0:.1f}s"
        )
    return results, epa_idx_by_lambda


def run_stage_blend(
    top3_elo: list[dict],
    elo_rows_cache: dict[tuple[float, float, float], dict[int, dict]],
    epa_idx_by_lambda: dict[float, dict],
    scoring_games: list[dict],
    market_by_game_id: dict[int, float],
    quick: bool,
) -> list[dict]:
    lambdas = list(epa_idx_by_lambda)
    weights = weight_grid(quick)
    logger.info(
        f"Stage 3 (blend): {len(top3_elo)} elo combo(s) x {len(lambdas)} lambda(s) x "
        f"{len(weights)} weight(s)"
    )
    results: list[dict] = []
    for elo_r in top3_elo:
        rows_by_game_id = elo_rows_cache[(elo_r["k"], elo_r["divisor"], elo_r["hfa"])]
        for lam in lambdas:
            idx = epa_idx_by_lambda[lam]
            for w in weights:
                t0 = time.monotonic()
                scoring_rows = _blend_scoring_rows(
                    scoring_games, rows_by_game_id, idx, market_by_game_id, w
                )
                metrics = score_margins(scoring_rows)
                results.append(
                    {
                        "k": elo_r["k"],
                        "divisor": elo_r["divisor"],
                        "hfa": elo_r["hfa"],
                        "lambda": lam,
                        "weight": w,
                        **metrics,
                    }
                )
                print(
                    f"TUNE_RESULT stage=blend K={int(elo_r['k'])} divisor={int(elo_r['divisor'])} "
                    f"hfa={int(elo_r['hfa'])} lambda={int(lam)} weight={w:.1f} "
                    f"mae={_fmt(metrics['mae'], '.3f')} ats3={_fmt(metrics['ats3'])} "
                    f"ats6={_fmt(metrics['ats6'])} n={metrics['n']}"
                )
                logger.info(
                    f"[blend] K={elo_r['k']} divisor={elo_r['divisor']} hfa={elo_r['hfa']} "
                    f"lambda={lam} weight={w} elapsed={time.monotonic() - t0:.1f}s"
                )
    return results


def print_baseline(
    elo_rows_cache: dict[tuple[float, float, float], dict[int, dict]],
    epa_idx_by_lambda: dict[float, dict],
    scoring_games: list[dict],
    market_by_game_id: dict[int, float],
    blend_results: list[dict],
) -> None:
    """TUNE_BASELINE line for the current ledger config, plus a rank summary
    against the swept blend combos (by walk-forward MAE)."""
    baseline_key = (BASELINE_K, BASELINE_DIVISOR, BASELINE_HFA)
    rows_by_game_id = elo_rows_cache.get(baseline_key)
    idx = epa_idx_by_lambda.get(BASELINE_LAMBDA)
    if rows_by_game_id is None or idx is None:
        logger.warning(
            "Current-ledger baseline (K=%s divisor=%s hfa=%s lambda=%s) is not covered by the "
            "swept grid -- skipping TUNE_BASELINE",
            BASELINE_K,
            BASELINE_DIVISOR,
            BASELINE_HFA,
            BASELINE_LAMBDA,
        )
        return

    scoring_rows = _blend_scoring_rows(
        scoring_games, rows_by_game_id, idx, market_by_game_id, BASELINE_WEIGHT
    )
    metrics = score_margins(scoring_rows)
    print(
        f"TUNE_BASELINE K={int(BASELINE_K)} divisor={int(BASELINE_DIVISOR)} "
        f"hfa={int(BASELINE_HFA)} lambda={int(BASELINE_LAMBDA)} weight={BASELINE_WEIGHT:.1f} "
        f"mae={_fmt(metrics['mae'], '.3f')} ats3={_fmt(metrics['ats3'])} "
        f"ats6={_fmt(metrics['ats6'])} n={metrics['n']}"
    )

    if math.isnan(metrics["mae"]):
        print("current ledger rank: n/a (no scored games for the baseline combo)")
        return
    other_maes = [r["mae"] for r in blend_results if not math.isnan(r["mae"])]
    rank, total = rank_baseline(metrics["mae"], other_maes)
    print(f"current ledger rank: {rank} of {total} (blend MAE vs swept combos, lower is better)")


def run_tuning(conn, start_season: int, end_season: int, quick: bool) -> None:
    replay_start = min(ELO_REPLAY_FLOOR_SEASON, start_season)
    plays_start = min(PLAYS_FLOOR_SEASON, start_season)

    logger.info(f"Fetching games {replay_start}-{end_season} for Elo replay...")
    games_by_season, scheduled_counts = fetch_elo_inputs(conn, replay_start, end_season)
    scoring_games = build_scoring_games(games_by_season, start_season, end_season)
    logger.info(f"Scoring window {start_season}-{end_season}: {len(scoring_games)} game(s)")

    game_ids = [g["game_id"] for g in scoring_games]
    logger.info(f"Fetching market spreads for {len(game_ids)} scoring-window game(s)...")
    market_by_game_id = fetch_market_spreads(conn, game_ids)

    logger.info(f"Fetching plays {plays_start}-{end_season} for EPA lambda evaluation...")
    plays_and_teams = fetch_plays_and_teams(conn, plays_start, end_season)

    elo_results, elo_rows_cache = run_stage_elo(
        games_by_season, scheduled_counts, scoring_games, market_by_game_id, quick
    )
    top3_elo = rank_by_mae(elo_results)[:3]

    lambda_results, epa_idx_by_lambda = run_stage_lambda(plays_and_teams, scoring_games, quick)

    blend_results = run_stage_blend(
        top3_elo, elo_rows_cache, epa_idx_by_lambda, scoring_games, market_by_game_id, quick
    )

    print_baseline(
        elo_rows_cache, epa_idx_by_lambda, scoring_games, market_by_game_id, blend_results
    )

    logger.info(
        f"Done: {len(elo_results)} elo combo(s), {len(lambda_results)} lambda(s), "
        f"{len(blend_results)} blend combo(s) scored."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Advisory in-memory tuning grid for Elo K/divisor/HFA, EPA lambda, and "
        "blend weight (Pillar B). Never writes to any table -- prints ranked results only; "
        "the user decides whether to change the ledger."
    )
    parser.add_argument(
        "--start-season",
        type=int,
        default=DEFAULT_START_SEASON,
        help=f"Scoring window start season (default {DEFAULT_START_SEASON})",
    )
    parser.add_argument(
        "--end-season",
        type=int,
        default=DEFAULT_END_SEASON,
        help=f"Scoring window end season (default {DEFAULT_END_SEASON})",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Shrink the grid to 2x2x2 Elo combos / 2 lambdas / 2 weights for smoke-testing.",
    )
    args = parser.parse_args()
    if args.start_season > args.end_season:
        logger.error(f"--start-season {args.start_season} is after --end-season {args.end_season}")
        sys.exit(1)

    import psycopg2

    t_start = time.monotonic()
    conn = psycopg2.connect(get_db_url())
    try:
        run_tuning(conn, args.start_season, args.end_season, quick=args.quick)
    except Exception:
        logger.exception("tune_params failed")
        sys.exit(1)
    finally:
        conn.close()

    logger.info(f"Total runtime: {time.monotonic() - t_start:.1f}s")


if __name__ == "__main__":
    main()
