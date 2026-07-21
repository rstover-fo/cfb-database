#!/usr/bin/env python3
"""Score games with a frozen ``fitted_v1`` fit into predictions.game_predictions.

Companion to scripts/train_model.py (which trains + freezes the walk-forward
fits) and a sibling of scripts/compute_predictions.py (the Elo/blend writer).
The feature vectorization + transforms (``build_feature_vector``, ``standardize``,
``sigmoid``, ``platt_transform`` and the ``FEATURE_NAMES`` /
``TEAM_WEEK_SOURCE_COLUMNS`` contract) are imported from ``train_model`` so train
and score share one implementation; the market lookup, edge math and the exact
``predictions.game_predictions`` upsert (``compute_edge``, ``write_backfill_season``,
``write_upcoming``, the market fetchers, ``get_db_url``) are imported from
``compute_predictions`` so ``fitted_v1`` rows are written byte-identically to the
existing models -- same ``(game_id, model_version, prediction_date)`` conflict key,
same ``edge = expected_home_margin + market_spread`` convention. ``fitted_v1``'s
``elo_margin`` / ``epa_margin`` columns are always NULL (it is neither an Elo nor a
blend model); ``home_elo_pregame`` / ``away_elo_pregame`` are populated from each
side's ``team_week.elo_pregame`` (the pregame Elo the ``d_elo`` feature used).

Frozen-fit selection (design section 2d / 3): for a backfill season ``S`` the fit
at ``train_through_season = S-1`` is used (hard error if it is missing -- never
silently fall back); for daily upcoming scoring the latest fit
(``MAX(train_through_season)``) is used.

Usage:
    python scripts/score_fitted.py --backfill 2018 2025
        Score every completed game in each season S with the FROZEN S-1 fit;
        prediction_date = the game's start_date::date (fallback Jan 1 of its
        season), idempotent under the daily conflict key. Market: betting.lines
        (closing-line proxy), same as compute_predictions --backfill.

    python scripts/score_fitted.py            # or --upcoming
        Score pending games (current season / published next-season schedule)
        with the latest frozen fit. prediction_date = today (UTC, SQL-side).
        Market: betting.line_snapshots if present else betting.lines, same
        fallback as compute_predictions upcoming mode.

Each scored season prints:
    SCORED_GATE season={s} rows={n} model=fitted_v1 train_through={t}
"""

import argparse
import logging
import sys
from collections import Counter
from datetime import date

import numpy as np

from scripts.compute_predictions import (
    compute_edge,
    fetch_market_from_lines,
    fetch_market_from_snapshots,
    get_db_url,
    table_exists,
    write_backfill_season,
    write_upcoming,
)
from scripts.train_model import (
    MODEL_VERSION,
    TEAM_WEEK_SOURCE_COLUMNS,
    build_feature_vector,
    platt_transform,
    standardize,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# =============================================================================
# Pure helpers -- no I/O, unit-tested directly.
# =============================================================================


def select_train_through(
    mode: str, score_season: int | None = None, available_train_through: list[int] | None = None
) -> int:
    """Which frozen ``train_through_season`` scores a game (design section 2d/3).

    ``mode='backfill'``: score season ``S`` -> ``S-1`` (walk-forward: the game's
    season was never in that fit's train window). ``mode='upcoming'``: the latest
    fit, ``max(available_train_through)``. Pure so both branches are testable
    without a DB.
    """
    if mode == "backfill":
        if score_season is None:
            raise ValueError("backfill selection needs score_season")
        return score_season - 1
    if mode == "upcoming":
        if not available_train_through:
            raise ValueError("no frozen fitted_v1 fits available for upcoming scoring")
        return max(available_train_through)
    raise ValueError(f"unknown selection mode {mode!r}")


def score_game(game: dict, fit: dict) -> tuple[float, float]:
    """Frozen-fit prediction for one game: ``(expected_home_margin, home_win_prob)``.

    Vectorizes with the fit's frozen imputation means, applies its frozen z-score
    stats, dots with the ridge-margin beta for the expected margin and with the
    IRLS beta for the logit, then Platt-calibrates. Returns Python floats (psycopg2
    does not adapt numpy scalars)."""
    x_raw = build_feature_vector(game, game["home_tw"], game["away_tw"], fit["feature_means"])
    x_std = standardize(x_raw, fit["diff_means"], fit["diff_stds"])
    expected_margin = float(x_std @ fit["beta_margin"])
    logit = float(x_std @ fit["beta_winprob"])
    win_prob = platt_transform(logit, fit["platt_a"], fit["platt_b"])
    return expected_margin, win_prob


def build_score_row(
    game: dict, expected_margin: float, win_prob: float, market: dict | None
) -> dict:
    """One predictions.game_predictions row dict for ``fitted_v1`` (all
    _ROW_COLUMNS keys compute_predictions' writers expect). ``elo_margin`` /
    ``epa_margin`` are NULL for this model; the edge is computed by the shared
    ``compute_edge`` so the ``edge = expected_home_margin + market_spread``
    convention is not duplicated here."""
    if market:
        market_provider = market.get("provider")
        market_spread = market.get("spread")
        market_home_margin = -market_spread if market_spread is not None else None
        market_captured_at = market.get("captured_at")
    else:
        market_provider = None
        market_spread = None
        market_home_margin = None
        market_captured_at = None

    edge, edge_pick = compute_edge(expected_margin, market_spread)
    return {
        "model_version": MODEL_VERSION,
        "game_id": game["game_id"],
        "season": game["season"],
        "week": game["week"],
        "season_type": game["season_type"],
        "home_team": game["home_team"],
        "away_team": game["away_team"],
        "neutral_site": bool(game["neutral_site"]),
        "home_elo_pregame": game["home_tw"].get("elo_pregame"),
        "away_elo_pregame": game["away_tw"].get("elo_pregame"),
        "elo_margin": None,
        "epa_margin": None,
        "expected_home_margin": expected_margin,
        "home_win_prob": win_prob,
        "market_provider": market_provider,
        "market_home_margin": market_home_margin,
        "market_spread": market_spread,
        "market_captured_at": market_captured_at,
        "edge": edge,
        "edge_pick": edge_pick,
    }


# =============================================================================
# --- I/O layer --- (fetch frozen fit + games + market, score, write)
# =============================================================================


def _score_games_query(where_clause: str) -> str:
    """core.games (with identity + start_date) joined to both team-week sides,
    filtered by ``where_clause``. Column list built from TEAM_WEEK_SOURCE_COLUMNS
    so it tracks the feature contract."""
    home_cols = ",\n           ".join(f"h.{c} AS home_{c}" for c in TEAM_WEEK_SOURCE_COLUMNS)
    away_cols = ",\n           ".join(f"a.{c} AS away_{c}" for c in TEAM_WEEK_SOURCE_COLUMNS)
    return f"""
        SELECT g.id AS game_id, g.season, g.season_type, g.week, g.start_date,
               g.neutral_site, g.home_team, g.away_team,
               g.home_points, g.away_points,
           {home_cols},
           {away_cols}
        FROM core.games g
        JOIN features.team_week h
          ON h.season = g.season AND h.season_type = g.season_type
         AND h.week = g.week AND h.team = g.home_team
        JOIN features.team_week a
          ON a.season = g.season AND a.season_type = g.season_type
         AND a.week = g.week AND a.team = g.away_team
        WHERE {where_clause}
        ORDER BY g.season, g.start_date NULLS LAST, g.id
    """


# Completed games of a single season (backfill scope).
_BACKFILL_WHERE = (
    "g.season = %s AND COALESCE(g.completed, false) "
    "AND g.home_points IS NOT NULL AND g.away_points IS NOT NULL"
)
# Pending games of the current-or-later season, mirroring
# compute_predictions.TARGET_GAMES_QUERY's selection.
_UPCOMING_WHERE = (
    "NOT COALESCE(g.completed, false) "
    "AND g.season >= (SELECT COALESCE(MAX(season), 0) FROM core.games WHERE completed)"
)


def _rows_to_games(raw: list[dict]) -> list[dict]:
    games: list[dict] = []
    for r in raw:
        games.append(
            {
                "game_id": r["game_id"],
                "season": r["season"],
                "season_type": r["season_type"],
                "week": r["week"],
                "start_date": r["start_date"],
                "neutral_site": r["neutral_site"],
                "home_team": r["home_team"],
                "away_team": r["away_team"],
                "home_points": r["home_points"],
                "away_points": r["away_points"],
                "home_tw": {c: r[f"home_{c}"] for c in TEAM_WEEK_SOURCE_COLUMNS},
                "away_tw": {c: r[f"away_{c}"] for c in TEAM_WEEK_SOURCE_COLUMNS},
            }
        )
    return games


def fetch_backfill_games(conn, season: int) -> list[dict]:
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_score_games_query(_BACKFILL_WHERE), (season,))
        return _rows_to_games(cur.fetchall())


def fetch_upcoming_games(conn) -> list[dict]:
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_score_games_query(_UPCOMING_WHERE))
        return _rows_to_games(cur.fetchall())


def fetch_available_train_through(conn) -> list[int]:
    """Every ``train_through_season`` with a persisted fitted_v1 fit."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT train_through_season FROM features.model_metadata "
            "WHERE model_version = %s",
            (MODEL_VERSION,),
        )
        return [int(row[0]) for row in cur.fetchall()]


def load_fit(conn, train_through: int) -> dict:
    """Load the frozen fitted_v1 fit for ``train_through_season``: the frozen
    imputation means / z-score stats + Platt params from
    features.model_metadata, and both coefficient vectors (ordered by
    FEATURE_NAMES position) from features.model_coefficients. Hard error if the
    metadata row is absent -- scoring must use the exact frozen fit, never
    improvise one."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT platt_a, platt_b, feature_means, feature_diff_means, feature_diff_stds
            FROM features.model_metadata
            WHERE model_version = %s AND train_through_season = %s
            """,
            (MODEL_VERSION, train_through),
        )
        meta = cur.fetchone()
        if meta is None:
            raise RuntimeError(
                f"No frozen {MODEL_VERSION} fit for train_through_season={train_through}; "
                "run scripts/train_model.py first"
            )
        platt_a, platt_b, feature_means, diff_means, diff_stds = meta

        cur.execute(
            """
            SELECT model_component, feature_order, feature_name, coefficient
            FROM features.model_coefficients
            WHERE model_version = %s AND train_through_season = %s
            ORDER BY model_component, feature_order
            """,
            (MODEL_VERSION, train_through),
        )
        coef_by_component: dict[str, dict[str, float]] = {}
        for component, _order, feature_name, coefficient in cur.fetchall():
            coef_by_component.setdefault(component, {})[feature_name] = float(coefficient)

    from scripts.train_model import FEATURE_NAMES

    def _beta(component: str) -> np.ndarray:
        coefs = coef_by_component.get(component)
        if not coefs:
            raise RuntimeError(
                f"{MODEL_VERSION} fit train_through_season={train_through} is missing "
                f"'{component}' coefficients"
            )
        return np.array([coefs[name] for name in FEATURE_NAMES], dtype=np.float64)

    return {
        "train_through": train_through,
        "feature_means": {
            k: (float(v) if v is not None else None) for k, v in feature_means.items()
        },
        "diff_means": {k: float(v) for k, v in diff_means.items()},
        "diff_stds": {k: float(v) for k, v in diff_stds.items()},
        "platt_a": float(platt_a),
        "platt_b": float(platt_b),
        "beta_margin": _beta("margin"),
        "beta_winprob": _beta("winprob"),
    }


def _prediction_date(game: dict):
    """Backfill prediction_date: the game's start_date::date, falling back to
    Jan 1 of its season when start_date is NULL -- matches
    compute_predictions.run_backfill so re-runs are idempotent."""
    start_date = game["start_date"]
    return start_date.date() if start_date is not None else date(game["season"], 1, 1)


def run_backfill(conn, start: int, end: int) -> None:
    if start > end:
        logger.error("--backfill start %d is after end %d", start, end)
        sys.exit(1)

    total_rows = 0
    for season in range(start, end + 1):
        train_through = select_train_through("backfill", score_season=season)
        fit = load_fit(conn, train_through)  # hard error if the frozen fit is missing

        games = fetch_backfill_games(conn, season)
        if not games:
            logger.info("season=%d: no completed games with team_week features, skipping", season)
            print(
                f"SCORED_GATE season={season} rows=0 model={MODEL_VERSION} "
                f"train_through={train_through}"
            )
            continue

        market_by_game = fetch_market_from_lines(conn, [g["game_id"] for g in games])
        rows: list[dict] = []
        n_with_market = 0
        for game in games:
            expected_margin, win_prob = score_game(game, fit)
            market = market_by_game.get(game["game_id"])
            if market and market.get("spread") is not None:
                n_with_market += 1
            row = build_score_row(game, expected_margin, win_prob, market)
            row["prediction_date"] = _prediction_date(game)
            rows.append(row)

        write_backfill_season(conn, rows)
        total_rows += len(rows)
        logger.info(
            "season=%d: %d game(s), wrote %d row(s) (%d with a market line), train_through=%d",
            season,
            len(games),
            len(rows),
            n_with_market,
            train_through,
        )
        print(
            f"SCORED_GATE season={season} rows={len(rows)} model={MODEL_VERSION} "
            f"train_through={train_through}"
        )

    logger.info("Backfill %d-%d: wrote %d row(s) total", start, end, total_rows)


def run_upcoming(conn) -> None:
    available = fetch_available_train_through(conn)
    if not available:
        logger.error(
            "No frozen %s fits in features.model_metadata; run train_model.py", MODEL_VERSION
        )
        sys.exit(1)
    train_through = select_train_through("upcoming", available_train_through=available)
    fit = load_fit(conn, train_through)
    logger.info("Upcoming scoring with latest frozen fit train_through=%d", train_through)

    games = fetch_upcoming_games(conn)
    if not games:
        logger.info("No pending games with team_week features; nothing to write")
        return

    game_ids = [g["game_id"] for g in games]
    if table_exists(conn, "betting", "line_snapshots"):
        market_by_game = fetch_market_from_snapshots(conn, game_ids)
        logger.info("Market source: betting.line_snapshots (latest per game, consensus preferred)")
    else:
        market_by_game = fetch_market_from_lines(conn, game_ids)
        logger.info("Market source: betting.lines (betting.line_snapshots not present)")

    rows: list[dict] = []
    n_with_market = 0
    for game in games:
        expected_margin, win_prob = score_game(game, fit)
        market = market_by_game.get(game["game_id"])
        if market and market.get("spread") is not None:
            n_with_market += 1
        rows.append(build_score_row(game, expected_margin, win_prob, market))

    write_upcoming(conn, rows)
    logger.info(
        "Upcoming: wrote %d row(s) for %d game(s) (%d with a market line)",
        len(rows),
        len(games),
        n_with_market,
    )

    per_season = Counter(g["season"] for g in games)
    for season in sorted(per_season):
        print(
            f"SCORED_GATE season={season} rows={per_season[season]} model={MODEL_VERSION} "
            f"train_through={train_through}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Score games with a frozen fitted_v1 fit into predictions.game_predictions"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--backfill",
        nargs=2,
        type=int,
        metavar=("START", "END"),
        help="Backfill completed games for seasons START..END using each season's "
        "frozen S-1 fit; prediction_date = the game's start_date.",
    )
    group.add_argument(
        "--upcoming",
        action="store_true",
        help="Score pending games with the latest frozen fit (default when no flag).",
    )
    args = parser.parse_args()

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        if args.backfill:
            run_backfill(conn, args.backfill[0], args.backfill[1])
        else:
            run_upcoming(conn)
    except Exception:
        conn.rollback()
        logger.exception("fitted_v1 scoring failed")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
