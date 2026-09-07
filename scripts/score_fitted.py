#!/usr/bin/env python3
"""Score games with a frozen ``fitted_v1`` fit into predictions.game_predictions.

Companion to scripts/train_model.py (which trains + freezes the walk-forward
fits) and a sibling of scripts/compute_predictions.py (the Elo/blend writer).
The feature vectorization + transforms (``build_feature_vector``, ``standardize``,
``sigmoid``, ``platt_transform`` and the ``FEATURE_NAMES`` /
``TEAM_WEEK_SOURCE_COLUMNS`` contract) are imported from ``train_model`` so train
and score share one implementation; the market lookup, edge math and the exact
``predictions.game_predictions`` append (``compute_edge``, ``write_backfill_season``,
``write_upcoming``, the market fetchers, ``get_db_url``) are imported from
``compute_predictions`` so ``fitted_v1`` rows are written byte-identically to the
existing models -- same ``edge = expected_home_margin + market_spread`` convention.
Every row carries the immutable frozen fit artifact and its exact feature/market
input snapshot. ``fitted_v1``'s
``elo_margin`` / ``epa_margin`` columns are always NULL (it is neither an Elo nor a
blend model); ``home_elo_pregame`` / ``away_elo_pregame`` are populated from each
side's ``team_week.elo_pregame`` (the pregame Elo the ``d_elo`` feature used).

Frozen-fit selection (design section 2d / 3): for a backfill season ``S`` the fit
at ``train_through_season = S-1`` is used (hard error if it is missing -- never
silently fall back); for daily upcoming scoring each season ``S`` uses the
latest fit with ``train_through_season < S``.
Upcoming fits must also satisfy the shared contiguous season-finality and
feature-compatibility policy. Every pending season must meet the coverage
threshold before any upcoming predictions are written.

Usage:
    python scripts/score_fitted.py --backfill 2018 2025
        Score every completed game in each season S with the FROZEN S-1 fit;
        prediction_date = the game's start_date::date, which must be known;
        every run appends a walk_forward_reconstruction. Market: betting.lines
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

import numpy as np

from scripts import compute_predictions as predictions_module
from scripts import train_model as training_module
from scripts.compute_predictions import (
    compute_edge,
    fetch_market_from_lines,
    fetch_market_from_snapshots,
    get_db_url,
    table_exists,
    write_backfill_season,
    write_upcoming,
)
from scripts.prediction_provenance import (
    ARTIFACT_SCHEMA_VERSION,
    INPUT_SCHEMA_VERSION,
    PUBLISHED_FORECAST,
    WALK_FORWARD_RECONSTRUCTION,
    attach_prediction_provenance,
    identify_model_artifact,
    implementation_fingerprint,
)
from scripts.train_model import (
    FEATURE_NAMES,
    MODEL_VERSION,
    TEAM_WEEK_SOURCE_COLUMNS,
    build_feature_vector,
    fetch_refit_state,
    platt_transform,
    standardize,
)
from src.pipelines.game_identity import eligible_game_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# =============================================================================
# Pure helpers -- no I/O, unit-tested directly.
# =============================================================================


# Minimum share of pending games in EACH season that must score for an upcoming run
# to be considered healthy. Below this, run_upcoming exits non-zero rather than
# logging "nothing to write" and returning success -- the exact silent path
# that left fitted_v1 with zero 2026 rows for six months of green workflow runs
# (features.team_week was never built for the upcoming season, so the INNER
# JOIN in _score_games_query matched nothing).
MIN_UPCOMING_COVERAGE = 0.90


def coverage_verdict(
    n_pending: int, n_scored: int, min_coverage: float = MIN_UPCOMING_COVERAGE
) -> tuple[bool, float]:
    """Decide whether an upcoming-scoring run is healthy, and its coverage.

    Two genuinely different situations must not be conflated:

    - ``n_pending == 0`` -- no pending games exist at all (e.g. mid-January,
      after bowls, before next season's schedule publishes). Nothing to do;
      that is success, and coverage is reported as 1.0.
    - ``n_pending > 0`` but few or none scored -- pending games exist and the
      feature substrate is missing for them. That is a *failure* even though
      the old code path returned normally.

    Returns ``(ok, coverage)``. Pure so both branches are testable without a DB.
    """
    if n_pending == 0:
        return True, 1.0
    coverage = n_scored / n_pending
    return coverage >= min_coverage, coverage


def select_train_through(
    mode: str, score_season: int | None = None, available_train_through: list[int] | None = None
) -> int:
    """Which frozen ``train_through_season`` scores a game (design section 2d/3).

    ``mode='backfill'``: score season ``S`` -> ``S-1`` (walk-forward: the game's
    season was never in that fit's train window). ``mode='upcoming'``: the latest
    available fit strictly before ``score_season``. Pure so both branches are
    testable without a DB.
    """
    if mode == "backfill":
        if score_season is None:
            raise ValueError("backfill selection needs score_season")
        return score_season - 1
    if mode == "upcoming":
        if score_season is None:
            raise ValueError("upcoming selection needs score_season")
        eligible = [season for season in available_train_through or [] if season < score_season]
        if not eligible:
            raise ValueError(
                f"no eligible frozen fitted_v1 fit with train_through_season < {score_season}"
            )
        return max(eligible)
    raise ValueError(f"unknown selection mode {mode!r}")


def score_game(game: dict, fit: dict) -> tuple[float, float]:
    """Frozen-fit prediction for one game: ``(expected_home_margin, home_win_prob)``.

    Vectorizes with the fit's frozen imputation means, applies its frozen z-score
    stats, dots with the ridge-margin beta for the expected margin and with the
    IRLS beta for the logit, then Platt-calibrates. Returns Python floats (psycopg2
    does not adapt numpy scalars)."""
    train_through = fit.get("train_through")
    if train_through is None or train_through >= game["season"]:
        raise ValueError(
            f"fit train_through_season={train_through} must be before "
            f"prediction season={game['season']}"
        )
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


def fitted_model_artifact(fit: dict) -> tuple[str, dict]:
    """Capture the exact frozen fitted_v1 state consumed while scoring."""
    coefficients = [
        {
            "feature_order": index,
            "feature_name": feature,
            "margin": float(fit["beta_margin"][index]),
            "winprob": float(fit["beta_winprob"][index]),
        }
        for index, feature in enumerate(FEATURE_NAMES)
    ]
    artifact = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "artifact_kind": "frozen_fitted_model",
        "train_through_season": int(fit["train_through"]),
        "feature_names": list(FEATURE_NAMES),
        "feature_means": fit["feature_means"],
        "feature_diff_means": fit["diff_means"],
        "feature_diff_stds": fit["diff_stds"],
        "coefficients": coefficients,
        "calibration": {"platt_a": fit["platt_a"], "platt_b": fit["platt_b"]},
        "implementation": {
            "source": implementation_fingerprint(
                predictions_module, training_module, sys.modules[__name__]
            ),
            "runtime_constants": {
                "max_logit": training_module._MAX_LOGIT,
                "diff_feature_columns": list(training_module.DIFF_FEATURE_COLUMNS),
                "feature_names": list(FEATURE_NAMES),
                "team_week_source_columns": list(TEAM_WEEK_SOURCE_COLUMNS),
            },
        },
    }
    return identify_model_artifact(MODEL_VERSION, artifact)


def fitted_input_snapshot(game: dict, market: dict | None) -> dict:
    """Capture the exact raw game and team-week feature inputs consumed."""
    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "game": {
            "game_id": game["game_id"],
            "season": game["season"],
            "season_type": game["season_type"],
            "week": game["week"],
            "start_date": game.get("start_date"),
            "home_team": game["home_team"],
            "away_team": game["away_team"],
            "neutral_site": game["neutral_site"],
        },
        "team_week": {
            "home": {column: game["home_tw"].get(column) for column in TEAM_WEEK_SOURCE_COLUMNS},
            "away": {column: game["away_tw"].get(column) for column in TEAM_WEEK_SOURCE_COLUMNS},
        },
        "market": market,
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
          ON h.game_id = g.id AND h.team = g.home_team
        JOIN features.team_week a
          ON a.game_id = g.id AND a.team = g.away_team
        WHERE {where_clause}
        ORDER BY g.season, g.start_date NULLS LAST, g.id
    """


# Completed games of a single season (backfill scope).
_BACKFILL_WHERE = (
    "g.season = %s AND COALESCE(g.completed, false) "
    "AND g.home_points IS NOT NULL AND g.away_points IS NOT NULL "
    f"AND {eligible_game_sql('g.id')}"
)
# Pending games of the current-or-later season, mirroring
# compute_predictions.TARGET_GAMES_QUERY's selection.
PENDING_GAMES_WHERE = (
    "NOT COALESCE(g.completed, false) "
    f"AND {eligible_game_sql('g.id')} "
    "AND g.season >= (SELECT COALESCE(MAX(season), 0) FROM core.games "
    f"WHERE completed AND {eligible_game_sql('id')})"
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
        cur.execute(_score_games_query(PENDING_GAMES_WHERE))
        return _rows_to_games(cur.fetchall())


def fetch_pending_game_counts(conn) -> dict[int, int]:
    """Pending games in the projection window, counted from ``core.games``
    ALONE -- deliberately without the ``features.team_week`` join.

    This is the denominator the coverage gate needs: joining team_week here
    would make the count agree with the scored count by construction and the
    gate could never fire. Grouping by season prevents a large covered season
    from hiding missing features in a smaller pending season. Predicate matches
    ``PENDING_GAMES_WHERE`` exactly.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT g.season, COUNT(*)
            FROM core.games g
            WHERE {PENDING_GAMES_WHERE}
            GROUP BY g.season
            ORDER BY g.season
            """
        )
        return {int(season): int(count) for season, count in cur.fetchall()}


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
    """Backfill date from a known kickoff; historical time is never fabricated."""
    start_date = game.get("start_date")
    if start_date is None:
        raise ValueError(
            f"game_id={game['game_id']} has no kickoff; cannot create historical provenance"
        )
    return start_date.date()


def run_backfill(conn, start: int, end: int) -> None:
    if start > end:
        logger.error("--backfill start %d is after end %d", start, end)
        sys.exit(1)

    total_rows = 0
    for season in range(start, end + 1):
        train_through = select_train_through("backfill", score_season=season)
        fit = load_fit(conn, train_through)  # hard error if the frozen fit is missing
        fit_id, artifact = fitted_model_artifact(fit)

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
            rows.append(
                attach_prediction_provenance(
                    {**row, "prediction_date": _prediction_date(game)},
                    evaluation_mode=WALK_FORWARD_RECONSTRUCTION,
                    fit_id=fit_id,
                    model_artifact=artifact,
                    input_snapshot=fitted_input_snapshot(game, market),
                    simulated_as_of_at=game["start_date"],
                )
            )

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
    # Each denominator is counted before the feature join.
    pending_counts = fetch_pending_game_counts(conn)
    games = fetch_upcoming_games(conn)
    per_season = Counter(g["season"] for g in games)
    n_pending = sum(pending_counts.values())

    if not pending_counts and not games:
        print(
            "FITTED_COVERAGE_GATE pending=0 scored=0 "
            f"coverage=1.000 threshold={MIN_UPCOMING_COVERAGE:.2f}"
        )
        logger.info("No pending games at all; nothing to write")
        return

    failed_seasons = []
    for season in sorted(set(pending_counts) | set(per_season)):
        pending = pending_counts.get(season, 0)
        scored = per_season[season]
        ok, coverage = coverage_verdict(pending, scored)
        print(
            f"FITTED_COVERAGE_GATE season={season} pending={pending} scored={scored} "
            f"coverage={coverage:.3f} threshold={MIN_UPCOMING_COVERAGE:.2f}"
        )
        # A season appearing only after the denominator query is not a verified
        # scoring population; require a retry rather than bypassing its gate.
        if not ok or season not in pending_counts:
            failed_seasons.append(season)
    _, total_coverage = coverage_verdict(n_pending, len(games))
    print(
        f"FITTED_COVERAGE_GATE pending={n_pending} scored={len(games)} "
        f"coverage={total_coverage:.3f} threshold={MIN_UPCOMING_COVERAGE:.2f}"
    )
    if failed_seasons:
        logger.error(
            "Pending season(s) %s failed the per-season coverage gate; no predictions written. "
            "Run scripts/build_features.py --incremental and retry.",
            failed_seasons,
        )
        sys.exit(1)

    frontier, available = fetch_refit_state(conn)
    if frontier is None:
        raise ValueError("no safe closed training frontier for upcoming fitted_v1 scoring")
    # The shared state also rejects incompatible stored feature contracts. The
    # strict per-game season guard remains independent of that lifecycle gate.
    available = [season for season in available if season <= frontier]
    train_through_by_season = {
        season: select_train_through("upcoming", season, available)
        for season in sorted(pending_counts)
    }
    fits = {
        train_through: load_fit(conn, train_through)
        for train_through in sorted(set(train_through_by_season.values()))
    }
    artifacts = {train_through: fitted_model_artifact(fit) for train_through, fit in fits.items()}
    logger.info("Upcoming frozen fits by prediction season: %s", train_through_by_season)

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
        train_through = train_through_by_season[game["season"]]
        fit = fits[train_through]
        expected_margin, win_prob = score_game(game, fit)
        market = market_by_game.get(game["game_id"])
        if market and market.get("spread") is not None:
            n_with_market += 1
        fit_id, artifact = artifacts[train_through]
        rows.append(
            attach_prediction_provenance(
                build_score_row(game, expected_margin, win_prob, market),
                evaluation_mode=PUBLISHED_FORECAST,
                fit_id=fit_id,
                model_artifact=artifact,
                input_snapshot=fitted_input_snapshot(game, market),
            )
        )

    write_upcoming(conn, rows)
    logger.info(
        "Upcoming: wrote %d row(s) for %d game(s) (%d with a market line)",
        len(rows),
        len(games),
        n_with_market,
    )

    for season in sorted(per_season):
        print(
            f"SCORED_GATE season={season} rows={per_season[season]} model={MODEL_VERSION} "
            f"train_through={train_through_by_season[season]}"
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
        help="Score each pending season with its latest strictly prior frozen fit "
        "(default when no flag).",
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
