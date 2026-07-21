#!/usr/bin/env python3
"""Calibrate the house live win-probability sigma (Tier 3 Gate D).

docs/plans/2026-07-21-tier3-analytics-plan.md, Pillar D, Phase 7. Reconstructs
in-game states from metrics.win_probability (loaded by
scripts/backfill_ingame_wp.py via
src.pipelines.sources.metrics.win_probability_by_game_resource), joins each
state to its game's final outcome (core.games) and pregame expected margin
(analytics.house_elo_game, via scripts.compute_predictions.elo_margin --
imported, not re-derived), then grid-searches sigma in
scripts.poll_scoreboard.house_live_home_wp (also imported, not re-derived) to
minimize Brier score against those outcomes.

Architecture mirrors the rest of the Tier 3 compute scripts: the grid search,
Brier scoring, decile calibration, sigma-grid parsing, and in-game-state
reconstruction (`build_states`) are pure -- plain dicts/floats, no I/O -- so
they are fully unit-testable without a database (see
tests/test_live_wp.py, which recovers a known synthetic sigma). Everything
below `# --- I/O layer ---` is a thin wrapper: verify metrics.win_probability's
actual columns, fetch + join, drive the pure functions, print the report, and
optionally write live.wp_params.

metrics.win_probability's schema is NOT hardcoded here. win_probability_by_game_resource
is new (this same Tier 3 phase) and this script has no guarantee it has ever
been run against a live database when this script itself is exercised, so
`discover_wp_columns` verifies the table's actual columns via
information_schema at runtime and fails with a clear message (listing what
IS there) if the expected shape isn't found, rather than assuming column
names and getting an opaque "column does not exist" from Postgres.

Usage:
    python scripts/calibrate_live_wp.py
        Grid-search sigma over the default grid (10..24 step 1), print the
        per-sigma Brier table, the decile calibration curve for the winning
        sigma, and house-vs-CFBD Brier on the same states. Advisory only --
        does not write live.wp_params.

    python scripts/calibrate_live_wp.py --sigma-grid 12:20:0.5
        Custom grid: 'min:max:step' (inclusive of max), or a single value.

    python scripts/calibrate_live_wp.py --write
        Same as above, then UPDATEs live.wp_params id=1 (sigma,
        fitted_through_season, n_games, brier, updated_at) with the winning
        fit.

Prints, at the end:
    LIVE_WP_GATE sigma=<s> brier=<b:.4f> cfbd_brier=<c:.4f> n_states=<n>
"""

import argparse
import logging
import sys
from collections import defaultdict

from scripts.compute_predictions import elo_margin
from scripts.poll_scoreboard import house_live_home_wp, parse_clock

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_SIGMA_GRID_SPEC = "10:24:1"
DEFAULT_N_DECILES = 10

# =============================================================================
# Pure core -- no I/O, no DB, unit-tested directly (tests/test_live_wp.py).
# =============================================================================


def parse_sigma_grid(spec: str) -> list[float]:
    """Parse --sigma-grid: 'min:max:step' (inclusive of max), or a single
    numeric value."""
    parts = spec.split(":")
    if len(parts) == 1:
        return [float(parts[0])]
    if len(parts) != 3:
        raise ValueError(f"Invalid --sigma-grid {spec!r} (expected 'min:max:step' or a number)")
    lo, hi, step = (float(p) for p in parts)
    if step <= 0:
        raise ValueError(f"Invalid --sigma-grid {spec!r}: step must be positive")
    if hi < lo:
        raise ValueError(f"Invalid --sigma-grid {spec!r}: max must be >= min")
    n_steps = int(round((hi - lo) / step)) + 1
    return [round(lo + i * step, 6) for i in range(n_steps)]


def brier_score(predictions: list[float], outcomes: list[float]) -> float:
    """Mean squared error between `predictions` (probabilities) and
    `outcomes` (0/1). NaN for an empty input (no states to score)."""
    n = len(predictions)
    if n == 0:
        return float("nan")
    return sum((p - o) ** 2 for p, o in zip(predictions, outcomes)) / n


def predict_states(states: list[dict], sigma: float) -> list[float]:
    """house_live_home_wp for every state at a given sigma. Each state dict
    needs current_margin, pregame_expected_margin, seconds_remaining."""
    return [
        house_live_home_wp(
            s["current_margin"], s["pregame_expected_margin"], s["seconds_remaining"], sigma
        )
        for s in states
    ]


def grid_search_sigma(states: list[dict], sigma_grid: list[float]) -> list[tuple[float, float]]:
    """[(sigma, brier), ...] for every sigma in `sigma_grid`, scored against
    `states`' actual outcomes (state["home_win"], 0/1)."""
    outcomes = [s["home_win"] for s in states]
    return [(sigma, brier_score(predict_states(states, sigma), outcomes)) for sigma in sigma_grid]


def best_sigma(results: list[tuple[float, float]]) -> tuple[float, float]:
    """(sigma, brier) minimizing brier."""
    return min(results, key=lambda r: r[1])


def decile_calibration(
    predictions: list[float], outcomes: list[float], n_buckets: int = DEFAULT_N_DECILES
) -> list[dict]:
    """Sort states by predicted probability, split into `n_buckets` equal-
    count buckets, and report each bucket's mean predicted probability vs.
    empirical (actual) win rate -- a calibration curve. Buckets with zero
    rows (fewer states than buckets) report n=0 rather than dividing by
    zero."""
    paired = sorted(zip(predictions, outcomes), key=lambda t: t[0])
    n = len(paired)
    buckets = []
    for i in range(n_buckets):
        lo = (i * n) // n_buckets
        hi = ((i + 1) * n) // n_buckets
        chunk = paired[lo:hi]
        if not chunk:
            buckets.append(
                {"decile": i + 1, "n": 0, "predicted_mean": None, "empirical_rate": None}
            )
            continue
        preds = [p for p, _o in chunk]
        outs = [o for _p, o in chunk]
        buckets.append(
            {
                "decile": i + 1,
                "n": len(chunk),
                "predicted_mean": sum(preds) / len(preds),
                "empirical_rate": sum(outs) / len(outs),
            }
        )
    return buckets


def build_states(rows: list[dict], columns: dict) -> list[dict]:
    """Reconstruct in-game states from fetched metrics.win_probability rows
    (already joined to core.games + analytics.house_elo_game -- see
    fetch_wp_rows). Pure: takes plain dicts, does no I/O.

    `rows` need: game_id, home_score, away_score, cfbd_wp, order_key,
    home_points, away_points, home_pregame_elo, away_pregame_elo,
    neutral_site, season, and (if `columns` has period/clock resolved)
    period/clock.

    seconds_remaining: uses period+clock via parse_clock when both columns
    are present in the source table; otherwise (the expected case for
    win_probability_by_game_resource today -- it carries no clock, only a
    play_number ordinal) falls back to play-order-as-fraction-of-game: for
    each game, seconds_remaining = round(3600 * (1 - rank/span)) where rank
    is this row's order_key position between the game's min and max
    order_key. This is a documented approximation, not a true per-play
    clock read.

    A game contributes no states if it's missing a pregame Elo pair or its
    final score is a tie (no meaningful home_win label).
    """
    has_clock = bool(columns.get("period")) and bool(columns.get("clock"))

    by_game: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        by_game[row["game_id"]].append(row)

    states: list[dict] = []
    for game_id, game_rows in by_game.items():
        first = game_rows[0]
        home_points = first.get("home_points")
        away_points = first.get("away_points")
        if home_points is None or away_points is None or home_points == away_points:
            continue  # no final score, or a tie -- no usable home_win label
        home_win = 1.0 if home_points > away_points else 0.0

        home_elo = first.get("home_pregame_elo")
        away_elo = first.get("away_pregame_elo")
        if home_elo is None or away_elo is None:
            continue  # no pregame Elo -- can't compute pregame_expected_margin
        neutral = bool(first.get("neutral_site"))
        pregame_expected_margin = elo_margin(float(home_elo), float(away_elo), neutral)

        order_keys = [r["order_key"] for r in game_rows if r.get("order_key") is not None]
        lo = min(order_keys) if order_keys else 0
        hi = max(order_keys) if order_keys else 0
        span = hi - lo

        for row in game_rows:
            seconds_remaining = None
            if has_clock:
                seconds_remaining = parse_clock(row.get("clock"), row.get("period"))
            if seconds_remaining is None:
                order_key = row.get("order_key")
                if order_key is None or span <= 0:
                    fraction_elapsed = 0.0
                else:
                    fraction_elapsed = (order_key - lo) / span
                fraction_elapsed = min(max(fraction_elapsed, 0.0), 1.0)
                seconds_remaining = round(3600 * (1 - fraction_elapsed))

            if row.get("home_score") is None or row.get("away_score") is None:
                continue
            if row.get("cfbd_wp") is None:
                continue

            states.append(
                {
                    "game_id": game_id,
                    "season": first.get("season"),
                    "current_margin": float(row["home_score"]) - float(row["away_score"]),
                    "pregame_expected_margin": pregame_expected_margin,
                    "seconds_remaining": seconds_remaining,
                    "home_win": home_win,
                    "cfbd_wp": float(row["cfbd_wp"]),
                }
            )

    return states


# =============================================================================
# --- I/O layer --- (thin: verify columns, fetch + join, print, write)
# =============================================================================


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Copied from scripts/compute_house_elo.py's get_db_url pattern (each
    compute_*.py / calibration script keeps its own copy rather than
    importing across scripts for this one utility).
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


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
        return cur.fetchone()[0] is not None


WP_TABLE_SCHEMA = "metrics"
WP_TABLE_NAME = "win_probability"

# Primary candidates match src.pipelines.sources.metrics.win_probability_by_game_resource's
# documented dlt-snake-cased columns (gameId -> game_id, homeScore ->
# home_score, homeWinProbability -> home_win_probability, playNumber ->
# play_number); extra fallbacks are defensive, not confirmed.
_GAME_ID_CANDIDATES = ("game_id",)
_HOME_SCORE_CANDIDATES = ("home_score",)
_AWAY_SCORE_CANDIDATES = ("away_score",)
_HOME_WIN_PROB_CANDIDATES = ("home_win_probability", "home_win_prob")
_ORDER_CANDIDATES = ("play_number", "play_num", "play_id")
_PERIOD_CANDIDATES = ("period",)
_CLOCK_CANDIDATES = ("clock",)


def discover_wp_columns(conn) -> dict:
    """Resolve metrics.win_probability's logical columns via
    information_schema instead of hardcoding names. Raises RuntimeError with
    a clear message -- listing the columns that ARE there -- if a required
    logical field can't be resolved to any actual column."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (WP_TABLE_SCHEMA, WP_TABLE_NAME),
        )
        available = {row[0] for row in cur.fetchall()}

    if not available:
        raise RuntimeError(
            f"{WP_TABLE_SCHEMA}.{WP_TABLE_NAME} does not exist or has no columns -- run "
            "scripts/backfill_ingame_wp.py (Tier 3 Pillar D Phase 7 backfill) before "
            "calibrating."
        )

    def pick(*candidates: str) -> str | None:
        for candidate in candidates:
            if candidate in available:
                return candidate
        return None

    resolved = {
        "game_id": pick(*_GAME_ID_CANDIDATES),
        "home_score": pick(*_HOME_SCORE_CANDIDATES),
        "away_score": pick(*_AWAY_SCORE_CANDIDATES),
        "home_win_prob": pick(*_HOME_WIN_PROB_CANDIDATES),
        "order_col": pick(*_ORDER_CANDIDATES),
        "period": pick(*_PERIOD_CANDIDATES),
        "clock": pick(*_CLOCK_CANDIDATES),
    }

    required = ("game_id", "home_score", "away_score", "home_win_prob", "order_col")
    missing = [name for name in required if resolved[name] is None]
    if missing:
        raise RuntimeError(
            f"{WP_TABLE_SCHEMA}.{WP_TABLE_NAME} is missing expected column(s) for: "
            f"{', '.join(missing)}. Available columns: {sorted(available)}. Update "
            "calibrate_live_wp.py's column candidates to match the actual "
            "win_probability_by_game_resource schema."
        )

    return resolved


def fetch_wp_rows(conn, columns: dict) -> list[dict]:
    """metrics.win_probability rows joined to core.games (final score) and
    analytics.house_elo_game (pregame Elo), using the resolved column names
    from discover_wp_columns."""
    import psycopg2.extras

    select_parts = [
        f"w.{columns['game_id']} AS game_id",
        f"w.{columns['home_score']} AS home_score",
        f"w.{columns['away_score']} AS away_score",
        f"w.{columns['home_win_prob']} AS cfbd_wp",
        f"w.{columns['order_col']} AS order_key",
    ]
    if columns.get("period"):
        select_parts.append(f"w.{columns['period']} AS period")
    if columns.get("clock"):
        select_parts.append(f"w.{columns['clock']} AS clock")

    query = f"""
        SELECT {", ".join(select_parts)},
               g.season, g.home_points, g.away_points,
               e.home_pregame_elo, e.away_pregame_elo, e.neutral_site
        FROM {WP_TABLE_SCHEMA}.{WP_TABLE_NAME} w
        JOIN core.games g ON g.id = w.{columns["game_id"]}
        JOIN analytics.house_elo_game e ON e.game_id = w.{columns["game_id"]}
        WHERE g.completed = true
          AND g.home_points IS NOT NULL
          AND g.away_points IS NOT NULL
          AND w.{columns["home_win_prob"]} IS NOT NULL
          AND w.{columns["home_score"]} IS NOT NULL
          AND w.{columns["away_score"]} IS NOT NULL
        ORDER BY w.{columns["game_id"]}, w.{columns["order_col"]}
    """

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        return [dict(row) for row in cur.fetchall()]


_WRITE_WP_PARAMS_SQL = """
    INSERT INTO live.wp_params (id, sigma, fitted_through_season, n_games, brier, updated_at)
    VALUES (1, %s, %s, %s, %s, now())
    ON CONFLICT (id) DO UPDATE SET
        sigma = EXCLUDED.sigma,
        fitted_through_season = EXCLUDED.fitted_through_season,
        n_games = EXCLUDED.n_games,
        brier = EXCLUDED.brier,
        updated_at = EXCLUDED.updated_at
"""


def write_wp_params(
    conn, sigma: float, fitted_through_season: int | None, n_games: int, brier: float
) -> None:
    with conn.cursor() as cur:
        cur.execute(_WRITE_WP_PARAMS_SQL, (sigma, fitted_through_season, n_games, brier))
    conn.commit()


def print_sigma_table(results: list[tuple[float, float]]) -> None:
    print("\n===== SIGMA GRID (Brier, house model) =====")
    for sigma, brier in results:
        print(f"sigma={sigma:g} brier={brier:.4f}")


def print_decile_table(deciles: list[dict]) -> None:
    print("\n===== DECILE CALIBRATION (house model, best sigma) =====")
    for d in deciles:
        if d["n"] == 0:
            print(f"decile={d['decile']:>2} n=0 predicted_mean=- empirical_rate=-")
            continue
        print(
            f"decile={d['decile']:>2} n={d['n']:>6} "
            f"predicted_mean={d['predicted_mean']:.4f} empirical_rate={d['empirical_rate']:.4f}"
        )


def run(conn, sigma_grid: list[float], write: bool) -> int:
    for schema, table in (("core", "games"), ("analytics", "house_elo_game")):
        if not table_exists(conn, schema, table):
            logger.error(f"{schema}.{table} is missing -- cannot reconstruct in-game states.")
            return 1

    columns = discover_wp_columns(conn)
    logger.info(f"metrics.win_probability columns resolved: {columns}")

    rows = fetch_wp_rows(conn, columns)
    if not rows:
        logger.error(
            "No metrics.win_probability rows joined to core.games + "
            "analytics.house_elo_game -- has scripts/backfill_ingame_wp.py run yet?"
        )
        return 1

    states = build_states(rows, columns)
    if not states:
        logger.error("Reconstructed zero usable in-game states from the fetched rows.")
        return 1

    results = grid_search_sigma(states, sigma_grid)
    print_sigma_table(results)

    sigma, brier = best_sigma(results)
    outcomes = [s["home_win"] for s in states]
    house_preds = predict_states(states, sigma)
    cfbd_preds = [s["cfbd_wp"] for s in states]
    cfbd_brier = brier_score(cfbd_preds, outcomes)

    print_decile_table(decile_calibration(house_preds, outcomes))

    n_states = len(states)
    n_games = len({s["game_id"] for s in states})
    seasons = [s["season"] for s in states if s.get("season") is not None]
    fitted_through_season = max(seasons) if seasons else None

    print(
        f"LIVE_WP_GATE sigma={sigma:g} brier={brier:.4f} cfbd_brier={cfbd_brier:.4f} "
        f"n_states={n_states}"
    )
    logger.info(f"n_games={n_games} fitted_through_season={fitted_through_season}")

    if write:
        write_wp_params(conn, sigma, fitted_through_season, n_games, brier)
        logger.info("live.wp_params id=1 updated")
    else:
        logger.info("--write not set: advisory-only, live.wp_params unchanged")

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calibrate the house live win-probability sigma against a "
        "metrics.win_probability in-game state reconstruction (Tier 3 Gate D)."
    )
    parser.add_argument(
        "--sigma-grid",
        default=DEFAULT_SIGMA_GRID_SPEC,
        help=f"Grid spec 'min:max:step' (default {DEFAULT_SIGMA_GRID_SPEC!r}) or a single value",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the winning sigma (+ fit metadata) to live.wp_params id=1. "
        "Without this flag the run is advisory-only.",
    )
    args = parser.parse_args()

    try:
        sigma_grid = parse_sigma_grid(args.sigma_grid)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        exit_code = run(conn, sigma_grid, args.write)
    except Exception:
        conn.rollback()
        logger.exception("Calibration failed")
        sys.exit(1)
    finally:
        conn.close()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
