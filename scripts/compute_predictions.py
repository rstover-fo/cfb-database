#!/usr/bin/env python3
"""Compute house predictions (Elo + ridge-EPA blend) vs the market line.

Architecture mirrors scripts/compute_house_elo.py: the prediction math
(`elo_margin`, `epa_margin`, `blend_margin`, `elo_win_prob`, `compute_edge`,
plus the small lookup helpers below them) is pure -- plain floats/dicts, no
I/O -- so it is fully unit-testable without a database (see
tests/test_predictions.py). Everything below `# --- I/O layer ---` is a thin
wrapper: fetch games + ratings + market lines, feed them through the pure
functions, upsert the results into predictions.game_predictions
(src/schemas/migrations/024_predictions_schema.sql).

House Elo's `expected_score` (the 400-scale logistic) and the season-carryover
constants (`EloEngine.SEED`, `EloEngine.CARRYOVER`) are imported from
compute_house_elo rather than re-derived, per
docs/plans/2026-07-21-tier2-analytics-plan.md.

Every game gets TWO rows, one per model_version:
  - MODEL_ELO ("elo_v1"): expected_home_margin = elo_margin, epa_margin
    column is always NULL (this model never looks at EPA).
  - MODEL_BLEND ("elo_epa_blend_v1"): expected_home_margin =
    blend_margin(elo_margin, epa_margin); epa_margin column holds the
    computed value, or NULL if either team lacks a usable ridge-EPA fit
    (blend then falls back to Elo-only for expected_home_margin too).
home_win_prob is Elo-only in BOTH rows.

Usage:
    python scripts/compute_predictions.py
        Score upcoming/pending games: NOT completed games in the current
        season or any already-published next-season schedule. Ratings come
        from the live analytics.house_elo_current snapshot (with carryover
        applied for teams whose snapshot predates the game's season) and the
        current analytics.adjusted_epa_build fit (current-or-previous season
        only, per team). Market line: latest betting.line_snapshots per game
        if that table exists, else betting.lines. prediction_date is written
        as `(now() AT TIME ZONE 'utc')::date` -- a SQL-side expression, not a
        Python-computed value -- so every row in the batch gets the exact
        same date the DB itself would use, one single commit.

    python scripts/compute_predictions.py --backfill 2015 2025
        Retroactively score completed games for seasons 2015-2025 (inclusive)
        using each game's true walk-forward pregame Elo from
        analytics.house_elo_game and SAME-SEASON-ONLY ridge-adjusted EPA
        (documented leaky-for-blend -- see marts/038_prediction_accuracy.sql's
        header). Market line: betting.lines only (closing-line proxy).
        prediction_date = the game's start_date::date, falling back to
        make_date(season, 1, 1) when start_date is NULL, so re-running a
        backfill is idempotent under the (game_id, model_version,
        prediction_date) unique key. Commits once per season.
"""

import argparse
import logging
import sys
from datetime import date

from scripts.compute_house_elo import EloEngine, expected_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# Pure prediction math -- no I/O, no DB, unit-tested directly.
# Parameters are FINAL per the design doc; see the plan's tunable ledger.
# =============================================================================

PLAYS = 68
BLEND_ELO = 0.6
BLEND_EPA = 0.4
MODEL_ELO = "elo_v1"
MODEL_BLEND = "elo_epa_blend_v1"


def elo_margin(home_elo: float, away_elo: float, neutral: bool) -> float:
    """Elo-implied expected home margin: elo_diff/DIVISOR, plus a home-field
    bump of HFA/DIVISOR unless the game is at a neutral site."""
    hfa = 0.0 if neutral else EloEngine.HFA
    return (home_elo - away_elo + hfa) / EloEngine.DIVISOR


def elo_win_prob(home_elo: float, away_elo: float, neutral: bool) -> float:
    """Elo-only home win probability (canonical 400-scale logistic), used as
    home_win_prob for BOTH model_versions -- the blend only changes the
    expected margin, never the win probability."""
    hfa = 0.0 if neutral else EloEngine.HFA
    return expected_score(home_elo - away_elo + hfa)


def epa_margin(
    off_h: float, def_h: float, off_a: float, def_a: float, hfa_coef: float, neutral: bool
) -> float:
    """Ridge-adjusted-EPA implied expected home margin.

    Sign convention (analytics.adjusted_epa_build): off_coef higher = better
    offense; def_coef LOWER/more negative = better defense (EPA allowed
    above average), so `def_a` is ADDED (a stingy away defense pulls the
    home margin down) and `def_h` is SUBTRACTED (a stingy home defense pulls
    the home margin up). hfa_coef is the home team's row's fitted
    home-field-advantage coefficient for that team's season; zeroed at a
    neutral site.
    """
    per_play_edge = (off_h + def_a) - (off_a + def_h)
    hfa_term = hfa_coef * PLAYS * (0 if neutral else 1)
    return per_play_edge * PLAYS + hfa_term


def blend_margin(elo_m: float, epa_m: float | None) -> float:
    """0.6*Elo + 0.4*EPA blend; falls back to Elo-only when EPA is
    unavailable (thin/pre-2004 data, or either team missing a ridge fit)."""
    if epa_m is None:
        return elo_m
    return BLEND_ELO * elo_m + BLEND_EPA * epa_m


def compute_edge(
    expected_home_margin: float, market_spread: float | None
) -> tuple[float | None, str | None]:
    """Compare the model's expected home margin against the market.

    market_home_margin = -market_spread (negative spread = home favored,
    matching api/003_game_detail.sql's cover logic). edge =
    expected_home_margin - market_home_margin = expected_home_margin +
    market_spread; edge > 0 means the model likes home more than the market
    does (home undervalued) -> pick home. edge == 0 also picks home (no
    "push" pick). Returns (None, None) when there is no market line to
    compare against.
    """
    if market_spread is None:
        return None, None
    edge = expected_home_margin + market_spread
    edge_pick = "home" if edge >= 0 else "away"
    return edge, edge_pick


def carryover_rating(rating: float, elapsed: int) -> float:
    """Regress `rating` toward EloEngine.SEED for `elapsed` seasons with no
    persisted house_elo_game activity, replicating EloEngine.start_season's
    per-team carryover exactly (SEED/CARRYOVER imported, not re-derived).
    A no-op when elapsed <= 0 (rating is already current)."""
    if elapsed <= 0:
        return rating
    offset = rating - EloEngine.SEED
    return EloEngine.SEED + offset * (EloEngine.CARRYOVER**elapsed)


def resolve_elo(team: str, season: int, elo_current: dict[str, tuple[float, int]]) -> float:
    """Rating to use for `team` in a game played in `season`, from the
    analytics.house_elo_current snapshot ({team: (rating, snapshot_season)}).
    Missing team -> EloEngine.SEED (1500). A snapshot older than `season`
    gets carryover_rating applied for the elapsed seasons."""
    entry = elo_current.get(team)
    if entry is None:
        return EloEngine.SEED
    rating, snapshot_season = entry
    return carryover_rating(rating, season - snapshot_season)


def lookup_epa_coefs(
    epa_by_team_season: dict[tuple[str, int], dict], team: str, season: int, lookback: int
) -> dict | None:
    """Return `team`'s analytics.adjusted_epa_build row for the greatest
    season in [season - lookback, season], or None if none qualifies.

    lookback=1 (upcoming mode): current-or-immediately-previous season only.
    lookback=0 (backfill mode): same-season only (documented leaky-for-blend
    -- see marts/038_prediction_accuracy.sql's header).
    """
    for candidate in range(season, season - lookback - 1, -1):
        row = epa_by_team_season.get((team, candidate))
        if row is not None:
            return row
    return None


def build_predictions_for_game(
    game: dict,
    home_elo: float,
    away_elo: float,
    epa_by_team_season: dict[tuple[str, int], dict],
    epa_lookback: int,
    market: dict | None,
) -> list[dict]:
    """Build the two model_version rows (elo_v1, elo_epa_blend_v1) for one
    game. `game` needs: game_id, season, week, season_type, home_team,
    away_team, neutral_site. `market` is {"provider", "spread",
    "captured_at"} or None. Returns dicts with every predictions.game_predictions
    value column except computed_at/prediction_date (added by the caller)."""
    neutral = bool(game.get("neutral_site"))
    elo_m = elo_margin(home_elo, away_elo, neutral)
    win_prob = elo_win_prob(home_elo, away_elo, neutral)

    home_epa_row = lookup_epa_coefs(
        epa_by_team_season, game["home_team"], game["season"], epa_lookback
    )
    away_epa_row = lookup_epa_coefs(
        epa_by_team_season, game["away_team"], game["season"], epa_lookback
    )
    epa_m = None
    if home_epa_row is not None and away_epa_row is not None:
        epa_m = epa_margin(
            home_epa_row["off_coef"],
            home_epa_row["def_coef"],
            away_epa_row["off_coef"],
            away_epa_row["def_coef"],
            home_epa_row["hfa_coef"],
            neutral,
        )

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

    base = {
        "game_id": game["game_id"],
        "season": game["season"],
        "week": game["week"],
        "season_type": game["season_type"],
        "home_team": game["home_team"],
        "away_team": game["away_team"],
        "neutral_site": neutral,
        "home_elo_pregame": home_elo,
        "away_elo_pregame": away_elo,
        "elo_margin": elo_m,
        "home_win_prob": win_prob,
        "market_provider": market_provider,
        "market_home_margin": market_home_margin,
        "market_spread": market_spread,
        "market_captured_at": market_captured_at,
    }

    rows = []
    for model_version, expected, epa_col in (
        (MODEL_ELO, elo_m, None),
        (MODEL_BLEND, blend_margin(elo_m, epa_m), epa_m),
    ):
        edge, edge_pick = compute_edge(expected, market_spread)
        rows.append(
            {
                **base,
                "model_version": model_version,
                "epa_margin": epa_col,
                "expected_home_margin": expected,
                "edge": edge,
                "edge_pick": edge_pick,
            }
        )
    return rows


# =============================================================================
# --- I/O layer --- (thin: fetch games/ratings/market, drive the math, write)
# =============================================================================


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Copied from scripts/compute_house_elo.py's get_db_url pattern (each
    compute_*.py script keeps its own copy rather than importing across
    scripts for this one utility).
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


TARGET_GAMES_QUERY = """
    SELECT id AS game_id, season, week, season_type, start_date, neutral_site,
           home_team, away_team
    FROM core.games
    WHERE NOT COALESCE(completed, false)
      AND season >= (SELECT COALESCE(MAX(season), 0) FROM core.games WHERE completed)
    ORDER BY season, start_date NULLS LAST, id
"""

BACKFILL_GAMES_QUERY = """
    SELECT game_id, season, week, season_type, start_date, neutral_site,
           home_team, away_team, home_pregame_elo, away_pregame_elo
    FROM analytics.house_elo_game
    WHERE season = %s
    ORDER BY start_date NULLS LAST, game_id
"""

MARKET_SNAPSHOTS_QUERY = """
    SELECT DISTINCT ON (game_id) game_id, provider, spread, captured_at
    FROM betting.line_snapshots
    WHERE game_id = ANY(%s)
    ORDER BY game_id, CASE WHEN provider = 'consensus' THEN 0 ELSE 1 END, captured_at DESC
"""

MARKET_LINES_QUERY = """
    SELECT DISTINCT ON (game_id) game_id, provider, spread
    FROM betting.lines
    WHERE game_id = ANY(%s)
    ORDER BY game_id, CASE WHEN provider = 'consensus' THEN 0 ELSE 1 END, provider
"""

# Column order matches predictions.game_predictions exactly (see
# src/schemas/migrations/024_predictions_schema.sql), minus computed_at and
# prediction_date, which the two write_* functions handle separately.
_ROW_COLUMNS = [
    "model_version",
    "game_id",
    "season",
    "week",
    "season_type",
    "home_team",
    "away_team",
    "neutral_site",
    "home_elo_pregame",
    "away_elo_pregame",
    "elo_margin",
    "epa_margin",
    "expected_home_margin",
    "home_win_prob",
    "market_provider",
    "market_home_margin",
    "market_spread",
    "market_captured_at",
    "edge",
    "edge_pick",
]

_UPDATE_SET_COLUMNS = [
    "computed_at",
    "season",
    "week",
    "season_type",
    "home_team",
    "away_team",
    "neutral_site",
    "home_elo_pregame",
    "away_elo_pregame",
    "elo_margin",
    "epa_margin",
    "expected_home_margin",
    "home_win_prob",
    "market_provider",
    "market_home_margin",
    "market_spread",
    "market_captured_at",
    "edge",
    "edge_pick",
]

_UPSERT_SQL = """
    INSERT INTO predictions.game_predictions (
        computed_at, prediction_date, model_version, game_id, season, week,
        season_type, home_team, away_team, neutral_site,
        home_elo_pregame, away_elo_pregame, elo_margin, epa_margin,
        expected_home_margin, home_win_prob,
        market_provider, market_home_margin, market_spread, market_captured_at,
        edge, edge_pick
    ) VALUES %s
    ON CONFLICT (game_id, model_version, prediction_date) DO UPDATE SET
        {update_set}
""".format(update_set=",\n        ".join(f"{c} = EXCLUDED.{c}" for c in _UPDATE_SET_COLUMNS))

_ROW_PLACEHOLDERS = ", ".join(["%s"] * len(_ROW_COLUMNS))
# Upcoming mode: prediction_date is a fixed SQL-side expression (same value
# for every row in the batch -- one INSERT statement, one transaction-local
# `now()`), never a Python-computed date.
_UPCOMING_TEMPLATE = f"(now(), (now() AT TIME ZONE 'utc')::date, {_ROW_PLACEHOLDERS})"
# Backfill mode: prediction_date varies per game (each game's start_date),
# so it is a bound parameter, prepended ahead of the shared row values.
_BACKFILL_TEMPLATE = f"(now(), %s, {_ROW_PLACEHOLDERS})"


def _row_values(row: dict) -> tuple:
    return tuple(row[c] for c in _ROW_COLUMNS)


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
        return cur.fetchone()[0] is not None


def fetch_target_games(conn) -> list[dict]:
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(TARGET_GAMES_QUERY)
        return [dict(row) for row in cur.fetchall()]


def fetch_backfill_games(conn, season: int) -> list[dict]:
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(BACKFILL_GAMES_QUERY, (season,))
        return [dict(row) for row in cur.fetchall()]


def fetch_elo_current(conn) -> dict[str, tuple[float, int]]:
    result: dict[str, tuple[float, int]] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT team, rating, season FROM analytics.house_elo_current")
        for team, rating, season in cur.fetchall():
            if rating is None or season is None:
                continue
            result[team] = (float(rating), int(season))
    return result


def fetch_epa_coefs(conn, season: int | None = None) -> dict[tuple[str, int], dict]:
    query = "SELECT team, season, off_coef, def_coef, hfa_coef FROM analytics.adjusted_epa_build"
    params: tuple = ()
    if season is not None:
        query += " WHERE season = %s"
        params = (season,)

    result: dict[tuple[str, int], dict] = {}
    with conn.cursor() as cur:
        cur.execute(query, params)
        for team, row_season, off_coef, def_coef, hfa_coef in cur.fetchall():
            if off_coef is None or def_coef is None or hfa_coef is None:
                continue
            result[(team, row_season)] = {
                "off_coef": float(off_coef),
                "def_coef": float(def_coef),
                "hfa_coef": float(hfa_coef),
            }
    return result


def fetch_market_from_snapshots(conn, game_ids: list[int]) -> dict[int, dict]:
    if not game_ids:
        return {}
    result: dict[int, dict] = {}
    with conn.cursor() as cur:
        cur.execute(MARKET_SNAPSHOTS_QUERY, (game_ids,))
        for game_id, provider, spread, captured_at in cur.fetchall():
            result[game_id] = {
                "provider": provider,
                "spread": float(spread) if spread is not None else None,
                "captured_at": captured_at,
            }
    return result


def fetch_market_from_lines(conn, game_ids: list[int]) -> dict[int, dict]:
    if not game_ids:
        return {}
    result: dict[int, dict] = {}
    with conn.cursor() as cur:
        cur.execute(MARKET_LINES_QUERY, (game_ids,))
        for game_id, provider, spread in cur.fetchall():
            result[game_id] = {
                "provider": provider,
                "spread": float(spread) if spread is not None else None,
                "captured_at": None,
            }
    return result


def write_upcoming(conn, rows: list[dict]) -> None:
    from psycopg2.extras import execute_values

    if not rows:
        return
    values = [_row_values(r) for r in rows]
    with conn.cursor() as cur:
        execute_values(cur, _UPSERT_SQL, values, template=_UPCOMING_TEMPLATE)
    conn.commit()


def write_backfill_season(conn, rows: list[dict]) -> None:
    from psycopg2.extras import execute_values

    if not rows:
        return
    values = [(r["prediction_date"], *_row_values(r)) for r in rows]
    with conn.cursor() as cur:
        execute_values(cur, _UPSERT_SQL, values, template=_BACKFILL_TEMPLATE)
    conn.commit()


def run_upcoming(conn) -> None:
    games = fetch_target_games(conn)
    logger.info(f"Targeted {len(games)} upcoming/pending game(s)")
    if not games:
        logger.info("No target games; nothing to write")
        return

    elo_current = fetch_elo_current(conn)
    epa_by_team_season = fetch_epa_coefs(conn)

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
        home_elo = resolve_elo(game["home_team"], game["season"], elo_current)
        away_elo = resolve_elo(game["away_team"], game["season"], elo_current)
        market = market_by_game.get(game["game_id"])
        if market and market.get("spread") is not None:
            n_with_market += 1
        rows.extend(
            build_predictions_for_game(
                game, home_elo, away_elo, epa_by_team_season, epa_lookback=1, market=market
            )
        )

    write_upcoming(conn, rows)
    logger.info(
        f"Wrote {len(rows)} prediction row(s) for {len(games)} game(s) "
        f"({n_with_market} with a market line)"
    )


def run_backfill(conn, start: int, end: int) -> None:
    total_games = total_rows = total_with_market = 0
    for season in range(start, end + 1):
        games = fetch_backfill_games(conn, season)
        if not games:
            logger.info(
                f"season={season}: no completed games in analytics.house_elo_game, skipping"
            )
            continue

        epa_by_team_season = fetch_epa_coefs(conn, season=season)
        game_ids = [g["game_id"] for g in games]
        market_by_game = fetch_market_from_lines(conn, game_ids)

        rows: list[dict] = []
        n_with_market = 0
        for game in games:
            home_elo = (
                float(game["home_pregame_elo"])
                if game["home_pregame_elo"] is not None
                else EloEngine.SEED
            )
            away_elo = (
                float(game["away_pregame_elo"])
                if game["away_pregame_elo"] is not None
                else EloEngine.SEED
            )
            market = market_by_game.get(game["game_id"])
            if market and market.get("spread") is not None:
                n_with_market += 1

            start_date = game["start_date"]
            prediction_date = start_date.date() if start_date is not None else date(season, 1, 1)

            game_rows = build_predictions_for_game(
                game, home_elo, away_elo, epa_by_team_season, epa_lookback=0, market=market
            )
            for row in game_rows:
                row["prediction_date"] = prediction_date
            rows.extend(game_rows)

        write_backfill_season(conn, rows)
        logger.info(
            f"season={season}: {len(games)} game(s), wrote {len(rows)} row(s) "
            f"({n_with_market} with a market line)"
        )
        total_games += len(games)
        total_rows += len(rows)
        total_with_market += n_with_market

    logger.info(
        f"Backfill {start}-{end}: {total_games} game(s) total, {total_rows} row(s) written, "
        f"{total_with_market} with a market line"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute house predictions (Elo + ridge-EPA blend) into "
        "predictions.game_predictions"
    )
    parser.add_argument(
        "--backfill",
        nargs=2,
        type=int,
        metavar=("START", "END"),
        help="Backfill completed games for seasons START..END (inclusive) using "
        "walk-forward pregame Elo + same-season EPA; prediction_date = each "
        "game's start_date (fallback: Jan 1 of its season). Default (no flag): "
        "score upcoming/pending games using current ratings.",
    )
    args = parser.parse_args()

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        if args.backfill:
            start, end = args.backfill
            if start > end:
                logger.error(f"--backfill start {start} is after end {end}")
                sys.exit(1)
            run_backfill(conn, start, end)
        else:
            run_upcoming(conn)
    except Exception:
        conn.rollback()
        logger.exception("Predictions compute failed")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
