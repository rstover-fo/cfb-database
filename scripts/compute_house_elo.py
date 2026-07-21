#!/usr/bin/env python3
"""Compute house Elo ratings from core.games (docs/plans/2026-07-21-tier2-analytics-plan.md).

Architecture: the Elo math (`expected_score`, `mov_multiplier`, `EloEngine`) is
pure -- it operates on plain dicts/floats and touches no I/O, so it is fully
unit-testable without a database (see tests/test_house_elo.py). Everything
below `# --- I/O layer ---` is a thin wrapper: fetch rows from core.games,
feed them through the engine, write the results to
analytics.house_elo_game / analytics.house_elo_current.

Parameters (SEED, K, HFA, DIVISOR, CARRYOVER, POOL_THRESHOLD) are FINAL per
the design doc -- do not retune here; see the plan's tunable ledger.

Usage:
    python scripts/compute_house_elo.py --full
        Recompute all of history: seasons 1869..max(core.games.season),
        rewriting analytics.house_elo_game season by season and the
        analytics.house_elo_current snapshot at the end. Idempotent.

    python scripts/compute_house_elo.py --season 2024
        Recompute a single season. Engine state is rebuilt from each team's
        latest analytics.house_elo_game row with season < 2024 (no replay of
        prior history needed), then that season's games are (re)computed and
        the season + snapshot are rewritten.

    python scripts/compute_house_elo.py --incremental
        Shorthand for `--season <max season in core.games with completed
        games>`. What the daily workflow runs.

Both modes print a validation line after writing:
    ELO_VALIDATION n=<rows> pearson_r=<r to 4 decimals>
comparing our home_pregame_elo to CFBD's own home_pregame_elo for
season >= 2015 rows where CFBD's value is present (expect r >~ 0.9).
"""

import argparse
import logging
import math
import sys
from collections import Counter, defaultdict

import dlt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_START_SEASON = 1869
VALIDATION_MIN_SEASON = 2015


# =============================================================================
# Pure Elo engine -- no I/O, no DB, unit-tested directly.
# =============================================================================


def expected_score(elo_diff: float) -> float:
    """Canonical 400-scale logistic win probability for `elo_diff` = own - opponent
    (already including home-field advantage, if any)."""
    return 1.0 / (1.0 + 10 ** (-elo_diff / 400.0))


def mov_multiplier(margin: int, elo_diff_winner: float) -> float:
    """FiveThirtyEight-style margin-of-victory multiplier.

    `margin` may be signed or unsigned -- only its magnitude is used.
    `elo_diff_winner` is the pregame Elo differential (including HFA) from the
    winning side's perspective; callers pass 0 for a tie.

    A tie (margin == 0) gives ln(1) == 0, so the multiplier -- and therefore
    process_game's rating delta -- is exactly 0 for ties. That is intentional
    per the design: the actual-minus-expected term would otherwise still
    carry a direction signal, but zeroing it out via the multiplier is the
    accepted simplification (ties are rare in the modern game, where
    sudden-death overtime has applied since 1996).
    """
    return math.log(abs(margin) + 1) * (2.2 / (0.001 * elo_diff_winner + 2.2))


class EloEngine:
    """Stateful, pure (no I/O) house-Elo rating engine.

    Call sequence per season: `start_season(season, team_game_counts)` once,
    then `process_game(game)` once per completed game in that season in
    chronological order, then (optionally, once per snapshot needed)
    `current_snapshot(season)`.
    """

    SEED = 1500.0
    K = 20.0
    HFA = 65.0
    DIVISOR = 25.0
    CARRYOVER = 2.0 / 3.0
    POOL_THRESHOLD = 4
    POOLED = "__FCS__"

    def __init__(self) -> None:
        # Rating store, keyed by "alias identity": a real team name for teams
        # tracked individually, or POOLED for the shared low-sample bucket.
        self.ratings: dict[str, float] = {}
        # Real team name -> season of that team's most recent processed game.
        # This is both the carryover clock (elapsed = target - last_season)
        # and the "last played" field surfaced in current_snapshot().
        self.last_season: dict[str, int] = {}
        # Real team name -> this season's alias (POOLED or itself), rebuilt
        # by every start_season() call.
        self.alias: dict[str, str] = {}
        # Real team name -> games played in the *current* season only, reset
        # by every start_season() call.
        self.games_played: dict[str, int] = {}
        self.last_game_id: dict[str, int] = {}
        self.last_game_date: dict[str, object] = {}

    def resolve(self, team: str) -> str:
        """Return the alias identity a real team's games are recorded under
        for the season most recently started (itself, or POOLED)."""
        return self.alias.get(team, team)

    def start_season(self, season: int, team_game_counts: dict[str, int]) -> None:
        """Begin a new season: rebuild the pooling alias map and apply
        season-carryover regression to teams about to play.

        team_game_counts: {team: games this team plays in `season`}. Teams
        with fewer than POOL_THRESHOLD games alias to the shared POOLED
        bucket for the season (their individual rating is left untouched,
        frozen, until they return to individual tracking).

        Carryover only applies to teams already known (i.e. with a prior
        last_season entry) that are about to play this season -- a brand
        new team has nothing to carry over from and simply starts at SEED
        on its first process_game() call. Because last_season is only ever
        updated by process_game (never by start_season itself), a team that
        sits out N seasons and then returns has elapsed = N computed in one
        shot from its true last-played rating -- no compounding across
        empty seasons.
        """
        self.alias = {}
        self.games_played = {}
        for team, count in team_game_counts.items():
            self.alias[team] = self.POOLED if count < self.POOL_THRESHOLD else team

        # The pooled bucket has no persistent identity of its own -- it
        # resets to the seed rating at the start of every season.
        self.ratings[self.POOLED] = self.SEED

        for team in team_game_counts:
            if team not in self.last_season:
                continue
            elapsed = season - self.last_season[team]
            if elapsed <= 0:
                continue
            current = self.ratings.get(team, self.SEED)
            offset = current - self.SEED
            self.ratings[team] = self.SEED + offset * (self.CARRYOVER**elapsed)

    def process_game(self, game: dict) -> dict:
        """Process one completed game, updating engine state in place and
        returning the full analytics.house_elo_game row (dict) for it.

        `game` keys: game_id, season, week, season_type, start_date,
        neutral_site, home_team, away_team, home_points, away_points,
        cfbd_home_pregame_elo, cfbd_away_pregame_elo.
        """
        season = game["season"]
        home_team = game["home_team"]
        away_team = game["away_team"]
        neutral_site = bool(game.get("neutral_site"))

        home_alias = self.resolve(home_team)
        away_alias = self.resolve(away_team)
        same_bucket = home_alias == away_alias

        home_pregame = self.ratings.get(home_alias, self.SEED)
        away_pregame = home_pregame if same_bucket else self.ratings.get(away_alias, self.SEED)

        hfa_elo = 0.0 if neutral_site else self.HFA
        elo_diff_home = home_pregame - away_pregame + hfa_elo
        exp_home = expected_score(elo_diff_home)
        expected_home_margin = elo_diff_home / self.DIVISOR

        home_points = game["home_points"]
        away_points = game["away_points"]
        actual_margin = home_points - away_points

        if actual_margin > 0:
            actual_home = 1.0
            elo_diff_w = elo_diff_home
        elif actual_margin < 0:
            actual_home = 0.0
            elo_diff_w = -elo_diff_home
        else:
            actual_home = 0.5
            elo_diff_w = 0.0

        mult = mov_multiplier(actual_margin, elo_diff_w)
        delta = self.K * mult * (actual_home - exp_home)

        if same_bucket:
            # Both sides are low-sample teams sharing the pooled bucket (an
            # FCS-vs-FCS-caliber matchup). The two deltas would otherwise
            # collide into a single dict slot; net movement of a bucket
            # playing itself isn't well-defined, so we leave it unchanged.
            # Documented limitation -- not expected to occur in FBS-centric
            # core.games data.
            home_postgame = home_pregame
            away_postgame = away_pregame
            self.ratings[home_alias] = home_pregame
        else:
            home_postgame = home_pregame + delta
            away_postgame = away_pregame - delta
            self.ratings[home_alias] = home_postgame
            self.ratings[away_alias] = away_postgame

        # Bookkeeping keyed by REAL team names (never aliases) for the
        # current-season snapshot.
        game_id = game.get("game_id")
        start_date = game.get("start_date")
        for team in (home_team, away_team):
            self.games_played[team] = self.games_played.get(team, 0) + 1
            self.last_game_id[team] = game_id
            self.last_game_date[team] = start_date
            self.last_season[team] = season

        return {
            "game_id": game_id,
            "season": season,
            "week": game.get("week"),
            "season_type": game.get("season_type"),
            "start_date": start_date,
            "neutral_site": neutral_site,
            "home_team": home_team,
            "away_team": away_team,
            "home_pregame_elo": home_pregame,
            "away_pregame_elo": away_pregame,
            "home_postgame_elo": home_postgame,
            "away_postgame_elo": away_postgame,
            "home_win_prob": exp_home,
            "expected_home_margin": expected_home_margin,
            "actual_home_margin": actual_margin,
            "mov_multiplier": mult,
            "cfbd_home_pregame_elo": game.get("cfbd_home_pregame_elo"),
            "cfbd_away_pregame_elo": game.get("cfbd_away_pregame_elo"),
        }

    def current_snapshot(self, season: int) -> list[dict]:
        """Return one row per real team ever seen, for
        analytics.house_elo_current (updated_at is left to the caller/SQL
        `now()`).

        `rating` is read through the team's alias for the most recently
        started season, so a currently-pooled team reports the shared
        pooled rating (matching what its games actually used).
        """
        rows = []
        for team, last_season in self.last_season.items():
            if last_season > season:
                continue  # defensive: shouldn't happen, we never look ahead
            alias = self.resolve(team)
            rating = self.ratings.get(alias, self.SEED)
            games_played = self.games_played.get(team, 0)
            low_confidence = games_played < self.POOL_THRESHOLD or last_season < 1900
            rows.append(
                {
                    "team": team,
                    "season": last_season,
                    "rating": rating,
                    "games_played": games_played,
                    "last_game_id": self.last_game_id.get(team),
                    "last_game_date": self.last_game_date.get(team),
                    "low_confidence": low_confidence,
                }
            )
        return rows


def pearson_r(xs: list[float], ys: list[float]) -> float:
    """Pure-Python Pearson correlation coefficient (stdlib only, no numpy)."""
    n = len(xs)
    if n == 0 or n != len(ys):
        return float("nan")
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    denom = math.sqrt(var_x * var_y)
    if denom == 0:
        return float("nan")
    return cov / denom


# =============================================================================
# --- I/O layer --- (thin: fetch core.games, drive the engine, write results)
# =============================================================================


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Copied from scripts/refresh_marts.py's get_db_url pattern.
    """
    import os

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


GAMES_QUERY = """
    SELECT id, season, week, season_type, start_date, neutral_site,
           home_team, away_team, home_points, away_points,
           home_pregame_elo, away_pregame_elo
    FROM core.games
    WHERE completed = true
      AND home_points IS NOT NULL
      AND away_points IS NOT NULL
      AND season BETWEEN %s AND %s
    ORDER BY start_date NULLS LAST, id
"""

# Seeds engine state for --season/--incremental: each team's most recent
# postgame rating and season, read back from what we've already written.
# Ordered by season DESC first (not start_date) so a team's true last-played
# *season* always wins the tiebreak, even for old rows with NULL start_date.
SEED_STATE_QUERY = """
    SELECT DISTINCT ON (team) team, season, postgame
    FROM (
        SELECT home_team AS team, season, start_date, game_id,
               home_postgame_elo AS postgame
        FROM analytics.house_elo_game
        WHERE season < %s
        UNION ALL
        SELECT away_team AS team, season, start_date, game_id,
               away_postgame_elo AS postgame
        FROM analytics.house_elo_game
        WHERE season < %s
    ) t
    ORDER BY team, season DESC, start_date DESC NULLS LAST, game_id DESC
"""


def to_engine_game(row: dict) -> dict:
    """Map a core.games DB row to the dict shape EloEngine.process_game
    expects, renaming CFBD's own pregame Elo columns so they never collide
    with the keys the engine computes itself."""
    cfbd_home = row["home_pregame_elo"]
    cfbd_away = row["away_pregame_elo"]
    return {
        "game_id": row["id"],
        "season": row["season"],
        "week": row["week"],
        "season_type": row["season_type"],
        "start_date": row["start_date"],
        "neutral_site": row["neutral_site"],
        "home_team": row["home_team"],
        "away_team": row["away_team"],
        "home_points": row["home_points"],
        "away_points": row["away_points"],
        "cfbd_home_pregame_elo": float(cfbd_home) if cfbd_home is not None else None,
        "cfbd_away_pregame_elo": float(cfbd_away) if cfbd_away is not None else None,
    }


def compute_team_game_counts(season_games: list[dict]) -> dict[str, int]:
    """Count games per team within a single season's already-buffered rows."""
    counts: Counter[str] = Counter()
    for g in season_games:
        counts[g["home_team"]] += 1
        counts[g["away_team"]] += 1
    return dict(counts)


def load_games_by_season(conn, start_season: int, end_season: int) -> dict[int, list[dict]]:
    """Stream core.games via a named server-side cursor, bucketed by season.

    We fetch everything with one query (ORDER BY start_date NULLS LAST, id,
    per the plan) and bucket rows into a dict keyed by season in Python,
    rather than relying on the SQL ordering to keep seasons contiguous --
    games with a NULL start_date (common pre-modern-era) sort to the very
    end of the *entire* result set under NULLS LAST, which would otherwise
    interleave seasons. Bucketing by season is correct regardless of stream
    order; within a season's bucket, rows keep the relative order they
    arrived in (dated games first in chronological order, undated ones
    trailing in id order) -- a reasonable placement when true chronological
    order isn't known.

    A season is at most a few thousand rows, and full history is well under
    a million, so buffering everything in memory is simple and cheap
    (one query total, per the plan).
    """
    import psycopg2.extras

    buckets: dict[int, list[dict]] = defaultdict(list)
    with conn.cursor(
        name="house_elo_games_cursor", cursor_factory=psycopg2.extras.RealDictCursor
    ) as cur:
        cur.itersize = 5000
        cur.execute(GAMES_QUERY, (start_season, end_season))
        for row in cur:
            buckets[row["season"]].append(dict(row))
    return buckets


def seed_state(engine: EloEngine, conn, target_season: int) -> None:
    """Rebuild engine.ratings/last_season from the latest persisted
    analytics.house_elo_game rows before `target_season`, so --season and
    --incremental never need to replay full history."""
    with conn.cursor() as cur:
        cur.execute(SEED_STATE_QUERY, (target_season, target_season))
        for team, season, postgame in cur.fetchall():
            if postgame is not None:
                engine.ratings[team] = float(postgame)
            engine.last_season[team] = season


def fetch_max_season(conn) -> int | None:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(season) FROM core.games")
        row = cur.fetchone()
    return row[0] if row else None


def fetch_max_completed_season(conn) -> int | None:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(season) FROM core.games WHERE completed = true")
        row = cur.fetchone()
    return row[0] if row else None


def write_season(conn, season: int, rows: list[dict]) -> None:
    """Idempotent per-season write: DELETE then bulk INSERT, one commit."""
    from psycopg2.extras import execute_values

    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM analytics.house_elo_game WHERE season = %s", (season,))
        if rows:
            values = [
                (
                    r["game_id"],
                    r["season"],
                    r["week"],
                    r["season_type"],
                    r["start_date"],
                    r["neutral_site"],
                    r["home_team"],
                    r["away_team"],
                    r["home_pregame_elo"],
                    r["away_pregame_elo"],
                    r["home_postgame_elo"],
                    r["away_postgame_elo"],
                    r["home_win_prob"],
                    r["expected_home_margin"],
                    r["actual_home_margin"],
                    r["mov_multiplier"],
                    r["cfbd_home_pregame_elo"],
                    r["cfbd_away_pregame_elo"],
                )
                for r in rows
            ]
            execute_values(
                cur,
                """
                INSERT INTO analytics.house_elo_game (
                    game_id, season, week, season_type, start_date, neutral_site,
                    home_team, away_team, home_pregame_elo, away_pregame_elo,
                    home_postgame_elo, away_postgame_elo, home_win_prob,
                    expected_home_margin, actual_home_margin, mov_multiplier,
                    cfbd_home_pregame_elo, cfbd_away_pregame_elo
                ) VALUES %s
                """,
                values,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def write_snapshot(conn, rows: list[dict]) -> None:
    """Full replace of analytics.house_elo_current (updated_at = now() in SQL)."""
    from psycopg2.extras import execute_values

    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM analytics.house_elo_current")
        if rows:
            values = [
                (
                    r["team"],
                    r["season"],
                    r["rating"],
                    r["games_played"],
                    r["last_game_id"],
                    r["last_game_date"],
                    r["low_confidence"],
                )
                for r in rows
            ]
            execute_values(
                cur,
                """
                INSERT INTO analytics.house_elo_current (
                    team, season, rating, games_played, last_game_id,
                    last_game_date, low_confidence, updated_at
                ) VALUES %s
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, now())",
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def run_full(conn, start_season: int, end_season: int) -> list[dict]:
    logger.info(f"Loading core.games {start_season}-{end_season}")
    buckets = load_games_by_season(conn, start_season, end_season)
    engine = EloEngine()
    all_rows: list[dict] = []
    last_season_processed = None
    for season in sorted(buckets):
        season_games = buckets[season]
        team_game_counts = compute_team_game_counts(season_games)
        engine.start_season(season, team_game_counts)
        rows = [engine.process_game(to_engine_game(g)) for g in season_games]
        write_season(conn, season, rows)
        all_rows.extend(rows)
        last_season_processed = season
        logger.info(f"  season {season}: {len(rows)} games written")

    if last_season_processed is not None:
        snapshot = engine.current_snapshot(last_season_processed)
        write_snapshot(conn, snapshot)
        logger.info(
            f"Snapshot written: {len(snapshot)} teams (as of season {last_season_processed})"
        )
    else:
        logger.warning("No completed games found in the requested season range")

    return all_rows


def run_season(conn, season: int) -> list[dict]:
    engine = EloEngine()
    logger.info(f"Seeding engine state from analytics.house_elo_game (season < {season})")
    seed_state(engine, conn, season)
    logger.info(f"Loading core.games for season {season}")
    buckets = load_games_by_season(conn, season, season)
    season_games = buckets.get(season, [])
    team_game_counts = compute_team_game_counts(season_games)
    engine.start_season(season, team_game_counts)
    rows = [engine.process_game(to_engine_game(g)) for g in season_games]
    logger.info(f"Season {season}: {len(rows)} games computed")
    write_season(conn, season, rows)

    snapshot = engine.current_snapshot(season)
    write_snapshot(conn, snapshot)
    logger.info(f"Snapshot written: {len(snapshot)} teams (as of season {season})")

    return rows


def print_validation(rows: list[dict]) -> None:
    pairs = [
        (r["home_pregame_elo"], r["cfbd_home_pregame_elo"])
        for r in rows
        if r["season"] >= VALIDATION_MIN_SEASON and r["cfbd_home_pregame_elo"] is not None
    ]
    n = len(pairs)
    r = pearson_r([p[0] for p in pairs], [p[1] for p in pairs]) if n else float("nan")
    print(f"ELO_VALIDATION n={n} pearson_r={r:.4f}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute house Elo ratings from core.games into analytics.house_elo_*"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--full",
        action="store_true",
        help=f"Recompute all history: seasons {DEFAULT_START_SEASON}..max(core.games.season)",
    )
    mode.add_argument(
        "--season",
        type=int,
        help="Recompute a single season (engine state rebuilt from prior house_elo_game rows)",
    )
    mode.add_argument(
        "--incremental",
        action="store_true",
        help="Recompute the current season (max season in core.games with completed games)",
    )
    args = parser.parse_args()

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        if args.full:
            max_season = fetch_max_season(conn)
            if max_season is None:
                logger.error("core.games is empty -- nothing to compute")
                sys.exit(1)
            rows = run_full(conn, DEFAULT_START_SEASON, max_season)
        elif args.season is not None:
            rows = run_season(conn, args.season)
        else:
            max_completed = fetch_max_completed_season(conn)
            if max_completed is None:
                logger.error("No completed games found in core.games")
                sys.exit(1)
            rows = run_season(conn, max_completed)

        print_validation(rows)
    except Exception:
        conn.rollback()
        logger.exception("House Elo compute failed")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
