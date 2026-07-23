#!/usr/bin/env python3
"""Verify a season load actually landed: partition, counts, coverage, freshness.

Intended to run right after scripts/load_season.py (the daily workflow does),
failing loudly so a silent bad load surfaces as a red job instead of a stale
dashboard weeks later.

Usage:
    python scripts/verify_load.py                   # Verify current season
    python scripts/verify_load.py --season 2026     # Verify a specific season
    python scripts/verify_load.py --strict          # Treat staleness as failure year-round

Checks:
    1. plays partition for the season exists (core.plays_yNNNN)
    2. core.games row count for the season matches the CFBD /games count (1 API call)
    3. completed FBS-involved games have game_team_stats rows (lower-division
       games are excluded -- CFBD only reliably publishes box scores for games
       with an FBS side; small tolerance for stragglers)
    4. completed FBS-involved games have plays rows (same scope + tolerance)
    5. marts.data_freshness is_stale flags -- heuristic only: the matview infers
       freshness from pg_stat vacuum/analyze timestamps, so staleness WARNs
       off-season and FAILs in-season (or with --strict)
    6. ratings.massey_composite has a recent, full-coverage snapshot for the
       season (in-season only; WARNs if migration 041 isn't applied yet)
    7. meta.flat_file_loads has a recent successful 'availability' load
       (in-season only; never FAILs -- external conference sites are flaky)

Pre-season semantics: with no completed games, checks 3-4 pass vacuously and
check 2 is the meaningful one (schedules publish in July, so core.games must
already have rows for the upcoming season).
"""

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Completed games allowed to lack box scores / plays before failing.
# FCS opponents and canceled-but-marked-completed games routinely have none.
MISSING_STATS_TOLERANCE = 10

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"


def is_in_season(month: int) -> bool:
    """September through January: weekly tables must be actively refreshing."""
    return month in {9, 10, 11, 12, 1}


def evaluate_missing(missing: int, tolerance: int = MISSING_STATS_TOLERANCE) -> str:
    """Grade a count of completed games missing downstream rows."""
    if missing == 0:
        return PASS
    if missing <= tolerance:
        return WARN
    return FAIL


def evaluate_game_counts(api_count: int, db_count: int) -> str:
    """Grade API-vs-DB game count reconciliation for a season."""
    if api_count > 0 and db_count == 0:
        return FAIL
    if db_count < api_count:
        return FAIL
    return PASS


def evaluate_staleness(frequency: str, in_season: bool, strict: bool) -> str:
    """Grade an is_stale row from marts.data_freshness."""
    if frequency == "weekly" and (in_season or strict):
        return FAIL
    return WARN


class Report:
    def __init__(self) -> None:
        self.failures = 0

    def record(self, status: str, name: str, detail: str) -> None:
        print(f"[{status}] {name}: {detail}")
        if status == FAIL:
            self.failures += 1


def check_partition(cur, season: int, report: Report) -> None:
    cur.execute("SELECT to_regclass(%s)", (f"core.plays_y{season}",))
    exists = cur.fetchone()[0] is not None
    report.record(
        PASS if exists else FAIL,
        "plays_partition",
        f"core.plays_y{season} {'exists' if exists else 'MISSING'}",
    )


def check_game_counts(cur, season: int, report: Report) -> None:
    from src.pipelines.sources.base import make_request
    from src.pipelines.utils.api_client import get_client

    client = get_client()
    try:
        api_count = len(make_request(client, "/games", params={"year": season}))
    finally:
        client.close()

    cur.execute("SELECT COUNT(*) FROM core.games WHERE season = %s", (season,))
    db_count = cur.fetchone()[0]
    report.record(
        evaluate_game_counts(api_count, db_count),
        "game_counts",
        f"api={api_count} db={db_count}",
    )


# core.games mirrors CFBD /games, which spans every classification (FBS, FCS,
# II, III -- 3800+ rows/season), but box scores and plays are only reliably
# published for games involving an FBS team. Coverage checks therefore scope
# to FBS-involved games; a season-wide count would "miss" ~2K lower-division
# games by construction and could never clear MISSING_STATS_TOLERANCE.
FBS_INVOLVED = "(g.home_classification = 'fbs' OR g.away_classification = 'fbs')"


def check_completed_have_team_stats(cur, season: int, report: Report) -> None:
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM core.games g
        WHERE g.season = %s AND g.completed AND {FBS_INVOLVED}
          AND NOT EXISTS (SELECT 1 FROM core.game_team_stats s WHERE s.id = g.id)
        """,
        (season,),
    )
    missing = cur.fetchone()[0]
    report.record(
        evaluate_missing(missing),
        "completed_have_team_stats",
        f"{missing} completed FBS game(s) missing box scores (tolerance {MISSING_STATS_TOLERANCE})",
    )


def check_completed_have_plays(cur, season: int, report: Report) -> None:
    # p.season predicate enables partition pruning; plays has no week column
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM core.games g
        WHERE g.season = %s AND g.completed AND {FBS_INVOLVED}
          AND NOT EXISTS (
              SELECT 1 FROM core.plays p WHERE p.season = %s AND p.game_id = g.id
          )
        """,
        (season, season),
    )
    missing = cur.fetchone()[0]
    report.record(
        evaluate_missing(missing),
        "completed_have_plays",
        f"{missing} completed FBS game(s) missing plays (tolerance {MISSING_STATS_TOLERANCE})",
    )


def _current_in_season() -> bool:
    """Wrapper around is_in_season(now); a thin seam tests can monkeypatch."""
    from datetime import datetime

    return is_in_season(datetime.now().month)


def evaluate_snapshot_freshness(
    days_old: int | None, in_season: bool, warn_days: int, fail_days: int
) -> str:
    """Grade the age of the latest snapshot in a weekly-refreshed table.

    Off-season, staleness is expected and never graded. In-season, no
    snapshots at all (days_old is None) only WARNs -- the subsystem may be
    newly deployed and shouldn't fail an otherwise-healthy load.
    """
    if not in_season:
        return PASS
    if days_old is None:
        return WARN
    if days_old <= warn_days:
        return PASS
    if days_old <= fail_days:
        return WARN
    return FAIL


def evaluate_snapshot_team_count(count: int | None, in_season: bool) -> str:
    """Grade team coverage of the latest snapshot.

    A partial snapshot is worse than none -- it signals a parser/crosswalk
    problem rather than a simply-missing load -- so this is stricter than
    evaluate_snapshot_freshness about low (but present) counts.
    """
    if not in_season or count is None:
        return PASS
    if count >= 120:
        return PASS
    if count >= 100:
        return WARN
    return FAIL


def check_massey_composite(cur, season: int, report: Report) -> None:
    # Migration 041 may not be applied yet; to_regclass returns NULL for a
    # missing relation instead of raising, so no transaction-poisoning risk.
    cur.execute("SELECT to_regclass('ratings.massey_composite')")
    if cur.fetchone()[0] is None:
        report.record(WARN, "massey_composite", "table absent (migration 041 not applied)")
        return

    in_season = _current_in_season()

    cur.execute(
        "SELECT MAX(snapshot_date) FROM ratings.massey_composite WHERE season = %s", (season,)
    )
    latest = cur.fetchone()[0]

    days_old = None
    team_count = None
    if latest is not None:
        from datetime import date

        days_old = (date.today() - latest).days
        cur.execute(
            """
            SELECT COUNT(*) FROM ratings.massey_composite
            WHERE season = %s AND snapshot_date = %s
            """,
            (season, latest),
        )
        team_count = cur.fetchone()[0]

    report.record(
        evaluate_snapshot_freshness(days_old, in_season, warn_days=8, fail_days=14),
        "massey_freshness",
        f"latest snapshot {latest} ({days_old if days_old is not None else 'n/a'} days old)",
    )
    report.record(
        evaluate_snapshot_team_count(team_count, in_season),
        "massey_team_count",
        f"{team_count if team_count is not None else 'n/a'} team(s) in latest snapshot",
    )


def check_availability_archive(cur, season: int, report: Report) -> None:
    # Same missing-migration guard as check_massey_composite.
    cur.execute("SELECT to_regclass('meta.flat_file_loads')")
    if cur.fetchone()[0] is None:
        report.record(WARN, "availability_archive", "table absent (migration 041 not applied)")
        return

    in_season = _current_in_season()

    cur.execute(
        """
        SELECT MAX(loaded_at) FROM meta.flat_file_loads
        WHERE source = 'availability' AND status = 'loaded'
        """
    )
    latest = cur.fetchone()[0]

    if not in_season:
        report.record(PASS, "availability_archive", f"off-season, latest load {latest}")
        return

    if latest is None:
        report.record(WARN, "availability_archive", "no successful availability load recorded")
        return

    from datetime import UTC, datetime

    days_old = (datetime.now(UTC) - latest).days
    # Never FAIL: external conference/archive sites are flaky and staleness
    # here must not fail the whole daily load.
    status = PASS if days_old <= 8 else WARN
    report.record(
        status,
        "availability_archive",
        f"latest load {days_old} day(s) old (source=availability)",
    )


def check_freshness(cur, in_season: bool, strict: bool, report: Report) -> None:
    cur.execute(
        """
        SELECT schema_name, table_name, expected_refresh_frequency, days_since_activity
        FROM marts.data_freshness
        WHERE is_stale
        ORDER BY schema_name, table_name
        """
    )
    rows = cur.fetchall()
    if not rows:
        report.record(PASS, "data_freshness", "no stale tables")
        return

    for schema_name, table_name, frequency, days in rows:
        report.record(
            evaluate_staleness(frequency, in_season, strict),
            "data_freshness",
            f"{schema_name}.{table_name} stale ({frequency}, {days} days since activity)",
        )


def verify(season: int, strict: bool) -> int:
    """Run all checks. Returns the number of failures."""
    from datetime import datetime

    import psycopg2

    from scripts.refresh_marts import get_db_url

    in_season = is_in_season(datetime.now().month)
    report = Report()

    conn = psycopg2.connect(get_db_url())
    try:
        with conn.cursor() as cur:
            check_partition(cur, season, report)
            check_game_counts(cur, season, report)
            check_completed_have_team_stats(cur, season, report)
            check_completed_have_plays(cur, season, report)
            check_freshness(cur, in_season, strict, report)
            check_massey_composite(cur, season, report)
            check_availability_archive(cur, season, report)
    finally:
        conn.close()

    if report.failures:
        logger.error(f"{report.failures} check(s) FAILED for season {season}")
    else:
        logger.info(f"All checks passed for season {season}")
    return report.failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify a season load")
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Season to verify (default: current season)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat weekly-table staleness as failure even off-season",
    )
    args = parser.parse_args()

    season = args.season
    if season is None:
        from src.pipelines.config.years import get_current_season

        season = get_current_season()
        logger.info(f"No --season given; verifying current season {season}")

    sys.exit(1 if verify(season, strict=args.strict) else 0)


if __name__ == "__main__":
    main()
