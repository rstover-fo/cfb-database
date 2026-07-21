#!/usr/bin/env python3
"""Presence/recon check for Tier 1 and Tier 2 gate tables (Phase 0 of the analytics-unlock sprint).

Prints live row counts, column dumps, ref.play_stat_types contents, and an
athlete_id/roster overlap sample for the tables that gate the WEPA, player-EPA,
havoc, returning-production, player-usage, and ATS work
(docs/plans/2026-07-19-tier1-analytics-unlock-plan.md, Phase 0). Also reports
Tier 2 tables (house-Elo, EPA, predictions, line snapshots) and depth metrics
for backtest benchmarks. Output is grouped into clearly delimited sections so a
GitHub Actions job log can be read directly without a live DB connection.

Usage:
    python scripts/check_presence.py             # recon mode: always exit 0
    python scripts/check_presence.py --strict     # exit 1 if any of the five
                                                    # gate tables is missing/empty
"""

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# (schema, table) pairs whose row counts/columns are reported, in the order
# listed in the sprint plan's Phase 0 section.
GATE_TABLES: list[tuple[str, str]] = [
    ("stats", "play_stats"),
    ("stats", "game_havoc"),
    ("stats", "player_usage"),
    ("stats", "player_returning"),
    ("betting", "team_ats"),
    ("ref", "play_stat_types"),
    ("betting", "lines"),
    ("metrics", "wepa_team_season"),
    ("core", "game_player_stats"),
]

# The subset --strict enforces must be present and non-empty (the Phase 0
# decision gate: proceed vs. push a deploy/tier1-backfill run).
STRICT_GATE_TABLES: list[tuple[str, str]] = [
    ("stats", "play_stats"),
    ("stats", "game_havoc"),
    ("stats", "player_usage"),
    ("stats", "player_returning"),
    ("betting", "team_ats"),
]

# Tier 2 tables (advanced analytics, predictions, lines) reported for context
# but NOT enforced in --strict mode (legitimately absent before Phase 1/2).
TIER_2_GATE_TABLES: list[tuple[str, str]] = [
    ("analytics", "house_elo_current"),
    ("analytics", "house_elo_game"),
    ("analytics", "adjusted_epa_build"),
    ("predictions", "game_predictions"),
    ("betting", "line_snapshots"),
]

# Play-by-play athlete_id linkage only goes back to ~2014; a single recent
# season keeps the overlap sample cheap even once play_stats is fully backfilled.
OVERLAP_SAMPLE_SEASON = 2024


def section(title: str) -> None:
    print(f"\n===== {title} =====")


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
    return cur.fetchone()[0] is not None


def row_count(cur, schema: str, table: str) -> int:
    from psycopg2 import sql

    query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
        sql.Identifier(schema), sql.Identifier(table)
    )
    cur.execute(query)
    return cur.fetchone()[0]


def print_row_counts(cur) -> dict[tuple[str, str], int | None]:
    """Print row counts for GATE_TABLES; return {(schema, table): count or None}."""
    section("ROW COUNTS")
    counts: dict[tuple[str, str], int | None] = {}
    for schema, table in GATE_TABLES:
        if not table_exists(cur, schema, table):
            print(f"{schema}.{table}: MISSING")
            counts[(schema, table)] = None
            continue
        count = row_count(cur, schema, table)
        counts[(schema, table)] = count
        print(f"{schema}.{table}: {count:,}")
    return counts


def print_columns(cur, counts: dict[tuple[str, str], int | None]) -> None:
    """Dump information_schema.columns (name:type) for every table that exists."""
    section("COLUMNS")
    for schema, table in GATE_TABLES:
        if counts.get((schema, table)) is None:
            continue
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        cols = cur.fetchall()
        col_str = ", ".join(f"{name}:{dtype}" for name, dtype in cols)
        print(f"{schema}.{table} ({len(cols)} cols): {col_str}")


def print_play_stat_types(cur, counts: dict[tuple[str, str], int | None]) -> None:
    section("ref.play_stat_types CONTENTS")
    if counts.get(("ref", "play_stat_types")) is None:
        print("ref.play_stat_types: MISSING")
        return
    cur.execute("SELECT id, name FROM ref.play_stat_types ORDER BY id")
    for stat_id, name in cur.fetchall():
        print(f"{stat_id}\t{name}")


def print_athlete_overlap(cur, counts: dict[tuple[str, str], int | None]) -> None:
    section(f"ATHLETE_ID OVERLAP (stats.play_stats x core.roster, season={OVERLAP_SAMPLE_SEASON})")
    if counts.get(("stats", "play_stats")) is None:
        print("stats.play_stats: MISSING - overlap check skipped")
        return
    if not table_exists(cur, "core", "roster"):
        print("core.roster: MISSING - overlap check skipped")
        return

    cur.execute(
        """
        WITH sample AS (
            SELECT DISTINCT athlete_id
            FROM stats.play_stats
            WHERE game_id IN (SELECT id FROM core.games WHERE season = %s)
        )
        SELECT
            COUNT(*) AS total_distinct,
            COUNT(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM core.roster r WHERE r.id::text = sample.athlete_id
                )
            ) AS matched
        FROM sample
        """,
        (OVERLAP_SAMPLE_SEASON,),
    )
    total, matched = cur.fetchone()
    pct = (matched / total * 100) if total else 0.0
    print(f"distinct athlete_id in sample: {total:,}")
    print(f"matched to core.roster.id: {matched:,}")
    print(f"match rate: {pct:.1f}%")


def print_tier_2_row_counts(cur) -> dict[tuple[str, str], int | None]:
    """Print row counts for TIER_2_GATE_TABLES; return {(schema, table): count or None}.

    Tier 2 tables are reported for context but are not enforced in strict mode.
    """
    section("ROW COUNTS - TIER 2 (advanced analytics / predictions)")
    counts: dict[tuple[str, str], int | None] = {}
    for schema, table in TIER_2_GATE_TABLES:
        if not table_exists(cur, schema, table):
            print(f"{schema}.{table}: MISSING")
            counts[(schema, table)] = None
            continue
        count = row_count(cur, schema, table)
        counts[(schema, table)] = count
        print(f"{schema}.{table}: {count:,}")
    return counts


def print_depth_checks(cur) -> None:
    """Print season depth and row counts for backtest benchmark tables."""
    section("DEPTH CHECKS - BACKTEST BENCHMARKS")

    # marts.play_epa depth
    if table_exists(cur, "marts", "play_epa"):
        cur.execute(
            """
            SELECT MIN(season), MAX(season), COUNT(*)
            FROM marts.play_epa
            """
        )
        min_season, max_season, total_rows = cur.fetchone()
        print(
            f"marts.play_epa depth: "
            f"MIN(season)={min_season}, MAX(season)={max_season}, COUNT(*)={total_rows:,}"
        )
    else:
        print("marts.play_epa: MISSING")

    # metrics.pregame_win_probability depth
    if table_exists(cur, "metrics", "pregame_win_probability"):
        cur.execute(
            """
            SELECT MIN(season), MAX(season), COUNT(*)
            FROM metrics.pregame_win_probability
            """
        )
        min_season, max_season, total_rows = cur.fetchone()
        print(
            f"metrics.pregame_win_probability depth: "
            f"MIN(season)={min_season}, MAX(season)={max_season}, COUNT(*)={total_rows:,}"
        )
    else:
        print("metrics.pregame_win_probability: MISSING")


def evaluate_strict(counts: dict[tuple[str, str], int | None]) -> bool:
    """Return True (pass) iff every strict gate table is present and non-empty."""
    return all(counts.get(t) for t in STRICT_GATE_TABLES)


def run(strict: bool) -> int:
    import psycopg2

    from scripts.refresh_marts import get_db_url

    conn = psycopg2.connect(get_db_url())
    try:
        with conn.cursor() as cur:
            counts = print_row_counts(cur)
            print_columns(cur, counts)
            print_play_stat_types(cur, counts)
            print_athlete_overlap(cur, counts)
            print_tier_2_row_counts(cur)
            print_depth_checks(cur)
    finally:
        conn.close()

    section("STRICT GATE SUMMARY" if strict else "RECON SUMMARY (non-strict)")
    for schema, table in STRICT_GATE_TABLES:
        count = counts.get((schema, table))
        status = "MISSING" if count is None else ("EMPTY" if count == 0 else f"OK ({count:,} rows)")
        print(f"{schema}.{table}: {status}")

    if not strict:
        return 0

    if not evaluate_strict(counts):
        logger.error("strict presence check FAILED: one or more gate tables missing or empty")
        return 1
    logger.info("strict presence check PASSED")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Presence/recon check for Tier 1 gate tables")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 if any of the five gate tables is missing or empty "
        "(default: recon mode, always exit 0)",
    )
    args = parser.parse_args()
    sys.exit(run(args.strict))


if __name__ == "__main__":
    main()
