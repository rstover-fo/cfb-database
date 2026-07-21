#!/usr/bin/env python3
"""Backtest gate report for Tier 2 predictions (read-only, no writes).

Reads marts.prediction_accuracy (src/schemas/marts/038_prediction_accuracy.sql,
docs/plans/2026-07-21-tier2-analytics-plan.md Phase 5) and prints:

  1. Per-season rows at edge_threshold=0 for each model_version: n_games,
     margin_mae, brier vs cfbd_brier -- the walk-forward-honest per-season
     view (edge_threshold=0 is the only row where these are the whole-season
     "every game" numbers; see 038's header).
  2. Cross-season aggregates for each model_version at all four edge
     thresholds (0/3/6/10), computed with a direct GROUP BY over the mart
     (not by re-deriving anything from predictions.game_predictions):
     ats_hit_rate is re-aggregated from summed ats_wins/ats_losses (not
     averaged season-hit-rates), margin_mae is n_games-weighted, and
     brier/cfbd_brier are n_scored_win_prob-weighted (their own valid
     population, which can differ from n_games).
  3. One BACKTEST_GATE line per model_version -- the Phase 5 gate artifact.
     ats6_hit_rate/ats6_n come from the edge_threshold=6 aggregate (the
     plan's "ATS at |edge|>=6 > 50%" criterion); brier/cfbd_brier/margin_mae
     come from the edge_threshold=0 aggregate (the unconditional
     whole-season baseline, not filtered to only high-edge games).

This script never fails the gate itself -- reading the printed numbers
against the plan's thresholds and deciding whether to re-tune (K, lambda,
blend weights) is a human call in the Phase 5 loop, per the plan. The only
failure mode here is the mart itself being missing or empty, which means
`compute_predictions.py --backfill <start> <end>` and a mart refresh haven't
run yet.

Usage:
    python scripts/check_backtest.py
"""

import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EDGE_THRESHOLDS = (0, 3, 6, 10)
BASELINE_THRESHOLD = 0
GATE_THRESHOLD = 6

AGGREGATE_QUERY = """
    SELECT
        model_version,
        edge_threshold,
        SUM(n_games) AS n_games,
        SUM(ats_wins) AS ats_wins,
        SUM(ats_losses) AS ats_losses,
        SUM(ats_pushes) AS ats_pushes,
        ROUND(
            SUM(ats_wins)::numeric / NULLIF(SUM(ats_wins) + SUM(ats_losses), 0), 4
        ) AS ats_hit_rate,
        ROUND(SUM(margin_mae * n_games) / NULLIF(SUM(n_games), 0), 4) AS margin_mae,
        SUM(n_scored_win_prob) AS n_scored_win_prob,
        ROUND(
            SUM(brier * n_scored_win_prob) / NULLIF(SUM(n_scored_win_prob), 0), 6
        ) AS brier,
        ROUND(
            SUM(cfbd_brier * n_scored_win_prob) / NULLIF(SUM(n_scored_win_prob), 0), 6
        ) AS cfbd_brier
    FROM marts.prediction_accuracy
    GROUP BY model_version, edge_threshold
    ORDER BY model_version, edge_threshold
"""

PER_SEASON_QUERY = """
    SELECT model_version, season, n_games, margin_mae, brier, cfbd_brier
    FROM marts.prediction_accuracy
    WHERE edge_threshold = %s
    ORDER BY model_version, season
"""


def section(title: str) -> None:
    print(f"\n===== {title} =====")


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
    return cur.fetchone()[0] is not None


def print_per_season(cur) -> None:
    section("PER-SEASON (edge_threshold=0)")
    cur.execute(PER_SEASON_QUERY, (BASELINE_THRESHOLD,))
    rows = cur.fetchall()
    if not rows:
        print("(no rows)")
        return
    for model_version, season, n_games, margin_mae, brier, cfbd_brier in rows:
        print(
            f"model={model_version} season={season} n={n_games} "
            f"margin_mae={margin_mae} brier={brier} cfbd_brier={cfbd_brier}"
        )


_AGGREGATE_COLUMNS = [
    "model_version",
    "edge_threshold",
    "n_games",
    "ats_wins",
    "ats_losses",
    "ats_pushes",
    "ats_hit_rate",
    "margin_mae",
    "n_scored_win_prob",
    "brier",
    "cfbd_brier",
]


def fetch_aggregates(cur) -> dict[tuple[str, int], dict]:
    """Cross-season, n-weighted aggregate per (model_version, edge_threshold),
    keyed for easy lookup by the gate-line builder below."""
    cur.execute(AGGREGATE_QUERY)
    aggregates: dict[tuple[str, int], dict] = {}
    for values in cur.fetchall():
        row = dict(zip(_AGGREGATE_COLUMNS, values))
        aggregates[(row["model_version"], row["edge_threshold"])] = row
    return aggregates


def print_aggregates(aggregates: dict[tuple[str, int], dict]) -> None:
    section("CROSS-SEASON AGGREGATE (n-weighted, all edge thresholds)")
    if not aggregates:
        print("(no rows)")
        return
    for key in sorted(aggregates):
        row = aggregates[key]
        print(
            f"model={row['model_version']} edge_threshold={row['edge_threshold']} "
            f"n_games={row['n_games']} ats_wins={row['ats_wins']} "
            f"ats_losses={row['ats_losses']} ats_hit_rate={row['ats_hit_rate']} "
            f"margin_mae={row['margin_mae']} brier={row['brier']} "
            f"cfbd_brier={row['cfbd_brier']}"
        )


def print_gate_lines(aggregates: dict[tuple[str, int], dict]) -> None:
    """The Phase 5 gate artifact: one BACKTEST_GATE line per model_version."""
    section("BACKTEST GATE")
    model_versions = sorted({model for model, _threshold in aggregates})
    if not model_versions:
        print("(no rows)")
        return

    for model_version in model_versions:
        gate_row = aggregates.get((model_version, GATE_THRESHOLD))
        baseline_row = aggregates.get((model_version, BASELINE_THRESHOLD))

        ats6_hit_rate = gate_row["ats_hit_rate"] if gate_row else None
        ats6_n = (gate_row["ats_wins"] + gate_row["ats_losses"]) if gate_row else None
        brier = baseline_row["brier"] if baseline_row else None
        cfbd_brier = baseline_row["cfbd_brier"] if baseline_row else None
        margin_mae = baseline_row["margin_mae"] if baseline_row else None

        print(
            f"BACKTEST_GATE model={model_version} ats6_hit_rate={ats6_hit_rate} "
            f"ats6_n={ats6_n} brier={brier} cfbd_brier={cfbd_brier} margin_mae={margin_mae}"
        )


def run() -> int:
    import psycopg2

    from scripts.refresh_marts import get_db_url

    conn = psycopg2.connect(get_db_url())
    try:
        with conn.cursor() as cur:
            if not table_exists(cur, "marts", "prediction_accuracy"):
                logger.error(
                    "marts.prediction_accuracy is MISSING -- run "
                    "`python scripts/compute_predictions.py --backfill <start> <end>` "
                    "then refresh marts (python scripts/refresh_marts.py) before retrying."
                )
                return 1

            cur.execute("SELECT COUNT(*) FROM marts.prediction_accuracy")
            (count,) = cur.fetchone()
            if count == 0:
                logger.error(
                    "marts.prediction_accuracy is EMPTY -- run "
                    "`python scripts/compute_predictions.py --backfill <start> <end>` "
                    "then refresh marts (python scripts/refresh_marts.py) before retrying."
                )
                return 1

            print_per_season(cur)
            aggregates = fetch_aggregates(cur)
            print_aggregates(aggregates)
            print_gate_lines(aggregates)
    finally:
        conn.close()

    return 0


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
