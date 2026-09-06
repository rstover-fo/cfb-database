"""Run the reviewed September 2026 compute recovery without ingest or training.

Defaults to printing the commands. --execute writes warehouse derived tables;
invoke only for the explicitly authorized production recovery.
"""

import argparse
import subprocess
import sys
from pathlib import Path

COMMANDS = (
    ("refresh_marts", "--views", "marts.play_epa,marts.returning_production"),
    ("compute_house_elo", "--season", "2026"),
    ("compute_adjusted_epa", "--season", "2026"),
    ("compute_adjusted_epa_week", "--season", "2026"),
    ("compute_predictions",),
    ("build_features", "--season", "2026"),
    ("score_fitted", "--upcoming"),
    ("simulate_season", "--season", "2026", "--model", "fitted_v1"),
    (
        "refresh_marts",
        "--views",
        "marts.house_elo,marts.house_elo_game,marts.team_adjusted_epa,"
        "marts.scored_matchup_edges,marts.prediction_accuracy,"
        "marts.team_week_features,marts.adjusted_epa_week",
    ),
)


def execute_commands(commands=COMMANDS):
    for name, *args in commands:
        path = Path(__file__).resolve().parent / f"{name}.py"
        subprocess.run([sys.executable, str(path), *args], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    for command in COMMANDS:
        print(" ".join(command), flush=True)
    if not args.execute:
        return

    import psycopg2

    from scripts.run_migrations import get_db_url

    # Do not retrain during recovery, or let the outstanding F03 selection
    # issue silently select an in-season fit.
    with psycopg2.connect(get_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '10s'")
            cur.execute("SET LOCAL idle_in_transaction_session_timeout = 0")
            # SHARE permits scorer reads while preventing any concurrent fit
            # writer from changing the selected vintage or coefficient vectors.
            cur.execute(
                "LOCK TABLE features.model_metadata, features.model_coefficients IN SHARE MODE"
            )
            cur.execute(
                "SELECT MAX(train_through_season) FROM features.model_metadata "
                "WHERE model_version = 'fitted_v1'"
            )
            train_through = cur.fetchone()[0]
            if train_through != 2025:
                raise RuntimeError(f"Recovery requires the frozen 2025 fit; found {train_through}")
            cur.execute(
                "SELECT DISTINCT season FROM core.games "
                "WHERE NOT COALESCE(completed, false) AND season >= "
                "(SELECT COALESCE(MAX(season), 0) FROM core.games WHERE completed)"
            )
            targets = {row[0] for row in cur.fetchall()}
            if targets != {2026}:
                raise RuntimeError(f"Recovery requires only 2026 pending targets; found {targets}")
        execute_commands()


if __name__ == "__main__":
    main()
