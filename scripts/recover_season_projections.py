"""Run the reviewed September 2026 compute recovery without ingest or training.

Defaults to printing the commands. --check performs read-only production preflight.
--execute writes warehouse derived tables;
invoke only for the explicitly authorized production recovery.
"""

import argparse
import math
import subprocess
import sys
from pathlib import Path

COMMANDS = (
    ("refresh_marts", "--views", "marts.play_epa,marts.returning_production"),
    ("compute_house_elo", "--season", "2024"),
    ("compute_house_elo", "--season", "2025"),
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
        "marts.team_week_features,marts.adjusted_epa_week,marts.data_freshness",
    ),
)


def execute_commands(commands=COMMANDS):
    for name, *args in commands:
        path = Path(__file__).resolve().parent / f"{name}.py"
        subprocess.run([sys.executable, str(path), *args], check=True)


def check_recovery_state(conn):
    """Fail before writes if the merged scorer cannot use the approved 2025 fit."""
    from scripts.score_fitted import fetch_pending_game_counts, load_fit, score_game
    from scripts.train_model import TEAM_WEEK_SOURCE_COLUMNS, fetch_refit_state

    frontier, eligible_fits = fetch_refit_state(conn)
    pending = fetch_pending_game_counts(conn)
    print(
        f"RECOVERY_PREFLIGHT closed_frontier={frontier} "
        f"eligible_fits={eligible_fits} pending_by_season={pending}",
        flush=True,
    )
    if 2025 not in eligible_fits:
        raise RuntimeError(
            f"Recovery requires an eligible frozen 2025 fit; closed frontier={frontier}, "
            f"eligible fits={eligible_fits}"
        )
    if set(pending) != {2026}:
        raise RuntimeError(f"Recovery requires only 2026 pending targets; found {pending}")
    # fetch_refit_state validates season finality and the metadata feature
    # contract. Loading and exercising the fit proves both coefficient vectors
    # and every frozen scaling statistic are usable before execute mode changes
    # derived data.
    fit = load_fit(conn, 2025)
    neutral_features = dict.fromkeys(TEAM_WEEK_SOURCE_COLUMNS, 0.0)
    margin, probability = score_game(
        {
            "season": 2026,
            "neutral_site": False,
            "home_tw": neutral_features,
            "away_tw": neutral_features,
        },
        fit,
    )
    if not (math.isfinite(margin) and math.isfinite(probability)):
        raise RuntimeError(
            "Recovery requires the frozen 2025 fit to produce finite margin and probability"
        )


def execute_recovery(conn):
    """Validate the complete scoring artifact before starting any subprocess."""

    check_recovery_state(conn)
    execute_commands()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--check", action="store_true")
    args = parser.parse_args()
    for command in COMMANDS:
        print(" ".join(command), flush=True)
    if not (args.execute or args.check):
        return

    import psycopg2

    from scripts.run_migrations import get_db_url

    # Preserve the approved frozen fit throughout the derived-data rebuild.
    with psycopg2.connect(get_db_url()) as conn:
        if args.check:
            conn.set_session(readonly=True)
            check_recovery_state(conn)
            return
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
        execute_recovery(conn)


if __name__ == "__main__":
    main()
