#!/usr/bin/env python3
"""Walk-forward (as-of-week) ridge-adjusted EPA (Tier 3 analytics, Pillar A).

Sibling of scripts/compute_adjusted_epa.py, which fits ONE ridge-adjusted EPA
model per season using every non-garbage-time play. This script instead
streams each season's plays in week order and solves the SAME model at every
week boundary, using only the plays accumulated so far -- producing a
walk-forward "as of entering week W" rating that never sees week W or any
later week's results. That is what makes it safe to join onto week W's games
without leaking the game's own outcome into its own pregame rating (see
docs/plans/2026-07-21-tier3-analytics-plan.md, Pillar A, and
src/schemas/migrations/026_adjusted_epa_week_staging.sql's header).

The ridge math itself (RidgeAccumulator, the LAMBDA penalty, the team-list and
play-query patterns) is imported unchanged from compute_adjusted_epa -- this
module only adds the week-boundary bookkeeping on top.

week_index convention (shared with features.team_week, see
docs/brainstorms/2026-07-21-team-week-feature-design.md): CFBD restarts week
numbering at 1 for season_type='postseason' (bowls are week 1), so raw week
cannot order a season monotonically. We compute, per play (via its game):

    week_index = week            for season_type = 'regular'
    week_index = 100 + week      for season_type = 'postseason'

For each season we make ONE streaming pass over that season's plays ordered
by week_index (then game_id for determinism), folding them into a fresh
RidgeAccumulator. Team identity/column layout is fixed up front from the
FULL season's team list (same DISTINCT offense/defense union query as
compute_adjusted_epa.py) -- that is not leakage, only the fitted coefficients
are as-of. Whenever the incoming play's week_index differs from the previous
play's AND the accumulator already holds >= 1 play, we solve the accumulated
(pre-that-week) state and emit one row per team in the layout: the ridge
coefficients ENTERING that week. We then continue folding that week's plays.
No boundary is emitted before the first week (the accumulator starts empty),
and no trailing boundary is emitted after the season's last play -- the
full-season fit already lives in analytics.adjusted_epa_build.

Because postseason week_index values (101, 102, ...) sort after every regular
week_index, the first postseason boundary lands at 100 + <first bowl week>
and its state is the ENTIRE regular season -- this is the "entering
postseason" row that bowl games resolve to via a "greatest week_index <= WI"
lookup.

Each row also records that team's accumulated OFFENSIVE play count entering
the boundary (a Counter-style tally, incremented as plays are folded), so
downstream consumers can gauge how thin an early-season rating is.

Writes to analytics.adjusted_epa_week_build (migration 026): per-season
DELETE + batched INSERT, one transaction per season -- same idempotency
pattern as compute_adjusted_epa.py's write to analytics.adjusted_epa_build.

After writing, each season prints a validation line comparing its LAST
emitted boundary (the highest week_index, i.e. the most fully-informed
walk-forward state) against the full-season fit in
analytics.adjusted_epa_build for the same season, via Pearson r over
off_coef and def_coef across teams (scripts.compute_house_elo.pearson_r,
which returns nan rather than raising on too few points):

    EPA_WEEK_GATE season={s} boundaries={n} teams={t} rows={r} \
        r_off_vs_full={x:.4f} r_def_vs_full={y:.4f}

Expected r >~ 0.97 -- the last boundary's state differs from the full-season
fit only by excluding that season's postseason plays, so the two fits should
be nearly identical. If analytics.adjusted_epa_build has no row yet for that
season, r prints as nan and a log line notes the skip.

Usage:
    python scripts/compute_adjusted_epa_week.py --season 2024
    python scripts/compute_adjusted_epa_week.py --from 2004      # 2004..max season present
    python scripts/compute_adjusted_epa_week.py --incremental    # current season only
"""

import argparse
import logging
import sys
from collections.abc import Iterable

from scripts.compute_adjusted_epa import (
    CURSOR_ITERSIZE,
    LAMBDA,
    RidgeAccumulator,
    get_db_url,
    get_max_season,
    get_season_teams,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Play row: (offense, defense, is_home_offense, epa) from compute_adjusted_epa's
# PLAY_QUERY, plus week_index computed in SQL from the joined game's week /
# season_type (see module docstring for the week_index convention).
PLAY_QUERY_WEEK = """
    SELECT
        pe.offense,
        pe.defense,
        pe.epa,
        (pe.offense = g.home_team AND NOT COALESCE(g.neutral_site, false)) AS is_home_offense,
        CASE WHEN g.season_type = 'postseason' THEN 100 + g.week ELSE g.week END AS week_index
    FROM marts.play_epa pe
    JOIN core.games g ON g.id = pe.game_id
    WHERE pe.season = %s
      AND NOT pe.is_garbage_time
      AND pe.epa IS NOT NULL
    ORDER BY week_index, pe.game_id
"""

# A play row as consumed by the pure boundary-walking function below.
PlayRow = tuple[str, str, bool, float, int]


def _boundary_rows(
    accumulator: RidgeAccumulator, season: int | None, week_index: int, lam: float
) -> list[dict]:
    """Solve `accumulator`'s current (pre-this-week) state into one row per team."""
    mu, hfa, off_coef, def_coef, _n_plays = accumulator.solve(lam)
    n_teams = accumulator.n_teams
    return [
        {
            "team": team,
            "season": season,
            "week_index": week_index,
            "off_coef": off_coef[team],
            "def_coef": def_coef[team],
            "hfa_coef": hfa,
            "mu": mu,
            "plays": int(accumulator.off_play_counts[i]),
            "lambda": lam,
            "n_teams": n_teams,
        }
        for i, team in enumerate(accumulator.teams)
    ]


def compute_week_boundaries(
    plays: Iterable[PlayRow],
    teams: list[str],
    lam: float = LAMBDA,
    season: int | None = None,
) -> list[dict]:
    """Stream `plays` (already ordered by week_index) into week-boundary fits.

    Pure, DB-free, and unit-testable: no I/O, only RidgeAccumulator math and
    bookkeeping. `teams` fixes the column layout up front (the full season's
    team list -- see module docstring on why that's not leakage).

    Returns a list of row dicts (team, season, week_index, off_coef, def_coef,
    hfa_coef, mu, plays, lambda, n_teams), one per team per week boundary, in
    the order the boundaries were crossed. A boundary for week_index W is
    emitted the moment a play with that week_index is seen AND the
    accumulator already holds >= 1 play from strictly earlier week_index
    values -- i.e. its state reflects exactly the plays with week_index < W.
    No boundary is emitted before the first week (empty initial state) and
    none after the last play (no trailing boundary; the full-season fit is
    computed separately by compute_adjusted_epa.py).
    """
    accumulator = RidgeAccumulator(teams)
    boundary_rows: list[dict] = []
    prev_week_index: int | None = None

    for off_team, def_team, is_home_offense, epa, week_index in plays:
        if (
            prev_week_index is not None
            and week_index != prev_week_index
            and accumulator.n_plays > 0
        ):
            boundary_rows.extend(_boundary_rows(accumulator, season, week_index, lam))

        accumulator.add_play(off_team, def_team, is_home_offense, epa)
        prev_week_index = week_index

    return boundary_rows


def fit_season_weeks(conn, season: int, lam: float = LAMBDA) -> tuple[list[dict], list[str]] | None:
    """Stream one season's plays into week-boundary fits. None if no data at all."""
    with conn.cursor() as cur:
        teams = get_season_teams(cur, season)

    if not teams:
        logger.info(f"season={season}: no teams found in marts.play_epa, clean no-op")
        return None

    logger.info(f"season={season}: {len(teams)} teams, streaming plays for week boundaries...")

    n_plays = 0

    def _play_stream(cur) -> Iterable[PlayRow]:
        nonlocal n_plays
        for offense, defense, epa, is_home_offense, week_index in cur:
            n_plays += 1
            yield offense, defense, bool(is_home_offense), float(epa), int(week_index)

    cursor_name = f"adjusted_epa_week_plays_{season}"
    with conn.cursor(name=cursor_name) as cur:
        cur.itersize = CURSOR_ITERSIZE
        cur.execute(PLAY_QUERY_WEEK, (season,))
        boundary_rows = compute_week_boundaries(_play_stream(cur), teams, lam=lam, season=season)

    if n_plays == 0:
        logger.warning(f"season={season}: 0 qualifying plays, skipping")
        return None

    logger.info(
        f"season={season}: accumulated {n_plays} plays, emitted {len(boundary_rows)} boundary rows"
    )
    return boundary_rows, teams


def write_season(conn, season: int, boundary_rows: list[dict]) -> None:
    from psycopg2.extras import execute_values

    with conn.cursor() as cur:
        cur.execute("DELETE FROM analytics.adjusted_epa_week_build WHERE season = %s", (season,))
        if boundary_rows:
            rows = [
                (
                    r["team"],
                    r["season"],
                    r["week_index"],
                    r["off_coef"],
                    r["def_coef"],
                    r["hfa_coef"],
                    r["mu"],
                    r["plays"],
                    r["lambda"],
                    r["n_teams"],
                )
                for r in boundary_rows
            ]
            execute_values(
                cur,
                """
                INSERT INTO analytics.adjusted_epa_week_build
                    (team, season, week_index, off_coef, def_coef, hfa_coef, mu, plays, lambda,
                     n_teams)
                VALUES %s
                """,
                rows,
            )
    conn.commit()
    logger.info(
        f"season={season}: wrote {len(boundary_rows)} rows to analytics.adjusted_epa_week_build"
    )


def validate_season(conn, season: int, boundary_rows: list[dict], teams: list[str]) -> None:
    """Print an EPA_WEEK_GATE line comparing the last boundary to the full-season fit."""
    from scripts.compute_house_elo import pearson_r

    n_teams = len(teams)
    n_rows = len(boundary_rows)

    if not boundary_rows:
        logger.info(f"season={season}: no week boundaries emitted, skipping validation")
        print(
            f"EPA_WEEK_GATE season={season} boundaries=0 teams={n_teams} rows=0 "
            "r_off_vs_full=nan r_def_vs_full=nan"
        )
        return

    n_boundaries = len({r["week_index"] for r in boundary_rows})
    max_week_index = max(r["week_index"] for r in boundary_rows)
    last_boundary = {r["team"]: r for r in boundary_rows if r["week_index"] == max_week_index}

    with conn.cursor() as cur:
        cur.execute(
            "SELECT team, off_coef, def_coef FROM analytics.adjusted_epa_build WHERE season = %s",
            (season,),
        )
        full_rows = cur.fetchall()

    if not full_rows:
        logger.info(
            f"season={season}: no analytics.adjusted_epa_build rows yet, "
            "skipping full-season comparison"
        )

    week_off, week_def, full_off, full_def = [], [], [], []
    for team, full_off_coef, full_def_coef in full_rows:
        if team not in last_boundary or full_off_coef is None or full_def_coef is None:
            continue
        week_off.append(float(last_boundary[team]["off_coef"]))
        week_def.append(float(last_boundary[team]["def_coef"]))
        full_off.append(float(full_off_coef))
        full_def.append(float(full_def_coef))

    r_off = pearson_r(week_off, full_off)
    r_def = pearson_r(week_def, full_def)
    print(
        f"EPA_WEEK_GATE season={season} boundaries={n_boundaries} teams={n_teams} rows={n_rows} "
        f"r_off_vs_full={r_off:.4f} r_def_vs_full={r_def:.4f}"
    )


def compute_seasons(seasons: list[int]) -> int:
    """Fit and write each season's week boundaries. Returns count of failed seasons."""
    import psycopg2

    db_url = get_db_url()
    conn = psycopg2.connect(db_url)

    failures = 0
    try:
        for season in seasons:
            try:
                result = fit_season_weeks(conn, season)
                if result is None:
                    # No play data at all yet for this season (e.g. the Aug 1
                    # get_current_season() rollover, weeks before
                    # marts.play_epa has rows) is a legitimate pre-season
                    # state, not a failure -- mirrors compute_adjusted_epa.py's
                    # Codex-P2 fix (PR #18): the daily workflow must not die
                    # in that window.
                    logger.info(f"season={season}: no play data yet, clean no-op")
                    continue
                boundary_rows, teams = result
                write_season(conn, season, boundary_rows)
                validate_season(conn, season, boundary_rows, teams)
            except Exception as e:
                conn.rollback()
                logger.error(f"season={season}: FAILED: {e}")
                failures += 1
    finally:
        conn.close()

    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Fit walk-forward per-week ridge-adjusted EPA")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--from",
        dest="from_season",
        type=int,
        help="Fit every season from YYYY through the max season in marts.play_epa",
    )
    group.add_argument(
        "--season",
        dest="season",
        type=int,
        help="Fit a single season",
    )
    group.add_argument(
        "--incremental",
        action="store_true",
        help="Fit only the current season (src.pipelines.config.years.get_current_season())",
    )
    args = parser.parse_args()

    if args.season is not None:
        seasons = [args.season]
    elif args.incremental:
        from src.pipelines.config.years import get_current_season

        seasons = [get_current_season()]
    else:
        import psycopg2

        conn = psycopg2.connect(get_db_url())
        try:
            with conn.cursor() as cur:
                max_season = get_max_season(cur)
        finally:
            conn.close()

        if max_season is None:
            logger.error("marts.play_epa has no rows; cannot determine max season")
            sys.exit(1)
        if args.from_season > max_season:
            logger.error(f"--from {args.from_season} is after max season present ({max_season})")
            sys.exit(1)
        seasons = list(range(args.from_season, max_season + 1))

    logger.info(
        f"Fitting walk-forward per-week ridge-adjusted EPA for {len(seasons)} season(s): {seasons}"
    )
    failures = compute_seasons(seasons)

    if failures:
        logger.warning(f"{failures} season(s) failed")
    else:
        logger.info("All seasons fit successfully")

    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
