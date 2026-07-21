#!/usr/bin/env python3
"""Ridge-regressed opponent-adjusted EPA (Tier 2 analytics).

Fits, per season, the model

    epa ~ mu + off[team] + def[team] + hfa * is_home_offense

over every non-garbage-time play in ``marts.play_epa`` (2004+), with a ridge
penalty on the team coefficients only. Writes one row per (team, season) to
``analytics.adjusted_epa_build`` (src/schemas/migrations/025_tier2_analytics_staging.sql).

Sign convention: off_coef higher = better offense; def_coef LOWER / more
negative = better defense (EPA allowed above average).

See docs/plans/2026-07-21-tier2-analytics-plan.md ("Ridge-adjusted EPA")
for the design this implements.

Usage:
    python scripts/compute_adjusted_epa.py --season 2024
    python scripts/compute_adjusted_epa.py --from 2004     # 2004..max season present
"""

import argparse
import logging
import sys
from collections.abc import Iterable

import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Ridge penalty applied to the offense/defense team columns only (mu, hfa are
# unpenalized). ~200 "pseudo-plays" worth of shrinkage toward 0 per team
# coefficient. Recorded per row in analytics.adjusted_epa_build so historical
# fits stay auditable even if this tunable changes later.
LAMBDA = 200.0

# Server-side cursor fetch batch size for the per-season play stream.
CURSOR_ITERSIZE = 10_000

PLAY_QUERY = """
    SELECT
        pe.offense,
        pe.defense,
        pe.epa,
        (pe.offense = g.home_team AND NOT COALESCE(g.neutral_site, false)) AS is_home_offense
    FROM marts.play_epa pe
    JOIN core.games g ON g.id = pe.game_id
    WHERE pe.season = %s
      AND NOT pe.is_garbage_time
      AND pe.epa IS NOT NULL
"""

TEAM_LIST_QUERY = """
    SELECT DISTINCT offense AS team FROM marts.play_epa WHERE season = %s
    UNION
    SELECT DISTINCT defense AS team FROM marts.play_epa WHERE season = %s
"""


class RidgeAccumulator:
    """Streaming accumulator for the ridge-adjusted EPA normal equations.

    Fixed column layout for a given season's team list (``teams``, assumed
    already sorted by the caller so results are reproducible):

        index 0        -> mu (intercept)
        index 1        -> hfa (home-field advantage)
        index 2..T+1   -> offense indicator per team (team order as given)
        index T+2..2T+1 -> defense indicator per team (team order as given)

    where T = len(teams). Each play is a design-matrix row with exactly four
    nonzero entries: mu=1, hfa=is_home_offense (0/1), one offense indicator=1,
    one defense indicator=1. ``add_play`` folds a row's contribution straight
    into the dense XtX/Xty accumulators via that row's 4x4 outer product --
    the full (n_plays x 2T+2) design matrix X is never built.

    Why the ridge penalty is required for a unique solution: on defense and
    offense alike, every row has exactly one offense indicator set and one
    defense indicator set, so summing the offense-indicator columns
    reproduces the mu column exactly (and likewise for the defense-indicator
    columns). That is an exact linear dependency among X's columns -- without
    penalization XtX is rank-deficient (singular) and infinitely many beta
    solve the normal equations equivalently (any additive shift moved between
    mu and the team coefficients cancels). Adding `lam * P` with `P` an
    identity matrix zeroed at the mu/hfa indices only touches the team-column
    block, which is exactly where the null-space vectors of that dependency
    have their support, so for lam > 0 the penalized system XtX + lam*P is
    strictly positive definite and `np.linalg.solve` returns a unique beta.
    mu/hfa stay unpenalized (P is 0 there) because they are not part of that
    collinearity and are quantities we want a genuine unshrunk estimate of.
    """

    MU_IDX = 0
    HFA_IDX = 1

    def __init__(self, teams: list[str]):
        self.teams = list(teams)
        self.n_teams = len(self.teams)
        self._team_idx = {team: i for i, team in enumerate(self.teams)}

        size = 2 * self.n_teams + 2
        self.xtx = np.zeros((size, size), dtype=np.float64)
        self.xty = np.zeros(size, dtype=np.float64)
        self.off_play_counts = np.zeros(self.n_teams, dtype=np.int64)
        self.n_plays = 0

    @property
    def off_start(self) -> int:
        return 2

    @property
    def def_start(self) -> int:
        return 2 + self.n_teams

    def add_play(self, off_team: str, def_team: str, is_home_offense: bool, epa: float) -> None:
        """Fold one play's row into XtX/Xty via its 4x4 outer product."""
        off_i = self._team_idx[off_team]
        def_i = self._team_idx[def_team]
        idx_off = self.off_start + off_i
        idx_def = self.def_start + def_i
        hfa_val = 1.0 if is_home_offense else 0.0

        idxs = np.array((self.MU_IDX, self.HFA_IDX, idx_off, idx_def))
        vals = np.array((1.0, hfa_val, 1.0, 1.0), dtype=np.float64)

        # Outer product of the row's 4 nonzero entries = the 16 XtX cells it
        # touches; vals * epa = the 4 Xty cells it touches. Everywhere else
        # in the row is 0, so this is exactly x @ x.T and x * epa restricted
        # to the nonzero support, without ever materializing the full row.
        self.xtx[np.ix_(idxs, idxs)] += np.outer(vals, vals)
        self.xty[idxs] += vals * epa

        self.off_play_counts[off_i] += 1
        self.n_plays += 1

    def add_plays(self, plays: Iterable[tuple[str, str, bool, float]]) -> None:
        """Batch variant of add_play for streaming a cursor efficiently."""
        for off_team, def_team, is_home_offense, epa in plays:
            self.add_play(off_team, def_team, is_home_offense, epa)

    def solve(self, lam: float) -> tuple[float, float, dict[str, float], dict[str, float], int]:
        """Solve the ridge-penalized normal equations.

        Returns (mu, hfa, off_coef, def_coef, n_plays) where off_coef and
        def_coef are {team: coefficient} dicts covering every team passed to
        __init__.
        """
        size = 2 * self.n_teams + 2
        penalty = np.eye(size, dtype=np.float64)
        penalty[self.MU_IDX, self.MU_IDX] = 0.0
        penalty[self.HFA_IDX, self.HFA_IDX] = 0.0

        beta = np.linalg.solve(self.xtx + lam * penalty, self.xty)

        mu = float(beta[self.MU_IDX])
        hfa = float(beta[self.HFA_IDX])
        off_coef = {team: float(beta[self.off_start + i]) for i, team in enumerate(self.teams)}
        def_coef = {team: float(beta[self.def_start + i]) for i, team in enumerate(self.teams)}
        return mu, hfa, off_coef, def_coef, self.n_plays


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Adds statement_timeout=0 since a full-history fit streams millions of
    plays per season through a server-side cursor.
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


def get_season_teams(cur, season: int) -> list[str]:
    """Cheap pass to fix the sorted team list for a season before streaming plays.

    Teams present on either side (offense or defense) get both an offense and
    a defense column, even if they never appear on the other side that year.
    """
    cur.execute(TEAM_LIST_QUERY, (season, season))
    return sorted(row[0] for row in cur.fetchall())


def get_max_season(cur) -> int | None:
    cur.execute("SELECT MAX(season) FROM marts.play_epa")
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def fit_season(conn, season: int) -> tuple[RidgeAccumulator, list[str]] | None:
    """Stream one season's plays into a RidgeAccumulator. None if no teams/plays."""
    with conn.cursor() as cur:
        teams = get_season_teams(cur, season)

    if not teams:
        logger.warning(f"season={season}: no teams found in marts.play_epa, skipping")
        return None

    logger.info(f"season={season}: {len(teams)} teams, streaming plays...")

    accumulator = RidgeAccumulator(teams)
    cursor_name = f"adjusted_epa_plays_{season}"
    with conn.cursor(name=cursor_name) as cur:
        cur.itersize = CURSOR_ITERSIZE
        cur.execute(PLAY_QUERY, (season,))
        for offense, defense, epa, is_home_offense in cur:
            accumulator.add_play(offense, defense, bool(is_home_offense), float(epa))

    if accumulator.n_plays == 0:
        logger.warning(f"season={season}: 0 qualifying plays, skipping")
        return None

    logger.info(f"season={season}: accumulated {accumulator.n_plays} plays")
    return accumulator, teams


def write_season(conn, season: int, accumulator: RidgeAccumulator, lam: float) -> None:
    from psycopg2.extras import execute_values

    mu, hfa, off_coef, def_coef, _n_plays = accumulator.solve(lam)
    n_teams = accumulator.n_teams

    rows = [
        (
            team,
            season,
            off_coef[team],
            def_coef[team],
            hfa,
            mu,
            int(accumulator.off_play_counts[i]),
            lam,
            n_teams,
        )
        for i, team in enumerate(accumulator.teams)
    ]

    with conn.cursor() as cur:
        cur.execute("DELETE FROM analytics.adjusted_epa_build WHERE season = %s", (season,))
        execute_values(
            cur,
            """
            INSERT INTO analytics.adjusted_epa_build
                (team, season, off_coef, def_coef, hfa_coef, mu, plays, lambda, n_teams)
            VALUES %s
            """,
            rows,
        )
    conn.commit()
    logger.info(f"season={season}: wrote {len(rows)} rows to analytics.adjusted_epa_build")


def validate_season(conn, season: int, accumulator: RidgeAccumulator) -> None:
    """Print a Pearson-r sanity check against marts.team_wepa_season, if present."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT team, epa_total, epa_allowed_total
            FROM marts.team_wepa_season
            WHERE season = %s
            """,
            (season,),
        )
        wepa_rows = cur.fetchall()

    if not wepa_rows:
        logger.info(f"season={season}: no marts.team_wepa_season rows, skipping validation")
        return

    _mu, _hfa, off_coef, def_coef, _n_plays = accumulator.solve(LAMBDA)

    wepa_off, wepa_def, adj_off, adj_def = [], [], [], []
    for team, epa_total, epa_allowed_total in wepa_rows:
        if team not in off_coef or epa_total is None or epa_allowed_total is None:
            continue
        wepa_off.append(float(epa_total))
        wepa_def.append(float(epa_allowed_total))
        adj_off.append(off_coef[team])
        adj_def.append(def_coef[team])

    n = len(wepa_off)
    if n < 2:
        logger.info(
            f"season={season}: only {n} overlapping team(s) with marts.team_wepa_season, "
            "skipping validation"
        )
        return

    r_off = float(np.corrcoef(adj_off, wepa_off)[0, 1])
    r_def = float(np.corrcoef(adj_def, wepa_def)[0, 1])
    print(f"ADJEPA_VALIDATION season={season} n={n} r_off={r_off:.4f} r_def={r_def:.4f}")


def compute_seasons(seasons: list[int]) -> int:
    """Fit and write each season. Returns count of failed/skipped seasons."""
    import psycopg2

    db_url = get_db_url()
    conn = psycopg2.connect(db_url)

    failures = 0
    try:
        for season in seasons:
            try:
                result = fit_season(conn, season)
                if result is None:
                    failures += 1
                    continue
                accumulator, _teams = result
                write_season(conn, season, accumulator, LAMBDA)
                validate_season(conn, season, accumulator)
            except Exception as e:
                conn.rollback()
                logger.error(f"season={season}: FAILED: {e}")
                failures += 1
    finally:
        conn.close()

    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Fit ridge-adjusted EPA per season")
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
    args = parser.parse_args()

    if args.season is not None:
        seasons = [args.season]
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

    logger.info(f"Fitting ridge-adjusted EPA for {len(seasons)} season(s): {seasons}")
    failures = compute_seasons(seasons)

    if failures:
        logger.warning(f"{failures} season(s) failed or had no data")
    else:
        logger.info("All seasons fit successfully")

    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
