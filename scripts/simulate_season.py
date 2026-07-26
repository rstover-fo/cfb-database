#!/usr/bin/env python3
"""Simulate season outcomes into predictions.season_projections.

Plan: docs/plans/2026-07-25-preseason-outlook-model-plan.md Phase 4. Target
DDL: src/schemas/migrations/043_season_projections.sql.

WHY THIS EXISTS
---------------
Everything the warehouse predicted before this was a per-game point estimate.
Asked for a team's season outlook, the only honest answer was a list of twelve
spreads -- which is not a win total, carries no interval, and cannot say
"62% to reach 9 wins". This script composes the per-game predictions into the
season-level object that question actually needs.

METHOD
------
Monte Carlo over the team's schedule:

  - Completed games contribute their ACTUAL result (so an in-season projection
    is "record so far + simulated remainder", not a fantasy re-run of games
    already played).
  - Each remaining game draws ``margin ~ Normal(expected_home_margin, sigma)``
    and the home team wins iff the drawn margin is positive.
  - Wins accumulate per team per simulation, giving a full distribution rather
    than a point estimate.

``sigma`` is MEASURED per model from completed games -- ``stddev_pop(actual -
predicted)`` -- never hardcoded, and is written to every row so a projection
always carries the assumption that produced it. For fitted_v1 on 2025 this is
about 18.5 points.

Drawing the margin (rather than flipping the stored ``home_win_prob``) keeps a
single, explicit, tunable uncertainty parameter instead of relying on per-game
Platt-calibrated probabilities that were never jointly calibrated across a
season.

KNOWN v1 LIMITATION -- INDEPENDENT DRAWS
----------------------------------------
Each game is drawn independently. Real season outcomes are correlated: a team
genuinely better than its rating beats *everyone* more often, so its wins
cluster. Independent draws therefore understate BOTH tails -- 11-win and 2-win
seasons are each less likely under this model than in reality. Central
tendency (``projected_wins``, ``median_wins``) is essentially unaffected;
``wins_p10``/``wins_p90``/``p_ten_plus`` are conservative at the edges. The
fix (draw one per-team season-strength offset per simulation and apply it to
all of that team's games) is a small change, deliberately deferred to v1.1 so
the honest version ships first. The limitation is also recorded in the
``n_sims`` column comment for downstream consumers.

Usage:
    python scripts/simulate_season.py
        Simulate every projection season (the current season plus any later
        season with a published schedule).

    python scripts/simulate_season.py --season 2026
    python scripts/simulate_season.py --season 2026 --model fitted_v1 --sims 10000

Each season prints:
    SIM_GATE season={s} model={m} teams={t} sims={n} sigma={g} complete={c}
"""

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_MODEL = "fitted_v1"
DEFAULT_SIMS = 10_000

# Fixed so a re-run reproduces the same projection given the same inputs.
# Without this, the daily snapshot would jitter by simulation noise alone and
# the append-only history would show movement that is not a change of opinion.
DEFAULT_SEED = 20260726

# A schedule is "complete" enough for a season win total to mean what a reader
# assumes at this many regular-season games. Below it the projection is still
# written -- over the games that exist -- but flagged.
COMPLETE_SCHEDULE_GAMES = 11

# Bowl eligibility, and the "double-digit season" threshold the outlook surface
# reports.
BOWL_ELIGIBLE_WINS = 6
TEN_PLUS_WINS = 10

# Refuse to simulate on a residual SD estimated from fewer than this many
# completed games -- sigma drives every probability the script writes, so a
# noisy estimate is worse than an explicit failure.
MIN_SIGMA_GAMES = 100

# Projections describe the REGULAR season. CFBD marks conference championship
# games as season_type='regular' (they are the final regular week), so this
# filter keeps CCGs while excluding bowls and playoff games -- which would
# otherwise inflate games_scheduled, make p_bowl_eligible circular, and cause
# projections to jump as the postseason bracket is published.
PROJECTION_SEASON_TYPE = "regular"


# =============================================================================
# Pure functions -- no I/O, no DB, unit-tested directly
# (tests/test_simulate_season.py).
# =============================================================================


def simulate_wins(games, n_sims, sigma, seed=DEFAULT_SEED):
    """Simulate `n_sims` seasons over `games`.

    Returns a dict of per-team results:

    ``wins``            {team: ndarray[n_sims]} -- wins over all counted games
    ``conf_wins``       {team: ndarray[n_sims]} -- wins in conference games only
    ``games_simulated`` {team: int}             -- games that actually counted
    ``conf_games``      {team: int}             -- conference games that counted

    ``games`` is a list of dicts with ``home_team``, ``away_team``,
    ``completed``, ``home_win`` (read only when completed),
    ``expected_home_margin`` (read only when pending) and ``conference_game``.

    Completed games add a deterministic win to the actual winner in every
    simulation -- results already in the book are not re-rolled. Remaining games
    draw a margin per simulation.

    Vectorized: one ``(n_pending, n_sims)`` normal draw, then per-team
    accumulation. At ~1,600 games x 10,000 sims this is ~16M doubles (~128MB)
    and runs in about a second, where a per-simulation Python loop would take
    minutes.

    **Conference wins are accumulated from the SAME draws**, not a second
    simulation. Re-simulating would let a game be a win in the overall tally and
    a loss in the conference tally within one "season", making title odds
    inconsistent with the win totals shown beside them.

    A game whose ``expected_home_margin`` is None is SKIPPED entirely -- it
    counts toward neither wins nor ``games_simulated``. Inventing a 50/50 game
    would bias projections toward .500, and counting it in the denominator
    without letting it produce a win would make it a guaranteed loss. Callers
    must use ``games_simulated``, not the raw schedule length, as the
    denominator.
    """
    import numpy as np

    teams = sorted({g["home_team"] for g in games} | {g["away_team"] for g in games})
    index = {t: i for i, t in enumerate(teams)}
    n_teams = len(teams)
    wins = np.zeros((n_teams, n_sims), dtype=np.int32)
    conf_wins = np.zeros((n_teams, n_sims), dtype=np.int32)
    games_simulated = dict.fromkeys(teams, 0)
    conf_games = dict.fromkeys(teams, 0)

    def _count(g):
        games_simulated[g["home_team"]] += 1
        games_simulated[g["away_team"]] += 1
        if g.get("conference_game"):
            conf_games[g["home_team"]] += 1
            conf_games[g["away_team"]] += 1

    pending = []
    for g in games:
        if g["completed"]:
            winner = g["home_team"] if g["home_win"] else g["away_team"]
            wins[index[winner], :] += 1
            if g.get("conference_game"):
                conf_wins[index[winner], :] += 1
            _count(g)
        elif g.get("expected_home_margin") is not None:
            pending.append(g)
            _count(g)

    if pending:
        rng = np.random.default_rng(seed)
        mu = np.array([g["expected_home_margin"] for g in pending], dtype=np.float64)
        draws = rng.normal(loc=mu[:, None], scale=sigma, size=(len(pending), n_sims))
        home_won = draws > 0.0
        for row, g in enumerate(pending):
            h, a = index[g["home_team"]], index[g["away_team"]]
            wins[h, :] += home_won[row]
            wins[a, :] += ~home_won[row]
            if g.get("conference_game"):
                conf_wins[h, :] += home_won[row]
                conf_wins[a, :] += ~home_won[row]

    return {
        "wins": {t: wins[index[t], :] for t in teams},
        "conf_wins": {t: conf_wins[index[t], :] for t in teams},
        "games_simulated": games_simulated,
        "conf_games": conf_games,
    }


def win_distribution(team_wins, games_simulated):
    """``{win_count: probability}`` over 0..games_simulated, summing to 1.

    The range is the number of games actually SIMULATED, not the number
    scheduled. Spanning the full schedule when some games were skipped would
    reserve probability mass for win totals the simulation can never produce.

    Keys are ints covering the full range (zeros included) so consumers can
    index without checking membership; the DDL stores it as JSONB with string
    keys, which the writer converts.
    """
    import numpy as np

    n_sims = len(team_wins)
    counts = np.bincount(team_wins, minlength=games_simulated + 1)
    return {w: float(counts[w]) / n_sims for w in range(games_simulated + 1)}


def summarize(team_wins, games_simulated):
    """Central tendency, percentiles and threshold probabilities for one team.

    ``games_simulated`` -- NOT the scheduled count -- is the denominator for
    ``projected_losses``. Using the schedule length would turn every game the
    simulation skipped (no prediction available) into a guaranteed loss, which
    is a stronger and more wrong claim than the coin flip skipping was meant to
    avoid.

    Percentiles use the ``lower`` interpolation method: win totals are integers,
    and reporting a p10 of 7.4 wins would imply a precision the quantity does
    not have.
    """
    import numpy as np

    return {
        "projected_wins": float(np.mean(team_wins)),
        "projected_losses": float(games_simulated - np.mean(team_wins)),
        "median_wins": float(np.percentile(team_wins, 50, method="lower")),
        "wins_p10": float(np.percentile(team_wins, 10, method="lower")),
        "wins_p25": float(np.percentile(team_wins, 25, method="lower")),
        "wins_p75": float(np.percentile(team_wins, 75, method="lower")),
        "wins_p90": float(np.percentile(team_wins, 90, method="lower")),
        "p_bowl_eligible": float(np.mean(team_wins >= BOWL_ELIGIBLE_WINS)),
        "p_ten_plus": float(np.mean(team_wins >= TEN_PLUS_WINS)),
    }


def schedule_strength(team, games, ratings):
    """Average opponent rating over `team`'s scheduled games.

    Opponents missing from `ratings` are skipped rather than scored 0 -- an
    FCS opponent with no rating would otherwise drag a schedule's average down
    as if it were a merely-bad FBS team. Returns None when no opponent has a
    rating, which the writer stores as NULL.
    """
    opp_ratings = []
    for g in games:
        if g["home_team"] == team:
            opp = g["away_team"]
        elif g["away_team"] == team:
            opp = g["home_team"]
        else:
            continue
        if opp in ratings and ratings[opp] is not None:
            opp_ratings.append(ratings[opp])
    if not opp_ratings:
        return None
    return sum(opp_ratings) / len(opp_ratings)


def conference_title_probs(conf_wins_by_team, conf_by_team, conf_games_played):
    """P(best CONFERENCE record) per team, ties split evenly.

    Takes **conference-only** wins and game counts. Using overall record here
    would let a team buy title odds with non-conference wins -- beating three
    weak out-of-conference opponents would outrank a better league record,
    which is not how any conference decides a champion.

    Win *percentage* rather than raw wins, because conference members can play
    different numbers of league games (uneven schedules are routine in
    16-team leagues).

    **v1 crude, deliberately.** Real championships turn on head-to-head and
    divisional tiebreakers and a title game; none of that is modeled. A team's
    odds here are the share of simulations in which it holds (or shares) the
    best conference win percentage.
    """
    import numpy as np

    by_conf = {}
    for team, conf in conf_by_team.items():
        if conf and team in conf_wins_by_team and conf_games_played.get(team, 0) > 0:
            by_conf.setdefault(conf, []).append(team)

    probs = {}
    for _conf, members in by_conf.items():
        if len(members) < 2:
            continue
        pct = np.vstack([conf_wins_by_team[t] / conf_games_played[t] for t in members])
        best = pct.max(axis=0)
        is_best = pct >= best  # ties included
        share = is_best / is_best.sum(axis=0)  # split evenly among tied teams
        for i, t in enumerate(members):
            probs[t] = float(share[i].mean())
    return probs


def build_projection_row(
    team,
    team_wins,
    games,
    ratings,
    conf,
    model_version,
    n_sims,
    sigma,
    conf_title_prob,
    games_simulated,
):
    """One predictions.season_projections row dict for `team`.

    ``games_scheduled`` is reported for transparency, but every projected
    quantity is computed over ``games_simulated`` -- the games that actually
    contributed an outcome. When a pending game has no prediction the two
    differ, and folding that gap into ``projected_losses`` would present a
    missing game as a certain defeat.
    """
    team_games = [g for g in games if team in (g["home_team"], g["away_team"])]
    completed = [g for g in team_games if g["completed"]]
    actual_wins = sum(
        1
        for g in completed
        if (g["home_team"] == team and g["home_win"])
        or (g["away_team"] == team and not g["home_win"])
    )
    games_scheduled = len(team_games)

    row = {
        "model_version": model_version,
        "season": games[0]["season"] if games else None,
        "team": team,
        "conference": conf,
        "games_scheduled": games_scheduled,
        "games_simulated": games_simulated,
        "games_completed": len(completed),
        "actual_wins": actual_wins,
        "schedule_complete": games_scheduled >= COMPLETE_SCHEDULE_GAMES,
        "p_win_dist": {str(k): v for k, v in win_distribution(team_wins, games_simulated).items()},
        "sos_rating": schedule_strength(team, team_games, ratings),
        "sos_rank": None,  # filled after all teams are summarized
        "conf_title_prob": conf_title_prob,
        "playoff_prob": None,  # see migration 043 -- not modeled in v1
        "n_sims": n_sims,
        "residual_sigma": sigma,
    }
    row.update(summarize(team_wins, games_simulated))
    return row


def assign_sos_ranks(rows):
    """Rank rows by descending sos_rating (1 = toughest). Rows with a NULL
    rating keep sos_rank None. Mutates and returns `rows`."""
    rated = [r for r in rows if r["sos_rating"] is not None]
    for rank, row in enumerate(sorted(rated, key=lambda r: -r["sos_rating"]), start=1):
        row["sos_rank"] = rank
    return rows


# =============================================================================
# --- I/O layer ---
# =============================================================================


def get_db_url() -> str:
    """Database URL from dlt secrets or environment (same pattern the other
    compute scripts use -- each keeps its own copy of this one utility)."""
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
    return url


# Latest snapshot per game for the chosen model. predictions.game_predictions
# is append-only across days, so DISTINCT ON picks the most recent read rather
# than an arbitrary historical one -- the same idiom
# compute_predictions.MARKET_SNAPSHOTS_QUERY uses.
SEASON_GAMES_QUERY = """
    SELECT g.id AS game_id, g.season, g.home_team, g.away_team,
           COALESCE(g.completed, false) AS completed,
           g.home_points, g.away_points,
           g.home_conference, g.away_conference,
           COALESCE(g.conference_game, false) AS conference_game,
           p.expected_home_margin
    FROM core.games g
    LEFT JOIN LATERAL (
        SELECT gp.expected_home_margin
        FROM predictions.game_predictions gp
        WHERE gp.game_id = g.id AND gp.model_version = %(model)s
        ORDER BY gp.prediction_date DESC, gp.computed_at DESC
        LIMIT 1
    ) p ON true
    WHERE g.season = %(season)s
      AND g.season_type = %(season_type)s
    ORDER BY g.id
"""

# Team rating for the schedule-strength calculation: the most recent pregame
# Elo the model recorded for that team this season.
TEAM_RATINGS_QUERY = """
    SELECT team, rating FROM analytics.house_elo_current
"""

# ONE snapshot per game before the aggregate. predictions.game_predictions is
# append-only daily, so a naive join contributes a game once per day it was
# predicted: games that sat on the board for weeks would dominate the estimate,
# and their early, worse predictions would mix with the final pregame one. That
# is not a per-game residual SD, and since sigma drives every simulated
# probability, the error would propagate into every number this script writes.
# DISTINCT ON takes the last snapshot at or before kickoff -- the pregame read
# the simulation is actually modelling.
RESIDUAL_SIGMA_QUERY = """
    WITH latest AS (
        SELECT DISTINCT ON (p.game_id)
               p.game_id,
               p.expected_home_margin,
               g.home_points - g.away_points AS actual_margin
        FROM predictions.game_predictions p
        JOIN core.games g ON g.id = p.game_id
        WHERE p.model_version = %(model)s
          AND COALESCE(g.completed, false)
          AND g.home_points IS NOT NULL AND g.away_points IS NOT NULL
          AND p.expected_home_margin IS NOT NULL
          AND (g.start_date IS NULL OR p.prediction_date <= g.start_date::date)
        ORDER BY p.game_id, p.prediction_date DESC, p.computed_at DESC
    )
    SELECT stddev_pop(actual_margin::double precision - expected_home_margin),
           COUNT(*)
    FROM latest
"""


def fetch_sigma(conn, model: str) -> float:
    """Measured residual SD for `model`, one pregame snapshot per game.

    Hard error rather than a default: a wrong sigma produces confident,
    well-formatted, wrong win totals, and a silent fallback is exactly how that
    ships unnoticed. A tiny sample is refused for the same reason -- an SD
    estimated from a handful of games is not a usable uncertainty parameter.
    """
    with conn.cursor() as cur:
        cur.execute(RESIDUAL_SIGMA_QUERY, {"model": model})
        sigma, n_games = cur.fetchone()
    if sigma is None or n_games < MIN_SIGMA_GAMES:
        raise RuntimeError(
            f"Only {n_games or 0} completed game(s) with a pregame {model} prediction "
            f"(need >= {MIN_SIGMA_GAMES}); cannot measure a trustworthy residual sigma. "
            "Backfill predictions before simulating."
        )
    logger.info("Measured residual sigma for %s: %.2f over %d game(s)", model, sigma, n_games)
    return float(sigma)


def fetch_season_games(conn, season: int, model: str) -> list[dict]:
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            SEASON_GAMES_QUERY,
            {"season": season, "model": model, "season_type": PROJECTION_SEASON_TYPE},
        )
        rows = [dict(r) for r in cur.fetchall()]

    games = []
    for r in rows:
        home_win = None
        if r["completed"] and r["home_points"] is not None and r["away_points"] is not None:
            home_win = r["home_points"] > r["away_points"]
        games.append(
            {
                "game_id": r["game_id"],
                "season": r["season"],
                "home_team": r["home_team"],
                "away_team": r["away_team"],
                # A game flagged completed but missing a score cannot contribute
                # a result; treat it as pending so it is simulated rather than
                # awarded to nobody.
                "completed": home_win is not None,
                "home_win": home_win,
                "home_conference": r["home_conference"],
                "away_conference": r["away_conference"],
                "conference_game": bool(r["conference_game"]),
                "expected_home_margin": (
                    float(r["expected_home_margin"])
                    if r["expected_home_margin"] is not None
                    else None
                ),
            }
        )
    return games


def fetch_team_ratings(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(TEAM_RATINGS_QUERY)
        return {team: float(rating) for team, rating in cur.fetchall() if rating is not None}


_ROW_COLUMNS = [
    "model_version",
    "season",
    "team",
    "conference",
    "games_scheduled",
    "games_simulated",
    "games_completed",
    "actual_wins",
    "schedule_complete",
    "projected_wins",
    "projected_losses",
    "median_wins",
    "wins_p10",
    "wins_p25",
    "wins_p75",
    "wins_p90",
    "p_win_dist",
    "p_bowl_eligible",
    "p_ten_plus",
    "sos_rating",
    "sos_rank",
    "conf_title_prob",
    "playoff_prob",
    "n_sims",
    "residual_sigma",
]

# The conflict-key columns are the ones NOT refreshed on a same-day re-run.
_CONFLICT_COLUMNS = ("season", "team", "model_version")
_UPDATE_ASSIGNMENTS = ", ".join(
    f"{c} = EXCLUDED.{c}" for c in _ROW_COLUMNS if c not in _CONFLICT_COLUMNS
)

# Same-day convergence only -- a re-run updates today's snapshot, never a
# prior day's (migration 043, mirroring migration 024's semantics).
_UPSERT_SQL = f"""
    INSERT INTO predictions.season_projections ({", ".join(_ROW_COLUMNS)})
    VALUES %s
    ON CONFLICT (season, team, model_version, projection_date) DO UPDATE SET
        {_UPDATE_ASSIGNMENTS},
        computed_at = now()
"""


def write_projections(conn, rows: list[dict]) -> None:
    import json

    from psycopg2.extras import execute_values

    if not rows:
        return
    values = [
        tuple(json.dumps(r[c]) if c == "p_win_dist" else r[c] for c in _ROW_COLUMNS) for r in rows
    ]
    with conn.cursor() as cur:
        execute_values(cur, _UPSERT_SQL, values)
    conn.commit()


def simulate_one_season(conn, season: int, model: str, n_sims: int, seed: int) -> int:
    """Simulate and write one season. Returns the number of team rows written."""
    games = fetch_season_games(conn, season, model)
    if not games:
        logger.info("season=%d: no games in core.games, skipping", season)
        return 0

    scored = sum(1 for g in games if not g["completed"] and g["expected_home_margin"] is not None)
    pending = sum(1 for g in games if not g["completed"])
    if pending and not scored:
        logger.warning(
            "season=%d: %d pending game(s) but NONE have %s predictions -- "
            "projections will reflect completed games only",
            season,
            pending,
            model,
        )

    sigma = fetch_sigma(conn, model)
    ratings = fetch_team_ratings(conn)
    sim = simulate_wins(games, n_sims, sigma, seed=seed)
    wins_by_team = sim["wins"]

    conf_by_team = {}
    for g in games:
        for side, conf in (("home_team", "home_conference"), ("away_team", "away_conference")):
            conf_by_team.setdefault(g[side], g[conf])

    # Conference-only records, from the same draws as the overall tally.
    title_probs = conference_title_probs(sim["conf_wins"], conf_by_team, sim["conf_games"])

    rows = [
        build_projection_row(
            team,
            team_wins,
            games,
            ratings,
            conf_by_team.get(team),
            model,
            n_sims,
            sigma,
            title_probs.get(team),
            sim["games_simulated"][team],
        )
        for team, team_wins in wins_by_team.items()
    ]
    assign_sos_ranks(rows)
    write_projections(conn, rows)

    complete = sum(1 for r in rows if r["schedule_complete"])
    unscored = sum(r["games_scheduled"] - r["games_simulated"] for r in rows)
    if unscored:
        logger.warning(
            "season=%d: %d team-game(s) had no %s prediction and were excluded from "
            "projections (games_simulated < games_scheduled on those rows)",
            season,
            unscored,
            model,
        )
    print(
        f"SIM_GATE season={season} model={model} teams={len(rows)} sims={n_sims} "
        f"sigma={sigma:.2f} complete={complete} unscored_team_games={unscored}"
    )
    logger.info(
        "season=%d: wrote %d team projection(s), sigma=%.2f, %d with a complete schedule",
        season,
        len(rows),
        sigma,
        complete,
    )
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate season outcomes into predictions.season_projections"
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Season to simulate (default: every projection season)",
    )
    parser.add_argument(
        "--model", default=DEFAULT_MODEL, help=f"Model version (default {DEFAULT_MODEL})"
    )
    parser.add_argument(
        "--sims",
        type=int,
        default=DEFAULT_SIMS,
        help=f"Simulations per season (default {DEFAULT_SIMS})",
    )
    parser.add_argument(
        "--seed", type=int, default=DEFAULT_SEED, help="RNG seed; fixed so re-runs reproduce"
    )
    args = parser.parse_args()

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        if args.season is not None:
            seasons = [args.season]
        else:
            from src.pipelines.config.years import get_projection_seasons

            seasons = get_projection_seasons(conn)
            logger.info("Projection seasons: %s", seasons)

        total = 0
        for season in seasons:
            total += simulate_one_season(conn, season, args.model, args.sims, args.seed)
        logger.info("Wrote %d projection row(s) across %d season(s)", total, len(seasons))
    except Exception:
        conn.rollback()
        logger.exception("season simulation failed")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
