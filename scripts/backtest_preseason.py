"""Walk-forward PRESEASON backtest of fitted_v1 season projections.

Preseason outlook plan (docs/plans/2026-07-25-preseason-outlook-model-plan.md),
section 4.3/4.5 -- the gate that must run before a 2026 win total means
anything.

WHY THIS IS A SEPARATE SCORING PASS, NOT A QUERY
================================================
``score_fitted.py --backfill`` stamps each historical prediction with the
game's own ``start_date``, so ``predictions.game_predictions`` holds
**as-of-game-day** rows: a week-12 game was scored with eleven games of that
season's form already in the feature vector. Simulating a season from those
rows and calling the result a preseason backtest would measure in-season
accuracy, report a flatteringly low error, and be wrong in the one direction
nobody would notice.

A genuine preseason backtest re-scores every game of season S from each team's
**week-1** ``features.team_week`` vector -- the as-of-season-start state, all
that is knowable in August -- using the frozen fit trained through S-1. That is
what this script does.

The scoring itself reuses ``score_fitted.score_game`` unchanged, so the design
matrix, imputation, standardization and Platt calibration are byte-identical to
production. The ONLY difference is which ``team_week`` row is joined.
``build_feature_vector`` reads just ``neutral_site`` off the game plus the two
team-week dicts, so there is no hidden week dependence for that swap to break.

LEAK CONTROL
============
Three places a backtest like this leaks, all handled explicitly:

1. **The fit.** Season S is scored with ``train_through = S-1``
   (``select_train_through('backfill', S)``), the same walk-forward rule
   production uses. S is never in its own fit's train window.

2. **Sigma.** The simulation's residual SD is measured from PRESEASON
   residuals of seasons **strictly before S**, accumulated as this script
   walks forward. Using season S's own residuals -- or the production
   ``fetch_sigma``, which reads as-of-game-day snapshots -- would hand the
   simulation the answer's spread. A season with no prior residuals is scored
   but cannot be simulated, and is reported as excluded rather than dropped
   silently.

3. **Actual results.** Every game is simulated as PENDING even though it is
   complete. ``simulate_wins`` gives completed games a deterministic win, so
   passing them through as completed would return the actual record and score
   a perfect backtest.

WHAT IS COMPARED
================
Projected wins are compared to actual wins **over exactly the games that were
simulated**, never the full schedule. A game is droppable here (an opponent
with no week-1 vector fails the join), and comparing a short simulated slate
against a full actual record would manufacture error that is really missing
data. ``games_simulated`` is reported per season so the coverage is visible.

Metrics are reported for FBS teams by default -- that is the audience -- but a
team's slate keeps its FCS opponents, because dropping those games would
shorten real schedules.

BASELINES
=========
An MAE means nothing alone. Two denominator-consistent baselines run beside the
model: prior-season win RATE scaled to the simulated slate, and a flat .500. A
preseason model that cannot beat "last year's record" has earned nothing.

Read-only: reports to stdout, writes no rows.

Usage:
    python scripts/backtest_preseason.py                    # default range
    python scripts/backtest_preseason.py --start 2018 --end 2025
    python scripts/backtest_preseason.py --all-divisions
"""

import argparse
import logging
import sys

import numpy as np

from scripts.compute_predictions import get_db_url
from scripts.score_fitted import (
    fetch_available_train_through,
    load_fit,
    score_game,
    select_train_through,
)
from scripts.simulate_season import (
    BOWL_ELIGIBLE_WINS,
    DEFAULT_SEED,
    DEFAULT_SIMS,
    DEFAULT_STRENGTH_SHARE,
    TEN_PLUS_WINS,
    simulate_wins,
    summarize,
)
from scripts.train_model import MODEL_VERSION, TEAM_WEEK_SOURCE_COLUMNS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_START = 2018
DEFAULT_END = 2025

# Regular season only, matching simulate_season.PROJECTION_SEASON_TYPE. Note
# CFBD labels FCS/D2 playoff bracket games 'regular', which is why the default
# reporting scope is FBS.
BACKTEST_SEASON_TYPE = "regular"

# Minimum preseason residuals before a sigma estimate is trusted. Mirrors
# simulate_season.MIN_SIGMA_GAMES.
MIN_SIGMA_GAMES = 100

# Chronological per-team slate cap. A preseason FBS schedule is twelve games;
# anything beyond is a conference championship or a playoff round, whose
# participants were decided by that season's results. See
# drop_outcome_dependent.
PRESEASON_SLATE_GAMES = 12

# Calibration buckets for p_bowl_eligible / p_ten_plus reliability.
CALIBRATION_EDGES = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

# A preseason win-total model landing inside this is respectable (plan 4.5).
# Advisory only -- this script reports what it measures and does not gate.
RESPECTABLE_WIN_MAE = 1.5


# =============================================================================
# --- Pure metrics --- (no DB, unit-tested directly)
# =============================================================================


def win_error_metrics(projected: list[float], actual: list[float]) -> dict:
    """MAE, RMSE and signed bias of projected vs actual wins.

    Bias is ``mean(projected - actual)``: a preseason model can be accurate on
    average and still systematically optimistic, and only the signed term shows
    that.
    """
    if not projected:
        return {"n": 0, "mae": None, "rmse": None, "bias": None}
    p = np.asarray(projected, dtype=np.float64)
    a = np.asarray(actual, dtype=np.float64)
    err = p - a
    return {
        "n": len(projected),
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(np.sqrt(np.mean(err**2))),
        "bias": float(np.mean(err)),
    }


def interval_coverage(actual: list[float], p10: list[float], p90: list[float]) -> float | None:
    """Fraction of teams whose actual wins fell inside [p10, p90].

    Nominal is 0.80. Materially below means the distribution is too narrow --
    the projection claims more confidence than it has. Materially above means
    it is too wide to be useful. v1 draws games independently, which understates
    both tails, so a reading under 0.80 is the expected direction of miss.
    """
    if not actual:
        return None
    inside = sum(1 for a, lo, hi in zip(actual, p10, p90, strict=True) if lo <= a <= hi)
    return inside / len(actual)


def calibration_table(probs: list[float], outcomes: list[bool]) -> list[dict]:
    """Reliability buckets: predicted probability vs observed frequency.

    A projection saying "70% bowl eligible" is only meaningful if teams given
    70% actually reach six wins about 70% of the time. Empty buckets are
    omitted rather than reported as 0/0.
    """
    if not probs:
        return []
    rows = []
    # Slice both sides so the lengths match and strict= stays meaningful.
    for lo, hi in zip(CALIBRATION_EDGES[:-1], CALIBRATION_EDGES[1:], strict=True):
        # Final bucket is closed on the right so p == 1.0 is counted.
        idx = [
            i
            for i, p in enumerate(probs)
            if (lo <= p < hi) or (hi == CALIBRATION_EDGES[-1] and p == 1.0)
        ]
        if not idx:
            continue
        rows.append(
            {
                "bucket": f"{lo:.1f}-{hi:.1f}",
                "n": len(idx),
                "mean_predicted": float(np.mean([probs[i] for i in idx])),
                "observed": float(np.mean([1.0 if outcomes[i] else 0.0 for i in idx])),
            }
        )
    return rows


def residual_quantiles(projected: list[float], actual: list[float]) -> dict | None:
    """Empirical quantiles of (actual - projected) wins.

    Codex review on PR #51, P1. MAE is an average loss across team-seasons, not
    an error bar: for a roughly normal error with MAE 1.79 the SD is about
    1.79 * 1.253 ~ 2.24, so "+/- MAE" spans only about +/-0.8 SD -- near 58%
    coverage, not the ~80-90% a reader assumes from a stated range. Quoting a
    point estimate "+/- MAE" as an honest range is simply wrong.

    These are the empirical quantiles of the realized error instead, so a
    stated interval means what it says. Interpreting them for one team assumes
    that team's error is exchangeable with the backtest population -- a real
    assumption, but a far weaker one than treating MAE as a half-width.
    """
    if not projected:
        return None
    err = np.asarray(actual, dtype=np.float64) - np.asarray(projected, dtype=np.float64)
    return {
        "n": len(err),
        "p05": float(np.percentile(err, 5)),
        "p10": float(np.percentile(err, 10)),
        "p25": float(np.percentile(err, 25)),
        "p50": float(np.percentile(err, 50)),
        "p75": float(np.percentile(err, 75)),
        "p90": float(np.percentile(err, 90)),
        "p95": float(np.percentile(err, 95)),
    }


def baseline_prior_rate(prior_wins: float | None, prior_games: int | None, games: int) -> float:
    """Prior-season win RATE scaled to this season's simulated slate.

    Rate rather than raw count so the baseline shares the model's denominator;
    comparing a 12-game record against a 9-game simulated slate would make the
    baseline look bad for a reason that has nothing to do with prediction. A
    team with no prior season falls back to .500.
    """
    if not prior_games or prior_wins is None:
        return 0.5 * games
    return (prior_wins / prior_games) * games


def brier(probs: list[float], outcomes: list[bool]) -> float | None:
    """Mean squared error of a probability forecast. Lower is better."""
    if not probs:
        return None
    p = np.asarray(probs, dtype=np.float64)
    o = np.asarray([1.0 if x else 0.0 for x in outcomes], dtype=np.float64)
    return float(np.mean((p - o) ** 2))


# =============================================================================
# --- I/O layer ---
# =============================================================================


def _week1_games_query() -> str:
    """Season-S games joined to each side's WEEK-1 team_week vector.

    ``DISTINCT ON (team) ORDER BY week_index, game_id`` inside the CTE takes
    each team's earliest row of the season -- the as-of-season-start state.
    The column list is built from TEAM_WEEK_SOURCE_COLUMNS so it tracks the
    feature contract rather than drifting from it.
    """
    tw_cols = ", ".join(TEAM_WEEK_SOURCE_COLUMNS)
    home_cols = ",\n           ".join(f"h.{c} AS home_{c}" for c in TEAM_WEEK_SOURCE_COLUMNS)
    away_cols = ",\n           ".join(f"a.{c} AS away_{c}" for c in TEAM_WEEK_SOURCE_COLUMNS)
    return f"""
        WITH week1 AS (
            SELECT DISTINCT ON (team)
                   team, week_index, games_played_to_date, {tw_cols}
            FROM features.team_week
            WHERE season = %(season)s
            ORDER BY team, week_index, game_id
        )
        SELECT g.id AS game_id, g.season, g.season_type, g.week, g.start_date,
               g.neutral_site, g.home_team, g.away_team,
               g.home_points, g.away_points,
               COALESCE(g.conference_game, false) AS conference_game,
               g.home_classification, g.away_classification,
               h.games_played_to_date AS home_gp, a.games_played_to_date AS away_gp,
           {home_cols},
           {away_cols}
        FROM core.games g
        JOIN week1 h ON h.team = g.home_team
        JOIN week1 a ON a.team = g.away_team
        WHERE g.season = %(season)s
          AND g.season_type = %(season_type)s
          AND COALESCE(g.completed, false)
          AND g.home_points IS NOT NULL
          AND g.away_points IS NOT NULL
        ORDER BY g.id
    """


def fetch_preseason_games(conn, season: int) -> list[dict]:
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            _week1_games_query(),
            {"season": season, "season_type": BACKTEST_SEASON_TYPE},
        )
        raw = [dict(r) for r in cur.fetchall()]
    games = []
    for r in raw:
        games.append(
            {
                "game_id": r["game_id"],
                "season": r["season"],
                "week": r["week"],
                "start_date": r["start_date"],
                "neutral_site": r["neutral_site"],
                "home_team": r["home_team"],
                "away_team": r["away_team"],
                "home_points": r["home_points"],
                "away_points": r["away_points"],
                "conference_game": r["conference_game"],
                "home_classification": r["home_classification"],
                "away_classification": r["away_classification"],
                "home_gp": r["home_gp"],
                "away_gp": r["away_gp"],
                "home_tw": {c: r[f"home_{c}"] for c in TEAM_WEEK_SOURCE_COLUMNS},
                "away_tw": {c: r[f"away_{c}"] for c in TEAM_WEEK_SOURCE_COLUMNS},
            }
        )
    return games


def fetch_scheduled_counts(conn, season: int) -> dict:
    """Regular-season games on each team's schedule, for coverage reporting."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT team, COUNT(*) FROM (
                SELECT home_team AS team FROM core.games
                WHERE season = %(season)s AND season_type = %(season_type)s
                UNION ALL
                SELECT away_team FROM core.games
                WHERE season = %(season)s AND season_type = %(season_type)s
            ) t GROUP BY team
            """,
            {"season": season, "season_type": BACKTEST_SEASON_TYPE},
        )
        return {row[0]: int(row[1]) for row in cur.fetchall()}


def fetch_fbs_teams(conn, season: int) -> set:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT team FROM (
                SELECT home_team AS team, home_classification AS c FROM core.games
                WHERE season = %(season)s
                UNION ALL
                SELECT away_team, away_classification FROM core.games
                WHERE season = %(season)s
            ) t WHERE c = 'fbs'
            """,
            {"season": season},
        )
        return {row[0] for row in cur.fetchall()}


# =============================================================================
# --- Backtest ---
# =============================================================================


def drop_outcome_dependent(games: list[dict]) -> tuple[list[dict], int]:
    """Drop games whose PARTICIPANTS were decided by that season's results.

    Codex review on PR #51, P1. ``season_type = 'regular'`` is not enough.
    CFBD files conference championship games as 'regular', and below FBS it
    files the entire FCS/D2 **playoff bracket** as 'regular' too. Nobody could
    know in August that Georgia would play Texas for the SEC, so handing a team
    that thirteenth game hands the projection an outcome. It inflates the
    simulated slate and the actual record together, for exactly the strongest
    teams, and -- because sigma pools every scored game regardless of the FBS
    reporting filter -- it contaminated the residual pool even in the default
    scope.

    The rule: order each team's games chronologically and keep only its first
    PRESEASON_SLATE_GAMES. A preseason FBS schedule is twelve games; a
    thirteenth is earned, never scheduled. A game is dropped if it is beyond
    the cap for EITHER side, so both participants keep identical slates and
    the actual-wins comparison stays matched.

    Deliberate, conservative imprecision: a team with the Hawaii 13th-game
    exemption loses a genuinely scheduled game, and its opponent loses that
    game too. Measured at 0.3-1.2% of team-games per season (2019-2025), and
    it errs toward dropping real data rather than admitting a leak -- the
    right direction for a leak check. The count is reported per season so the
    cost stays visible.
    """
    order: dict = {}
    for g in sorted(
        games, key=lambda x: (x.get("start_date") is None, x.get("start_date"), x["game_id"])
    ):
        for team in (g["home_team"], g["away_team"]):
            order[team] = order.get(team, 0) + 1
            g[f"_n_{team}"] = order[team]

    kept, dropped = [], 0
    for g in games:
        hn = g.get(f"_n_{g['home_team']}", 1)
        an = g.get(f"_n_{g['away_team']}", 1)
        if hn > PRESEASON_SLATE_GAMES or an > PRESEASON_SLATE_GAMES:
            dropped += 1
            continue
        kept.append(g)
    return kept, dropped


def score_season_preseason(conn, season: int, fit: dict) -> tuple[list[dict], int]:
    """Every completed regular-season game of `season`, scored from week-1
    vectors, with outcome-dependent games removed. Returns the game dicts
    carrying ``expected_home_margin`` and the realized ``actual_margin``,
    plus the number of games dropped by the slate cap."""
    games, dropped = drop_outcome_dependent(fetch_preseason_games(conn, season))
    for g in games:
        expected_margin, win_prob = score_game(g, fit)
        g["expected_home_margin"] = expected_margin
        g["home_win_prob"] = win_prob
        g["actual_margin"] = float(g["home_points"] - g["away_points"])
        # Simulated as PENDING: simulate_wins would otherwise hand the actual
        # winner a deterministic win and score a perfect backtest.
        g["completed"] = False
    return games, dropped


def actual_wins_over(games: list[dict]) -> dict:
    """Actual wins per team over exactly `games` -- the simulated subset."""
    wins: dict = {}
    played: dict = {}
    for g in games:
        h, a = g["home_team"], g["away_team"]
        played[h] = played.get(h, 0) + 1
        played[a] = played.get(a, 0) + 1
        winner = h if g["actual_margin"] > 0 else a if g["actual_margin"] < 0 else None
        for t in (h, a):
            wins.setdefault(t, 0)
        if winner is not None:
            wins[winner] += 1
    return {"wins": wins, "played": played}


def preseason_sigma(residuals: list[float]) -> float | None:
    """Residual SD from PRESEASON residuals of earlier seasons only."""
    if len(residuals) < MIN_SIGMA_GAMES:
        return None
    sd = float(np.std(np.asarray(residuals, dtype=np.float64)))
    return sd if sd > 0 else None


def run_backtest(
    conn, start: int, end: int, n_sims: int, seed: int, fbs_only: bool, shares: list[float]
) -> int:
    available = fetch_available_train_through(conn)
    logger.info("frozen fits available for train_through: %s", sorted(available))

    # Codex review on PR #51, P2: iteration used to begin at `start`, so the
    # first requested season always had an empty residual pool and was silently
    # consumed as a sigma seed rather than reported. `--start 2025 --end 2025`
    # scored 2025, simulated nothing and exited 1 even with years of usable
    # history sitting there. Walk from the earliest season that has a frozen
    # S-1 fit so sigma is seeded BEFORE `start`, and restrict only the OUTPUT
    # to the requested range.
    earliest = min(available) + 1 if available else start
    walk_from = min(start, earliest)
    walk = [s for s in range(walk_from, end + 1) if (s - 1) in available]
    seasons = [s for s in range(start, end + 1)]
    skipped_no_fit = [s for s in seasons if (s - 1) not in available]
    if skipped_no_fit:
        logger.warning(
            "no frozen S-1 fit for season(s) %s -- excluded from the backtest", skipped_no_fit
        )
    seed_only = [s for s in walk if s < start]
    if seed_only:
        logger.info("scoring season(s) %s for sigma seeding only (not reported)", seed_only)
    usable = walk
    if not usable:
        logger.error("no season in %d-%d has a frozen S-1 fit; nothing to backtest", start, end)
        return 1

    # PASS 1 -- score. Walk forward so residuals accumulate from earlier
    # seasons ONLY; the sigma used for season S never sees season S. Nothing
    # here depends on strength_share (scoring residuals are a property of the
    # fit, not of the simulation), so a sweep re-simulates from this one pass
    # instead of re-scoring per candidate.
    prior_residuals: list[float] = []
    prior_actuals: dict = {}
    scored: list[dict] = []

    for season in usable:
        train_through = select_train_through("backfill", season)
        fit = load_fit(conn, train_through)
        games, dropped = score_season_preseason(conn, season, fit)
        if not games:
            logger.warning("season %d: no scorable games, skipping", season)
            continue

        gp_max = max(max(g["home_gp"] or 0, g["away_gp"] or 0) for g in games)
        margin_mae = float(
            np.mean([abs(g["expected_home_margin"] - g["actual_margin"]) for g in games])
        )

        sigma = preseason_sigma(prior_residuals)
        if sigma is None:
            logger.warning(
                "season %d: only %d prior preseason residuals (need %d) -- scored but not "
                "simulated; it still seeds sigma for later seasons",
                season,
                len(prior_residuals),
                MIN_SIGMA_GAMES,
            )
            prior_residuals.extend(g["actual_margin"] - g["expected_home_margin"] for g in games)
            prior_actuals[season] = actual_wins_over(games)
            continue

        if season < start:
            # Seed-only season: its residuals inform sigma for the reported
            # range, but it contributes nothing to the output.
            prior_residuals.extend(g["actual_margin"] - g["expected_home_margin"] for g in games)
            prior_actuals[season] = actual_wins_over(games)
            continue

        scored.append(
            {
                "season": season,
                "train_through": train_through,
                "games": games,
                "sigma": sigma,
                "sigma_n": len(prior_residuals),
                "dropped": dropped,
                "gp_max": gp_max,
                "margin_mae": margin_mae,
                "truth": actual_wins_over(games),
                "prior": dict(prior_actuals.get(season - 1, {})),
                "scheduled": fetch_scheduled_counts(conn, season),
                "fbs": fetch_fbs_teams(conn, season) if fbs_only else None,
            }
        )
        prior_residuals.extend(g["actual_margin"] - g["expected_home_margin"] for g in games)
        prior_actuals[season] = scored[-1]["truth"]

    if not scored:
        logger.error("no season could be simulated (insufficient prior residuals throughout)")
        return 1

    # PASS 2 -- simulate, once per candidate strength share.
    sweep_rows = []
    for share in shares:
        agg = _simulate_pass(scored, n_sims=n_sims, seed=seed, strength_share=share)
        sweep_rows.append(agg)
        if len(shares) == 1:
            _report(agg, fbs_only, share)

    if len(shares) > 1:
        _report_sweep(sweep_rows)
    return 0


def _simulate_pass(scored, n_sims: int, seed: int, strength_share: float) -> dict:
    """Simulate every scored season at one `strength_share` and aggregate."""
    per_season = []
    all_proj, all_act = [], []
    all_p10, all_p90 = [], []
    bowl_probs, bowl_out, ten_probs, ten_out = [], [], [], []
    base_prior, base_flat = [], []

    for s in scored:
        season, games, sigma = s["season"], s["games"], s["sigma"]
        sim = simulate_wins(
            games, n_sims=n_sims, sigma=sigma, seed=seed, strength_share=strength_share
        )
        truth, scheduled, fbs = s["truth"], s["scheduled"], s["fbs"]

        rows = []
        for team, team_wins in sim["wins"].items():
            gs = sim["games_simulated"][team]
            if gs == 0:
                continue
            if fbs is not None and team not in fbs:
                continue
            summ = summarize(team_wins, gs)
            actual = truth["wins"].get(team, 0)
            prior = s["prior"]
            rows.append(
                {
                    "team": team,
                    "games_simulated": gs,
                    "games_scheduled": scheduled.get(team, gs),
                    "projected_wins": summ["projected_wins"],
                    "actual_wins": actual,
                    "p10": summ["wins_p10"],
                    "p90": summ["wins_p90"],
                    "p_bowl": summ["p_bowl_eligible"],
                    "p_ten": summ["p_ten_plus"],
                    "baseline_prior": baseline_prior_rate(
                        prior.get("wins", {}).get(team),
                        prior.get("played", {}).get(team),
                        gs,
                    ),
                    "baseline_flat": 0.5 * gs,
                }
            )

        m = win_error_metrics([r["projected_wins"] for r in rows], [r["actual_wins"] for r in rows])
        cov = interval_coverage(
            [r["actual_wins"] for r in rows], [r["p10"] for r in rows], [r["p90"] for r in rows]
        )
        bp = win_error_metrics(
            [r["baseline_prior"] for r in rows], [r["actual_wins"] for r in rows]
        )
        bf = win_error_metrics([r["baseline_flat"] for r in rows], [r["actual_wins"] for r in rows])

        per_season.append(
            {
                "season": season,
                "train_through": s["train_through"],
                "teams": len(rows),
                "games": len(games),
                "sigma": sigma,
                "sigma_n": s["sigma_n"],
                "margin_mae": s["margin_mae"],
                "max_games_played": s["gp_max"],
                "dropped_outcome_dependent": s["dropped"],
                "win_mae": m["mae"],
                "win_rmse": m["rmse"],
                "bias": m["bias"],
                "coverage": cov,
                "baseline_prior_mae": bp["mae"],
                "baseline_flat_mae": bf["mae"],
            }
        )

        all_proj += [r["projected_wins"] for r in rows]
        all_act += [r["actual_wins"] for r in rows]
        all_p10 += [r["p10"] for r in rows]
        all_p90 += [r["p90"] for r in rows]
        base_prior += [r["baseline_prior"] for r in rows]
        base_flat += [r["baseline_flat"] for r in rows]
        bowl_probs += [r["p_bowl"] for r in rows]
        bowl_out += [r["actual_wins"] >= BOWL_ELIGIBLE_WINS for r in rows]
        ten_probs += [r["p_ten"] for r in rows]
        ten_out += [r["actual_wins"] >= TEN_PLUS_WINS for r in rows]

    return {
        "strength_share": strength_share,
        "per_season": per_season,
        "proj": all_proj,
        "act": all_act,
        "p10": all_p10,
        "p90": all_p90,
        "base_prior": base_prior,
        "base_flat": base_flat,
        "bowl_probs": bowl_probs,
        "bowl_out": bowl_out,
        "ten_probs": ten_probs,
        "ten_out": ten_out,
        "overall": win_error_metrics(all_proj, all_act),
        "coverage": interval_coverage(all_act, all_p10, all_p90),
        "quantiles": residual_quantiles(all_proj, all_act),
        "bowl_brier": brier(bowl_probs, bowl_out),
        "ten_brier": brier(ten_probs, ten_out),
    }


def _report_sweep(rows: list[dict]) -> None:
    """Coverage vs strength share -- how rho is chosen rather than guessed.

    Nominal p10-p90 coverage is 80%. The share whose coverage lands nearest
    that is the calibrated value; win MAE is shown alongside because a
    correlation term that widened the distribution at the cost of the point
    estimate would be a bad trade, and this is where that would show.
    """
    print(f"\n{'=' * 78}")
    print("STRENGTH-SHARE SWEEP -- calibrating the v1.1 correlated draw")
    print(f"{'=' * 78}")
    print(
        f"{'rho':>6} {'coverage':>9} {'|gap|':>7} {'winMAE':>8} {'RMSE':>7} "
        f"{'bias':>7} {'bowlBri':>8} {'tenBri':>7} {'p10':>7} {'p90':>7}"
    )
    best = min(rows, key=lambda r: abs(r["coverage"] - 0.80))
    for r in rows:
        mark = "  <-- nearest 80%" if r is best else ""
        q = r["quantiles"]
        print(
            f"{r['strength_share']:>6.2f} {r['coverage']:>9.1%} "
            f"{abs(r['coverage'] - 0.80):>7.3f} {r['overall']['mae']:>8.3f} "
            f"{r['overall']['rmse']:>7.3f} {r['overall']['bias']:>+7.3f} "
            f"{r['bowl_brier']:>8.4f} {r['ten_brier']:>7.4f} "
            f"{q['p10']:>+7.2f} {q['p90']:>+7.2f}{mark}"
        )
    print(f"{'-' * 78}")
    print(
        f"SWEEP_GATE best_strength_share={best['strength_share']:.2f} "
        f"coverage={best['coverage']:.3f} win_mae={best['overall']['mae']:.3f} "
        f"bowl_brier={best['bowl_brier']:.4f} ten_brier={best['ten_brier']:.4f}"
    )
    print(f"{'=' * 78}\n")


def _report(agg, fbs_only, strength_share):
    per_season = agg["per_season"]
    proj, act = agg["proj"], agg["act"]
    p10, p90 = agg["p10"], agg["p90"]
    base_prior, base_flat = agg["base_prior"], agg["base_flat"]
    bowl_probs, bowl_out = agg["bowl_probs"], agg["bowl_out"]
    ten_probs, ten_out = agg["ten_probs"], agg["ten_out"]
    scope = "FBS only" if fbs_only else "all divisions"
    print(f"\n{'=' * 78}")
    print(
        f"PRESEASON BACKTEST -- {MODEL_VERSION} -- week-1 vectors, frozen S-1 fits "
        f"({scope}, strength_share={strength_share:.2f})"
    )
    print(f"{'=' * 78}")
    print(
        f"{'season':>6} {'fit':>5} {'teams':>6} {'games':>6} {'drop':>5} {'maxGP':>6} {'sigma':>7} "
        f"{'mgnMAE':>7} {'winMAE':>7} {'RMSE':>6} {'bias':>6} {'p10-90':>7} "
        f"{'basePri':>8} {'baseFlat':>8}"
    )
    for r in per_season:
        print(
            f"{r['season']:>6} {r['train_through']:>5} {r['teams']:>6} {r['games']:>6} "
            f"{r['dropped_outcome_dependent']:>5} {r['max_games_played']:>6} "
            f"{r['sigma']:>7.2f} {r['margin_mae']:>7.2f} "
            f"{r['win_mae']:>7.2f} {r['win_rmse']:>6.2f} {r['bias']:>+6.2f} "
            f"{r['coverage']:>7.1%} {r['baseline_prior_mae']:>8.2f} {r['baseline_flat_mae']:>8.2f}"
        )

    # maxGP is the direct evidence that the joined vectors really are
    # as-of-season-start. Anything above 0 means a "week-1" row already had
    # games of its own season baked in, and every number above is optimistic.
    leaky = [r["season"] for r in per_season if r["max_games_played"]]
    if leaky:
        print(
            f"\n*** WARNING: season(s) {leaky} selected a week-1 vector with "
            "games_played_to_date > 0. Those rows are NOT preseason and their "
            "errors are understated. ***"
        )

    overall = win_error_metrics(proj, act)
    bp = win_error_metrics(base_prior, act)
    bf = win_error_metrics(base_flat, act)
    cov = interval_coverage(act, p10, p90)
    print(f"{'-' * 78}")
    print(
        f"OVERALL n={overall['n']} win_mae={overall['mae']:.3f} rmse={overall['rmse']:.3f} "
        f"bias={overall['bias']:+.3f} p10_p90_coverage={cov:.1%} (nominal 80%)"
    )
    print(
        f"BASELINE prior_season_rate_mae={bp['mae']:.3f}  flat_500_mae={bf['mae']:.3f}  "
        f"model_beats_prior={'YES' if overall['mae'] < bp['mae'] else 'NO'}  "
        f"model_beats_flat={'YES' if overall['mae'] < bf['mae'] else 'NO'}"
    )

    for label, probs, outs in (
        ("p_bowl_eligible", bowl_probs, bowl_out),
        ("p_ten_plus", ten_probs, ten_out),
    ):
        b = brier(probs, outs)
        print(f"\n{label} calibration (brier={b:.4f})")
        print(f"  {'bucket':>10} {'n':>6} {'predicted':>10} {'observed':>9}")
        for row in calibration_table(probs, outs):
            print(
                f"  {row['bucket']:>10} {row['n']:>6} {row['mean_predicted']:>10.3f} "
                f"{row['observed']:>9.3f}"
            )

    q = residual_quantiles(proj, act)
    if q:
        print(
            f"\nRESIDUAL QUANTILES of (actual - projected) wins, n={q['n']} -- use THESE for an\n"
            f"interval, not +/- MAE (which spans only ~58% for a normal error):"
        )
        print(
            f"  p05={q['p05']:+.2f}  p10={q['p10']:+.2f}  p25={q['p25']:+.2f}  "
            f"p50={q['p50']:+.2f}  p75={q['p75']:+.2f}  p90={q['p90']:+.2f}  p95={q['p95']:+.2f}"
        )
        print(
            f"  => an 80% empirical interval around a projection is "
            f"[proj{q['p10']:+.2f}, proj{q['p90']:+.2f}]"
        )

    verdict = "within" if overall["mae"] <= RESPECTABLE_WIN_MAE else "above"
    print(
        f"\nBACKTEST_GATE model={MODEL_VERSION} n={overall['n']} "
        f"win_mae={overall['mae']:.3f} rmse={overall['rmse']:.3f} bias={overall['bias']:+.3f} "
        f"coverage={cov:.3f} baseline_prior_mae={bp['mae']:.3f} "
        f"baseline_flat_mae={bf['mae']:.3f} "
        f"resid_p10={q['p10']:+.3f} resid_p90={q['p90']:+.3f} "
        f"verdict={verdict}_{RESPECTABLE_WIN_MAE}"
    )
    print(f"{'=' * 78}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=int, default=DEFAULT_START, help="first season to report")
    parser.add_argument("--end", type=int, default=DEFAULT_END, help="last season to report")
    parser.add_argument("--sims", type=int, default=DEFAULT_SIMS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--strength-share",
        type=float,
        default=DEFAULT_STRENGTH_SHARE,
        help="Share of margin variance carried by the per-team season-strength "
        "offset (v1.1 correlated draws). 0 reproduces v1.",
    )
    parser.add_argument(
        "--sweep-strength-share",
        help="Comma-separated shares to sweep, e.g. 0,0.1,0.2,0.3. Scores once "
        "and re-simulates per candidate; reports coverage vs the nominal 80%%.",
    )
    parser.add_argument(
        "--all-divisions",
        action="store_true",
        help="report every division, not just FBS (FCS/D2 playoff games inflate slates)",
    )
    args = parser.parse_args()

    if args.start > args.end:
        parser.error("--start must not be after --end")

    if args.sweep_strength_share:
        try:
            shares = [float(x) for x in args.sweep_strength_share.split(",") if x.strip()]
        except ValueError:
            parser.error("--sweep-strength-share must be comma-separated numbers")
        if not shares:
            parser.error("--sweep-strength-share listed no values")
    else:
        shares = [args.strength_share]
    for sh in shares:
        if not 0.0 <= sh < 1.0:
            parser.error(f"strength share must be in [0, 1), got {sh}")

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        rc = run_backtest(
            conn,
            start=args.start,
            end=args.end,
            n_sims=args.sims,
            seed=args.seed,
            fbs_only=not args.all_divisions,
            shares=shares,
        )
    finally:
        conn.close()
    sys.exit(rc)


if __name__ == "__main__":
    main()
