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

CORRELATED DRAWS (v1.1)
-----------------------
v1 drew every game independently, which treats a team's true strength as
perfectly known and every deviation as game-level noise. Real seasons are
correlated -- a team better than its rating beats *everyone* more often -- so
independent draws understate BOTH tails. The section 4.5 backtest measured it:
p10-p90 coverage 71.7% against a nominal 80%, with p_bowl_eligible
overconfident at the top and p_ten_plus underconfident in the middle.

v1.1 draws one season-strength offset per team per simulation and applies it to
every game that team plays. ``strength_sd`` splits sigma so total per-game
margin variance is UNCHANGED (2*tau^2 + game_sd^2 == sigma^2), so single-game
predictions stay exactly as calibrated as before and only the correlation
structure moves. The share was calibrated against backtest coverage rather than
chosen: 0.15 lands coverage at 79.6%, and win MAE was 1.784 at every share
swept, confirming the point estimate is untouched.

Residual limitation: the offsets are independent ACROSS teams, so a conference
whose teams all outperform together is still underweighted. Set
``--strength-share 0`` to reproduce v1 exactly.

DIVISION AWARENESS (2026-07-26)
-------------------------------
The simulator covers all four CFBD classifications, but two of the quantities
it wrote were FBS rules applied to everybody. ``schedule_complete`` compared
every team to a flat 11 games, which flagged all 8 Ivy League teams in 2026 at
10 of 10 games with nothing unscored -- a finished schedule reported as a
partial one, and downstream that became a "these are floors" warning about the
Ivy League. ``p_bowl_eligible`` was P(6+ wins) for every team, including Yale
(0.888), in divisions with no bowl system. Both now key off the division the
team actually played in that season (``team_classifications``, from
core.games' per-game classification with ref.teams as a fallback):
completeness is measured against the conference's modal slate clamped to the
division standard (``expected_slate_games``) and p_bowl_eligible is NULL
outside FBS.

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
import math
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_MODEL = "fitted_v1"
DEFAULT_SIMS = 10_000

# Fixed so a re-run reproduces the same projection given the same inputs.
# Without this, the daily snapshot would jitter by simulation noise alone and
# the append-only history would show movement that is not a change of opinion.
DEFAULT_SEED = 20260726

# --- Schedule completeness ---------------------------------------------------
# A schedule is "complete" enough for a season win total to mean what a reader
# assumes when it covers the team's DIVISION's full slate. Below that the
# projection is still written -- over the games that exist -- but flagged, and
# cfb-app turns the flag into a "these are floors, not full-season projections"
# caveat.
#
# WHY THIS IS NOT ONE NUMBER ANY MORE. It was: COMPLETE_SCHEDULE_GAMES = 11, a
# flat threshold derived from the 12-game FBS slate. That flagged all 8 Ivy
# League teams in 2026 with games_scheduled = games_simulated = 10 and
# games_unscored = 0 -- schedules that are not short, they are *finished*: the
# Ivy plays a 10-game regular season. cfb-app was warning users about the Ivy
# League for no reason (reported 2026-07-26).
#
# The standard regular-season slate per CFBD classification. Only 'fbs' and
# 'fcs' are spellings this repo uses anywhere else (verify_load.py,
# generate_recaps.py, api/005); 'ii'/'iii' come from CFBD's own vocabulary. A
# key that turns out to be misspelled is not a silent corruption -- it falls
# through to DEFAULT_STANDARD_SLATE and the conference's modal slate still
# carries the real norm; the tripwire is
# TestSeasonOutlook::test_classification_values_are_known_divisions.
STANDARD_SLATE_GAMES = {"fbs": 12, "fcs": 11, "ii": 11, "iii": 10}

# Used when the division cannot be determined at all. Deliberately the FCS
# number rather than the FBS one: an unclassifiable team is far more likely to
# be a small-college opponent than a Power conference member, and the cost of
# guessing high is a false "incomplete" warning on a whole season.
DEFAULT_STANDARD_SLATE = 11

# A conference needs this many projected members before its modal slate length
# is treated as evidence of a local norm. Two teams agreeing is a coincidence;
# it is also the shape a half-published schedule takes.
MIN_SLATE_COHORT = 3

# --- Thresholds --------------------------------------------------------------
# Bowl eligibility is an FBS rule. p_bowl_eligible was previously computed for
# every division, which is how Yale -- an Ivy League team that cannot play in a
# bowl at all -- ended up with a p_bowl_eligible of 0.888. A probability about
# an event that does not exist is worse than no column: it reads as a fact.
# Outside FBS the field is NULL (same review, 2026-07-26).
BOWL_ELIGIBLE_WINS = 6
BOWL_CLASSIFICATIONS = frozenset({"fbs"})

# The "double-digit season" threshold, which is meaningful in every division
# and so is NOT gated on classification.
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

# --- v1.1 correlated draws ---------------------------------------------------
# Share of margin variance attributed to a per-team SEASON-STRENGTH offset,
# drawn once per simulation and applied to every game that team plays, rather
# than independently per game.
#
# WHY. v1 drew each game independently, which treats a team's true strength as
# perfectly known and every deviation as game-level noise. Real seasons do not
# work that way: a team better than its rating beats *everyone* more often, so
# win totals cluster far more than independent draws imply. The section 4.5
# backtest measured the consequence -- p10-p90 coverage of 71.7% against a
# nominal 80%, with p_bowl_eligible overconfident at the top (0.848 predicted
# vs 0.720 observed) and p_ten_plus underconfident in the middle (0.246 vs
# 0.426). Too little mass in BOTH tails is one phenomenon, and this is it.
#
# THE VARIANCE CONSTRAINT. The total per-game margin variance must stay exactly
# sigma^2, or single-game predictions stop being calibrated -- the correlated
# variant is meant to fix season totals without touching a quantity that was
# already right. With independent per-team offsets u ~ N(0, tau^2) on each
# side and game noise eps ~ N(0, game_sd^2):
#
#     Var(margin) = 2*tau^2 + game_sd^2  ==  sigma^2
#
# So a share rho of the variance moved into team strength gives
# tau = sigma * sqrt(rho/2) and game_sd = sigma * sqrt(1 - rho). rho = 0
# reproduces v1 exactly.
#
# CALIBRATED, NOT GUESSED: chosen by sweeping rho in the preseason backtest
# and taking the value whose p10-p90 coverage lands nearest the nominal 80%
# (scripts/backtest_preseason.py --sweep-strength-share). See appendix A7.
# Calibrated 2026-07-26 by --sweep-strength-share over 2019-2025 (n=921):
# coverage 71.7% at 0.00 -> 79.6% at 0.15, against a nominal 80%. Win MAE was
# 1.784 at EVERY share in the sweep, which is the variance constraint above
# doing its job -- the point estimate is untouched and only the spread moves.
DEFAULT_STRENGTH_SHARE = 0.15


# =============================================================================
# Pure functions -- no I/O, no DB, unit-tested directly
# (tests/test_simulate_season.py).
# =============================================================================


# Provenance is stored as NUMERIC(4,3), so the share is normalized to this
# many decimals BEFORE it reaches the simulation. Codex review on PR #52: the
# CLI accepted arbitrary precision while the row rounded, so 0.0004 simulated
# as correlated but recorded as 0.000 (indistinguishable from independent
# draws), and 0.9999 recorded as 1.000 -- a value strength_sd REJECTS, i.e. a
# row describing a configuration that cannot be re-run. Normalizing at the
# boundary makes "recorded == used" structural rather than coincidental.
STRENGTH_SHARE_DECIMALS = 3


def normalize_strength_share(strength_share: float) -> float:
    """Round the share to stored precision and validate the ROUNDED value.

    Validating after rounding is the point: 0.9999 is in range but rounds to
    1.000, which leaves no game-level noise at all. Rejecting it here beats
    storing a share the simulation never used and strength_sd would refuse.
    """
    value = round(float(strength_share), STRENGTH_SHARE_DECIMALS)
    if not 0.0 <= value < 1.0:
        raise ValueError(
            f"strength_share must be in [0, 1) after rounding to "
            f"{STRENGTH_SHARE_DECIMALS} decimals, got {strength_share!r} -> {value!r}"
        )
    return value


def strength_sd(sigma: float, strength_share: float) -> tuple[float, float]:
    """Split `sigma` into (team-strength SD, game-noise SD) for a given share.

    Returns ``(tau, game_sd)`` with ``2*tau**2 + game_sd**2 == sigma**2``, so
    the total per-game margin variance is unchanged and single-game
    predictions stay exactly as calibrated as they were in v1. Only the
    *correlation structure* across a team's games changes.

    ``strength_share`` is the fraction of margin variance carried by the two
    teams' season-strength offsets combined. 0.0 reproduces v1 exactly; 1.0
    would make every game deterministic given the offsets, which is why it is
    rejected rather than clamped -- a season with no game-level noise is not a
    model anyone wants by accident.
    """
    if not 0.0 <= strength_share < 1.0:
        raise ValueError(f"strength_share must be in [0, 1), got {strength_share!r}")
    # Negative is nonsense; zero is degenerate but well defined, and
    # simulate_wins' documented sigma=0 behaviour (every draw collapses onto
    # its mean) is pinned by a test. fetch_sigma is what refuses a
    # non-positive sigma in production -- rejecting it a second time here
    # would break the test that exists to explain why that guard is there.
    if sigma < 0:
        raise ValueError(f"sigma must not be negative, got {sigma!r}")
    tau = sigma * math.sqrt(strength_share / 2.0)
    game_sd = sigma * math.sqrt(1.0 - strength_share)
    return tau, game_sd


def simulate_wins(games, n_sims, sigma, seed=DEFAULT_SEED, strength_share=DEFAULT_STRENGTH_SHARE):
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

    # Normalized once, up front: everything below -- and the value returned
    # for the writer to record -- uses this exact number.
    strength_share = normalize_strength_share(strength_share)

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
        tau, game_sd = strength_sd(sigma, strength_share)

        # One season-strength offset per team per simulation, reused across
        # every game that team plays. This is what makes outcomes correlate:
        # in a simulation where a team drew a positive offset it is better
        # than its rating against ALL its opponents, which is how real seasons
        # produce 11-win and 3-win records far more often than independent
        # coin flips do.
        u = (
            rng.normal(0.0, tau, size=(n_teams, n_sims))
            if tau > 0.0
            else np.zeros((n_teams, n_sims), dtype=np.float64)
        )

        # Game noise generated in one block, as in v1. The per-row offsets are
        # then added in place, so peak memory is unchanged -- no second
        # (n_pending, n_sims) array is ever materialized.
        eps = rng.normal(loc=0.0, scale=game_sd, size=(len(pending), n_sims))
        for row, g in enumerate(pending):
            h, a = index[g["home_team"]], index[g["away_team"]]
            margin = eps[row]
            margin += mu[row]
            if tau > 0.0:
                margin += u[h]
                margin -= u[a]
            home_won_row = margin > 0.0
            wins[h, :] += home_won_row
            wins[a, :] += ~home_won_row
            if g.get("conference_game"):
                conf_wins[h, :] += home_won_row
                conf_wins[a, :] += ~home_won_row

    return {
        "wins": {t: wins[index[t], :] for t in teams},
        "conf_wins": {t: conf_wins[index[t], :] for t in teams},
        "games_simulated": games_simulated,
        "conf_games": conf_games,
        # The share ACTUALLY used, post-normalization. The writer records this
        # rather than its own copy of the argument, so a stored row can always
        # reproduce the run that produced it.
        # Normalized here too, so a caller that bypasses simulate_wins cannot
        # store a share Postgres would silently round to something else.
        # Idempotent for the normal path.
        "strength_share": strength_share,
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


def standard_slate(classification):
    """Full regular-season game count for `classification`."""
    if not classification:
        return DEFAULT_STANDARD_SLATE
    return STANDARD_SLATE_GAMES.get(str(classification).strip().lower(), DEFAULT_STANDARD_SLATE)


def bowls_apply(classification):
    """Whether bowl eligibility is a concept for `classification`.

    Unknown counts as "no": p_bowl_eligible is a claim about postseason
    access, and a division we could not identify is not evidence that the
    claim holds. An honest NULL beats a number nobody can check.
    """
    return bool(classification) and str(classification).strip().lower() in BOWL_CLASSIFICATIONS


def team_classifications(games, fallback=None):
    """{team: division} from the games the team actually played this season.

    Reads core.games' home_classification/away_classification, so a team that
    changed divisions is described by the season in front of it rather than by
    where it plays today -- the realignment defect fixed for
    api.leaderboard_teams on 2026-07-22, which would otherwise reappear here as
    a 12-game FBS threshold applied to a team's final FCS season.

    Ties are broken alphabetically, matching Postgres'
    ``mode() WITHIN GROUP (ORDER BY classification)`` so this agrees with the
    identical derivation in api.season_outlook rather than merely resembling
    it. `fallback` (ref.teams, current membership) fills teams whose games
    carry no classification at all.
    """
    sides = (("home_team", "home_classification"), ("away_team", "away_classification"))
    counts = {}
    for g in games:
        for side, key in sides:
            value = g.get(key)
            if value:
                counts.setdefault(g[side], {})
                counts[g[side]][value] = counts[g[side]].get(value, 0) + 1

    resolved = {}
    for team, tally in counts.items():
        resolved[team] = sorted(tally.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]

    if fallback:
        for g in games:
            for side in ("home_team", "away_team"):
                if g[side] not in resolved and fallback.get(g[side]):
                    resolved[g[side]] = fallback[g[side]]
    return resolved


def expected_slate_games(scheduled_by_team, conf_by_team, class_by_team):
    """{team: games a complete schedule holds for that team this season}.

    The rule, in one sentence: **the modal number of games scheduled by the
    team's conference peers, clamped to [standard - 1, standard] for its
    division.**

    Each half earns its place.

    - The MODE is what lets the Ivy League be complete at 10 games without
      hardcoding "the Ivy plays 10": all eight members schedule 10, so 10 is
      the norm they are measured against. A flat FCS threshold could not tell
      that apart from an 11-game FCS team missing a game.
    - The CLAMP is what stops the mode from rubber-stamping a season nobody
      has finished publishing. In March a Big Ten cohort might modally show 4
      games; without a floor every member would read "complete". standard - 1
      keeps the previous FBS behaviour (>= 11 of 12) as the loosest the rule
      can ever get. The upper end of the clamp matters for completed non-FBS
      seasons, where playoff bracket games are labelled season_type='regular'
      and can drag a small conference's mode above its real slate.
    - A conference smaller than MIN_SLATE_COHORT, or absent entirely (13 of
      2026's 350 teams have a NULL conference), has no norm to appeal to and
      falls back to standard - 1.

    Consequences worth stating: an Ivy team at 10 of 10 is COMPLETE; an FBS
    team at 6 of 12 is not, and neither is one at 11 of 12 whose conference
    modally plays 12 -- that projection really is missing a game, and warning
    is the safe direction for a flag whose whole job is to say "this is a
    floor".
    """
    cohorts = {}
    for team, conf in conf_by_team.items():
        if conf and team in scheduled_by_team:
            cohorts.setdefault(conf, []).append(scheduled_by_team[team])

    modal = {}
    for conf, counts in cohorts.items():
        if len(counts) < MIN_SLATE_COHORT:
            continue
        tally = {}
        for n in counts:
            tally[n] = tally.get(n, 0) + 1
        # Ties resolve to the LONGER slate: over-flagging produces a caveat,
        # under-flagging produces a win total presented as a full season.
        modal[conf] = sorted(tally.items(), key=lambda kv: (-kv[1], -kv[0]))[0][0]

    expected = {}
    for team in scheduled_by_team:
        standard = standard_slate(class_by_team.get(team))
        norm = modal.get(conf_by_team.get(team))
        if norm is None:
            expected[team] = standard - 1
        else:
            expected[team] = min(max(norm, standard - 1), standard)
    return expected


def summarize(team_wins, games_simulated, bowl_eligible=True):
    """Central tendency, percentiles and threshold probabilities for one team.

    ``games_simulated`` -- NOT the scheduled count -- is the denominator for
    ``projected_losses``. Using the schedule length would turn every game the
    simulation skipped (no prediction available) into a guaranteed loss, which
    is a stronger and more wrong claim than the coin flip skipping was meant to
    avoid.

    Percentiles use the ``lower`` interpolation method: win totals are integers,
    and reporting a p10 of 7.4 wins would imply a precision the quantity does
    not have.

    ``bowl_eligible=False`` returns ``p_bowl_eligible = None`` rather than
    P(6+ wins). Below FBS there is no bowl system to be eligible for, so the
    probability is well-defined arithmetic about nothing -- and a consumer
    reading 0.888 next to an Ivy League team has no way to know that.
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
        "p_bowl_eligible": (
            float(np.mean(team_wins >= BOWL_ELIGIBLE_WINS)) if bowl_eligible else None
        ),
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
    strength_share,
    classification=None,
    expected_slate=None,
):
    """One predictions.season_projections row dict for `team`.

    ``games_scheduled`` is reported for transparency, but every projected
    quantity is computed over ``games_simulated`` -- the games that actually
    contributed an outcome. When a pending game has no prediction the two
    differ, and folding that gap into ``projected_losses`` would present a
    missing game as a certain defeat.

    ``classification`` is the division the team played in this season (see
    team_classifications). It gates two things and is stored in neither: which
    slate length counts as complete, and whether p_bowl_eligible is a
    meaningful quantity at all. It defaults to None -- unknown -- rather than
    to 'fbs', because a default that silently claims FBS is the same
    plausible-number-instead-of-an-absence failure the games_simulated split
    exists to prevent.

    ``expected_slate`` is the cohort-derived complete-schedule length from
    expected_slate_games. Omitted, it falls back to the division's standard
    slate minus one, which is the loosest the cohort rule can ever be.
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
    if expected_slate is None:
        expected_slate = standard_slate(classification) - 1

    row = {
        "model_version": model_version,
        "season": games[0]["season"] if games else None,
        "team": team,
        "conference": conf,
        "games_scheduled": games_scheduled,
        "games_simulated": games_simulated,
        "games_completed": len(completed),
        "actual_wins": actual_wins,
        "schedule_complete": games_scheduled >= expected_slate,
        "p_win_dist": {str(k): v for k, v in win_distribution(team_wins, games_simulated).items()},
        "sos_rating": schedule_strength(team, team_games, ratings),
        "sos_rank": None,  # filled after all teams are summarized
        "conf_title_prob": conf_title_prob,
        "playoff_prob": None,  # see migration 043 -- not modeled in v1
        "n_sims": n_sims,
        "residual_sigma": sigma,
        # v1.1 provenance, stored per row for the same reason residual_sigma
        # is: two projections drawn under different correlation structures
        # must not be indistinguishable after the fact.
        # Normalized here too, so a caller that bypasses simulate_wins
        # cannot store a share Postgres would silently round to something
        # else. Idempotent for the normal path.
        "strength_share": normalize_strength_share(strength_share),
    }
    row.update(summarize(team_wins, games_simulated, bowl_eligible=bowls_apply(classification)))
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
           -- The division each side carried in THIS game, not where the team
           -- plays today: schedule_complete and p_bowl_eligible both depend on
           -- it, and ref.teams would misclassify a team's final season before
           -- a move up (the 2026-07-22 realignment fix, same source).
           g.home_classification, g.away_classification,
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

# Fallback division per school, for teams whose games carry no classification
# at all. Current membership, so it is the fallback and never the primary --
# same precedence api.season_outlook and api.leaderboard_teams use. ref.teams
# has ~35 duplicate school names; 'fbs' sorts first, so DISTINCT ON picks the
# FBS row on a name collision.
REF_CLASSIFICATION_QUERY = """
    SELECT DISTINCT ON (school) school, classification
    FROM ref.teams
    ORDER BY school, classification NULLS LAST
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
    if sigma <= 0.0:
        # Degenerate rather than merely small: every draw would collapse onto
        # its mean, so every game would resolve deterministically and every
        # win probability would be exactly 0 or 1. That is a broken input
        # (identical residuals across hundreds of games), not a confident
        # model, and it must not pass silently.
        raise RuntimeError(
            f"Residual sigma for {model} measured as {sigma} over {n_games} game(s); "
            "a non-positive sigma would make every simulated game deterministic."
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
                "home_classification": r["home_classification"],
                "away_classification": r["away_classification"],
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


def fetch_ref_classifications(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(REF_CLASSIFICATION_QUERY)
        return {school: cls for school, cls in cur.fetchall() if cls is not None}


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
    "strength_share",
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


def simulate_one_season(
    conn, season: int, model: str, n_sims: int, seed: int, strength_share: float
) -> int:
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
    sim = simulate_wins(games, n_sims, sigma, seed=seed, strength_share=strength_share)
    wins_by_team = sim["wins"]

    conf_by_team = {}
    for g in games:
        for side, conf in (("home_team", "home_conference"), ("away_team", "away_conference")):
            conf_by_team.setdefault(g[side], g[conf])

    # Division per team, and from it the slate length a complete schedule has
    # and whether bowl eligibility is a concept. Both were previously FBS
    # constants applied to all four divisions.
    class_by_team = team_classifications(games, fetch_ref_classifications(conn))
    scheduled_by_team = {}
    for g in games:
        for side in ("home_team", "away_team"):
            scheduled_by_team[g[side]] = scheduled_by_team.get(g[side], 0) + 1
    expected_slate = expected_slate_games(scheduled_by_team, conf_by_team, class_by_team)

    unclassified = [t for t in scheduled_by_team if not class_by_team.get(t)]
    if unclassified:
        logger.warning(
            "season=%d: %d team(s) have no classification in core.games or ref.teams; "
            "they get p_bowl_eligible=NULL and are measured for completeness against "
            "the default %d-game slate (e.g. %s)",
            season,
            len(unclassified),
            DEFAULT_STANDARD_SLATE,
            ", ".join(sorted(unclassified)[:5]),
        )

    # Conference-only records, from the same draws as the overall tally.
    title_probs = conference_title_probs(sim["conf_wins"], conf_by_team, sim["conf_games"])

    # A team none of whose games could be scored gets NO row rather than a row
    # of zeros. `projected_wins = 0.0` with `p_bowl_eligible = 0.0` reads as
    # "the model expects this team to win nothing", when the truth is that the
    # model has no opinion at all -- the same plausible-number-instead-of-an-
    # absence failure the games_simulated split exists to prevent, reintroduced
    # one level up.
    unprojectable = [t for t in wins_by_team if sim["games_simulated"][t] == 0]
    if unprojectable:
        logger.warning(
            "season=%d: %d team(s) had no scorable game and are omitted from "
            "projections entirely (e.g. %s)",
            season,
            len(unprojectable),
            ", ".join(sorted(unprojectable)[:5]),
        )

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
            sim["strength_share"],
            classification=class_by_team.get(team),
            expected_slate=expected_slate.get(team),
        )
        for team, team_wins in wins_by_team.items()
        if sim["games_simulated"][team] > 0
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
        f"strength_share={sim['strength_share']:.3f} "
        f"sigma={sigma:.2f} complete={complete} unscored_team_games={unscored} "
        f"omitted_teams={len(unprojectable)}"
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
    parser.add_argument(
        "--strength-share",
        type=float,
        default=DEFAULT_STRENGTH_SHARE,
        help=f"Share of margin variance carried by the per-team season-strength "
        f"offset (default {DEFAULT_STRENGTH_SHARE}; 0 reproduces v1)",
    )
    args = parser.parse_args()

    try:
        args.strength_share = normalize_strength_share(args.strength_share)
    except ValueError as e:
        parser.error(str(e))

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
            total += simulate_one_season(
                conn, season, args.model, args.sims, args.seed, args.strength_share
            )
        logger.info("Wrote %d projection row(s) across %d season(s)", total, len(seasons))
    except Exception:
        conn.rollback()
        logger.exception("season simulation failed")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
