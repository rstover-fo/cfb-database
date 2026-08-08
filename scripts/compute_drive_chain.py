#!/usr/bin/env python3
"""Estimate the drive-state Markov chain and solve house expected points.

Design: docs/plans/2026-08-08-drive-chain-ep-model-plan.md (P1).

Architecture (compute_house_elo.py conventions): everything above the
`# --- I/O layer ---` marker is pure -- state bucketing, transition
counting, empirical-Bayes shrinkage, the absorbing-chain solve, and the
game-cluster bootstrap all operate on plain dicts/arrays and are
unit-tested without a database (tests/test_drive_chain.py). The I/O layer
fetches scrimmage plays (garbage-time filtered through marts.play_epa so
there is ONE garbage-time definition in the codebase), feeds them through
the engine, and rewrites analytics.drive_chain_transitions /
analytics.ep_states idempotently (DELETE per era + INSERT).

A transition is snapshot -> snapshot between consecutive scrimmage plays of
a drive: each play carries its own (down, distance, yards_to_goal), so a
penalty between two scrimmage plays is absorbed into the observed next
snapshot without modeling the 113k penalty rows. The last scrimmage play of
a drive transitions to the absorbing outcome mapped from
core.drives.drive_result.

Usage:
    python scripts/compute_drive_chain.py --full            # all eras
    python scripts/compute_drive_chain.py --era 2021+       # one era
    python scripts/compute_drive_chain.py --full --validate # + gates 1/3
    python scripts/compute_drive_chain.py --full --no-bootstrap

Prints one machine-readable line per era after writing:
    EP_VALIDATION era=<e> states=<n> monotone_zone=<pass|FAIL> \
        monotone_down=<pass|FAIL> calib_mae_td=<x.xxxx>
"""

import argparse
import logging
import sys
from collections import Counter, defaultdict

import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Eras: rules/scoring environments estimated separately (design doc section
# "Estimation"). Bounds are inclusive; the open era picks up new seasons
# automatically so the daily/deploy path never needs a code change at rollover.
ERAS: dict[str, tuple[int, int | None]] = {
    "2004-2013": (2004, 2013),
    "2014-2020": (2014, 2020),
    "2021+": (2021, None),
}

# Scrimmage play types: rows that represent a snap from scrimmage with a
# meaningful pre-snap state. Everything else (Penalty, Timeout, End Period,
# kickoffs, punts, FG rows...) is either a non-play or a special-teams play
# that ends/starts possessions -- those enter through the ABSORBING outcome,
# not as transient states (P2 models them explicitly).
SCRIMMAGE_TYPES = frozenset(
    {
        "Rush",
        "Pass Reception",
        "Pass Incompletion",
        "Pass Completion",
        "Sack",
        "Rushing Touchdown",
        "Passing Touchdown",
        "Interception",
        "Pass Interception Return",
        "Interception Return Touchdown",
        "Fumble Recovery (Own)",
        "Fumble Recovery (Opponent)",
        "Fumble Return Touchdown",
        "Safety",
    }
)

# drive_result -> absorbing state. Verified against the live vocabulary
# 2026-08-08 (design doc "Data foundation"); anything unmapped is dropped
# and counted, and the build fails if drops exceed MAX_UNMAPPED_SHARE.
ABSORB_MAP: dict[str, str] = {
    "TD": "TD",
    "RUSHING TD": "TD",
    "PASSING TD": "TD",
    "FG": "FG",
    "MISSED FG": "MISSED_FG",
    "BLOCKED FG": "MISSED_FG",
    "MISSED FG TD": "TURNOVER_TD",  # returned for a defensive score
    "PUNT": "PUNT",
    "BLOCKED PUNT": "PUNT",
    "PUNT RETURN TD": "TURNOVER_TD",
    "PUNT TD": "TURNOVER_TD",
    "BLOCKED PUNT TD": "TURNOVER_TD",
    "INT": "TURNOVER",
    "FUMBLE": "TURNOVER",
    "INT TD": "TURNOVER_TD",
    "FUMBLE TD": "TURNOVER_TD",
    "FUMBLE RETURN TD": "TURNOVER_TD",
    "DOWNS": "DOWNS",
    # 2004-2013 legacy vocabulary (first full-era run, deploy 31257283280:
    # 18.24% of that decade's drives were unmapped; these seven synonyms are
    # verified against the live 2004-2013 drive_result distribution).
    "FG GOOD": "FG",
    "MADE FG": "FG",
    "FG MISSED": "MISSED_FG",
    "TURNOVER ON DOWNS": "DOWNS",
    "POSS. ON DOWNS": "DOWNS",
    "INT RETURN TOUCH": "TURNOVER_TD",
    "PUNT RETURN TD TD": "TURNOVER_TD",  # double-suffix typo, 44 drives
    "DOWNS TD": "TURNOVER_TD",
    "SF": "SAFETY",
    "END OF HALF": "END_OF_HALF",
    "END OF HALF TD": "END_OF_HALF",
    "END OF GAME": "END_OF_HALF",
    "END OF 4TH QUARTER": "END_OF_HALF",
}

ABSORBING = (
    "TD",
    "FG",
    "MISSED_FG",
    "PUNT",
    "TURNOVER",
    "TURNOVER_TD",
    "DOWNS",
    "SAFETY",
    "END_OF_HALF",
)

# Goldner-basis absorbing values. TD carries the league PAT expectation
# (~0.97 XP make x 1 + small 2pt mix); TURNOVER_TD is the mirror. Everything
# else is worth 0 on the DRIVE basis -- field position handoff is P2 (ep_net).
TD_VALUE = 6.97
ABSORB_VALUES: dict[str, float] = {
    "TD": TD_VALUE,
    "FG": 3.0,
    "SAFETY": -2.0,
    "TURNOVER_TD": -TD_VALUE,
}

# Uncategorized drives run ~1% in modern eras; 2x headroom. 2004-2013 gets a
# looser floor by evidence, not convenience: after mapping the legacy synonyms
# above, ~3.5% of that decade's drives remain unmapped -- KICKOFF (3,836),
# last-play labels (RUSH/SACK/PASS COMPLETE/INCOMPLETE, ~800) and
# UNCATEGORIZED (2,232), ALL verified non-scoring (0% offense-scored). Their
# outcomes are unrecoverable, so they are dropped knowingly; the slight bias
# toward completed drives is acceptable for the legacy era.
MAX_UNMAPPED_SHARE = 0.02
MAX_UNMAPPED_SHARE_BY_ERA = {"2004-2013": 0.05}

# Empirical-Bayes shrinkage strength (pseudo-observations given to the parent
# distribution). With median state support ~4.9k, alpha=50 is negligible for
# healthy states and decisive for the 38 starved ones (support < 500).
DEFAULT_ALPHA = 50.0

BOOTSTRAP_B = 200
BOOTSTRAP_SEED = 20260808


# =============================================================================
# Pure engine -- no I/O, no DB, unit-tested directly.
# =============================================================================


def field_zone(yards_to_goal: int) -> int:
    """10-yard band, 1 (goal line) .. 10 (own goal line)."""
    return min(10, max(1, (int(yards_to_goal) + 9) // 10))


def distance_bucket(down: int, distance: int, yards_to_goal: int) -> str:
    """Down-aware distance bucket (design doc section 4).

    A flat grid starves structurally rare corners (38 of 160 states under
    500 plays -- e.g. 1st-and-short away from the goal line), so 1st down
    gets its own vocabulary: goal_to_go / standard (=10) / short (<10,
    post-penalty) / long (>10). Downs 2-4 use short/med/long/xlong with
    goal_to_go overriding when the line to gain is the goal line.
    """
    if distance >= yards_to_goal:
        return "goal"
    if down == 1:
        if distance == 10:
            return "standard"
        return "short" if distance < 10 else "long"
    if distance <= 3:
        return "short"
    if distance <= 6:
        return "med"
    if distance <= 10:
        return "long"
    return "xlong"


def state_key(down: int, distance: int, yards_to_goal: int) -> str:
    return f"d{down}|{distance_bucket(down, distance, yards_to_goal)}|z{field_zone(yards_to_goal)}"


def parent_key(state: str) -> str:
    """Shrinkage parent: drop the distance bucket, keep down x zone."""
    down, _, zone = state.split("|")
    return f"{down}|*|{zone}"


def grandparent_key(state: str) -> str:
    """Second-level parent: down only."""
    down, _, _ = state.split("|")
    return f"{down}|*|*"


def build_transitions(plays: list[dict]) -> tuple[Counter, Counter, int]:
    """Count snapshot->snapshot transitions from ordered scrimmage plays.

    `plays` rows need: game_id, drive_id, play_number, down, distance,
    yards_to_goal, drive_result. Rows are grouped by (game_id, drive_id) and
    ordered by play_number; the last play of a drive absorbs into
    ABSORB_MAP[drive_result].

    Returns (transition counts keyed (from_state, to_state),
             per-game counts keyed (game_id, from_state, to_state) -- the
             bootstrap resampling unit,
             drive_outcomes keyed (first_play_state, absorbing) -- the
             realized-outcome sample the calibration gate compares against,
             n_mapped drives, and the count of drives dropped for an
             unmapped drive_result). Drive counts are exact (PR #66 review,
             P2): the unmapped-share guard must divide by drives, not by
             game-state pairs.
    """
    transitions: Counter = Counter()
    per_game: Counter = Counter()
    drive_outcomes: Counter = Counter()
    unmapped_drives = 0
    n_mapped = 0

    by_drive: dict[tuple, list[dict]] = defaultdict(list)
    for row in plays:
        by_drive[(row["game_id"], row["drive_id"])].append(row)

    for (game_id, _), rows in by_drive.items():
        rows.sort(key=lambda r: r["play_number"])
        result = (rows[0].get("drive_result") or "").upper()
        absorb = ABSORB_MAP.get(result)
        if absorb is None:
            unmapped_drives += 1
            continue
        n_mapped += 1
        states = [state_key(r["down"], r["distance"], r["yards_to_goal"]) for r in rows]
        for a, b in zip(states, states[1:]):
            transitions[(a, b)] += 1
            per_game[(game_id, a, b)] += 1
        transitions[(states[-1], absorb)] += 1
        per_game[(game_id, states[-1], absorb)] += 1
        drive_outcomes[(states[0], absorb)] += 1

    return transitions, per_game, drive_outcomes, n_mapped, unmapped_drives


def shrink(transitions: Counter, alpha: float = DEFAULT_ALPHA) -> dict[tuple, float]:
    """Empirical-Bayes shrink each row toward its parent's distribution.

    p_shrunk(to | from) = (n(from,to) + alpha * p_parent(to | parent(from)))
                          / (n(from) + alpha)

    The parent pools all distance buckets of the same down x zone; the parent
    itself is smoothed toward the grandparent (down only) with the same
    alpha, so even an empty parent row is defined. Healthy states
    (n >> alpha) are barely moved; starved states inherit their parent.
    """
    row_totals: Counter = Counter()
    parent_counts: Counter = Counter()
    parent_totals: Counter = Counter()
    grand_counts: Counter = Counter()
    grand_totals: Counter = Counter()
    for (a, b), n in transitions.items():
        row_totals[a] += n
        parent_counts[(parent_key(a), b)] += n
        parent_totals[parent_key(a)] += n
        grand_counts[(grandparent_key(a), b)] += n
        grand_totals[grandparent_key(a)] += n

    def p_parent(a: str, b: str) -> float:
        pk, gk = parent_key(a), grandparent_key(a)
        g = grand_counts[(gk, b)] / grand_totals[gk] if grand_totals[gk] else 0.0
        num = parent_counts[(pk, b)] + alpha * g
        den = parent_totals[pk] + alpha
        return num / den if den else 0.0

    # Candidate targets per from-state: observed targets plus every target the
    # parent OR grandparent has seen. The grandparent union is load-bearing
    # (PR #66 review, P1): p_parent() mixes grandparent probability into every
    # target, so a target observed only elsewhere in the same down would carry
    # positive probability that the row never emits -- the row would sum to
    # less than 1 and solve_ep() would leak that mass out of the chain,
    # biasing every absorption probability.
    targets_by_state: dict[str, set] = defaultdict(set)
    parent_targets: dict[str, set] = defaultdict(set)
    grand_targets: dict[str, set] = defaultdict(set)
    for pk, b in parent_counts:
        parent_targets[pk].add(b)
    for gk, b in grand_counts:
        grand_targets[gk].add(b)
    for a, b in transitions:
        targets_by_state[a].add(b)
    for a in row_totals:
        targets_by_state[a] |= parent_targets[parent_key(a)]
        targets_by_state[a] |= grand_targets[grandparent_key(a)]

    shrunk: dict[tuple, float] = {}
    for a in row_totals:
        den = row_totals[a] + alpha
        for b in targets_by_state[a]:
            shrunk[(a, b)] = (transitions[(a, b)] + alpha * p_parent(a, b)) / den
    return shrunk


def _chain_matrices(probs: dict[tuple, float]):
    """Shared chain assembly: transient ordering, N = (I-Q)^-1, and R.

    (I - Q) is invertible because every state reaches an absorbing outcome
    (every drive ends). A target that is neither transient nor absorbing
    cannot occur: shrink() only emits observed/parent targets.
    """
    transient = sorted({a for (a, _) in probs})
    t_index = {s: i for i, s in enumerate(transient)}
    a_index = {s: i for i, s in enumerate(ABSORBING)}
    nt, na = len(transient), len(ABSORBING)

    Q = np.zeros((nt, nt))
    R = np.zeros((nt, na))
    for (a, b), p in probs.items():
        if b in t_index:
            Q[t_index[a], t_index[b]] = p
        elif b in a_index:
            R[t_index[a], a_index[b]] = p

    N = np.linalg.solve(np.eye(nt) - Q, np.eye(nt))
    return transient, t_index, a_index, N, R


def solve_ep(probs: dict[tuple, float]) -> tuple[dict[str, float], dict[tuple, float]]:
    """Solve the absorbing chain: EP per transient state + absorption probs.

    EP = (I - Q)^-1 R v with Q transient->transient, R transient->absorbing,
    v the ABSORB_VALUES vector. Returns (ep by state, absorption probability
    by (state, absorbing)).
    """
    transient, t_index, a_index, N, R = _chain_matrices(probs)
    B = N @ R  # absorption probabilities
    v = np.array([ABSORB_VALUES.get(s, 0.0) for s in ABSORBING])
    ep = B @ v

    ep_by_state = {s: float(ep[i]) for s, i in t_index.items()}
    absorb_probs = {
        (s, abs_s): float(B[t_index[s], a_index[abs_s]]) for s in transient for abs_s in ABSORBING
    }
    return ep_by_state, absorb_probs


def bootstrap_se(
    per_game: Counter,
    alpha: float = DEFAULT_ALPHA,
    n_boot: int = BOOTSTRAP_B,
    seed: int = BOOTSTRAP_SEED,
) -> dict[str, float]:
    """Game-cluster bootstrap SE of ep_drive per state.

    Resample games with replacement, rebuild counts, re-shrink, re-solve.
    Clustering by game (not play) preserves within-game correlation --
    the Brill/Yurko/Wyner objection to naive play-level resampling.
    """
    games = sorted({g for (g, _, _) in per_game})
    by_game: dict = defaultdict(Counter)
    for (g, a, b), n in per_game.items():
        by_game[g][(a, b)] += n

    rng = np.random.default_rng(seed)
    samples: dict[str, list[float]] = defaultdict(list)
    for _ in range(n_boot):
        counts: Counter = Counter()
        for g in rng.choice(games, size=len(games), replace=True):
            counts.update(by_game[g])
        ep, _ = solve_ep(shrink(counts, alpha))
        for s, val in ep.items():
            samples[s].append(val)
    return {s: float(np.std(vals, ddof=1)) for s, vals in samples.items() if len(vals) > 1}


# =============================================================================
# Net next-score EP (P2) -- design doc section 6, v1.5.
# =============================================================================

# Absorbing outcomes that hand possession over WITHOUT a score: the
# next-score recursion continues through these (worth minus the opponent's
# net EP at their observed starting field position). Scoring outcomes and
# END_OF_HALF terminate the recursion -- the next score has happened, or
# there is none.
HANDOFF_ABSORBING = ("PUNT", "TURNOVER", "DOWNS", "MISSED_FG")

# Laplace strength pulling a starved exit zone's handoff distribution toward
# the outcome's marginal (punts from inside the opponent 30 are rare and
# weird -- 11/33/85 observations in zones 1-3 for 2021+ -- while zones 4-10
# carry 2k-15k each).
HANDOFF_ALPHA = 20.0


def handoff_state(zone: int) -> str:
    """The opponent's first-and-10 state for a possession starting in `zone`.

    Zone 1 is inside the 10, where every first down is goal-to-go, so
    d1|standard|z1 does not exist in the grid; the handoff lands on
    d1|goal|z1 instead.
    """
    return "d1|goal|z1" if zone == 1 else f"d1|standard|z{zone}"


def build_handoffs(
    pair_counts: dict[tuple, int], alpha: float = HANDOFF_ALPHA
) -> dict[str, dict[int, dict[int, float]]]:
    """Empirical handoff distributions from consecutive drive pairs.

    `pair_counts` maps (absorb_class, exit_zone, opp_start_zone) -> n,
    observed from core.drives: a drive ending in `absorb_class`, whose last
    scrimmage play was snapped in `exit_zone` (the PRE-snap zone -- the same
    convention as the chain's transient exit state, which is where
    solve_net_ep looks these rows up), was followed (same game, same half,
    offense flipped) by an opponent drive starting at `opp_start_zone`. ONE estimator covers punts,
    turnovers, downs and missed FGs at once -- net punt distance, return
    yards, and spot-of-kick rules are all baked into what actually happened
    next, per era. Rows are Laplace-smoothed toward the outcome's marginal
    start distribution so starved exit zones inherit it.
    """
    by_class: dict[str, Counter] = defaultdict(Counter)
    marginals: dict[str, Counter] = defaultdict(Counter)
    for (cls, exit_zone, opp_zone), n in pair_counts.items():
        if cls not in HANDOFF_ABSORBING:
            continue
        by_class[cls][(exit_zone, opp_zone)] += n
        marginals[cls][opp_zone] += n

    handoffs: dict[str, dict[int, dict[int, float]]] = {}
    for cls in HANDOFF_ABSORBING:
        marg_total = sum(marginals[cls].values())
        marg = {z: (marginals[cls][z] / marg_total if marg_total else 0.1) for z in range(1, 11)}
        handoffs[cls] = {}
        for exit_zone in range(1, 11):
            row_n = {z: by_class[cls][(exit_zone, z)] for z in range(1, 11)}
            total = sum(row_n.values())
            handoffs[cls][exit_zone] = {
                z: (row_n[z] + alpha * marg[z]) / (total + alpha) for z in range(1, 11)
            }
    return handoffs


def zone_of(state: str) -> int:
    return int(state.rsplit("z", 1)[1])


def solve_net_ep(
    probs: dict[tuple, float], handoffs: dict[str, dict[int, dict[int, float]]]
) -> dict[str, float]:
    """Next-score expected points per transient state (the CFBD-comparable
    basis: value of the NEXT scoring event in the half, sign = this offense).

    Exit-state-aware absorption: P(exit at transient t, absorb a | start s)
    = N[s,t] * R[t,a], so a punt from midfield and a punt from the shadow of
    the goalposts hand the opponent different field position. The recursion
        ep_net(s) = C(s) - sum_z D(s,z) * ep_net(handoff_state(z))
    (C = scoring/half-end terms, D = handoff mass landing the opponent in
    zone z) closes over only the 10 first-and-10 handoff states, so it is a
    10x10 LINEAR system, solved exactly -- no fixed-point iteration. It is a
    proper contraction because every drive leaks probability to scoring or
    END_OF_HALF outcomes; a zero-leak chain (pure punt alternation) is
    singular and cannot occur in real data.
    """
    transient, t_index, a_index, N, R = _chain_matrices(probs)
    nt = len(transient)
    zones = np.array([zone_of(s) for s in transient])

    # C[s]: terminal contributions. D[s, z]: handoff mass into opponent zone z.
    v_term = np.array([ABSORB_VALUES.get(a, 0.0) for a in ABSORBING])
    is_term = np.array([a not in HANDOFF_ABSORBING for a in ABSORBING])
    C = N @ (R[:, is_term] @ v_term[is_term])

    D = np.zeros((nt, 10))
    for cls in HANDOFF_ABSORBING:
        r_cls = R[:, a_index[cls]]  # P(absorb via cls | exit t)
        # mass(s, t) = N[s, t] * r_cls[t]; spread over opponent zones by the
        # exit zone's handoff row.
        H = np.zeros((nt, 10))
        for ti in range(nt):
            row = handoffs[cls][int(zones[ti])]
            for z in range(1, 11):
                H[ti, z - 1] = row[z]
        D += (N * r_cls[np.newaxis, :]) @ H

    # Restrict to the 10 handoff states and solve (I + D_h) x = C_h.
    h_states = [handoff_state(z) for z in range(1, 11)]
    missing = [h for h in h_states if h not in t_index]
    if missing:
        raise ValueError(f"handoff states absent from the chain: {missing}")
    h_idx = [t_index[h] for h in h_states]
    D_h = D[h_idx, :]
    C_h = C[h_idx]
    x = np.linalg.solve(np.eye(10) + D_h, C_h)

    ep_net = C - D @ x
    return {s: float(ep_net[i]) for s, i in t_index.items()}


# --- Validation gates (pure; consume engine outputs) -------------------------


def check_monotone_zone(ep: dict[str, float]) -> list[str]:
    """Gate 1a: EP must not increase as yards_to_goal grows, per down at
    standard distances. Returns violation descriptions (empty = pass).

    4th down is exempt here for the same reason as check_monotone_down: d4
    snapshots are go-for-it-conditional, and WHO goes for it varies wildly by
    field position (the 2014-2020 era run flagged d4|short rising z9->z10,
    0.68->1.02 -- a selection artifact of desperate/confident teams going at
    their own goal line, not an estimation failure).
    """
    violations = []
    for down, bucket in (("d1", "standard"), ("d2", "med"), ("d3", "med"), ("d3", "short")):
        curve = [
            (z, ep[f"{down}|{bucket}|z{z}"]) for z in range(1, 11) if f"{down}|{bucket}|z{z}" in ep
        ]
        for (z1, e1), (z2, e2) in zip(curve, curve[1:]):
            if e2 > e1 + 0.02:  # tolerance for estimation noise on adjacent bands
                violations.append(f"{down}/{bucket}: EP rises z{z1}->z{z2} ({e1:.2f}->{e2:.2f})")
    return violations


def check_monotone_down(ep: dict[str, float]) -> list[str]:
    """Gate 1b: at fixed medium distance and zone, earlier downs are worth
    at least as much as later downs -- checked for downs 2-3 ONLY.

    4th down is exempt by design, not tolerance: a d4 scrimmage snapshot
    exists only when the offense lined up to go (punts and FGs exit the
    chain from the 3rd-down play), so d4 states are go-for-it-conditional
    and can legitimately price above d3 -- the punt outcome is excluded
    from their mix. First observed on the real 2021+ chain (d4|med|z7 1.40
    vs d3|med|z7 1.37) and structural in any Goldner-style drive chain.
    P4's 4th-down model USES this: EP(go | state) is the d4-state value.
    """
    violations = []
    for z in range(2, 10):
        chain = []
        for down, bucket in (("d2", "med"), ("d3", "med")):
            key = f"{down}|{bucket}|z{z}"
            if key in ep:
                chain.append((key, ep[key]))
        for (k1, e1), (k2, e2) in zip(chain, chain[1:]):
            if e2 > e1 + 0.02:
                violations.append(f"z{z}: {k2} ({e2:.2f}) > {k1} ({e1:.2f})")
    return violations


def calibration_mae(
    absorb_probs: dict[tuple, float], drive_outcomes: Counter, outcome: str = "TD"
) -> float:
    """Gate 3: support-weighted mean |model absorption prob - REALIZED drive
    outcome rate| over drive-start states.

    Realized, not model-vs-model (PR #66 review, P2): the empirical rate for
    a state is the fraction of drives whose FIRST play was in that state that
    actually ended in `outcome`. Drive starts are held-out-in-spirit
    observations of the whole chain: a large MAE flags either miscalibration
    or a Markov violation, which is exactly what this gate exists to catch.
    """
    starts_total: Counter = Counter()
    starts_outcome: Counter = Counter()
    for (s, abs_s), n in drive_outcomes.items():
        starts_total[s] += n
        if abs_s == outcome:
            starts_outcome[s] += n

    num, den = 0.0, 0
    for s, n in starts_total.items():
        if (s, outcome) not in absorb_probs or n < 50:  # skip unsupported starts
            continue
        empirical = starts_outcome[s] / n
        num += n * abs(absorb_probs[(s, outcome)] - empirical)
        den += n
    return num / den if den else float("nan")


# =============================================================================
# --- I/O layer ---------------------------------------------------------------
# =============================================================================

# Garbage-time predicate inlined VERBATIM from marts.play_epa
# (src/schemas/marts/010_play_epa.sql). The mart itself must NOT be joined
# here (PR #66 review, P1): it is defined WHERE p.ppa IS NOT NULL, so an
# inner join silently drops any scrimmage play CFBD did not score --
# corrupting snapshot sequences and re-importing the CFBD-coverage
# dependency this house model exists to remove. If 010's definition ever
# changes, change this in the same commit.
GARBAGE_TIME_SQL = """(
      (p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) > 28) OR
      (p.period >= 3 AND ABS(COALESCE(p.score_diff, 0)) > 35)
)"""

PLAYS_QUERY = f"""
    SELECT p.game_id,
           p.drive_id,
           p.play_number,
           p.down,
           p.distance,
           p.yards_to_goal,
           d.drive_result
    FROM core.plays p
    JOIN core.drives d ON d.id = p.drive_id AND d.game_id = p.game_id
    WHERE p.season BETWEEN %(start)s AND %(end)s
      AND p.play_type = ANY(%(types)s)
      AND p.down BETWEEN 1 AND 4
      AND p.distance BETWEEN 1 AND 45
      AND p.yards_to_goal BETWEEN 1 AND 99
      AND NOT {GARBAGE_TIME_SQL}
      AND d.drive_result <> 'POSSESSION (FOR OT DRIVES)'
"""


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Copied from scripts/compute_house_elo.py's get_db_url pattern (each
    compute_*.py script keeps its own copy rather than importing across
    scripts for this one utility).
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
    return url.replace("postgres://", "postgresql://", 1)


# Consecutive-drive pairs: the empirical handoff estimator (design doc
# section 7, reworked in P2). For a drive ending in a handoff outcome, where
# did the opponent's SAME-HALF next drive actually start? Net punt distance,
# return yardage, and spot-of-kick rules are all baked into the observed next
# start. Deliberately NOT garbage-time filtered: field-position physics do
# not change with the score, and drive grain has no play-level filter anyway.
#
# exit_zone keying (PR #71 review, P1): the solver looks a handoff row up by
# the ZONE OF THE TRANSIENT STATE the chain absorbed from -- the PRE-snap
# yards_to_goal of the drive's last scrimmage play (punt/FG rows are not
# scrimmage types, so a punting drive exits from its 3rd-down snapshot). An
# earlier revision keyed rows by core.drives.end_yards_to_goal -- the POST-
# play drive-end spot, which differs by the last play's yardage (or an INT
# return): measured on 2021+, only 73-86% of handoff drives land in the same
# zone under both conventions. Training and lookup must condition on the
# same zone, so the last_snap CTE recovers the pre-snap spot with PLAYS_QUERY's
# play-shape filters (garbage time excepted, per the note above).
DRIVE_PAIRS_QUERY = """
    WITH last_snap AS (
      SELECT DISTINCT ON (game_id, drive_id)
        game_id, drive_id, yards_to_goal AS exit_ytg
      FROM core.plays
      WHERE season BETWEEN %(start)s AND %(end)s
        AND play_type = ANY(%(types)s)
        AND down BETWEEN 1 AND 4
        AND distance BETWEEN 1 AND 45
        AND yards_to_goal BETWEEN 1 AND 99
      ORDER BY game_id, drive_id, play_number DESC
    ),
    seq AS (
      SELECT d.game_id, d.drive_number, d.offense, upper(d.drive_result) AS dr,
        ls.exit_ytg,
        CASE WHEN d.start_period <= 2 THEN 1 ELSE 2 END AS half,
        lead(d.offense) OVER w AS next_offense,
        lead(d.start_yards_to_goal) OVER w AS next_start_ytg,
        lead(CASE WHEN d.start_period <= 2 THEN 1 ELSE 2 END) OVER w AS next_half
      -- LEFT JOIN, filtered in the outer query: every drive must stay in
      -- seq so lead() pairs each drive with its true successor even when
      -- the CURRENT drive has no qualifying scrimmage play.
      FROM core.drives d
      LEFT JOIN last_snap ls ON ls.game_id = d.game_id AND ls.drive_id = d.id
      WHERE d.season BETWEEN %(start)s AND %(end)s
        -- Regulation only, filtered BEFORE lead() (PR #71 review, P2):
        -- "period >= 3 means half 2" sweeps OT periods 5+ in, and OT
        -- possessions start at the prescribed 25-yard-line spot (zone 3)
        -- by RULE, not as the physical consequence of the preceding punt/
        -- turnover -- polluting exactly the zone-3 handoff mass. Filtering
        -- the CTE source means the last regulation drive's lead() is NULL,
        -- so regulation->OT pairs drop out entirely.
        AND d.start_period BETWEEN 1 AND 4
      WINDOW w AS (PARTITION BY d.game_id ORDER BY d.drive_number)
    )
    SELECT dr,
           LEAST(10, GREATEST(1, (exit_ytg + 9) / 10)) AS exit_zone,
           LEAST(10, GREATEST(1, (next_start_ytg + 9) / 10)) AS opp_zone,
           count(*) AS n
    FROM seq
    WHERE next_offense IS NOT NULL AND next_offense <> offense
      AND next_half = half
      AND exit_ytg IS NOT NULL
      AND next_start_ytg BETWEEN 1 AND 99
    GROUP BY 1, 2, 3
"""


def fetch_drive_pairs(conn, start: int, end: int) -> dict[tuple, int]:
    """(absorb_class, exit_zone, opp_zone) -> n, drive_result mapped through
    ABSORB_MAP; unmapped results simply don't contribute handoff mass."""
    with conn.cursor() as cur:
        cur.execute(
            DRIVE_PAIRS_QUERY,
            {"start": start, "end": end, "types": list(SCRIMMAGE_TYPES)},
        )
        rows = cur.fetchall()
    pairs: dict[tuple, int] = {}
    for dr, exit_zone, opp_zone, n in rows:
        cls = ABSORB_MAP.get(dr)
        if cls is None:
            continue
        key = (cls, int(exit_zone), int(opp_zone))
        pairs[key] = pairs.get(key, 0) + int(n)
    return pairs


def fetch_plays(conn, start: int, end: int) -> list[dict]:
    from psycopg2.extras import RealDictCursor

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(PLAYS_QUERY, {"start": start, "end": end, "types": list(SCRIMMAGE_TYPES)})
        return [dict(r) for r in cur.fetchall()]


def write_era(
    conn,
    era: str,
    transitions: Counter,
    shrunk: dict[tuple, float],
    ep: dict[str, float],
    ep_net: dict[str, float],
    absorb_probs: dict[tuple, float],
    se: dict[str, float],
) -> None:
    from psycopg2.extras import execute_values

    row_totals: Counter = Counter()
    for (a, _), n in transitions.items():
        row_totals[a] += n

    trans_rows = [
        (era, a, b, transitions.get((a, b), 0), transitions.get((a, b), 0) / row_totals[a], p)
        for (a, b), p in sorted(shrunk.items())
    ]
    state_rows = [
        (
            era,
            s,
            row_totals[s],
            ep[s],
            ep_net.get(s),
            absorb_probs[(s, "TD")],
            absorb_probs[(s, "FG")],
            absorb_probs[(s, "PUNT")],
            absorb_probs[(s, "TURNOVER")] + absorb_probs[(s, "TURNOVER_TD")],
            se.get(s),
        )
        for s in sorted(ep)
    ]

    with conn.cursor() as cur:
        cur.execute("DELETE FROM analytics.drive_chain_transitions WHERE era = %s", (era,))
        cur.execute("DELETE FROM analytics.ep_states WHERE era = %s", (era,))
        execute_values(
            cur,
            "INSERT INTO analytics.drive_chain_transitions "
            "(era, from_state, to_state, n, p_raw, p_shrunk) VALUES %s",
            trans_rows,
        )
        execute_values(
            cur,
            "INSERT INTO analytics.ep_states "
            "(era, state, n_obs, ep_drive, ep_net, p_td, p_fg, p_punt, p_turnover, se_boot) "
            "VALUES %s",
            state_rows,
        )
    conn.commit()
    logger.info("%s: wrote %d transitions, %d states", era, len(trans_rows), len(state_rows))


def run_era(conn, era: str, alpha: float, do_bootstrap: bool, do_validate: bool) -> int:
    start, end = ERAS[era]
    end = end if end is not None else 9999
    logger.info("Era %s: fetching scrimmage plays %d..%s", era, start, end)
    plays = fetch_plays(conn, start, end)
    logger.info("Era %s: %d plays", era, len(plays))
    if not plays:
        logger.error("Era %s: no plays -- is marts.play_epa refreshed?", era)
        return 1

    transitions, per_game, drive_outcomes, n_mapped, unmapped = build_transitions(plays)
    share = unmapped / max(1, n_mapped + unmapped)
    max_share = MAX_UNMAPPED_SHARE_BY_ERA.get(era, MAX_UNMAPPED_SHARE)
    logger.info(
        "Era %s: %d transitions, %d unmapped drives (%.2f%%)",
        era,
        len(transitions),
        unmapped,
        100 * share,
    )
    if share > max_share:
        logger.error("Era %s: unmapped drive share %.3f exceeds %.3f", era, share, max_share)
        return 1

    shrunk = shrink(transitions, alpha)
    ep, absorb_probs = solve_ep(shrunk)

    end_season = ERAS[era][1] if ERAS[era][1] is not None else 9999
    pairs = fetch_drive_pairs(conn, ERAS[era][0], end_season)
    handoffs = build_handoffs(pairs)
    ep_net = solve_net_ep(shrunk, handoffs)

    # Gates run BEFORE the write (PR #71 review, P1): write_era commits, so
    # validating afterwards published implausible values through
    # api.expected_points and merely exited nonzero -- the failed 2014-2020
    # zone-gate run of deploy 31257283280 did exactly that. A gated failure
    # now leaves the previously-published era untouched.
    if do_validate:
        vz = check_monotone_zone(ep)
        vd = check_monotone_down(ep)
        vnz = check_monotone_zone(ep_net)
        mae = calibration_mae(absorb_probs, drive_outcomes)
        for v in vz + vd:
            logger.warning("Era %s monotonicity: %s", era, v)
        for v in vnz:
            logger.warning("Era %s NET monotonicity: %s", era, v)
        net_own25 = ep_net.get("d1|standard|z8", float("nan"))
        # Net gate: from your own 25, the next score should favor the offense
        # but be worth far less than the drive basis (the opponent's counter-
        # value is netted out). Published CFB next-score EP sits ~0.3-1.2.
        net_sane = 0.0 < net_own25 < ep["d1|standard|z8"]
        print(
            f"EP_VALIDATION era={era} states={len(ep)} "
            f"monotone_zone={'pass' if not vz else 'FAIL'} "
            f"monotone_down={'pass' if not vd else 'FAIL'} "
            f"monotone_net={'pass' if not vnz else 'FAIL'} "
            f"net_own25={net_own25:.2f} net_sane={'pass' if net_sane else 'FAIL'} "
            f"calib_mae_td={mae:.4f}"
        )
        if vz or vd or vnz or not net_sane:
            logger.error(
                "Era %s: validation gate failed -- NOT writing; the previously "
                "published era is untouched",
                era,
            )
            return 1

    se = bootstrap_se(per_game, alpha) if do_bootstrap else {}
    write_era(conn, era, transitions, shrunk, ep, ep_net, absorb_probs, se)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Drive-chain EP model (P1)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full", action="store_true", help="All eras")
    group.add_argument("--era", choices=sorted(ERAS), help="Single era")
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA)
    parser.add_argument("--no-bootstrap", action="store_true")
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        eras = sorted(ERAS) if args.full else [args.era]
        rc = 0
        for era in eras:
            rc |= run_era(conn, era, args.alpha, not args.no_bootstrap, args.validate)
    finally:
        conn.close()
    sys.exit(rc)


if __name__ == "__main__":
    main()
