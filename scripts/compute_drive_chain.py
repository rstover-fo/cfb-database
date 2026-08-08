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


def solve_ep(probs: dict[tuple, float]) -> tuple[dict[str, float], dict[tuple, float]]:
    """Solve the absorbing chain: EP per transient state + absorption probs.

    EP = (I - Q)^-1 R v with Q transient->transient, R transient->absorbing,
    v the ABSORB_VALUES vector. Returns (ep by state, absorption probability
    by (state, absorbing)).
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
        # A target that is neither transient nor absorbing cannot occur:
        # shrink() only emits observed/parent targets, all of which are one
        # of the two.

    # Fundamental matrix; (I - Q) is invertible because every state reaches
    # an absorbing outcome (every drive ends).
    N = np.linalg.solve(np.eye(nt) - Q, np.eye(nt))
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
            "(era, state, n_obs, ep_drive, p_td, p_fg, p_punt, p_turnover, se_boot) VALUES %s",
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
    se = bootstrap_se(per_game, alpha) if do_bootstrap else {}

    write_era(conn, era, transitions, shrunk, ep, absorb_probs, se)

    if do_validate:
        vz = check_monotone_zone(ep)
        vd = check_monotone_down(ep)
        mae = calibration_mae(absorb_probs, drive_outcomes)
        for v in vz + vd:
            logger.warning("Era %s monotonicity: %s", era, v)
        print(
            f"EP_VALIDATION era={era} states={len(ep)} "
            f"monotone_zone={'pass' if not vz else 'FAIL'} "
            f"monotone_down={'pass' if not vd else 'FAIL'} "
            f"calib_mae_td={mae:.4f}"
        )
        if vz or vd:
            return 1
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
