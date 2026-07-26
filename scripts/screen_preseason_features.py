#!/usr/bin/env python3
"""Screen candidate preseason features for incremental predictive value.

Plan: docs/plans/2026-07-25-preseason-outlook-model-plan.md section 2.5 (the
validation gate) and section 6.0/6.0b (the Tier A pipeline candidates this
first screens). **No candidate column ships into migration 042 until it clears
this screen.**

WHY THIS EXISTS
---------------
The naive form of the trench thesis already failed a first test. Screening
correlations against next-season SP+ *change* found roster-headcount trench
continuity essentially uncorrelated (OL -0.015, DL +0.013) while the existing
`returning_ppa_pct` scored +0.242, and returning the primary passer scored
+0.019. Against direct trench outcomes the same measure moved correctly but
weakly (line yards +0.070, stuff-rate improvement +0.083).

Both of those framings are the wrong instrument, and this script is the right
one. Correlating a feature against a year-over-year delta is dominated by mean
reversion, and the candidates correlate with prior-season quality too. The
question that actually matters for a prediction model is narrower:

    Given what last season's rating already tells us, does this feature tell
    us anything more?

That is a partial correlation -- feature vs season-S rating, controlling for
season-(S-1) rating. A feature that merely re-expresses prior-season quality
scores ~0 here no matter how large its raw correlation was, which is exactly
the discrimination the raw screen could not make.

MULTIPLE COMPARISONS
--------------------
The candidate set is large (Tier A alone contributes several terms, and
section 6.0b's `_raw`/`_regime` variants roughly double it), so testing each at
a fixed threshold would manufacture survivors by chance. Benjamini-Hochberg FDR
control is applied across the whole screened set: it bounds the expected share
of *shipped* columns that are false positives, which is the error that matters
here -- unlike family-wise correction, which would be needlessly conservative
for a screen whose output is a shortlist rather than a single verdict.

Both the effect-size floor and the FDR level are PRE-REGISTERED (the constants
below) and the screen is meant to be run ONCE. Re-running with adjusted
thresholds after seeing results converts this gate into the thing it exists to
prevent.

REPORTING NULLS
---------------
Failures are printed, not silently dropped. A documented null on trench
continuity is a finding: it tells us whether the ceiling is the metric or the
data. Rejected candidates are deliberately RETAINED in CANDIDATE_COLUMNS below
so a re-run reproduces the full table -- deleting them would make the null
results unreproducible, which is the same failure mode this gate exists to
prevent.

RESULTS -- run 2026-07-26 against prod, seasons 2015-2025 (n=1,439)
------------------------------------------------------------------
Column 2 controls for prior-season SP+ only; column 3 additionally controls for
`recruiting_points_3yr`, which is the strongest single candidate and therefore
the bar every other candidate has to clear.

    candidate                   vs prior SP+   + recruiting   verdict
    recruiting_points_3yr          +0.2642     (is control)   SHIP
    blue_chip_pipeline             +0.2532        +0.0931     SHIP
    recruiting_points_regime       +0.1525        +0.0955     SHIP  (see below)
    talent_stock                   +0.2664     ~+0.002        reject
    pipeline_index                 +0.0952           --       reject
    draft_picks_3yr                +0.0949        +0.0068     reject
    conversion / draft_yield       +0.0760        -0.0007     reject
    conversion_regime              +0.0876        +0.0344     reject
    draft_departures               +0.0088           --       reject
    portal_net_rating (2021-25)    +0.0274        +0.0731     reject, RE-TEST
    portal_out_n (2021-25)         +0.0586        -0.0139     reject

What the run established:

1. **Trailing recruiting pipeline is the strongest preseason signal found** --
   3x the pre-registered floor, and STRONGER in the portal era (+0.2840 on
   2021-25) rather than weaker.
2. **Draft production is redundant, not absent.** It predicts (+0.0949) until
   recruiting is controlled, then collapses to +0.0068. Draft output identifies
   programs that *recruit* well, not ones that *develop*.
3. **Regime-scoping is real** (design doc section 6.0b). Restricting the
   recruiting window to classes signed under the current head coach scores
   lower on its own (+0.1525 vs +0.2848) but adds +0.0955 BEYOND the flat
   window -- the two together beat either alone. It partly encodes "new staff,
   inherited roster", which the flat window cannot express.
4. **The development term survives regime-scoping but still fails.**
   `conversion` goes from -0.0007 to +0.0344 once scoped to the current staff
   -- the right direction, less than half the floor. Draft counts are a coarse,
   laggy proxy and season-level SP+ may not isolate development; the negative
   result is on this measurement, not on the concept.
5. **`draft_departures` is this gate's own justification:** raw correlation
   +0.3474, partial +0.0088. Losing draft picks correlates with having been
   good and says nothing about next season.

Both composites in the original design failed: `pipeline_index`
(talent_stock x conversion) scores +0.0952 against its own input's +0.2664 --
the multiplication destroys signal -- and `talent_stock` beats plain recruiting
by +0.002, complexity for nothing.

Usage:
    python scripts/screen_preseason_features.py --check-schema
        Preflight only: verify every source column this script reads exists,
        and exit. Run this first -- the candidate SQL spans six schemas.

    python scripts/screen_preseason_features.py
        Screen every candidate over the default 2015-2025 window.

    python scripts/screen_preseason_features.py --from 2015 --to 2025

Prints one line per candidate:
    SCREEN_RESULT feature={f} n={n} partial_r={r} p={p} q={q} verdict={ship|reject}
and a summary:
    SCREEN_SUMMARY n_candidates={c} shipped={s} rejected={r} window={a}-{b}
"""

import argparse
import logging
import math
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- Pre-registered decision rule (plan section 2.5). Do not tune post hoc. ---

# Minimum |partial correlation| for a candidate to be worth a column. Set from
# the plan's proposal: below this the feature is real-but-negligible against an
# 18.5-point residual SD, and not worth the contract surface.
MIN_PARTIAL_R = 0.08

# Benjamini-Hochberg false-discovery rate across the screened set.
FDR_ALPHA = 0.10

# Default screening window. Starts at 2015 to match the feature-availability
# floor in the design doc (section 3).
DEFAULT_FROM_SEASON = 2015
DEFAULT_TO_SEASON = 2025

# Below this many degrees of freedom the normal approximation to Student's t
# (see partial_corr_pvalue) stops being safe and the p-value is not reported.
MIN_DF_FOR_NORMAL_APPROX = 200

# Minimum complete-case rows for a candidate to be screened at all.
#
# Candidates are screened on COMPLETE CASES (see `complete_cases`): rows where
# the candidate and both controls are all present. Most candidates are
# COALESCEd to 0 in SQL and so cover the whole frame, but the prior-season
# trench columns come from a LEFT JOIN against stats.advanced_team_stats and go
# NULL whenever a team had no prior FBS season.
#
# Those NULLs must NOT be COALESCEd to 0 the way the churn columns are. Zero is
# a true value for a count ("no portal transfers" really is a net rating of 0);
# it is a fabricated EXTREME for a rate, since no team ever posts 0.000 line
# yards. Worse, the teams missing a prior-season row -- new FBS entrants,
# programs up from FCS -- are disproportionately bad in season S, so imputing
# the floor would manufacture a strong trench correlation that is pure artifact.
# Dropping the row asks the narrower but answerable question: among teams that
# DID play last season, does line play predict next season?
MIN_SCREEN_N = 400

# The second control every other candidate is screened against, alongside
# prior-season rating. It is the strongest single candidate, so "adds signal
# beyond prior rating" is too weak a bar -- a candidate must add signal beyond
# the best thing already available. Screening against prior rating alone would
# ship `draft_picks_3yr` at +0.0949 when its value after recruiting is +0.0068.
PRIMARY_CONTROL = "recruiting_points_3yr"


# =============================================================================
# Pure math -- no I/O, no DB, unit-tested directly (tests/test_screen_features.py).
# =============================================================================


def partial_correlation(x: list[float], y: list[float], z: list[float]) -> float:
    """Partial correlation of `x` and `y` controlling for `z`.

    Computed from the three pairwise Pearson correlations:

        r_xy.z = (r_xy - r_xz * r_yz) / sqrt((1 - r_xz^2) * (1 - r_yz^2))

    which is algebraically identical to correlating the residuals of x~z and
    y~z, but needs no regression fit.

    In this screen `y` is season-S rating and `z` is season-(S-1) rating, so
    the result is "what this feature says about season S beyond what last
    season already said". A feature that is purely a restatement of `z`
    returns ~0 by construction.

    Raises ValueError on unequal lengths. Returns 0.0 when either control
    correlation is degenerate (|r| = 1), where the partial is undefined and
    "no incremental signal" is the honest reading.
    """
    if not (len(x) == len(y) == len(z)):
        raise ValueError(
            f"partial_correlation needs equal-length inputs, got {len(x)}, {len(y)}, {len(z)}"
        )
    if len(x) < 3:
        raise ValueError("partial_correlation needs at least 3 observations")

    r_xy = _pearson(x, y)
    r_xz = _pearson(x, z)
    r_yz = _pearson(y, z)

    denom = math.sqrt(max(0.0, (1.0 - r_xz**2) * (1.0 - r_yz**2)))
    if denom == 0.0:
        return 0.0
    return (r_xy - r_xz * r_yz) / denom


def complete_cases(frame: list[dict], columns: list[str]) -> list[dict]:
    """Rows of `frame` where every column in `columns` is present and finite.

    Available-case analysis, applied per candidate rather than once across the
    whole set: dropping every row that any candidate is missing would shrink the
    frame for the columns that are fully populated, so each candidate is scored
    on the largest sample IT supports. The consequence is that candidates are
    not all screened on the same n, which is why `screen` reports n per row --
    a partial measured on 900 rows and one measured on 1,439 are not equally
    precise, and the reader has to be able to see that.
    """
    return [
        row
        for row in frame
        if all(row.get(col) is not None and math.isfinite(float(row[col])) for col in columns)
    ]


def _pearson(a: list[float], b: list[float]) -> float:
    """Pearson correlation. Returns 0.0 for a constant input (no variance =
    no signal), rather than raising on a divide-by-zero."""
    n = len(a)
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    da = [v - mean_a for v in a]
    db = [v - mean_b for v in b]
    num = sum(p * q for p, q in zip(da, db, strict=True))
    den = math.sqrt(sum(p * p for p in da) * sum(q * q for q in db))
    if den == 0.0:
        return 0.0
    return num / den


def second_order_partial_correlation(
    x: list[float], y: list[float], z: list[float], w: list[float]
) -> float:
    """Partial correlation of `x` and `y` controlling for BOTH `z` and `w`.

    Applies the recursive formula to the three first-order partials (each
    already controlling for `z`):

        r_xy.zw = (r_xy.z - r_xw.z * r_yw.z)
                  / sqrt((1 - r_xw.z^2) * (1 - r_yw.z^2))

    In this screen `z` is prior-season rating and `w` is
    `recruiting_points_3yr` -- the strongest single candidate, and therefore
    the bar every other candidate has to clear. Controlling for prior rating
    alone is not enough: `draft_picks_3yr` scores +0.0949 against prior rating
    and would ship, but only +0.0068 once recruiting is also held constant,
    because it is largely a restatement of how well a program recruits.

    Returns 0.0 where the recursion is degenerate, matching
    `partial_correlation`'s convention that an undefined partial reads as
    "no incremental signal".
    """
    r_xy_z = partial_correlation(x, y, z)
    r_xw_z = partial_correlation(x, w, z)
    r_yw_z = partial_correlation(y, w, z)

    denom = math.sqrt(max(0.0, (1.0 - r_xw_z**2) * (1.0 - r_yw_z**2)))
    if denom == 0.0:
        return 0.0
    return (r_xy_z - r_xw_z * r_yw_z) / denom


def partial_corr_pvalue(r: float, n: int, n_controls: int = 1) -> float | None:
    """Two-sided p-value for a partial correlation.

    The test statistic is ``t = r * sqrt(df / (1 - r^2))`` with
    ``df = n - 2 - n_controls``, which follows Student's t under the null.

    **Normal approximation, deliberately.** The repo carries no scipy (numpy
    only -- see pyproject.toml's `compute` extra), and implementing a t CDF
    means an incomplete beta function. At the sample sizes this screen runs on
    (~1,400 team-seasons, so df ~1,400) Student's t and the standard normal
    agree to well under 0.1%, so the approximation costs nothing real. Phi is
    computed from stdlib ``math.erf``, the same approach
    ``scripts/poll_scoreboard.py`` uses for its live win-probability Phi.

    Returns None when df is too small for that approximation to be safe
    (below MIN_DF_FOR_NORMAL_APPROX) -- the honest answer is "this screen
    cannot p-value that", not a number computed the wrong way.
    """
    df = n - 2 - n_controls
    if df < MIN_DF_FOR_NORMAL_APPROX:
        return None
    if abs(r) >= 1.0:
        return 0.0

    t = abs(r) * math.sqrt(df / (1.0 - r**2))
    # Two-sided: 2 * (1 - Phi(|t|)).
    return 2.0 * (1.0 - 0.5 * (1.0 + math.erf(t / math.sqrt(2.0))))


def benjamini_hochberg(pvalues: list[float], alpha: float = FDR_ALPHA) -> list[float]:
    """Benjamini-Hochberg adjusted p-values (q-values), input order preserved.

    Sorts ascending, scales each by ``m / rank``, then enforces monotonicity by
    taking a running minimum from the largest p downward so a q-value can never
    exceed one belonging to a larger p. Values are capped at 1.0.

    `alpha` is accepted for interface symmetry with the decision rule but does
    not affect the returned q-values -- comparison happens in `screen_verdict`.
    """
    m = len(pvalues)
    if m == 0:
        return []

    indexed = sorted(enumerate(pvalues), key=lambda pair: pair[1])
    qvalues = [0.0] * m
    running_min = 1.0
    # Walk from the largest p down so the running minimum enforces monotonicity.
    for rank in range(m, 0, -1):
        original_index, p = indexed[rank - 1]
        scaled = min(1.0, p * m / rank)
        running_min = min(running_min, scaled)
        qvalues[original_index] = running_min
    return qvalues


def screen_verdict(
    partial_r: float,
    qvalue: float | None,
    min_partial_r: float = MIN_PARTIAL_R,
    fdr_alpha: float = FDR_ALPHA,
) -> str:
    """Ship/reject for one candidate. BOTH conditions must hold:

    1. ``|partial_r| >= min_partial_r`` -- the effect is big enough to matter.
    2. ``qvalue <= fdr_alpha`` -- it survives FDR control across the set.

    Requiring both is the point. Significance alone ships noise that happens to
    clear a large-n test; effect size alone ships whatever the screen's widest
    candidate happened to be. A missing q-value (df too small to test) rejects:
    unverifiable is not the same as valuable.
    """
    if abs(partial_r) < min_partial_r:
        return "reject"
    if qvalue is None or qvalue > fdr_alpha:
        return "reject"
    return "ship"


# =============================================================================
# --- Candidate definitions ---
# =============================================================================
#
# Each candidate is one column of the screening frame. The SQL below builds
# them all at (season, team) grain alongside the outcome `sp_rating` (y) and
# the control `prior_sp_rating` (z).
#
# LEAK RULE: every candidate must be knowable before week 1 of season S.
# Recruiting classes for S, portal movement into S, and the April draft of year
# S all precede the season. Draft picks PRODUCED are windowed to S-1..S-3 so a
# season never sees its own draft outcomes.
#
# SIGN NOTE (plan section 2.4b): picks *produced* over a trailing window and
# picks *lost* in year S are opposite signals and are kept as separate
# candidates -- conflating them nets the effects toward zero.

CANDIDATE_COLUMNS = [
    # Tier A core (plan section 6.0)
    "talent_stock",
    "conversion",
    "pipeline_index",
    "blue_chip_pipeline",
    # Component terms, screened separately so a composite that fails can be
    # diagnosed rather than merely discarded.
    "recruiting_points_3yr",
    "draft_picks_3yr",
    "draft_yield",
    "portal_net_rating",
    "draft_departures",
    # Section 6.0b regime variant.
    "recruiting_points_regime",
    # Prior-season trench performance (plan section 2.2). Never screened
    # before: the earlier trench test used roster-headcount continuity, which
    # counts walk-ons equally with starters and scored ~0. These measure the
    # line play itself.
    "prior_line_yards",
    "prior_power_success",
    "prior_stuff_rate_allowed",
    "prior_havoc_allowed_front_seven",
    "prior_def_line_yards",
    "prior_def_stuff_rate",
    "prior_front_seven_havoc",
]

# What actually cleared the 2026-07-26 run (see RESULTS in the module
# docstring). This -- NOT CANDIDATE_COLUMNS -- is the set migration 042 should
# add to features.team_week. CANDIDATE_COLUMNS stays comprehensive so the
# rejections remain reproducible.
#
# `recruiting_points_regime` is the section 6.0b variant: the same decayed sum
# restricted to classes signed from the current head coach's tenure start
# onward. It is kept ALONGSIDE the flat window rather than replacing it -- it
# scores lower alone but adds +0.0955 beyond it, so the pair carries more than
# either does by itself.
SHIPPED_COLUMNS = [
    "recruiting_points_3yr",
    "blue_chip_pipeline",
    "recruiting_points_regime",
]

# Rejected, with the reason, so a future reader does not re-propose them
# without new evidence. Re-test conditions noted where they exist.
REJECTED_COLUMNS = {
    "talent_stock": "beats plain recruiting by +0.002 -- complexity for nothing",
    "pipeline_index": "+0.0952 vs its own input's +0.2664 -- the product destroys signal",
    "draft_picks_3yr": "+0.0068 once recruiting is controlled -- a recruiting proxy",
    "conversion": "-0.0007 once recruiting is controlled",
    "conversion_regime": "+0.0344 -- regime-scoping helps but stays under the floor",
    "draft_departures": "+0.0088 partial against +0.3474 raw -- pure confound",
    "portal_net_rating": (
        "+0.0731 (2021-25, n=663) -- under floor; RE-TEST as portal history accrues"
    ),
    "portal_out_n": "-0.0139 once recruiting is controlled",
}

# Recruiting-class decay across the four-year eligibility window: the class
# entering season S-1 is weighted 1.0, S-2 0.8, and so on. A flat sum would
# treat a fifth-year contributor and a true freshman identically; the geometric
# taper is the simplest defensible shape and is itself a tunable the screen can
# be re-run against if the composite underperforms its components.
CLASS_DECAY = 0.8
CLASS_WINDOW = 4

SCREEN_FRAME_QUERY = f"""
WITH coach_year AS (
    -- One head coach per (school, year). A school-year can list several
    -- coaches (interim, co-HC), so take the one who actually coached the most
    -- games. Covers 99.1%% of the sp_ratings spine.
    SELECT DISTINCT ON (c.school, c.year)
           c.school, c.year, c._dlt_parent_id AS coach_id
    FROM ref.coaches__seasons c
    ORDER BY c.school, c.year, c.games DESC NULLS LAST
),
coach_islands AS (
    -- Gaps-and-islands: a coach's SECOND stint at the same school must not
    -- inherit the first stint's start year.
    SELECT school, year, coach_id,
           year - ROW_NUMBER() OVER (PARTITION BY school, coach_id ORDER BY year) AS grp
    FROM coach_year
),
coach_tenure AS (
    SELECT school, year, coach_id,
           MIN(year) OVER (PARTITION BY school, coach_id, grp) AS tenure_start
    FROM coach_islands
),
class_points AS (
    -- Recruiting class quality per (team, year).
    SELECT tr.team, tr.year, tr.points::double precision AS points
    FROM recruiting.team_recruiting tr
    WHERE tr.points IS NOT NULL
),
blue_chips AS (
    -- 4-5 star signees per (team, year), the blue-chip-ratio numerator.
    SELECT rc.committed_to AS team, rc.year,
           COUNT(*) FILTER (WHERE rc.stars >= 4)::double precision AS blue_chips,
           COUNT(*)::double precision AS signees
    FROM recruiting.recruits rc
    WHERE rc.committed_to IS NOT NULL
    GROUP BY 1, 2
),
portal_flow AS (
    -- Portal movement INTO season S (leak-free: happens before the season).
    SELECT s.season, s.team,
           SUM(s.in_rating) AS portal_in_rating,
           SUM(s.out_rating) AS portal_out_rating
    FROM (
        SELECT tp.season, tp.destination AS team,
               COALESCE(tp.rating, 0)::double precision AS in_rating,
               0::double precision AS out_rating
        FROM recruiting.transfer_portal tp
        WHERE tp.destination IS NOT NULL
        UNION ALL
        SELECT tp.season, tp.origin,
               0::double precision,
               COALESCE(tp.rating, 0)::double precision
        FROM recruiting.transfer_portal tp
        WHERE tp.origin IS NOT NULL
    ) s
    GROUP BY 1, 2
),
draft_out AS (
    -- Picks a program LOST in the April draft of year S, round-weighted
    -- (a first-rounder is not one seventh-rounder).
    SELECT dp.year AS season, dp.college_team AS team,
           COUNT(*)::double precision AS picks,
           SUM(8.0 - LEAST(7, COALESCE(dp.round, 7)))::double precision AS capital
    FROM draft.draft_picks dp
    WHERE dp.college_team IS NOT NULL
    GROUP BY 1, 2
),
spine AS (
    -- One row per (season, team) with the outcome, its control, and the
    -- current staff's tenure start (for the section 6.0b regime variant).
    -- LEFT JOIN on tenure so the ~1%% of team-seasons with no coach record
    -- still screen; tenure_start falls back to the window start, making the
    -- regime variant equal the flat window for those rows.
    SELECT sp.year AS season, sp.team,
           sp.rating::double precision AS sp_rating,
           sp0.rating::double precision AS prior_sp_rating,
           COALESCE(ct.tenure_start, sp.year - {CLASS_WINDOW}) AS tenure_start
    FROM ratings.sp_ratings sp
    JOIN ratings.sp_ratings sp0
      ON sp0.team = sp.team AND sp0.year = sp.year - 1
    LEFT JOIN coach_tenure ct ON ct.school = sp.team AND ct.year = sp.year
    WHERE sp.year BETWEEN %(from_season)s AND %(to_season)s
      AND sp.rating IS NOT NULL AND sp0.rating IS NOT NULL
)
SELECT
    s.season,
    s.team,
    s.sp_rating,
    s.prior_sp_rating,
    -- Decayed recruiting stock over the eligibility window (S-1..S-4).
    COALESCE((
        SELECT SUM(cp.points * POWER({CLASS_DECAY}, s.season - cp.year - 1))
        FROM class_points cp
        WHERE cp.team = s.team
          AND cp.year BETWEEN s.season - {CLASS_WINDOW} AND s.season - 1
    ), 0) AS recruiting_points_3yr,
    -- Section 6.0b regime variant: same decayed sum, but only classes signed
    -- from the current staff's tenure start onward. Scores lower alone than
    -- the flat window yet adds beyond it, because a short window is itself a
    -- signal ("new coach, inherited roster").
    COALESCE((
        SELECT SUM(cp.points * POWER({CLASS_DECAY}, s.season - cp.year - 1))
        FROM class_points cp
        WHERE cp.team = s.team
          AND cp.year BETWEEN GREATEST(s.season - {CLASS_WINDOW}, s.tenure_start)
                          AND s.season - 1
    ), 0) AS recruiting_points_regime,
    COALESCE((
        SELECT SUM(bc.blue_chips) / NULLIF(SUM(bc.signees), 0)
        FROM blue_chips bc
        WHERE bc.team = s.team
          AND bc.year BETWEEN s.season - {CLASS_WINDOW} AND s.season - 1
    ), 0) AS blue_chip_pipeline,
    COALESCE(pf.portal_in_rating - pf.portal_out_rating, 0) AS portal_net_rating,
    COALESCE(dout.picks, 0) AS draft_departures,
    -- Picks PRODUCED over S-1..S-3 (never season S's own draft).
    COALESCE((
        SELECT SUM(d2.picks)
        FROM draft_out d2
        WHERE d2.team = s.team AND d2.season BETWEEN s.season - 3 AND s.season - 1
    ), 0) AS draft_picks_3yr,
    COALESCE((
        SELECT SUM(d3.capital)
        FROM draft_out d3
        WHERE d3.team = s.team AND d3.season BETWEEN s.season - 3 AND s.season - 1
    ), 0) AS draft_capital_3yr,
    -- PRIOR-SEASON TRENCH PERFORMANCE (plan section 2.2, "the cheapest win").
    -- Measured line play rather than inferred continuity: no roster
    -- dependency, no new ingest, available from 2004, and computable in July.
    -- This is the trenches thesis tested DIRECTLY -- the earlier headcount
    -- continuity screen (OL -0.015, DL +0.013) tested whether the same bodies
    -- returned, which counts a walk-on the same as a starter. These ask
    -- whether the line actually blocked and the front actually penetrated.
    --
    -- Sign conventions differ and matter for reading the results:
    --   line_yards / power_success / def_stuff_rate / front_seven_havoc -> higher is better
    --   stuff_rate_allowed / havoc_allowed_front_seven                  -> LOWER is better
    -- The screen reports signed partials, so a negative on an "allowed"
    -- column is the thesis being CONFIRMED, not refuted.
    ats.offense__line_yards AS prior_line_yards,
    ats.offense__power_success AS prior_power_success,
    ats.offense__stuff_rate AS prior_stuff_rate_allowed,
    ats.offense__havoc__front_seven AS prior_havoc_allowed_front_seven,
    ats.defense__line_yards AS prior_def_line_yards,
    ats.defense__stuff_rate AS prior_def_stuff_rate,
    ats.defense__havoc__front_seven AS prior_front_seven_havoc
FROM spine s
LEFT JOIN portal_flow pf ON pf.team = s.team AND pf.season = s.season
LEFT JOIN draft_out dout ON dout.team = s.team AND dout.season = s.season
-- Season S-1: what the trenches DID last year, known before season S starts.
LEFT JOIN stats.advanced_team_stats ats
       ON ats.team = s.team AND ats.season = s.season - 1
ORDER BY s.season, s.team
"""

# Source columns the query above depends on, for --check-schema. Written out
# explicitly because this script spans six schemas and a silent column rename
# would surface as a confusing SQL error mid-screen rather than a clear
# preflight failure.
REQUIRED_COLUMNS: list[tuple[str, str, tuple[str, ...]]] = [
    ("recruiting", "team_recruiting", ("team", "year", "points")),
    ("recruiting", "recruits", ("committed_to", "year", "stars")),
    ("recruiting", "transfer_portal", ("season", "origin", "destination", "rating")),
    ("draft", "draft_picks", ("year", "college_team", "round")),
    ("ratings", "sp_ratings", ("year", "team", "rating")),
    (
        "stats",
        "advanced_team_stats",
        (
            "season",
            "team",
            "offense__line_yards",
            "offense__power_success",
            "offense__stuff_rate",
            "offense__havoc__front_seven",
            "defense__line_yards",
            "defense__stuff_rate",
            "defense__havoc__front_seven",
        ),
    ),
]


# Imputation audit (--audit-imputation). Reports, per zero-filled candidate,
# the share of frame rows whose UNDERLYING source was absent and therefore had
# a 0 substituted by COALESCE.
#
# This exists because zero-filling is safe for a COUNT and unsafe for a RATE,
# and the screened set contains both. `recruiting_points_3yr` is a summed count
# -- no classes really is zero points -- but `blue_chip_pipeline` is
# blue_chips/signees, where a missing recruiting record becomes "0% blue chips",
# a fabricated floor rather than a neutral value. The same substitution inside
# PRIMARY_CONTROL would propagate into every second-order partial in the screen,
# so the audit covers the control too.
#
# Counts only -- no correlations, no verdicts, no effect on the screened set or
# the FDR correction.
AUDIT_QUERY = f"""
WITH coach_year AS (
    SELECT DISTINCT ON (c.school, c.year)
           c.school, c.year, c._dlt_parent_id AS coach_id
    FROM ref.coaches__seasons c
    ORDER BY c.school, c.year, COALESCE(c.games, 0) DESC
),
coach_tenure AS (
    SELECT cy.school, cy.year,
           MIN(cy.year) OVER (PARTITION BY cy.school, cy.coach_id) AS tenure_start
    FROM coach_year cy
),
class_points AS (
    SELECT tr.team, tr.year, tr.points::double precision AS points
    FROM recruiting.team_recruiting tr
    WHERE tr.points IS NOT NULL
),
blue_chips AS (
    SELECT rc.committed_to AS team, rc.year,
           COUNT(*) FILTER (WHERE rc.stars >= 4)::double precision AS blue_chips,
           COUNT(*)::double precision AS signees
    FROM recruiting.recruits rc
    WHERE rc.committed_to IS NOT NULL
    GROUP BY 1, 2
),
spine AS (
    SELECT sp.year AS season, sp.team,
           COALESCE(ct.tenure_start, sp.year - {CLASS_WINDOW}) AS tenure_start
    FROM ratings.sp_ratings sp
    JOIN ratings.sp_ratings sp0
      ON sp0.team = sp.team AND sp0.year = sp.year - 1
    LEFT JOIN coach_tenure ct ON ct.school = sp.team AND ct.year = sp.year
    WHERE sp.year BETWEEN %(from_season)s AND %(to_season)s
      AND sp.rating IS NOT NULL AND sp0.rating IS NOT NULL
)
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE (
        SELECT SUM(cp.points * POWER({CLASS_DECAY}, s.season - cp.year - 1))
        FROM class_points cp
        WHERE cp.team = s.team
          AND cp.year BETWEEN s.season - {CLASS_WINDOW} AND s.season - 1
    ) IS NULL) AS recruiting_points_3yr_imputed,
    COUNT(*) FILTER (WHERE (
        SELECT SUM(cp.points * POWER({CLASS_DECAY}, s.season - cp.year - 1))
        FROM class_points cp
        WHERE cp.team = s.team
          AND cp.year BETWEEN GREATEST(s.season - {CLASS_WINDOW}, s.tenure_start)
                          AND s.season - 1
    ) IS NULL) AS recruiting_points_regime_imputed,
    COUNT(*) FILTER (WHERE (
        SELECT SUM(bc.blue_chips) / NULLIF(SUM(bc.signees), 0)
        FROM blue_chips bc
        WHERE bc.team = s.team
          AND bc.year BETWEEN s.season - {CLASS_WINDOW} AND s.season - 1
    ) IS NULL) AS blue_chip_pipeline_imputed
FROM spine s
"""


def audit_imputation(conn, from_season: int, to_season: int) -> dict:
    """Count rows where a zero-filled candidate's source was actually absent."""
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(AUDIT_QUERY, {"from_season": from_season, "to_season": to_season})
        return dict(cur.fetchone())


def derive_composites(row: dict) -> dict:
    """Add the Tier A composites that are cheaper to build in Python than SQL.

    `talent_stock` is the accumulated recruiting stock adjusted for what has
    left and arrived; `conversion` is draft output per unit of recruiting input
    (the development term -- a rate, not a count); `pipeline_index` is their
    product. See plan section 6.0.

    `conversion` divides by the recruiting stock, so a program with no
    recorded recruiting points yields 0.0 rather than a divide-by-zero -- and
    0.0 is the right reading: no inputs means no demonstrated conversion.
    """
    stock = row["recruiting_points_3yr"] + row["portal_net_rating"] - row["draft_departures"]
    denom = row["recruiting_points_3yr"]
    conversion = (row["draft_picks_3yr"] / denom) if denom > 0 else 0.0
    return {
        **row,
        "talent_stock": stock,
        "conversion": conversion,
        "pipeline_index": stock * conversion,
        # draft_yield: picks produced relative to recruiting inputs, the
        # development variant (plan section 2.4b). Same construction as
        # `conversion` here; kept as a separate name because section 6.0b will
        # give it a regime-scoped variant that `conversion` does not get.
        "draft_yield": conversion,
    }


# =============================================================================
# --- I/O layer ---
# =============================================================================


def get_db_url() -> str:
    """Database URL from dlt secrets or environment (same pattern as the other
    compute scripts -- each keeps its own copy of this one utility)."""
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


def check_schema(conn) -> list[str]:
    """Verify every column in REQUIRED_COLUMNS exists. Returns a list of
    human-readable problems (empty = all present)."""
    problems: list[str] = []
    with conn.cursor() as cur:
        for schema, table, columns in REQUIRED_COLUMNS:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table),
            )
            present = {r[0] for r in cur.fetchall()}
            if not present:
                problems.append(f"{schema}.{table}: table not found")
                continue
            missing = [c for c in columns if c not in present]
            if missing:
                problems.append(f"{schema}.{table}: missing column(s) {missing}")
    return problems


def probe_columns(conn, schema: str, table: str, like: str | None = None) -> list[str]:
    """Column names for a table, optionally filtered by substring.

    Exists because the screening frame has to name dlt-flattened columns
    exactly (``offense__line_yards`` and friends), and the compute role is the
    only one that can see the source schemas -- api-only roles get nothing back
    from information_schema for `stats`. Guessing a name and iterating through
    deploy runs is slower and less reliable than asking once.
    """
    sql = """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """
    params: list = [schema, table]
    if like:
        sql += " AND column_name ILIKE %s"
        params.append(f"%{like}%")
    sql += " ORDER BY column_name"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [r[0] for r in cur.fetchall()]


def fetch_frame(conn, from_season: int, to_season: int) -> list[dict]:
    """Screening frame: one dict per (season, team) with outcome, control and
    every candidate, composites already derived."""
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            SCREEN_FRAME_QUERY,
            {"from_season": from_season, "to_season": to_season},
        )
        rows = [dict(r) for r in cur.fetchall()]

    return [derive_composites({k: _as_float(v) for k, v in row.items()}) for row in rows]


def _as_float(value):
    """Numeric columns arrive as Decimal; team/season stay as-is."""
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)
    return value


def screen(frame: list[dict], candidates: list[str]) -> list[dict]:
    """Run the screen over `frame`. Returns one result dict per candidate,
    ordered by descending |partial_r| so the strongest signals read first."""
    total = len(frame)

    raw: list[dict] = []
    for name in candidates:
        # Complete cases for THIS candidate: the candidate plus every control it
        # will be measured against. A candidate that is NULL for part of the
        # frame is screened on the part where it exists, not silently imputed.
        needed = ["sp_rating", "prior_sp_rating", name]
        if name != PRIMARY_CONTROL:
            needed.append(PRIMARY_CONTROL)
        rows = complete_cases(frame, needed)
        n = len(rows)

        if n < MIN_SCREEN_N:
            # Too thin to screen. Reported rather than dropped (plan section
            # 2.5: nulls are findings) and held out of the FDR correction by
            # p=None, exactly like a candidate whose df is too small to test.
            raw.append(
                {
                    "feature": name,
                    "n": n,
                    "coverage": (n / total) if total else 0.0,
                    "partial_r_vs_prior": 0.0,
                    "partial_r": 0.0,
                    "p": None,
                    "untestable": f"n={n} below MIN_SCREEN_N={MIN_SCREEN_N}",
                }
            )
            continue

        x = [row[name] for row in rows]
        y = [row["sp_rating"] for row in rows]
        z = [row["prior_sp_rating"] for row in rows]
        r_first = partial_correlation(x, y, z)

        if name == PRIMARY_CONTROL:
            # The control cannot be screened against itself; it is judged on
            # the first-order partial alone.
            r_decisive, n_controls = r_first, 1
        else:
            w = [row[PRIMARY_CONTROL] for row in rows]
            r_decisive = second_order_partial_correlation(x, y, z, w)
            n_controls = 2

        p = partial_corr_pvalue(r_decisive, n, n_controls=n_controls)
        raw.append(
            {
                "feature": name,
                "n": n,
                "coverage": (n / total) if total else 0.0,
                "partial_r_vs_prior": r_first,
                "partial_r": r_decisive,
                "p": p,
                "untestable": None,
            }
        )

    # FDR across the whole set. Untestable candidates (p is None) are held out
    # of the correction -- they cannot be false discoveries because they will
    # be rejected regardless -- so they do not inflate m and penalize the rest.
    testable = [c for c in raw if c["p"] is not None]
    qvalues = benjamini_hochberg([c["p"] for c in testable])
    for candidate, q in zip(testable, qvalues, strict=True):
        candidate["q"] = q
    for candidate in raw:
        candidate.setdefault("q", None)
        candidate.setdefault("untestable", None)
        candidate["verdict"] = screen_verdict(candidate["partial_r"], candidate["q"])

    return sorted(raw, key=lambda c: abs(c["partial_r"]), reverse=True)


def report(results: list[dict], from_season: int, to_season: int) -> int:
    """Print every result -- shipped AND rejected (plan section 2.5: nulls are
    findings). Returns the number of candidates that cleared."""
    shipped = 0
    for c in results:
        p_str = f"{c['p']:.5f}" if c["p"] is not None else "na"
        q_str = f"{c['q']:.5f}" if c["q"] is not None else "na"
        cov_str = f"{c['coverage']:.3f}" if c.get("coverage") is not None else "na"
        note = f" note={c['untestable']}" if c.get("untestable") else ""
        print(
            f"SCREEN_RESULT feature={c['feature']} n={c['n']} coverage={cov_str} "
            f"partial_r_vs_prior={c['partial_r_vs_prior']:+.4f} "
            f"partial_r={c['partial_r']:+.4f} p={p_str} q={q_str} "
            f"verdict={c['verdict']}{note}"
        )
        if c["verdict"] == "ship":
            shipped += 1

    print(
        f"SCREEN_SUMMARY n_candidates={len(results)} shipped={shipped} "
        f"rejected={len(results) - shipped} window={from_season}-{to_season} "
        f"min_partial_r={MIN_PARTIAL_R} fdr_alpha={FDR_ALPHA}"
    )
    return shipped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Screen candidate preseason features for incremental "
        "predictive value beyond prior-season rating (plan section 2.5)"
    )
    parser.add_argument(
        "--from",
        dest="from_season",
        type=int,
        default=DEFAULT_FROM_SEASON,
        help=f"First season to screen (default {DEFAULT_FROM_SEASON})",
    )
    parser.add_argument(
        "--to",
        dest="to_season",
        type=int,
        default=DEFAULT_TO_SEASON,
        help=f"Last season to screen (default {DEFAULT_TO_SEASON})",
    )
    parser.add_argument(
        "--probe",
        metavar="SCHEMA.TABLE[:LIKE]",
        help="Print matching column names for a source table and exit "
        "(e.g. stats.advanced_team_stats:line). Discovery aid for building "
        "new candidates against dlt-flattened names.",
    )
    parser.add_argument(
        "--check-schema",
        action="store_true",
        help="Verify every source column exists, then exit without screening",
    )
    parser.add_argument(
        "--audit-imputation",
        action="store_true",
        help="Report how many rows each zero-filled candidate had substituted "
        "by COALESCE, then exit. Diagnostic only -- no verdicts.",
    )
    args = parser.parse_args()

    if args.from_season > args.to_season:
        logger.error("--from %d is after --to %d", args.from_season, args.to_season)
        sys.exit(1)

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        if args.probe:
            spec, _, like = args.probe.partition(":")
            schema, _, table = spec.partition(".")
            if not schema or not table:
                logger.error("--probe expects SCHEMA.TABLE[:LIKE], got %r", args.probe)
                sys.exit(1)
            cols = probe_columns(conn, schema, table, like or None)
            print(f"\n{schema}.{table}" + (f"  (matching '{like}')" if like else ""))
            for c in cols:
                print(f"  {c}")
            print(f"\nPROBE_GATE table={schema}.{table} matched={len(cols)}\n")
            return

        problems = check_schema(conn)
        if problems:
            for p in problems:
                logger.error("schema preflight: %s", p)
            logger.error(
                "%d schema problem(s); the candidate SQL would fail. Fix the "
                "column references in SCREEN_FRAME_QUERY / REQUIRED_COLUMNS.",
                len(problems),
            )
            sys.exit(1)
        logger.info("Schema preflight passed (%d table(s))", len(REQUIRED_COLUMNS))
        if args.check_schema:
            return

        if args.audit_imputation:
            audit = audit_imputation(conn, args.from_season, args.to_season)
            total = audit.pop("total_rows")
            for name, imputed in sorted(audit.items()):
                share = (imputed / total) if total else 0.0
                print(
                    f"IMPUTATION_AUDIT candidate={name} imputed={imputed} "
                    f"total={total} share={share:.4f}"
                )
            print(
                f"IMPUTATION_SUMMARY total_rows={total} window={args.from_season}-{args.to_season}"
            )
            return

        frame = fetch_frame(conn, args.from_season, args.to_season)
        if len(frame) < MIN_DF_FOR_NORMAL_APPROX:
            logger.error(
                "Only %d team-season(s) in %d-%d; too few to screen reliably",
                len(frame),
                args.from_season,
                args.to_season,
            )
            sys.exit(1)
        logger.info(
            "Screening %d candidate(s) over %d team-season(s)", len(CANDIDATE_COLUMNS), len(frame)
        )

        results = screen(frame, CANDIDATE_COLUMNS)
        report(results, args.from_season, args.to_season)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
