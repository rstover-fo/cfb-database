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

RESULTS -- run 2026-07-26 against prod, seasons 2015-2025
--------------------------------------------------------
Produced by THIS SCRIPT (deploy run 124). Every figure below is reproducible
by re-running it, which the earlier recorded table was not: two unescaped
percent signs meant the frame query had never once executed, so the original
verdicts came from ad-hoc MCP queries that no longer exist. Where the two
disagree, these numbers are the ones that stand.

Column 2 controls for prior-season SP+ only; column 3 additionally controls for
`recruiting_points_3yr`, which is the strongest single candidate and therefore
the bar every other candidate has to clear. `n`/`coverage` are per candidate:
each is screened on its own complete cases (see `complete_cases`).

    candidate                       n    cov   vs prior   +recr   verdict
    recruiting_points_3yr        1439  1.000    +0.2642  (ctrl)   SHIP
    hc_first_year_unproven       1322  0.919    -0.1800  -0.1844  ship (held)
    hc_first_year                1324  0.920    -0.1231  -0.1548  SHIP
    hc_first_year_prior_below    1322  0.919    -0.1195  -0.1345  ship (pooled)
    hc_first_year_rookie         1324  0.920    -0.1365  -0.1340  ship (pooled)
    prior_def_line_yards         1423  0.989    -0.0463  -0.0997  SHIP
    prior_def_stuff_rate         1423  0.989    +0.0536  +0.0816  SHIP (marginal)
    blue_chip_pipeline           1439  1.000    +0.2532  +0.0782  reject (see below)
    talent_stock                 1439  1.000    +0.2664  +0.0744  reject
    draft_departures             1439  1.000    +0.0088  -0.0728  reject
    recruiting_points_regime     1057  0.735    +0.0807  -0.0597  reject
    prior_power_success          1423  0.989    +0.0221  +0.0520  reject
    portal_net_rating            1439  1.000    +0.0247  +0.0481  reject
    prior_stuff_rate_allowed     1423  0.989    -0.0021  -0.0290  reject
    prior_line_yards             1423  0.989    -0.0069  +0.0223  reject
    pipeline_index               1439  1.000    +0.0952  +0.0072  reject
    draft_picks_3yr              1439  1.000    +0.0949  +0.0068  reject
    conversion / draft_yield     1439  1.000    +0.0760  -0.0007  reject
    hc_first_year_proven         1322  0.919    +0.0557  +0.0096  reject (null)
    hc_career_prior               113  0.079        --       --   untestable
    prior_havoc_allowed_front_7   251  0.174        --       --   untestable
    prior_front_seven_havoc       251  0.174        --       --   untestable

The five `hc_*` rows other than the flat binary come from deploy run 138, which
screened them on the same frame; every other figure is run 124's. Run 138
reproduced run 124 to four decimals on all eighteen candidates they share, so
the two sets are comparable. `ship (pooled)` and `ship (held)` are screen verdicts the
owner has not acted on -- see SUPERSEDED_COLUMNS and finding 8.

What the run established:

1. **Trailing recruiting pipeline is the strongest preseason signal found** --
   3x the pre-registered floor.
2. **The trenches thesis half-survives, and it is the DEFENSIVE half.**
   `prior_def_line_yards` (-0.0997; the column is yards ALLOWED, so negative
   confirms the thesis) and `prior_def_stuff_rate` (+0.0816) both clear. Every
   offensive-line measure fails: line yards +0.0223, power success +0.0520,
   stuff rate allowed -0.0290. Measured line play succeeds where the earlier
   roster-headcount continuity screen (OL -0.015, DL +0.013) found nothing.
3. **A first-year head coach is the second-strongest signal in the set**
   (-0.1548, q < 1e-5), and it was previously invisible. It only became
   measurable once it was separated from `recruiting_points_regime` -- see 4.
4. **The regime column's original +0.0955 was not a recruiting signal.** The
   regime window is empty exactly when the head coach is in year one, and
   zero-filling put a hard 0 on 291 of 1,439 rows (20.2%, found by
   --audit-imputation). That 0 is confounded with the coaching change itself,
   so the column silently blended recruiting with a new-coach indicator. Split
   apart, the recruiting half falls to -0.0597 and FAILS while the coaching
   half ships at -0.1548. A column had shipped on a number that measured
   something other than what the column claimed to measure.
5. **WITHDRAWN** (was: draft production is redundant, not absent, +0.0949 ->
   +0.0068). The column is a fabricated zero on 54.2% of rows -- see DEFECT
   FOUND BY THE SPLIT below. Unadjudicated, not rejected.
6. **WITHDRAWN** (was: `draft_departures` raw +0.3474, partial -0.0728, sign
   flipped). Same defect, 45.1% fabricated zeros. The gate's own justification
   has to come from a column that was actually measured.
7. **Two candidates are untestable, not null.** The havoc front-seven splits
   exist for only 17.4% of team-seasons, below MIN_SCREEN_N. That is a
   data-coverage finding; the thesis they would test is unadjudicated.
8. **The first-year penalty is not a penalty for changing coaches** (run 138).
   Split by the incoming coach's career record, all of it lives on one side:

       subgroup            positives   partial   implied gap (SD)
       unproven hire             184   -0.1844               0.53
         of which rookie         151   -0.1340               0.42
         of which prior-below     33   -0.1345               0.86
       proven hire                80   +0.0096               0.04
       flat binary               266   -0.1548               0.39

   A hire whose previous teams averaged at or above an average FBS team costs
   essentially nothing (+0.0096, p=0.73, SE ~0.028 -- powered enough to exclude
   the unproven effect). Pooling the two halves is what produced the flat
   binary's -0.1548: averaging a real penalty with a null understates the
   penalty by about 0.03 of partial correlation.

   The "implied gap" column is |r| / sqrt(p(1-p)), the standardized mean
   difference a point-biserial of that size implies at that prevalence. It is
   the reason `hc_first_year_prior_below` is pooled rather than shipped: a
   0.86-SD effect on 33 team-seasons is the largest per-hire number in the set
   and the one most likely to be a handful of rows.

   The cut was chosen after looking at subgroup means, so the AMENDMENT's
   split-window check was run immediately (runs 139 and 140):

       candidate                  2015-2025   2015-2020   2021-2025
       hc_first_year_unproven       -0.1844     -0.1417     -0.2257
       hc_first_year (flat)         -0.1548     -0.1332     -0.1738
       hc_first_year_prior_below    -0.1345     -0.1196     -0.1489
       hc_first_year_rookie         -0.1340     -0.0968     -0.1688
       hc_first_year_proven         +0.0096     -0.0155     +0.0368
       n (coach columns)               1322         730         592

   `unproven` beats the flat binary in every window, and `proven` is null in
   every window (all |r| < 0.04, all p > 0.37). Neither window is strictly
   held out -- the cut was chosen on data that includes both -- but the effect
   is LARGER in the portal era, not smaller, which is the opposite of what
   selection bias alone would produce.

   The coaching penalty is also growing: flat -0.1332 -> -0.1738 across the
   era break, unproven -0.1417 -> -0.2257. A plausible mechanism is that a new
   staff now loses the inherited roster through the portal rather than merely
   inheriting less recruiting momentum, but this screen does not test that.

Both composites in the original design failed: `pipeline_index`
(talent_stock x conversion) scores +0.0952 against its own input's +0.2664 --
the multiplication destroys signal -- and `talent_stock` (+0.0744) does not
clear the floor either.

RESOLVED (runs 139/140). The ad-hoc `blue_chip_pipeline` figure of +0.0931 was
a 2021-2025 number recorded in a 2015-2025 table. Run 140 reproduces it to four
decimals on the 2021-2025 window (+0.0931, n=663), and reproduces the plan's
recorded portal-era `portal_net_rating` pair (+0.0274 / +0.0731, n=663)
exactly. The full-window value really is +0.0782; the two numbers were never in
conflict, they were measuring different windows. `talent_stock`'s ad-hoc ~+0.002
still does not reconcile with any window (+0.0744 full, +0.1081 early, +0.0884
portal-era) and stays unexplained.

DEFECT FOUND BY THE SPLIT (runs 139/140) -- every draft verdict is void
-----------------------------------------------------------------------
Run 139 returned partial_r EXACTLY +0.0000 with p=1.00000 for
`draft_picks_3yr`, `conversion`, `draft_yield` and `pipeline_index`. Four
columns cannot land on exactly zero; they were CONSTANT.

`draft.draft_picks` holds 2020-2026 only -- 1,806 rows, seven drafts -- though
`years.py` configures the source for 2000-2026. It is a load gap, not a source
limit. `draft_out` is not year-filtered, so nothing errored: the S-1..S-3
lookback simply found no rows and `COALESCE(..., 0)` turned "this draft was
never ingested" into "this program produced zero NFL picks".

Measured against the 2015-2025 spine:

    column               fabricated zeros        total    share
    draft_picks_3yr      780 (all of 2015-2020)   1439    54.2%
    draft_departures     649 (all of 2015-2019)   1439    45.1%

So the recorded rejections of `draft_picks_3yr` (+0.0068), `conversion` /
`draft_yield` (-0.0007), `pipeline_index` (+0.0072), `draft_departures`
(-0.0728) and `talent_stock` (+0.0744) are NOT measurements of those
constructs. They are measurements of a column that is a fabricated zero on
roughly half its rows, which biases every one of them toward zero. Findings 5
and 6 above are withdrawn: draft production is UNTESTED here, not redundant,
and `conversion` -- the development term, and the half of the stated thesis
this gate was built to adjudicate -- was never actually put to the test.

This is the `recruiting_points_regime` failure again (finding 4) at twice the
scale, and --audit-imputation missed it because the audit has counters for
recruiting and blue-chip zero-fill but none for draft.

What it does NOT touch: the coaching columns (no draft input), the `prior_*`
line-play columns (advanced_team_stats, 98.9% coverage), and
`recruiting_points_3yr` (0.8% imputed). Every SHIPPED column is unaffected.

Re-test conditions, in order:
1. Backfill `draft.draft_picks` for 2000-2019 -- the source is configured for
   it and the flat-file nflverse mirror (1967+) is a second path. Note
   `meta.flat_file_loads` does not exist in prod, so the flat-file loader has
   never completed a run there either.
2. Add draft coverage counters to AUDIT_QUERY so a future gap fails loudly.
3. Re-run the screen. Only then do the draft verdicts mean anything.

Consequence for the section 6 oracle pre-test: it needs drafts S+1..S+3, so
against 2020-2026 data it can only run for S in 2019-2023 -- five seasons, not
the 2015-2022 the plan assumes. Backfill first or the pre-test inherits this
same defect.

AMENDMENT -- the regime-scoped coaching variants (screened, run 138)
--------------------------------------------------------------------
Five candidates were added AFTER run 124 and are now adjudicated by run 138;
see finding 8 and SUPERSEDED_COLUMNS. They decompose the shipped flat binary
`hc_first_year` by the incoming coach's career record (see the SQL).

Adding candidates after seeing results is a real cost and is stated here rather
than hidden. What is and is not compromised:

* NOT compromised: the decision rule. MIN_PARTIAL_R and FDR_ALPHA are unchanged,
  the new columns are screened by the same code on the same frame, and BH runs
  across the enlarged set, so their q-values already pay for the extra tests.
* Compromised: the *definitions* were chosen after an ad-hoc look at the
  subgroup means, so their effect sizes are optimistically biased in a way BH
  does not correct. BH controls false discovery across a family of tests; it
  does not correct a statistic for having chosen the cut that produced it. The
  answer to that is out-of-sample, not more correction -- re-run with
  `--from 2015 --to 2020` and `--from 2021 --to 2025` and see whether the split
  survives in a window it was not chosen on.
* Watch out: q-values are a property of the SET, not of a candidate. Every
  q printed by the next run differs from run 124's, including the q=0.0095
  quoted in SHIPPED_BY_DECISION. Recorded VERDICTS are unaffected -- every
  rejection above failed on the effect floor, not on q, and every ship had
  q <= 0.0095 against a 0.10 alpha -- but the numbers are not comparable across
  runs with different candidate sets, and enlarging the set does not uniformly
  raise q: BH scales by m/rank, so a strong new candidate that outranks an
  existing one raises its rank denominator too.

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

# The line between a "proven" and a "previously below-average" head-coaching
# hire, on the SP+ scale.
#
# SP+ is centred so an average FBS team rates 0.0. The threshold is therefore a
# property of the measurement -- "his previous teams averaged better than an
# average FBS team" -- and not a cut-point chosen to make a subgroup look good.
# Pre-registered on the same footing as MIN_PARTIAL_R: moving it after seeing a
# result is exactly the manoeuvre this gate exists to prevent.
#
# It assumes the centring actually holds on this frame. --audit-imputation
# reports `frame_prior_sp_at_or_above_zero`, which should sit near half the
# frame; a share far from 0.5 means the scale is not centred where this constant
# assumes and the split is measuring something other than "above average".
HC_PROVEN_SP_PLUS = 0.0


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


def check_one_row_per_team_season(frame: list[dict]) -> None:
    """Raise unless `frame` has exactly one row per (season, team).

    The screening frame is a (season, team) spine that candidates hang off, and
    every partial in this script assumes one observation per team-season. A join
    that fans out would not fail -- it would silently re-weight the duplicated
    teams in EVERY candidate at once, including the ones already on the record,
    and the only visible symptom would be a larger n.

    Cheap enough to run on every fetch, and it is checked rather than argued
    because the argument is exactly the kind that stays convincing after it
    stops being true: each new CTE is joined on a key that is unique *today*.
    """
    keys = {(row["season"], row["team"]) for row in frame}
    if len(keys) != len(frame):
        raise RuntimeError(
            f"screening frame has {len(frame)} rows for {len(keys)} team-seasons -- "
            "a join fanned out and every candidate's partial is now weighted by "
            "duplicate teams"
        )


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
    # Section 6.0b regime variant, plus the coaching-change indicator it used
    # to absorb via zero-fill (see the SQL and the 2026-07-26 audit).
    "recruiting_points_regime",
    "hc_first_year",
    # Section 6.0b second pass: the flat first-year binary decomposed by the
    # incoming coach's career record. Mutually exclusive by construction --
    # rookie + prior_below = unproven, and unproven + proven = hc_first_year --
    # so screening all four says WHERE the penalty lives rather than only that
    # it exists. `hc_career_prior` is the continuous form, defined only where a
    # career prior exists, and is expected to fall under MIN_SCREEN_N.
    "hc_first_year_rookie",
    "hc_first_year_prior_below",
    "hc_first_year_unproven",
    "hc_first_year_proven",
    "hc_career_prior",
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

# What the screen itself ships (deploy run 124; see RESULTS in the module
# docstring). CANDIDATE_COLUMNS stays comprehensive so the rejections remain
# reproducible.
#
# This list is exactly the set with verdict == "ship", so `screen()` reproduces
# it. Anything shipped for a reason the screen did not produce belongs in
# SHIPPED_BY_DECISION below, never here -- collapsing the two is how the gate
# stops being a gate.
SHIPPED_COLUMNS = [
    "recruiting_points_3yr",
    "hc_first_year_unproven",
    "prior_def_line_yards",
    "prior_def_stuff_rate",
    # Migration 047, shipped on the owner's call once the 2000-2019 backfill
    # made them measurable. Two columns rather than one net, because the signs
    # are opposite: picks PRODUCED over S-1..S-3 at +0.0834, picks LOST in year
    # S at -0.0925. Both earlier rejections were measured on fabricated zeros.
    "draft_picks_3yr",
    "draft_departures",
]

# Cleared the screen, awaiting the owner's ship call. Distinct from SUPERSEDED
# (held back for a structural reason I can decide myself) and from PENDING
# (not yet measured): these carry real numbers and nothing about the data
# argues against them. What they need is a migration, a rebuild, a retrain and
# a re-backtest, and section 2.5 selection is advisory -- the screen produces
# the number, the owner rules.
#
# Emptied when the draft pair shipped; kept as the mechanism for the next
# column that clears the floor between deploys.
AWAITING_SHIP_DECISION: dict[str, str] = {}

# Shipped by human decision DESPITE a reject verdict, with the argument on the
# record. Migration 042 adds SHIPPED_COLUMNS + these.
#
# Kept separate so the override is legible as an override. The screen's job is
# to produce a number and apply a pre-registered rule; overruling the rule is a
# judgment the owner is entitled to make, but it must not be laundered into
# looking like a measurement.
SHIPPED_BY_DECISION = {
    "blue_chip_pipeline": (
        "+0.0782 vs a 0.08 floor -- short by 0.07 standard errors at n=1,439, "
        "which the data cannot distinguish, and q=0.0095 says the effect is real. "
        "The floor separates 'matters' from 'negligible against an 18.5-point "
        "residual SD'; it was never meant to arbitrate the third decimal. "
        "STRENGTHENED by run 140: on 2021-2025 it clears the floor outright at "
        "+0.0931 (q=0.045, n=663), so the full-window shortfall is an average "
        "over an era where it was weaker, not a null. The override stands, and "
        "the reason it needed to be an override has narrowed."
    ),
}

# Rejected, with the reason, so a future reader does not re-propose them
# without new evidence. Re-test conditions noted where they exist.
REJECTED_COLUMNS = {
    "conversion": (
        "+0.0709 on real data (was a VOID -0.0007 on 54% fabricated zeros). "
        "This is the DEVELOPMENT term -- draft picks produced over S-1..S-3, "
        "residualized on the same window's recruiting -- and the honest "
        "reading is that it is real but under the floor. q=0.0101 says the "
        "effect exists; 0.0709 is 0.0091 short of the 0.08 bar, about a third "
        "of a standard error at n=1,439, which the data cannot resolve. "
        "Rejected on the pre-registered rule rather than on the evidence. "
        "Note it is also largely what the second-order partial already does to "
        "draft_picks_3yr (+0.0834), so shipping both would mostly duplicate."
    ),
    "draft_yield": "+0.0709 -- identical to conversion by construction",
    "recruiting_points_regime": (
        "-0.0597 on its 1,057 complete cases. Its earlier +0.0955 was an artifact of "
        "zero-filling 291 first-year-coach rows; that signal was the coaching change, "
        "and it now ships separately as hc_first_year (-0.1548)."
    ),
    "prior_line_yards": "+0.0223 -- offensive line play carries no signal past recruiting",
    "prior_power_success": "+0.0520 -- under the floor",
    "prior_stuff_rate_allowed": "-0.0290 -- under the floor",
    "portal_net_rating": (
        "+0.0481 -- under floor; RE-TEST as portal history accrues. Caveat: it is "
        "COALESCEd to 0 across the full window, but a pre-2021 zero means 'the portal "
        "did not exist', not 'net zero movement', so the measurement is contaminated "
        "the same way the trench columns were. Re-test on 2021+ complete cases."
    ),
    "hc_first_year_proven": (
        "+0.0096, p=0.73 (n=1,322, 80 positives) -- and this null is the point "
        "of the decomposition, not a leftover from it. A first-year hire whose "
        "previous teams averaged at or above an average FBS team costs his new "
        "team essentially nothing in year one, once prior SP+ and recruiting "
        "are controlled. At n=1,322 the standard error is about 0.028, so the "
        "interval excludes anything near the -0.1844 measured on unproven "
        "hires: this is a powered null, not an underpowered shrug. The "
        "first-year penalty is not a penalty for changing coaches -- it is a "
        "penalty for hiring someone without a record."
    ),
}

# Screened but not scored: too few complete cases to test at all. Recorded
# because "we could not measure this" is a different finding from "we measured
# it and it was nothing", and only the second one closes a question.
UNTESTABLE_COLUMNS = {
    "prior_havoc_allowed_front_seven": "n=251 (17.4% coverage) -- below MIN_SCREEN_N",
    "prior_front_seven_havoc": "n=251 (17.4% coverage) -- below MIN_SCREEN_N",
    "hc_career_prior": (
        "n=113 (7.9% coverage) -- below MIN_SCREEN_N, as the column's own "
        "rationale predicted. It is defined only on first-year hires with a "
        "ratable head-coaching record, which is 113 of 1,439 team-seasons. "
        "This is the reproducible answer to 'why not just use the continuous "
        "career prior instead of indicators': there is not enough of it."
    ),
}

# Cleared the screen and deliberately NOT shipped, because a column that IS
# shipped is a linear combination of it. Not a rejection: each of these beat
# the floor with q < 1e-5, and calling that "rejected" would make the record
# say the evidence went the other way.
#
# The mirror image of SHIPPED_BY_DECISION. There the screen said reject and a
# human shipped; here the screen said ship and a human held it back. Both are
# judgments layered on top of a measurement, and both are only honest if the
# measurement is recorded next to the judgment rather than replaced by it.
SUPERSEDED_COLUMNS = {
    "talent_stock": (
        "+0.0811 -- cleared the floor once the draft backfill landed, and is "
        "NOT shipped. It is recruiting + portal_in - draft_departures - "
        "portal_out, so every term in it is either the control or a column "
        "that ships on its own. It adds an arithmetic combination, not a "
        "construct, and its +0.0811 sits between its own components."
    ),
    "pipeline_index": (
        "+0.0846 -- cleared the floor after the backfill, reversing the "
        "recorded finding that the product destroys signal (that was measured "
        "on a conversion term made of fabricated zeros). Still NOT shipped: it "
        "is talent_stock x conversion, so it is a product of one superseded "
        "column and one rejected one, and it cannot be interpreted separately "
        "from draft_picks_3yr, which ships at a statistically indistinguishable "
        "+0.0834."
    ),
    "hc_first_year_rookie": (
        "-0.1340 (n=1,324, 151 positives) -- real, but an exact component of "
        "hc_first_year_unproven, which scores better. Shipping both would put "
        "rookie + prior_below = unproven into the vector as an exact linear "
        "dependence."
    ),
    "hc_first_year_prior_below": (
        "-0.1345 (n=1,322, 33 positives) -- the largest per-hire effect in the "
        "set and the least trustworthy number in it. At 2.5% prevalence a "
        "-0.1345 point-biserial implies a standardized gap near 0.86, roughly "
        "twice the rookie effect, resting on 33 team-seasons. Pooled into "
        "hc_first_year_unproven rather than shipped alone, because a "
        "coefficient fit on 33 rows would be applied with false confidence."
    ),
    "hc_first_year": (
        "-0.1548 (n=1,324, 266 positives) -- real, and it shipped in migration "
        "042. SUPERSEDED by hc_first_year_unproven (-0.1844) in migration 046, "
        "which is the same indicator with the proven hires taken out. The flat "
        "binary's number is the two subgroups averaged, and averaging a real "
        "penalty with a powered null costs about 0.03 of partial correlation. "
        "The column stays POPULATED in features.team_week -- it is cheap and "
        "downstream consumers may read it -- it just no longer enters the fit."
    ),
}

# Added to the candidate set but NOT YET SCREENED. A pending entry states what
# the candidate is for; it must not state what it will score.
#
# This bucket exists because the alternative is worse. The bookkeeping test
# requires every candidate to carry exactly one recorded verdict, and the only
# ways to satisfy it without running the screen are to guess a verdict or to
# leave the candidate out of the set -- the first fabricates a measurement, the
# second hides that the family being FDR-corrected has grown. An explicit
# "screened, unadjudicated" state says the true thing.
#
# On a run, each of these moves to SHIPPED_COLUMNS, REJECTED_COLUMNS,
# UNTESTABLE_COLUMNS or SUPERSEDED_COLUMNS with the number the screen produced,
# and is deleted here. Emptied by deploy run 138; kept as the mechanism for the
# next candidate added between runs.
PENDING_COLUMNS: dict[str, str] = {}

# Recruiting-class decay across the four-year eligibility window: the class
# entering season S-1 is weighted 1.0, S-2 0.8, and so on. A flat sum would
# treat a fifth-year contributor and a true freshman identically; the geometric
# taper is the simplest defensible shape and is itself a tunable the screen can
# be re-run against if the composite underperforms its components.
CLASS_DECAY = 0.8
CLASS_WINDOW = 4

# Head-coach tenure start per (school, year), and the incoming coach's career
# prior (coach_season / coach_prior, appended for the section 6.0b second pass).
# SHARED between the screen and the
# imputation audit -- defined once because the audit's whole job is to describe
# the frame the screen builds, and a hand-copied second version of this logic
# silently disagreed with it (PR #54 review): without the gaps-and-islands
# grouping a returning coach's second stint inherits the FIRST stint's start
# year, widening the regime window, so the audit found classes where the screen
# saw none and under-reported absent regime values.
_COACH_TENURE_CTE = """
coach_counts AS (
    -- How many coaches CFBD lists for a school-year. >1 means a mid-season
    -- change, and those school-years are EXCLUDED below -- see coach_tenure.
    SELECT school, year, COUNT(*) AS n_coaches
    FROM ref.coaches__seasons
    GROUP BY school, year
),
coach_year AS (
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
    -- LEAK GUARD (PR #55 review, P1). tenure_start is NULL for any school-year
    -- CFBD lists more than one coach for.
    --
    -- ref.coaches__seasons attributes a whole season to each coach it lists and
    -- cannot split a mid-season change (documented in
    -- src/schemas/api/038_coach_records.sql). Picking the most-games coach then
    -- means an early firing -- where the replacement finishes with more games
    -- than the man he replaced -- resolves to the REPLACEMENT, whose tenure
    -- starts that year, marking week 1 as "first-year coach" when it was not.
    --
    -- That is a leak, not a rounding error: a mid-season firing is CAUSED by
    -- season-S performance, so the flag would carry the season's own outcome
    -- into a feature that claims to be preseason-known. Worse, the bias runs
    -- one way -- the earlier the firing, the more games the replacement gets,
    -- and the worse the season was.
    --
    -- Measured on 2015-2025: 103 of 1,438 school-years are ambiguous (7.2%%),
    -- and 24 of the 300 hc_first_year=1 cases came from them (8.0%% of the
    -- positives). Excluded rather than guessed -- an unambiguous school-year
    -- has exactly one coach, who by definition started the season.
    SELECT i.school, i.year, i.coach_id,
           CASE WHEN cc.n_coaches > 1 THEN NULL
                ELSE MIN(i.year) OVER (PARTITION BY i.school, i.coach_id, i.grp)
           END AS tenure_start
    FROM coach_islands i
    JOIN coach_counts cc ON cc.school = i.school AND cc.year = i.year
),
coach_season AS (
    -- Every school-year that is UNAMBIGUOUSLY one coach's, carrying that
    -- team's season-final SP+ where the warehouse has one. Raw material for a
    -- hired coach's CAREER PRIOR (see coach_prior).
    --
    -- Ambiguous school-years are excluded for the same reason coach_tenure
    -- nulls them, though not for the same danger. Here it is an attribution
    -- error rather than a leak -- these are seasons at other schools, in years
    -- before season S -- but it runs one way: an interim promoted into a
    -- collapse inherits the whole season's rating and would then be scored
    -- "previously below average" for a season he did not run.
    --
    -- The cost of that exclusion, stated plainly: a coach whose entire prior
    -- record is ambiguous school-years reads here as having NO prior seasons,
    -- so hc_first_year_rookie will call him a first-time head coach. That is
    -- the intended trade. "No season he unambiguously ran" is the closest thing
    -- the source supports to "unproven", and the alternative credits him with
    -- somebody else's season.
    --
    -- rating is LEFT JOINed rather than required. A season with no SP+ row (an
    -- FCS stop, or a year older than the warehouse's ratings coverage) is still
    -- head-coaching EXPERIENCE; it just cannot be rated. Holding those two
    -- facts apart is what lets a genuine first-time head coach be told from a
    -- coach whose record simply cannot be scored -- one is a rookie, the other
    -- is unknown, and collapsing them would put a fabricated category where a
    -- missing value belongs.
    SELECT cy.coach_id, cy.school, cy.year,
           sp.rating::double precision AS rating
    FROM coach_year cy
    JOIN coach_counts cc ON cc.school = cy.school AND cc.year = cy.year
    LEFT JOIN ratings.sp_ratings sp
           ON sp.team = cy.school AND sp.year = cy.year
    WHERE cc.n_coaches = 1
),
coach_prior AS (
    -- Career prior of the coach listed at (school, year): what his teams did
    -- in seasons STRICTLY BEFORE that year, at any school.
    --
    -- LEAK RULE. `prev.year < cy.year` is strict, so season S can never enter
    -- -- not his row at this school, which is the outcome, and not any other.
    -- Season S-1 IS included, and that is correct rather than sloppy: S-1 ended
    -- in January of year S and its final SP+ is published months before week 1,
    -- so the hiring school knew it.
    --
    -- The seasons averaged are almost always at OTHER schools, but not by
    -- construction: a coach returning to team T for a second stint is a
    -- first-year coach under the gaps-and-islands tenure rule, and his earlier
    -- stint at T does enter his prior. That is still strictly before season S,
    -- so it is not a leak -- it is old news about T, of the same kind the
    -- prior-season control already carries. What it CANNOT include is T's
    -- season S-1: a coach listed at (T, S-1) in coach_year would make the
    -- island contiguous and so would not be a first-year coach at all.
    --
    -- prior_seasons counts experience and is never NULL. prior_sp_mean averages
    -- only seasons that have a rating and is NULL when none do -- AVG skips
    -- NULLs, so an unratable stop neither drags the mean down nor counts as a
    -- rated season. No value is substituted anywhere for a coach without a
    -- record; the absence is carried as an absence.
    --
    -- GROUP BY (cy.school, cy.year) on a coach_year that is already unique on
    -- that pair yields exactly one row per school-year, so joining this to the
    -- screening spine adds columns and cannot add rows. Every other candidate's
    -- n is unchanged by it.
    SELECT cy.school, cy.year,
           COUNT(prev.year) AS prior_seasons,
           AVG(prev.rating) AS prior_sp_mean
    FROM coach_year cy
    LEFT JOIN coach_season prev
           ON prev.coach_id = cy.coach_id
          AND prev.year < cy.year
    GROUP BY cy.school, cy.year
)"""

SCREEN_FRAME_QUERY = f"""
WITH {_COACH_TENURE_CTE},
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
    --
    -- tenure_start is deliberately NOT coalesced. The earlier fallback to the
    -- window start made the regime variant silently equal the flat window for
    -- the ~1%% of team-seasons with no coach record, so those rows entered the
    -- screen as duplicates of a different candidate. NULL here propagates to
    -- both regime columns below and drops the row from THEIR complete cases
    -- only, leaving every other candidate on the full frame.
    SELECT sp.year AS season, sp.team,
           sp.rating::double precision AS sp_rating,
           sp0.rating::double precision AS prior_sp_rating,
           ct.tenure_start AS tenure_start,
           cp.prior_seasons AS hc_prior_seasons,
           cp.prior_sp_mean AS hc_prior_sp_mean
    FROM ratings.sp_ratings sp
    JOIN ratings.sp_ratings sp0
      ON sp0.team = sp.team AND sp0.year = sp.year - 1
    LEFT JOIN coach_tenure ct ON ct.school = sp.team AND ct.year = sp.year
    -- One row per (school, year) by construction (see coach_prior), so this
    -- join widens the spine without lengthening it.
    LEFT JOIN coach_prior cp ON cp.school = sp.team AND cp.year = sp.year
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
    -- from the current staff's tenure start onward.
    --
    -- NOT zero-filled, and the reason is the 2026-07-26 imputation audit: the
    -- previous COALESCE(...,0) hit 291 of 1,439 rows (20.2%%). The regime window
    -- GREATEST(season-4, tenure_start)..season-1 is EMPTY exactly when
    -- tenure_start >= season -- a first-year head coach -- so a fifth of the
    -- sample carried a hard 0 that meant "no classes signed by this staff yet",
    -- not "this staff recruits badly". Worse, that 0 is confounded with the
    -- coaching change itself, since programs that just fired a coach were
    -- usually bad, so the column silently blended a continuous recruiting term
    -- with a de facto new-coach indicator and its partial could not be
    -- attributed to either.
    --
    -- The two are now separated: this column measures recruiting on the rows
    -- where the staff has actually signed classes, and `hc_first_year` below
    -- carries the coaching-change effect explicitly where it can be read.
    --
    -- CASE rather than a bare NULL tenure_start: Postgres GREATEST IGNORES
    -- NULL arguments, so GREATEST(season-4, NULL) returns season-4 and would
    -- quietly restore the flat window instead of yielding NULL.
    CASE WHEN s.tenure_start IS NULL THEN NULL ELSE (
        SELECT SUM(cp.points * POWER({CLASS_DECAY}, s.season - cp.year - 1))
        FROM class_points cp
        WHERE cp.team = s.team
          AND cp.year BETWEEN GREATEST(s.season - {CLASS_WINDOW}, s.tenure_start)
                          AND s.season - 1
    ) END AS recruiting_points_regime,
    -- The coaching-change effect the regime column used to absorb, as its own
    -- screened candidate. NULL where no coach record exists, so it is never
    -- inferred from missing data.
    CASE WHEN s.tenure_start IS NULL THEN NULL
         WHEN s.tenure_start >= s.season THEN 1.0
         ELSE 0.0 END AS hc_first_year,
    -- SECTION 6.0b, SECOND PASS -- the first-year penalty is not flat.
    --
    -- hc_first_year is an indicator on the coaching change alone, so it forces
    -- one number onto three different situations: a proven hire inheriting a
    -- roster, a first-time head coach, and a hire whose previous teams were
    -- below average. The columns below decompose it, so the screen can report
    -- where the penalty lives instead of averaging across it.
    --
    -- NO FABRICATED CONSTANT, and that is the design constraint. A first-time
    -- head coach has no career prior, and there is no value on the SP+ scale
    -- that means "none": a hand-picked -8.0 asserts he is terrible, and a 0.0
    -- asserts he is exactly average, and the scale cannot tell either apart
    -- from a measurement. That is the error the zero-filled regime window
    -- already made here once. So "no prior record" is carried as its own
    -- INDICATOR, where absence needs no number, and the continuous prior below
    -- is defined only where a prior actually exists.
    --
    -- Each column is 0.0 for a continuing staff, and 0.0 is a TRUE value there:
    -- there was no hire, so the hire was not unproven. NULL appears only where
    -- something is genuinely unknown -- an ambiguous school-year (no tenure),
    -- or a hire whose head-coaching seasons the warehouse cannot rate.
    --
    -- Invariants, over rows where the terms are non-NULL:
    --     rookie + prior_below = unproven
    --     unproven + proven    = hc_first_year
    CASE WHEN s.tenure_start IS NULL OR s.hc_prior_seasons IS NULL THEN NULL
         WHEN s.tenure_start < s.season THEN 0.0
         WHEN s.hc_prior_seasons = 0 THEN 1.0
         ELSE 0.0 END AS hc_first_year_rookie,
    CASE WHEN s.tenure_start IS NULL OR s.hc_prior_seasons IS NULL THEN NULL
         WHEN s.tenure_start < s.season THEN 0.0
         WHEN s.hc_prior_seasons = 0 THEN 0.0
         WHEN s.hc_prior_sp_mean IS NULL THEN NULL
         WHEN s.hc_prior_sp_mean < {HC_PROVEN_SP_PLUS} THEN 1.0
         ELSE 0.0 END AS hc_first_year_prior_below,
    CASE WHEN s.tenure_start IS NULL OR s.hc_prior_seasons IS NULL THEN NULL
         WHEN s.tenure_start < s.season THEN 0.0
         WHEN s.hc_prior_seasons = 0 THEN 1.0
         WHEN s.hc_prior_sp_mean IS NULL THEN NULL
         WHEN s.hc_prior_sp_mean < {HC_PROVEN_SP_PLUS} THEN 1.0
         ELSE 0.0 END AS hc_first_year_unproven,
    CASE WHEN s.tenure_start IS NULL OR s.hc_prior_seasons IS NULL THEN NULL
         WHEN s.tenure_start < s.season THEN 0.0
         WHEN s.hc_prior_seasons = 0 THEN 0.0
         WHEN s.hc_prior_sp_mean IS NULL THEN NULL
         WHEN s.hc_prior_sp_mean >= {HC_PROVEN_SP_PLUS} THEN 1.0
         ELSE 0.0 END AS hc_first_year_proven,
    -- The continuous form, for contrast. NULL for a continuing staff (no hire,
    -- so no incoming prior) and NULL for a first-time head coach (no prior to
    -- report) -- which leaves it defined on first-year hires with a ratable
    -- record only, a few hundred rows at most. The screen will say so itself
    -- through MIN_SCREEN_N rather than being told.
    CASE WHEN s.tenure_start IS NULL THEN NULL
         WHEN s.tenure_start < s.season THEN NULL
         ELSE s.hc_prior_sp_mean END AS hc_career_prior,
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
    # dlt child table of ref.coaches. `_dlt_parent_id` is the coach identity the
    # tenure and career-prior CTEs group on -- ref.coaches is merged on
    # (first_name, last_name), so one parent row is one coach across every
    # school he has worked at.
    ("ref", "coaches__seasons", ("school", "year", "games", "_dlt_parent_id")),
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
WITH {_COACH_TENURE_CTE},
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
draft_out AS (
    -- Which draft YEARS exist at all. Deliberately the same shape as
    -- SCREEN_FRAME_QUERY's draft_out so the coverage counters below describe
    -- the source the screen actually reads, not an approximation of it.
    SELECT dp.year AS season, dp.college_team AS team
    FROM draft.draft_picks dp
    WHERE dp.college_team IS NOT NULL
    GROUP BY 1, 2
),
spine AS (
    -- Mirrors SCREEN_FRAME_QUERY's spine, including the uncoalesced
    -- tenure_start, so the audit measures the frame the screen actually builds.
    SELECT sp.year AS season, sp.team,
           sp0.rating::double precision AS prior_sp_rating,
           ct.tenure_start AS tenure_start,
           cp.prior_seasons AS hc_prior_seasons,
           cp.prior_sp_mean AS hc_prior_sp_mean
    FROM ratings.sp_ratings sp
    JOIN ratings.sp_ratings sp0
      ON sp0.team = sp.team AND sp0.year = sp.year - 1
    LEFT JOIN coach_tenure ct ON ct.school = sp.team AND ct.year = sp.year
    LEFT JOIN coach_prior cp ON cp.school = sp.team AND cp.year = sp.year
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
    -- Now a coverage figure, not an imputation one: these rows are DROPPED
    -- from the regime column's complete cases rather than zero-filled. The
    -- CASE mirrors the frame query, since GREATEST ignores NULL arguments.
    COUNT(*) FILTER (WHERE (
        CASE WHEN s.tenure_start IS NULL THEN NULL ELSE (
            SELECT SUM(cp.points * POWER({CLASS_DECAY}, s.season - cp.year - 1))
            FROM class_points cp
            WHERE cp.team = s.team
              AND cp.year BETWEEN GREATEST(s.season - {CLASS_WINDOW}, s.tenure_start)
                              AND s.season - 1
        ) END
    ) IS NULL) AS recruiting_points_regime_absent,
    COUNT(*) FILTER (WHERE s.tenure_start IS NULL) AS hc_first_year_absent,
    COUNT(*) FILTER (WHERE (
        SELECT SUM(bc.blue_chips) / NULLIF(SUM(bc.signees), 0)
        FROM blue_chips bc
        WHERE bc.team = s.team
          AND bc.year BETWEEN s.season - {CLASS_WINDOW} AND s.season - 1
    ) IS NULL) AS blue_chip_pipeline_imputed,
    -- DRAFT COVERAGE. This counter exists because its absence is what let the
    -- draft verdicts stand for a month on a column that was mostly fabricated
    -- zeros (see DEFECT FOUND BY THE SPLIT in the module docstring).
    --
    -- The distinction it draws is the whole point. `draft_picks_3yr` and
    -- `draft_departures` are COALESCEd to 0, and a 0 there is AMBIGUOUS: it
    -- means either "this program produced no NFL picks", which is a true and
    -- useful measurement, or "no draft was ever ingested for those years",
    -- which is not a measurement at all. The counters below separate them by
    -- asking whether the SOURCE YEARS exist in draft.draft_picks, independent
    -- of whether this particular team appears in them. A zero-fill share is
    -- fine; a structural-absence share above a percent or two means the column
    -- is measuring the load state of the warehouse rather than football.
    -- ALL THREE, not any (PR #56 review, P2): a window holding two of its
    -- three drafts still yields an understated count, and an EXISTS-based
    -- counter would report full coverage over it. Mirrors the same predicate
    -- in build_features.py.
    COUNT(*) FILTER (
        WHERE (
            SELECT COUNT(DISTINCT d.season) FROM draft_out d
            WHERE d.season BETWEEN s.season - 3 AND s.season - 1
        ) < 3
    ) AS draft_picks_3yr_no_source_year,
    COUNT(*) FILTER (
        WHERE NOT EXISTS (SELECT 1 FROM draft_out d WHERE d.season = s.season)
    ) AS draft_departures_no_source_year,
    -- Subgroup sizes for the section 6.0b decomposition. Counts, not verdicts:
    -- the screen reports a partial correlation without saying how many rows
    -- carry the indicator, and a 0.15 partial resting on 30 positives is a
    -- different object from one resting on 300. These make that legible on the
    -- same run rather than requiring a separate ad-hoc query -- which is how
    -- the numbers this amendment came from were produced, and why they could
    -- not be reproduced.
    COUNT(*) FILTER (
        WHERE s.tenure_start IS NOT NULL AND s.tenure_start >= s.season
    ) AS hc_first_year_rows,
    COUNT(*) FILTER (
        WHERE s.tenure_start IS NOT NULL AND s.tenure_start >= s.season
          AND s.hc_prior_seasons = 0
    ) AS hc_first_year_rookie_rows,
    COUNT(*) FILTER (
        WHERE s.tenure_start IS NOT NULL AND s.tenure_start >= s.season
          AND s.hc_prior_seasons > 0
          AND s.hc_prior_sp_mean < {HC_PROVEN_SP_PLUS}
    ) AS hc_first_year_prior_below_rows,
    COUNT(*) FILTER (
        WHERE s.tenure_start IS NOT NULL AND s.tenure_start >= s.season
          AND s.hc_prior_seasons > 0
          AND s.hc_prior_sp_mean >= {HC_PROVEN_SP_PLUS}
    ) AS hc_first_year_proven_rows,
    -- Head-coaching experience the warehouse cannot rate: an FCS stop, or a
    -- season older than its SP+ coverage. These rows are NULL on the proven /
    -- unproven split rather than guessed into either side, so this count is
    -- how much of the first-year population that split cannot see.
    COUNT(*) FILTER (
        WHERE s.tenure_start IS NOT NULL AND s.tenure_start >= s.season
          AND s.hc_prior_seasons > 0 AND s.hc_prior_sp_mean IS NULL
    ) AS hc_career_prior_unratable,
    -- Centring check for HC_PROVEN_SP_PLUS: this share should sit near 0.5 if
    -- SP+ really is centred on an average FBS team over this frame.
    COUNT(*) FILTER (
        WHERE s.prior_sp_rating >= {HC_PROVEN_SP_PLUS}
    ) AS frame_prior_sp_at_or_above_zero
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

    check_one_row_per_team_season(rows)
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
