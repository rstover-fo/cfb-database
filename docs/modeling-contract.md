# Modeling contract

This document contains the durable domain rules for feature engineering,
screening, training, backtesting, and predictions in this repository. The
detailed `features.team_week` and `fitted_v1` specification remains
`docs/brainstorms/2026-07-21-team-week-feature-design.md`. Read the affected
sections and amend that design document before implementing a change it
governs.

## As-of data and nulls

A feature entering same-season `week_index = W` may use only observations with
`week_index < W`. Regular-season `week_index` is the natural week; postseason
uses `week + 100`. Cross-season inputs are allowed only when explicitly labeled
as prior-season or preseason-known fallbacks.

Preserve null for an unavailable rate, coefficient, or feature. Zero is valid
only when it represents an observed quantity, such as a count with confirmed
source coverage. The model may impute nulls; feature construction must not hide
missingness by inventing zero.

Verify the grain and temporal meaning of every join. Pregame features may not
read postgame or end-of-season state from the game they predict. Coverage loss,
duplicate identities, and same-week leakage invalidate a result even when the
aggregate metric looks plausible.

## Pre-registration and screening

Before measuring a candidate, record in the design document:

- the hypothesis and exact candidate definition;
- the evaluation window and eligible population;
- controls, baseline, and leakage exclusions;
- the primary metric, acceptance floor, and multiple-testing/FDR rule;
- whether the screen is run once and what would justify an amendment.

Record the result with sample size, measured values, uncertainty or adjusted
significance where applicable, and a verdict, including rejection or
untestable. Do not rerun a rejected candidate under altered framing without a
new pre-registration. Amendments respond to a demonstrated design flaw or new
data contract, not to an unfavorable result.

## Feature-vector compatibility

The ordered production feature list is a persistence contract. Adding,
removing, reordering, or changing the meaning of a feature invalidates stored
fit vintages that claim the old version. Coordinate the schema migration,
feature build, training/scoring code, version metadata, and backfill needed for
one compatible release.

Experimental evaluation must use an experiment-specific model version and must
not overwrite production coefficients or metadata. A successful univariate
screen does not grant a production vector slot; the candidate must also pass
the isolated walk-forward adoption gate defined in the design document.

## Walk-forward adoption gate

Use stored or reproducible time-ordered training vintages so every held-out
prediction is generated without future data. Compare on the same eligible
games and report per-game metrics. A production model change passes only when
held-out margin MAE improves and neither Brier score nor ATS performance
degrades under the pre-registered tolerances. Use
`marts.prediction_accuracy` or the design document's current contracted source,
not season-level summary metrics that change the evaluation grain.

A null result is a valid recorded outcome. An empty evaluation set is
`UNTESTABLE`: it is neither a rejection, a pass, nor evidence of no effect.

## Plausibility and investigation

Review sign conventions, units, calibration, sample coverage, season/team
counts, identity joins, and expected football scale before publishing an
analysis. Offensive EPA/PPA is higher-better; defensive EPA/PPA is
lower-better unless a derived field documents another convention.

Extreme margins, probabilities pinned near zero or one, or a strong program at
an implausible rank are investigation triggers. Check transformations, joins,
coverage, cohort definitions, and comparison grain before deciding whether the
output reflects a code defect, sparse data, a model limitation, or a real
outlier. An abnormal scale or surprising ranking is not by itself proof of a
bug.
