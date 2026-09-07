# F05 — immutable training fits and explicit deployment

Status: implemented and locally validated; production not deployed.

## Problem and intended behavior

Training previously deleted coefficients and metadata at a reused model/season
key. Freshness only compared feature-name sets and the newest season, hiding
formula/data changes and missing middle vintages. F04 preserved consumed scoring
artifacts; it did not make the upstream training lifecycle immutable.

F05 appends content-addressed training manifests and frozen parameters to
`features.training_fits`. A separate `features.model_deployments` pointer for
each model/train-through season selects the active candidate. Every pointer
change is journaled in `features.model_deployment_history`. The existing S−1
backfill and latest eligible strictly-prior upcoming selection rules operate
only over these explicitly promoted vintages. Candidate creation cannot affect
scoring. F04 artifacts additionally record the selected `training_fit_id`, while
old artifacts retain their original contents and identity.

Manifests bind the ordered feature contract, algorithm and transformation source,
numerical parameters, runtime, training seasons, data cutoff, exact consumed-row
digest/count, and code revision. Content identity includes the frozen parameters;
repeating an identical manifest and output is idempotent. Freshness compares
current input/specification content, not an unrelated repository commit date.
The input digest detects revisions; it does not archive the upstream data needed
to rerun a historical training job. Frozen coefficients and prediction input
snapshots remain recoverable, even if upstream warehouse rows change.

Refit discovery uses expected-vintage set difference within the F03 contiguous
closed frontier, and trains only the missing/stale windows. Formula changes,
parameter changes, and input corrections invalidate selected manifests. Legacy
imports record unknown lineage and exact stored parameters; they cannot count
as freshly trained under the new contract.

## Modeling boundary

No feature, ridge/IRLS algorithm, or calibration-method change is included.
Retain existing six-decimal coefficient/Platt storage precision so replacing
persistence does not silently alter score-time numerical behavior. The current
training-logit Platt calibration is explicitly recorded. An out-of-fold or
held-out alternative remains a separate preregistered experiment: train-only
Brier improvements do not justify production promotion. Use identical held-out
games and the existing MAE/Brier/ATS gates before adopting another method.

## Rollout (requires production authorization)

1. Record currently active warehouse workflows and wait for writers to finish;
   pause conflicting training/scoring/recovery jobs. Capture legacy fit counts,
   parameter checksums, current F04 ledger/artifact counts and selected vintages.
2. Apply only `src/schemas/migrations/064_immutable_training_fits.sql` through the
   existing Deploy Schema apply action. The manifest under `deploys/` is a
   reviewable input, not an automatic production deployment trigger. Apply twice
   to establish idempotence and verify actual caller roles.
3. Run `train_model.py --import-legacy` using the new code to preserve existing
   fitted_v1 parameters without inventing training provenance. Import itself
   does not select candidates. Verify imported parameters against the legacy
   tables, including ordered feature names and rounded coefficients.
4. Run the reviewed `--refit-if-stale --promote --promotion-reason ...` action
   under the unchanged production method. This rebuilds the required closed
   ladder with current manifests and explicitly selects the resulting fits.
   This is real model training, not a metadata-only migration; measure runtime
   and report numerical comparisons rather than claiming historical parity.
5. Verify every expected closed vintage is selected/fresh, no candidate beyond
   the safe frontier is selectable, and each pending season has an eligible
   strictly-prior fit. Score upcoming games, verify per-season coverage and
   F04 artifact training IDs, then refresh dependent prediction marts through
   the existing approved workflow. Legacy predictions and artifacts must retain
   their original rows and contents.
6. Verify public roles can read intended tables and cannot write; analyst_ro
   retains API-only access. No API-view shape changes require type regeneration
   for F05. F04's outstanding downstream adoption remains separate.
7. Restore the workflows to their recorded prior states. The daily refit step
   explicitly requests promotion and records its reason.

If validation fails, keep writers paused while investigating. Additive schema
can remain installed. Reverting application code restores reads of preserved
legacy tables; record the deployment history and any new published snapshots,
which must not be deleted or rewritten. Pointer rollback can reselect a retained
fit, but current freshness checks still reject stale/unknown manifests for
upcoming scoring. Do not defeat those checks to make a rollback appear healthy.

## Verification record

- Main suite: **2,425 passed, 497 skipped**. Configured warehouse integration
  tests were not opted into locally; the skips are not production evidence.
- MCP suite: **59 passed**.
- Disposable PostgreSQL 16: **47 passed** across the F04 and F05 SQL suites,
  including **26 F05 cases**. Applied migration 064 twice, executed exact legacy
  import, selection/rollback, immutable history and fit guards, actual caller-role
  checks, scoring linkage, and comparisons against PostgreSQL NUMERIC rounding.
- A synthetic training-to-registry-to-scoring regression covers NULL imputation
  and constant feature columns; CLI regressions cover explicit promotion and
  exact missing-vintage selection. No real warehouse fit was trained locally.
- Affected Python Ruff checks/format, agent-setup wiring and git whitespace
  checks passed. Unrelated untracked film work was excluded and preserved.
- Independent Astra review is clean after resolving source-fingerprint closure,
  legacy numeric-string compatibility, zero-variance compatibility and rounding
  findings. Concurrent selection and legacy import use transaction-held locks.

Production runtime, memory cost of training-data hashing, warehouse fit outputs
and prediction coverage still require the authorized rollout. No accuracy or
calibration improvement is claimed. No production F05 command has been executed.
