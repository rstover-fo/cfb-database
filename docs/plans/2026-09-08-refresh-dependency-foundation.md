# SQL refresh dependency foundation — F10/F11/F28

Status: implemented for the Python materialized-view refresher; no production
execution or schema changes. This is the first shared-planner slice, not closure
of F10, F11, or F28.

## Behavior

`src/pipelines/config/refresh_assets.py` declares the existing 55 materialized
views and their 48 external relation inputs. Each view records its canonical SQL
file, direct relation dependencies, and `whole_relation` refresh coverage. A
PostgreSQL parser test checks the entire canonical marts/analytics inventory and
every declared relation edge. Reviewed function-body dependencies include
`ref.get_era()` → `ref.eras`; source hashes and an explicit catalog-function
allowlist require dependency review when functions change or are added. `pglast` is a development-only dependency.

`scripts/refresh_marts.py` derives its former separate lists from this registry.
Full refreshes retain a stable order for independent views. Selected views are
validated and dependency-ordered, including transitive ordering across omitted
intermediates. Duplicate selections collapse to one refresh. Unknown or empty
explicit selections fail before opening a database connection.

The new `--changed` option treats each named relation as already committed and
selects its SQL materialized-view descendants. Changed roots themselves are not refreshed, including roots that depend on
another changed root; the caller asserts all named roots are already current. For example:

```sh
python scripts/refresh_marts.py --changed analytics.adjusted_epa_build --dry-run
# marts.team_adjusted_epa, then marts.epa_crossvalidation
python scripts/refresh_marts.py --changed ratings.sdv_ratings_weekly --dry-run
# marts.epa_crossvalidation
python scripts/refresh_marts.py --changed ref.coach_tenures --dry-run
# marts.coach_tenures
```

`--views` and `--changed` are mutually exclusive; either overrides `--schema`.
`--views` refreshes exactly the registered selection, assuming omitted parents
are current. Existing callers may continue using it. Arbitrary unregistered
names previously accepted by that option are now rejected.

If a selected refresh fails, the existing per-view transaction rolls back.
Selected descendants are then blocked, even across an unselected intermediate;
independent views continue. The process returns the count of failed plus blocked
views, preserving a nonzero workflow result. A no-op changed-input plan succeeds
without accessing the database.

## Boundaries and follow-up

This graph describes canonical SQL relation lineage. It does not schedule CFBD
requests, flat-file imports, EPA computations, or model runs, and does not infer
their dependencies. For example, declaring `core.plays` changed does not rebuild
`analytics.adjusted_epa_build`. Existing workflows still own those job boundaries.
API views, dynamically constructed dependencies, live database drift, and
source coverage are outside this inventory.

Failure gating applies within one Python refresh invocation. It does not prevent
mixed generations after a prior source/compute failure, coordinate concurrent
invocations, make the whole refresh atomic, or change `public.refresh_all_marts()`.
Durable receipt/generation checks, source and compute asset metadata, incremental
work units, and SQL RPC parity remain required follow-up. The F14/F19 operational
records proposal develops the durable evidence needed for those gates.

## Verification

- Registry inventory and edges checked using PostgreSQL parse trees, including
  canonical files rather than only paths listed in the registry.
- Planner tests cover transitive subset ordering, changed-input selection,
  duplicate inputs, invalid names, cycles, missing dependencies, and empty plans.
- Runner tests cover rollback, dependent blocking, independent continuation,
  database-free dry runs/no-ops, and explicit empty CLI arguments.
- 186 planner, refresh, season-load, and workflow regression tests passed;
  Ruff lint/format and diff checks passed. Independent review identified and
  resolved overlapping changed-root refreshes and function-hidden era inputs.
- No live SQL refresh or performance claim is made.
