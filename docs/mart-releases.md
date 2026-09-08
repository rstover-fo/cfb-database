# Dependency-aware mart releases

Mart definitions that use CASCADE must run as an explicit release, with every
affected consumer restored in the same transaction. Bare `run_marts.py`, real
`--only`/`--from` selection, and mart files passed to `run_migrations.py --file`
are rejected. Historical deployment manifests containing mart files need an
explicit release conversion before reuse; their old per-file sequence is not
an atomic recovery plan.

## Prepare and preview

Use `deploys/mart-releases/trajectory.json` as a bounded example. Its SQL files
rebuild the trajectory mart and restore its public invoker-rights wrapper. The
manifest pins exact file SHA256 checksums and declares relation roots, restoration
identities, and caller-role boolean SELECT assertions.

List known textual or dynamic function readers in the optional `consumers`
array, using the exact qualified identity reported by
`pg_get_function_identity_arguments` (including argument names). Each must
exist and be named in an assertion's `covers` array. These declarations add
validation coverage; they do not authorize recreating an unrelated function.
The trajectory example declares its RPC this way. Catalog inspection cannot
discover every such reader, so the release author must identify them.

```bash
export SUPABASE_DB_URL='postgresql://postgres:local-password@127.0.0.1:55437/postgres'
.venv/bin/python scripts/run_marts.py --release deploys/mart-releases/trajectory.json --plan
```

The plan reads PostgreSQL catalog dependencies and reports the affected closure,
missing restoration coverage, and unsupported objects. It executes no release
SQL. A plan is a preview, not a reservation: execution must recheck the catalog
under the shared migration lock. Review added consumers instead of simply
appending their names to make preflight green; their definitions and permissions
must survive the release as well.

## Execute and verify

```bash
.venv/bin/python scripts/run_marts.py --release deploys/mart-releases/trajectory.json
```

The schema workflow exposes `mart_release` and `plan` for the same operation.
Do not combine a release with extra per-file SQL or a separate refresh step.
All required definitions, indexes, and caller assertions belong in the release.
The engine uses one transaction and checks preserved contracts before committing.
Failure rolls back the replacement and its downstream work together. Inspect
the reported error and rerun the unchanged read-only plan after fixing the
reviewed release; do not repair consumers with a partially applied file list.

Existing consumer column types/order, object kinds, owner/security properties,
population state, and permissions are preservation contracts. Version 1 does not authorize a
breaking API change. Prepare a separately reviewed compatible migration when
the intended consumer contract must change.

Version 1 accepts view and materialized-view roots and their supported view and
function dependents. It rejects dependent stored tables and unsupported catalog
objects such as composite types. Recreate existing indexes, comments, column
grants, and other checked metadata in the release SQL; omitted metadata causes
rollback. Object owners and relation/function grants are restored automatically.
Multi-level grants delegated through other grantees may fail during replay; a
failure rolls back the release. Such grant chains need a separately rehearsed
restoration strategy before rollout.

## Operational boundaries

Production execution requires rollout authorization. Plan and rehearsal should
use the disposable F06 warehouse first, with representative populated fixtures
where behavior depends on data. The bundled trajectory example proves executable
definitions and access on an empty fixture, not production rebuild duration.

Transactional rebuilding can block readers and consume substantial storage or
work memory. Use bounded timeouts and a maintenance window without other schema
writers. The advisory lock coordinates managed tools, not unrelated admin DDL.
For long builds, design a separate staged replacement and dependency cutover;
renaming a new relation does not move views still bound to the old object.

Catalog dependencies omit some string-bodied and dynamic SQL references.
Explicit caller-role queries complement closure checks, but cannot prove every
application query. Release SQL is reviewed migration code, not hostile input;
these checks are not a general SQL security sandbox.

Validation queries must be side-effect-free SELECT assertions. Each runs under
its declared role inside a savepoint that is rolled back, so ordinary transactional
writes cannot persist. This is not enforced read-only execution: sequence advances
and external effects of called functions can survive a savepoint rollback. Review
the functions invoked by assertions as well as the query text.

The F06 production adoption receipt and immutable baseline remain unchanged.
The release manifest's file checksums bind this release's reviewed SQL; they do
not create historical migration entries or authorize changing already applied
immutable files in the production migration manifest.
