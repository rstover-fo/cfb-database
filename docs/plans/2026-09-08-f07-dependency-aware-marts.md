# F07 — dependency-aware mart releases

Status: implemented and locally verified in PR #131 from merged PR #130
(`4291e88`). Initial CI passed; all four automated findings have verified fixes
and independent review, pending CI on the follow-up commit. Production rollout
requires separate authorization; this goal uses disposable PostgreSQL.

## Outcome

A mart rebuild must not silently remove consumers through CASCADE. The supported
release path previews the live PostgreSQL dependency closure, requires explicit
restoration coverage, executes the ordered release in one transaction, and checks
consumer contracts and access before commit. A downstream SQL failure rolls back
the upstream replacement, leaving the prior API/public objects and grants intact.

## Delegation and ownership

- Lead: release design, representative catalog fixtures, executed integration,
  workflow wiring, documentation, integration, and PR publication.
- Explorer: read-only tracing of existing runners, dependency closure, and bypasses.
- Implementer: strict release manifest, catalog preflight, transactional engine,
  and focused engine tests.
- Implementer: mart CLI and schema deployer integration, fail-closed legacy paths,
  and CLI regression tests.
- Independent reviewer: substantive engine/access changes and executed failure
  scenarios after implementation; the explorer slot is reused for review.

## Scope and acceptance

1. Explicit manifests identify upstream roots, objects restored, ordered SQL files
   with exact checksums, and caller-role validation queries. Read-only planning
   lists the live closure and rejects missing/unsupported restoration coverage.
2. Execution rechecks under the shared migration lock and performs all release
   work in one transaction. Preserve existing consumer columns, object kinds,
   owners, security settings, and privileges; reject unexplained contract loss.
3. Bare mart deployment and real `--only`/`--from` calls fail before connecting.
   Raw mart files cannot pass through the old per-file migration/deploy route.
   Diagnostic listing and SQL previews remain available.
4. Disposable PostgreSQL tests cover an upstream mart, downstream materialized
   and regular views, public/API consumers and actual roles. Inject failure after
   the upstream DROP, then prove prior object identities, data and grants survive.
   Test incomplete closure, newly added unmanaged dependents, lost permissions,
   and incompatible replacement columns as failures rather than partial success.
5. Exercise a representative existing mart release on the F06 warehouse fixture.
   Add mandatory CI coverage, document recovery and blocking/long-build limits,
   resolve independent/PR findings, and publish the completed change.

## Limits to keep explicit

PostgreSQL catalog dependencies do not discover arbitrary dynamic SQL or every
string-bodied function reference. Explicit caller assertions complement catalog
closure checks. Reviewed SQL is trusted migration code; this is not a sandbox for
hostile arbitrary SQL. Long rebuilds can hold locks and consume resources; test
rollback correctness without claiming unmeasured production downtime or cost.
Renaming a staged object does not repoint existing dependencies. Specialized
staged rebuilds need their own reviewed dependency and cutover plan.

The F06 immutable baseline, adopted production receipt, and historical migration
files remain immutable. Unrelated film/tracking work is preserved.

## Verification evidence

- Executed PostgreSQL 17/pgvector: 16 F07 cases pass, including rollback after
  downstream failure, successful/repeated populated rebuild, incomplete/new
  dependencies, incompatible columns, lost indexes/comments/column grants/view
  rules, public consumer changes, default-grant isolation, composite-type rejection,
  row-type function coverage, and the existing trajectory release/RPC caller roles.
  PR regressions additionally cover population state, numeric assertions, and
  explicitly declared dynamic consumers.
- Combined F06 compatibility and F07 SQL suite: 26 passed before the final extra
  view-rule case; the final F07 suite was rerun successfully afterward.
- Main suite: 2,578 passed, 531 skipped (database-dependent tests opt in separately).
  MCP suite: 59 passed. Final engine/CLI checks after the Unicode identifier
  guard regression: 39 passed.
- Ruff check and format pass for tracked/project changes. Whole-workspace lint
  also finds existing untracked `scripts/film/update_storage_uri.py` issues; that
  unrelated user work is unchanged and excluded from this PR.
- Independent review findings on dependency closure, public objects, metadata,
  transaction parsing, and validation boundaries were addressed. See the release
  operations guide for remaining dynamic-SQL, grant-chain, and runtime limits.
- PR Lens architecture and transaction SVGs are attached to PR #131. Generated
  diagrams stay local in the ignored `.pr-lens/` directory.
