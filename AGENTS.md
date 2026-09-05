# CFB Database: shared working agreement

This repository owns the CFBD/dlt/Postgres warehouse, the private scouting
schema, and contracts consumed by cfb-app, cfb-scout, and the MCP server.
These instructions apply across models and local/cloud harnesses.

## Complete the requested work

- Preserve existing uncommitted work. Make the smallest complete change that
  satisfies the task, resolve material review findings, and verify the result.
- Continue through routine implementation and relevant local checks without
  repeated approval. Existing user authorization persists; preparation of a
  migration or experiment does not itself authorize applying it to production.
- Keep checks proportional to the change. After required checks pass, broaden
  or repeat them only for a new change, failure, or unresolved integration risk.
- Report what changed, checks and outcomes, and material limits. Missing data,
  skipped integration checks, and unmeasured performance are not verification.

## Delegation

- Delegate substantial independent exploration, implementation, or review when
  it improves quality or elapsed time; handle small or tightly coupled work
  directly. Keep useful work with the lead while workers run.
- Use at most three concurrent workers, or the runtime's lower limit. Give each
  a bounded objective, relevant context, file ownership, and completion criteria.
  Concurrent writers own disjoint files; shared-file changes are sequential.
- The lead integrates changes and arranges independent review for substantive
  correctness, data, or access changes, then resolves findings before returning.
  Workers return follow-up work to the lead instead of spawning more workers.
- Workers may inspect git read-only when their tools permit it. The lead owns
  staging, commits, branches, merges, and publishing within the requested scope.
- Shared role contracts live in `.agents/roles/`: explorer, implementer,
  reviewer, and modeling-scientist. Native adapters in `.codex/agents/` and
  `.claude/agents/` hold model choices and tool/sandbox settings. Preserve the
  user's lead model; explicit task overrides take precedence over role defaults.
- If only generic spawning is available, read the matching adapter and shared
  role, then pass their instructions and supported model/effort explicitly.
  Use a bounded handoff if a full-history fork cannot override the model.
  If a role/model/tool is unavailable, the lead completes the work using the
  available session or a user-authorized alternative and reports the limitation.

## Read what the task needs

Project paths below are relative to the repository root. Load only the relevant
sections. Shared skills are in `.agents/skills/`; Claude uses links to the same
files. A missing skill tool is not a blocker: read its `SKILL.md` directly.

| Task | Reference |
|---|---|
| Consumer schema, view/RPC, grant, or migration changes | `docs/SCHEMA_CONTRACT.md`; `.agents/skills/schema-migrations/SKILL.md` |
| CFBD requests, response fields, auth, or retries | `.agents/skills/cfbd-api/SKILL.md` |
| Ingestion resources, merge behavior, orchestration, or budgeting | `.agents/skills/dlt-pipelines/SKILL.md`; `docs/pipeline-manifest.md` for endpoint mappings |
| Features, statistical screening, training, or backtests | `docs/modeling-contract.md`; the design sections it identifies |
| Postgres query/schema performance | Relevant references in `.agents/skills/supabase-postgres-best-practices/SKILL.md`; project contracts govern project-specific design |
| Operating the warehouse or investigating past incidents | `docs/warehouse-operations.md` |
| Harness setup, cloud bootstrap, or instruction maintenance | `docs/agent-setup.md` |

## Warehouse invariants

- Preserve data grains, provider/merge keys, dlt parent relationships, variant
  columns, NULL semantics, and consumer contracts. Prefer verified identifiers
  or reviewed crosswalks to ambiguous team-name joins.
- Preserve the owner-rights API-view design and private scouting boundary
  unless the task changes that contract. Restore grants after view recreation
  and check dependency closure for materialized-view changes.
- Bound API fanout and distinguish expected no-data from transport, auth, and
  quota failures. Skip logic must account for upstream corrections.
- As-of features exclude same-season observations at or after the target week;
  postseason uses week + 100. Label prior-season fallbacks. Keep experimental
  fits separate from production and preserve the modeling evaluation gates.

## Setup and verification

Use Python 3.11+; `bash scripts/setup_dev.sh` installs the warehouse and MCP dev
dependencies into `.venv`. It does not provision or load the database.

- Run Python checks with `.venv/bin/ruff check` and `.venv/bin/ruff format --check`
  on affected files, and start with the relevant regression tests.
- Main tests: `PATH="$PWD/.venv/bin:$PATH" .venv/bin/python -m pytest -q --tb=short`.
- MCP tests: `.venv/bin/python -m pytest mcp/tests -q --tb=short`.
- Database fixtures skip unless `--live-db` is explicitly supplied. Use that
  option only for an authorized target; credentials alone do not opt in.
- SQL rollout verification needs representative executed SQL and actual caller
  roles when a suitable database is available. Prepare/review changes and report
  missing execution evidence when it is not; do not call text assertions a deploy.
- Instruction/configuration changes: `.venv/bin/python scripts/check_agent_setup.py`.
