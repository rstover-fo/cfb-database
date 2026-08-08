# Migrate cfb-scout into cfb-database (Monorepo Consolidation)

## Context

cfb-scout is the FastAPI scouting agent from the three-repo platform (~4,900 LOC, 30 endpoints, crawlers for 247Sports/Rivals/On3/PFF/Reddit, Claude summarization, pgvector embeddings). It has been **dormant since 2026-02-05** (last crawled report; last embedding 2026-02-02), was never deployed anywhere (Railway runs only cfb-app), and the user forgot it existed. Decision: **full monorepo merge** — code, tests, and `scouting` schema ownership all move into cfb-database; the cfb-scout repo is archived afterward.

User decisions (final):
- **Full merge** — FastAPI service, crawlers, LLM code, schema DDL, tests all move here.
- **Snapshot copy** — no git history preservation (history stays in the archived repo).
- **Park it** — code must be importable/lintable/testable, but crawlers/service are NOT revived; revival is a later project.
- **Unschedule** the daily `refresh-player-mart` pg_cron job (`30 12 * * *`) — it refreshes a matview over data frozen since Feb.

Live DB audit (Supabase project `ibobsbwlewpqslkqbrjd`, 2026-08-08): scouting schema has 13 tables + `player_mart` matview (~30k rows). Data: `reports` 203 rows, `players` 78, `player_embeddings` 26,128 rows / **418 MB**, nearly everything else empty. One active cron job (`refresh-player-mart`). No PostgREST grants on the schema (intentional).

**Blocking constraint:** the `rstover-fo/cfb-scout` GitHub repo is not accessible to this workspace yet — `add_repo` fails with access denied. The user must grant the GitHub app access before Phase 0 can run. Everything else (schema adoption from the live DB, doc updates) is plannable/executable without the code.

## What this session delivers vs. later sessions

This session's branch `claude/cfb-scout-migration-plan-dsyxuf` lands **the plan document only**: `docs/plans/2026-08-08-cfb-scout-merge-plan.md` (the content below), committed and pushed. Execution happens on separate branches (PR table at the end), most of it after repo access is granted.

## Phase 0 — Access + code audit (blocking)

1. User grants GitHub access to `rstover-fo/cfb-scout`; session attaches via `add_repo` (owner `rstover-fo`, repo `cfb-scout`), clones, `register_repo_root`.
2. Inventory actual code vs `docs/plans/2026-02-05-cross-repo-analysis.md`: module boundaries, uvicorn entry point, dependency list, what each of the 21 test files needs (DB / network / API keys).
3. **Py3.12 check:** grep for 3.12-only constructs (PEP 695 `class Foo[T]` / `type X = ...`, `itertools.batched`, `Path.walk`, `typing.override`). Default: down-patch scout to py311 to match this repo; bump repo-wide to 3.12 only if forced.
4. **Secrets sweep** of cfb-scout (`.env`, hardcoded keys — Anthropic, OpenAI, proxies, Reddit, Supabase). Produce the env-var list for the parked-service README; rotate anything live found in its history.
5. Schema drift: cfb-scout's `src/storage/schema.sql` is known incomplete — discard it; the **live DB is authoritative** (Phase 2 dump).
6. Append the final file-mapping table (cfb-scout path → cfb-database path) to the merge plan doc.

## Phase 1 — Target layout

`src/scout/` package, mirroring `src/pipelines/` (hatch already packages `src` — zero build-config change):

```
src/scout/
  api/          # FastAPI app; app factory at api/main.py (attr `app`)
  crawlers/     # 247Sports, Rivals, On3, PFF, Reddit
  llm/          # Claude summarization, embeddings
  storage/      # psycopg2 access layer (schema.sql NOT copied — superseded)
  config/       # settings/env handling
  README.md     # "PARKED" banner: status, required env vars, local run cmd, revival checklist
tests/scout/    # the 21 scout test files + scout-specific conftest.py
```

- No `[project.scripts]` entry (parked). Document `uvicorn src.scout.api.main:app --port 8000` in the README.
- No new workflow, no Railway service, no schedulers. CI lints + runs its non-network tests; nothing runs it.

## Phase 2 — Schema adoption (live DB = source of truth)

New directory **`src/schemas/scouting/`** (parallel to `public/`), applied only via `run_migrations.py --file`, all idempotent. These files are *codification* of what already exists in prod — applying them must be a no-op. Does NOT consume migration number 049 or touch `MIGRATION_ORDER`.

1. Dump: `pg_dump "$SUPABASE_DB_URL" --schema-only --schema=scouting --no-owner --no-privileges > scratch/scouting_dump.sql`
2. Curate into:
   - `001_tables.sql` — 13 tables + `CREATE EXTENSION IF NOT EXISTS vector` guard + indexes, all `IF NOT EXISTS`. Comment stating the schema intentionally has **no** anon/authenticated grants.
   - `002_player_mart.sql` — the 45-col matview (first version-controlled DDL for it, closing the gap flagged in the 2026-02-05 analysis) + indexes. Includes the `cron.schedule('refresh-player-mart', '30 12 * * *', ...)` call as a **commented revival step**.
   - `003_functions.sql` — `scouting.refresh_player_mart()` etc. Consolidate here and remove `src/schemas/functions/refresh_player_mart.sql` (check `refresh_all_marts.sql`/`refresh_all_views.sql` for references first); leave a pointer comment.
   - `004_portal_surveillance.sql` — `scouting.fn_evaluate_portal_value()` re-adopted verbatim from `docs/handoffs/2026-07-19-portal-surveillance-cron-to-cfb-scout.md`. **Function only**; cron scheduling stays commented out. This cleanly reverses the old 017 removal without resurrecting its number collision.
3. Fidelity check: apply each file against prod (idempotent no-op), re-dump, diff against the original dump.
4. **Unschedule the cron job** (user-approved): `SELECT cron.unschedule('refresh-player-mart');` — the only live mutation in the migration besides password rotation. Log it in the handoff doc.

## Phase 3 — Dependency + tooling merge

1. New optional extra in `pyproject.toml`:
   ```toml
   scout = ["fastapi>=0.110", "uvicorn>=0.29", "psycopg2-binary>=2.9",
            "anthropic>=0.40.0", "openai>=1.0", "pgvector>=0.2"]  # + crawler deps from Phase 0
   ```
   Daily-load/flat-files installs never pull `[scout]`; CI test job adds it so imports resolve.
2. Keep `requires-python >=3.11` and ruff `target-version = "py311"` unless Phase 0 finds hard 3.12 deps.
3. Ruff pass on moved code (`ruff check --fix` + `ruff format`) — **committed separately from the raw snapshot copy** so the diff stays reviewable.
4. Pytest: register marker `scout_network` for tests hitting crawl targets / LLM APIs; CI adds `-m "not scout_network"`. DB-only scout tests reuse the existing module-scoped `db_conn` fixture from `tests/conftest.py` (same live-Supabase pattern as `test_api_views.py`). `tests/scout/conftest.py` provides a FastAPI `TestClient` importable without secrets.
5. Pre-push hook: unchanged (ruff + pytest, green after the above).

## Phase 4 — Docs + contract updates

1. **`docs/SCHEMA_CONTRACT.md`**: drop cfb-scout as a downstream repo (lines 3, 12); rewrite the "Consumer: cfb-scout" section (719–760) — relabel as "src/scout (internal, parked)", **keep** the 5 api views + `get_player_search` contract-listed (cfb-app may use some; demotion is a separate audit); flip the scouting-schema table to "owned by this repo (`src/schemas/scouting/`), service parked, data frozen 2026-02-05"; note `player_embeddings` (418 MB, frozen, retained); update the dependency diagram (847–853).
2. **`CLAUDE.md`**: lines 20, 24, 59, 207, 243 — three-repo table becomes two repos + parked internal scout package.
3. **`.claude/agents/schema-architect.md`** (lines 3, 7–8) and **`.claude/skills/schema-migrations/SKILL.md`** (lines 3, 11): scouting now owned here; add `src/schemas/scouting/` to the skill's directory map (applied via `--file`).
4. **`docs/handoffs/2026-07-19-portal-surveillance-cron-to-cfb-scout.md`**: append a dated status note (SQL re-adopted; cron intentionally unscheduled). Don't rewrite history.
5. **Security scrub**: `docs/plans/2026-01-31-cfb-scout-phase1-implementation.md` (~line 253) contains a **live-looking Supabase DB password in plaintext**. Redact it in the doc AND **rotate the database password** (Supabase dashboard), then update `.dlt/secrets.toml`, the GitHub Actions `SUPABASE_DB_URL` secret, and any cfb-app/Railway env embedding it. Rotation is the real fix; history scrub optional (default: don't).
6. New docs: `docs/plans/2026-08-08-cfb-scout-merge-plan.md` (this plan) and post-execution `docs/handoffs/2026-08-XX-cfb-scout-merge-complete.md`.
7. `docs/pipeline-manifest.md`: one line noting scout crawlers exist but aren't in the manifest until revived.

## Phase 5 — Post-merge cleanup

1. `tests/test_marts.py`: make scouting first-class — delete the `scouting_schema_exists` skip fixture + `_skip_if_scouting_absent` (lines 76–86); `player_mart` (~30k rows) passes `test_view_has_rows`.
2. `player_embeddings` (418 MB): **keep** — irreplaceable without re-spending embedding API calls; immaterial at current tier. Documented in the contract.
3. Archive `rstover-fo/cfb-scout` (user action) after merge PRs land + one green CI run: tombstone README pointing at `cfb-database/src/scout/`, then GitHub → Archive.

## Verification

```bash
ruff check . && ruff format --check .
pytest -q --tb=short -m "not scout_network"
python -c "from src.scout.api.main import app; print(type(app))"   # parked app imports without secrets
python scripts/run_migrations.py --file src/schemas/scouting/001_tables.sql   # repeat 002–004; must be no-ops
pg_dump "$SUPABASE_DB_URL" --schema-only --schema=scouting --no-owner --no-privileges | diff - scratch/scouting_dump.sql
pytest tests/test_api_grants.py tests/test_marts.py -q
grep -rnE "postgres(ql)?://[^ ]+:[^ ]+@db\." docs/ && echo "FAIL: credentialed DSN still present" || echo "clean"
```
Plus one green `ci.yml` run and a `daily-load.yml` dispatch proving the pipeline install path ignores `[scout]`.

## PR / branch strategy

| # | Branch | Content | Risk |
|---|--------|---------|------|
| 0 | `claude/cfb-scout-migration-plan-dsyxuf` (this session) | Plan doc only | none |
| 1 | `feat/scouting-schema-adoption` | `src/schemas/scouting/001–004`, functions consolidation, cron unschedule, skill/agent dir-map updates | low (no-op DDL) |
| 2 | `feat/scout-code-move` | (a) raw snapshot into `src/scout/` + `tests/scout/`; (b) ruff pass + `[scout]` extra + pytest markers + ci.yml filter — two commits | medium (biggest diff) |
| 3 | `feat/scout-contract-flip` | SCHEMA_CONTRACT.md, CLAUDE.md, test_marts.py, password scrub (+ out-of-band rotation), handoff notes | low |
| 4 | (user) | Archive cfb-scout with tombstone README | — |

Order: PR1 → PR2 → PR3. PR2's tests assume versioned schema exists; PR3 last so docs describe reality.

## Open questions (deferred to execution)

- **O1** py311 vs py312 — resolved by Phase 0 audit (default: down-patch to 311).
- **O3** whether the 5 api views can eventually be demoted from the contract — separate audit of cfb-app usage.
- **O4** psycopg2 sync storage layer — keep as-is; unifying connection handling is revival-scope.
- **O5** git-history scrub of the leaked password — rotation makes it moot; default: don't.

## Critical files

- `pyproject.toml`, `scripts/run_migrations.py`
- `docs/SCHEMA_CONTRACT.md`, `CLAUDE.md`
- `tests/test_marts.py`, `tests/conftest.py`, `tests/test_api_grants.py`
- `docs/handoffs/2026-07-19-portal-surveillance-cron-to-cfb-scout.md` (portal-surveillance SQL source)
- `docs/plans/2026-01-31-cfb-scout-phase1-implementation.md` (password scrub target)
- Reuse: `db_conn` fixture (`tests/conftest.py`), `run_migrations.py --file` path, existing api-view grant pattern
