# Production receipts preflight — 2026-09-09

**September 10 follow-up:** the role compatibility correction and managed
production workflow actions are implemented and independently reviewed; see
[the deployment guide](../warehouse-bootstrap.md#managed-production-workflow).
Local checks passed: four focused non-superuser role tests, 175 migration,
publication and freshness SQL regressions, and 140 deployment/CLI tests.
A fresh read-only production check still found only the adopted root. No
production upgrade or runtime activation has been performed. Exact full-chain
execution under Supabase's event-trigger privileges remains a deployment check.
The findings below record the original preflight and remaining rollout work.

**Result: prepare a compatibility and deployment-runner change before applying
migrations 066–073.** A PostgreSQL 17 reproduction exposed a role-membership
guard failure under production's non-superuser migration-role model. Scheduled
publication also needs separate data repair and writer coordination.

This was a read-only production preflight, observed approximately 20:43–20:54 UTC
against Supabase project `ibobsbwlewpqslkqbrjd` (`cfbd-database`). The reviewed
checkout was `e626d8832173d1bb2b04fbae3c659c044b75e253` (merged PR #145).
Production DDL, data, grants, policies, workflows and provider requests were not
changed. A separate local PostgreSQL 17 reproduction was rolled back.

## Verified production state

- PostgreSQL 17.6; the connected `postgres` role has `CREATEROLE` and
  `BYPASSRLS`, but is **not a superuser**. It can create database objects and
  belongs to `supabase_privileged_role`. Event triggers are enabled and
  `session_replication_role` is `origin`.
- The managed ledger contains exactly the adopted production root,
  `warehouse.production.catalog-adopted.20260907.288b4eaf817b`, applied
  September 7 at 21:37:07 UTC. Its recorded SHA-256 is
  `d621e7026d135f6e16aede646628624357cbc51c734dc3b381b4d441d8ab7dcb`.
  There is no repeatable history. Comparing these observed rows with the local
  production manifest using the runner's planner returned eight pending
  migrations and no diagnostics. This was not a remote, advisory-locked plan.
- New quota/receipt/policy relations, warehouse runtime namespaces, the four
  bounded roles, and both new freshness RPCs are absent. No conflicting new
  namespace was found. `btree_gist` 1.7 is available but is not installed.
- The four SDV targets match migrations 071/072's reviewed ordered column
  names, types, nullability, primary keys and unique dlt identifiers: ratings
  19 columns, FPI 54, team crosswalk 16 and game crosswalk 15. Relevant source,
  Elo and flat-file-ledger relations have the expected ownership and kinds;
  no unexpected source triggers, inheritance or source RLS were found.
- Executed `SET LOCAL ROLE anon` and `authenticated` checks each returned
  24 legacy `get_data_freshness()` rows and successfully read
  `api.game_elo_history`. Both lacked private scouting/control-schema access;
  the managed ledger was inaccessible to them and `analyst_ro`. Direct access
  to the raw Elo mart was denied as expected by the owner-rights API contract.
- Nine GitHub workflows were active. No active warehouse transaction or
  conflicting writer lock was found in the database snapshot, and inspected
  recent writer runs had completed. This observation is not a maintenance
  window or protection against a later job starting.

The pending chain in [production-manifest.json](../../src/schemas/production-manifest.json)
is 066 quota ledger → 067 transport admission → 068 publication receipts →
069 Elo freshness → 070 generation refresh → 071 SDV ratings → 072 remaining
SDV sources → 073 SDV freshness. Use this adopted-production manifest, not the
disposable bootstrap manifest. Recompute checksums after the preparation change.

## Deployment blocker and delivery gap

PostgreSQL 17 automatically gives a non-superuser role creator an inbound
membership in a new role with `ADMIN=true`, `INHERIT=false`, `SET=false`.
Migration 071 creates `warehouse_source_publisher`; migration 072 then rejects
**any** inbound or outbound membership in that role. The exact 072 dependency
block failed with SQLSTATE `P0001` on an isolated PostgreSQL 17 instance using
a non-superuser `CREATEROLE` migration role. The transaction was rolled back.
The same assumption affects direct reapplication of 070 and 071. Earlier
superuser fixtures did not exercise this behavior.

Preparation must distinguish a narrowly validated creator administration edge
from runtime access or role escalation, preserve the ability to grant the later
runtime membership, and continue rejecting unrelated memberships. Blindly
revoking the creator's administration edge can strand later activation. Add
focused non-superuser PostgreSQL 17 regressions for role creation, guard
reapplication, the 072 dependency check, and runtime grant/role-assumption
behavior. Retain the full-chain superuser transaction tests. Stock PostgreSQL
does not allow this non-superuser to create event triggers; full-chain execution
under that identity needs a faithful Supautils equivalent or the authorized
Supabase upgrade with rollback/status verification. At the September 9 preflight,
the proposed fix had not yet been applied or tested.

The existing Deploy Schema workflow also lacks a first-class managed upgrade
action. Its compute allowlist does not admit `bootstrap_warehouse`, and its
environment does not map the production secret to `WAREHOUSE_DB_URL`, the only
credential variable accepted by that runner. The raw `run_migrations.py --file`
path bypasses the managed ledger and commits files independently. Prepare a
reviewed workflow action or use an authorized secure runner; do not substitute
raw-file application or hand-written ledger inserts.

Supabase documents that its privileged `postgres` role can manage event
triggers through Supautils. The observed role and settings are consistent with
that path, but read-only inspection does not prove execution of these exact DDL
statements on the managed platform. No full fresh catalog fingerprint, live
production migration, or production publication benchmark was performed.
See [Supabase event triggers](https://supabase.com/docs/guides/database/postgres/event-triggers)
and [PostgreSQL 17 role creation](https://www.postgresql.org/docs/17/sql-createrole.html).

## Existing data and workflow issues

The [September 9 daily load](https://github.com/rstover-fo/cfb-database/actions/runs/34362468024)
completed ingestion and flat-file loading but failed its `variant_twins`
verification check; downstream recaps were skipped. Production has 56 variant
columns on `stats.passing_player_season`, of which 55 are outside the current
allowlist. For 2026, 222 of 249 rows route `explosiveness` to a non-NULL twin
while its base is NULL; 201 do so for `success_rate`. Mart 045 does not select
those two fields or the location fields inspected. Do not infer consumer metric
loss solely from the raw-column count. Review the full set against consumed
fields, classify reviewed raw-only variants separately, and use a dependency-
complete mart release only where a consumed field needs repair. Migrations
066–073 do not fix this daily verification failure.

The full Elo publisher currently has two eligible completed games without final
scores: `401907219` (Missouri S&T–Westgate Christian, 2026 week 1) and
`401908553` (Centenary LA–Westgate Christian, 2026 week 1). The existing reviewed
cancelled-game exclusion accounts for the third NULL-score record. All five
existing superseded/replacement pairs matched. Resolve the two remaining games
from upstream evidence before full publication; do not invent scores or add
unreviewed exclusions.

Loaded SDV coverage is split across seasons:

| Source | Observed season and rows |
| --- | --- |
| `sdv_ratings_weekly` | 2026: 1,275 |
| `sdv_fpi_weekly` | 2025: 2,312; 2026: 138 |
| `sdv_team_xwalk` | 2025: 828 |
| `sdv_game_xwalk` | 2025: 1,689 |

These are database observations, not proof of current provider artifact
availability. Use explicit source/season pairs for initial publication; there
is no common currently loaded season across all four sources. Fetch cadence
does not define a freshness SLA. Migration 073 deliberately seeds no thresholds.

## Staged rollout and acceptance

1. **Preparation PR:** resolve the creator-membership guard, exercise the
   focused non-superuser guards and actual runtime role assumptions, retain
   full-chain transactional coverage, and add a managed
   production plan/upgrade/status delivery path. Independently review the
   changes. Repair the separate daily variant tripwire with consumer-aware
   evidence so normal verification can be useful during rollout.
2. **Authorized schema window:** quiesce warehouse writers, recheck active
   transactions and pending checksums, and run the managed production plan.
   From the reviewed secure workflow environment, the intended commands are:

   ```sh
   WAREHOUSE_DB_URL="$SUPABASE_DB_URL" python scripts/bootstrap_warehouse.py plan --manifest src/schemas/production-manifest.json
   WAREHOUSE_DB_URL="$SUPABASE_DB_URL" python scripts/bootstrap_warehouse.py upgrade --manifest src/schemas/production-manifest.json
   WAREHOUSE_DB_URL="$SUPABASE_DB_URL" python scripts/bootstrap_warehouse.py status --manifest src/schemas/production-manifest.json
   ```

   The runner applies the pending chain and ledger entries in one transaction
   under its advisory lock. A failed upgrade must leave the pre-upgrade ledger
   intact. Do not work around a failing guard by applying individual files.
   The runner sets neither `lock_timeout` nor `statement_timeout`; acquired DDL
   locks remain held until the transaction ends. Quiesce target DML and
   administrator DDL before dispatch, monitor the backend, and do not use
   cancellation as the normal timeout policy. If a workflow result or commit
   acknowledgement is ambiguous, wait for its backend and locks to disappear,
   then inspect managed `status` before retrying. A failed job alone does not
   prove rollback. Any added database timeout policy needs separate review.
3. **Post-schema verification:** check the eight new ledger entries and checksums
   (nine total including adoption), extension schema, runtime-role attributes and membership
   options. Execute the new RPCs as both `anon` and `authenticated`: Elo must
   return two rows, SDV four rows for each of 2025 and 2026, and the legacy RPC
   must retain its contract. Verify the six warehouse event triggers in the
   `warehouse_publication_*`, `warehouse_source_*` and
   `warehouse_source_batch_*` families are enabled (`O` or `A`), with existing
   platform triggers preserved. Check private table/schema/function denial,
   negative new-RPC calls as `analyst_ro` and a bystander without memberships,
   and owner-rights API reads. Existing broad public default privileges make
   effective ACL checks necessary. Missing receipts and unknown policies must
   remain explicit; do not seed guessed intervals or globally change defaults.
4. **SDV canary:** configure only the reviewed source-publisher runtime grant,
   verify the selected artifact's availability, then publish one explicit
   source/season, such as `sdv_fpi_weekly`/2026, using
   `load_flat_files.py --source sdv_fpi_weekly --season 2026 --require-receipts`.
   Verify target counts, dlt identifiers, receipt identity, current pointer,
   freshness response and the actual runtime role's permissions. Measure
   publication/lock duration. Extend to the other confirmed source/season pairs.
   A distinct runtime login needs `ADMIN=false`, `INHERIT=false`, `SET=true`.
   If the existing `postgres` login is reused, grant role switching with
   explicit `INHERIT FALSE, SET TRUE` while preserving its automatic
   administration edge. The September 10 stock PostgreSQL 17 regression found
   a separate self-granted runtime row alongside the bootstrap-superuser
   administration row; inspect all grantors on the actual target. The underlying
   credential still has broad `BYPASSRLS` access. Test the actual connection's
   role assumption. Activated memberships
   must not be mistaken for the pre-activation creator exception on direct
   migration reapplication; subsequent managed upgrades skip applied entries.
   Apply this rule separately for each later activation: map
   `warehouse_source_publisher` to the flat-file publisher connection,
   `warehouse_publisher` to the Elo compute connection, `warehouse_refresher`
   to the mart refresher connection, and `warehouse_ingest` to
   `CFBD_QUOTA_DB_URL`. Resolve each login from its actual `session_user`;
   do not assume the connections share an identity. No runtime grants precede
   the complete 066–073 commit. Test role assumption and only the allowlisted
   RPC permissions on each actual connection.
5. **Sustained receipt operation:** implement explicit source/season scheduling
   and reviewed freshness intervals. The current legacy `--due` path cannot be
   combined with `--require-receipts`; corrected legacy writes invalidate
   pointers. For Elo, resolve the two games, benchmark
   `compute_house_elo.py --full --publish-receipts`, and coordinate daily,
   backfill and historical writers/refreshes before cutover. Current incremental
   Elo cannot publish receipts; legacy Elo DML clears the source and mart pointers, and
   ordinary Elo mart refresh clears its pointer. Refresh APIs currently cannot
   exclude only the protected Elo mart from all these paths. A strict refresh
   afterward can restore evidence on success but cannot preserve the previous
   publication if it fails. Add the needed orchestration support before relying
   on current receipts continuously. Declare a positive Elo freshness interval
   before enabling generation-enforced refresh.
6. **Separate durable quota activation:** establish real provider headroom,
   explicit finite extraction and reconciliation periods/caps, and the dedicated
   connection that can assume `warehouse_ingest`. Set `CFBD_QUOTA_MODE=durable`,
   `CFBD_QUOTA_DB_URL` and `CFBD_QUOTA_ACCOUNT` only for the enrolled
   `load_season`, `backfill_refresh` and `poll_scoreboard` entrypoints. Verify
   admission, exhaustion, failure and recovery before extending coverage to
   other commands; their current default is legacy admission.

Schema deployment and each activation are separate decisions. After a committed
schema upgrade, disable a failing opt-in activation and preserve its receipt
history for diagnosis; use a reviewed forward correction rather than deleting
ledger entries or dropping receipt tables. Crossvalidation generation enforcement
still depends on later receipts for CFBD FPI, `ref.teams`, and the two EPA marts.
