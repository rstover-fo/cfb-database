# Scheduled FPI publication receipts

The `SDV FPI Receipts` workflow publishes the complete registered SDV FPI file
for the explicitly enrolled 2026 season. It runs daily at 10:17 UTC and supports
a manual dispatch of the same path. GitHub may delay scheduled jobs.

## Activation contract

The repository variable `SDV_FPI_RECEIPT_SEASON` is the shared activation switch.
Only the exact value `2026` enables the workflow and its matching legacy-loader
exclusion. A missing or different value leaves both inactive. The variable is
not a credential and cannot grant database access.

Activation also requires the deployed receipt schema, the existing loader's
reviewed ability to assume `warehouse_source_publisher`, and one exact private
policy row:

| Asset | Coverage | Maximum publication age |
| --- | --- | --- |
| `ratings.espn_fpi_weekly` | `season:2026` | 36 hours |

The daily interval leaves 12 hours for scheduling delays before a publication
is stale. This is an explicit publication-age policy. A successful run fetches,
validates, and republishes the artifact even if its bytes are unchanged. It
does not prove when the provider last updated its values or that the provider
has published every expected game/week.

The driver checks the intended project, managed migration ledger, actual
source-publisher role assumption, and exact policy before publication. The
workflow does not create grants, configure policies, infer new season scopes,
or opt into Elo publication, generation-enforced mart refresh, or CFBD quota
admission.

## Execution and coordination

The driver selects exactly `sdv_fpi_weekly` and 2026 through the normal source
publication adapter. It fetches the registered season URL, permits upstream
corrections, and uses the adapter's complete-file validation and atomic
publication. There is no older-season fallback or legacy hash skip. Missing
artifacts remain deferred outcomes with a nonzero exit; other failures also
fail the job. An uncertain commit must be reconciled by its operation and
generation IDs before retrying.

After success, the driver verifies the new generation and complete row counts
through `public.get_source_freshness(2026)` as both `anon` and `authenticated`.
The receipt must have registered-URL provenance, in-file season evidence,
the declared 36-hour interval, a successful latest attempt, and a non-stale
current publication. Results include operation and generation IDs for recovery.

The standalone workflow first acquires `daily-season-load`, then its publish
job acquires `flat-file-load`. This matches the daily parent's order and
serializes publication and refresh with daily, historical, backfill, recovery,
and flat-file work. The FPI job can run after an unsuccessful daily workflow;
it does not require that workflow to succeed. When activation is off, it uses
a unique outer group and cannot delay the daily queue.

All workflows sharing those groups use `queue: max` with cancellation disabled
so a new arrival preserves existing pending runs. GitHub permits up to 100
pending runs per group; excess runs are canceled. Long warehouse jobs can delay
the requested 10:17 start and can make the publication exceed its declared age
limit. See [GitHub's concurrency documentation](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency).

The standalone job refreshes `marts.epa_crossvalidation` after an attempted
publication, even if publication or verification failed, because a commit may
already have happened. Refreshing does not erase the earlier failure status. The refresh
uses the existing ordinary refresh mode.

While the switch is active, both implicit-current-season and explicit-season
legacy `--due` commands receive:

```text
--exclude-source-season sdv_fpi_weekly:2026
```

The exclusion is applied to the planned season before any fetch or fallback.
When that planned season is 2026, the entire legacy FPI attempt is omitted,
including its possible fallback to an older artifact. Explicit historical
2025 due plans still include FPI. Other sources and explicit manual `--source`
commands retain their existing behavior. Direct/manual legacy FPI writes can
still invalidate the current receipt pointer; operators must choose the receipt path when
current evidence is required. The exclusion option is restricted to `--due`.

## Rollout and recovery

1. Review and merge the workflow, driver, exclusion, and shared-queue changes
   while the repository activation variable is absent.
2. Pause competing flat-file producers and drain existing executions. Capture
   workflow states, target data, policy rows, and current receipt identities.
3. Configure only the exact 36-hour FPI/2026 policy after validating the owner,
   deployed schema, active source-only role, and recent complete publication.
   Reject a conflicting existing interval; preserve every other policy.
4. Verify idempotence and actual `anon`/`authenticated` policy exposure. Set
   `SDV_FPI_RECEIPT_SEASON=2026` and dispatch the standalone workflow once.
5. Verify its exact receipt, current pointer, target rows and identifiers,
   cleanup, refreshed consumer, private access restrictions, and preservation
   of unrelated data. Restore the previous workflow states.

For rollback, disable and drain the standalone job, clear the activation
variable, and remove only this 36-hour policy if it still matches the approved
value. Restore the recorded legacy workflow states. Keep receipt and operation
history for diagnosis; a later legacy write can invalidate current evidence.
Do not assume a failed workflow rolled back a successful publication.

## Season boundary

The driver uses the existing CFB ingest calendar: August through December maps
to the calendar year, and January through July maps to the previous year.
It therefore continues daily checks of 2026 through July 2027, including
off-season corrections. Once that calendar resolves another season, it makes
no provider or database calls and skips publication and refresh.

Before that boundary, separately review and configure the next season or
retire the 2026 activation and its policy. The old policy is not automatically
changed, so its publication will eventually become stale if daily publication
stops. No 2027 policy or publication is implied by this rollout.
