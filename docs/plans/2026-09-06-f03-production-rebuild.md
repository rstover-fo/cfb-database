# F03 production rebuild

The user approved the production rebuild after PR 123 merged. A read-only
[preflight](https://github.com/rstover-fo/cfb-database/actions/runs/34063729439)
stopped before writes: the contiguous closed frontier was 2019. A
[status diagnostic](https://github.com/rstover-fo/cfb-database/actions/runs/34063816688)
and [replacement diagnostic](https://github.com/rstover-fo/cfb-database/actions/runs/34063999225)
identified the exact causes below.

## Reviewed resolutions

| Provider IDs | Resolution and evidence |
|---|---|
| 2020 spring games | Recognize CFBD's documented `spring_regular` / `spring_postseason` for closure only; retain raw types and calendar guards. [CFBD schema](https://apinext.collegefootballdata.com/api/games). |
| 401540999, 401545766, 401545768, 401545773, 401545780, 401545781 | Alderson Broaddus suspended athletics before the 2023 season. [Conference statement](https://mountaineast.org/news/2023/8/11/general-mec-announces-2023-24-non-conference-scheduling-agreement-with-salem.aspx). |
| 401549719 → 401611307 | Bowdoin–Trinity moved from October 28 to November 18, 2023; replacement is 21–58. [Rescheduling notice](https://athletics.middlebury.edu/news/2023/10/27/nescac-football-schedule-adjusted.aspx), [Bowdoin results](https://athletics.bowdoin.edu/sports/football/opponent-history/trinity/119). |
| 401550299 → 401611308 | Bates–Williams moved to November 18, 2023; replacement is 0–43. [Williams schedule](https://ephsports.williams.edu/sports/football/schedule/2023). |
| 401552878 → 401611306 | Colby–Middlebury moved to November 18, 2023; replacement is 28–35. [Middlebury schedule](https://athletics.middlebury.edu/sports/football/schedule/2023). |
| 401552884 → 401612659 | Worcester–Framingham played November 11, 2023, 6–41. The November 12 stub is superseded by the matching completed provider record. [Worcester game log](https://worcester.prestosports.com/sports/fball/2023-24/bios/sturgis_turnbull_qc2l?view=gamelog). |
| 401640992 | App State–Liberty canceled September 28, 2024; explicitly not rescheduled. [App State announcement](https://appstatesports.com/news/2024/9/27/app-state-liberty-football-game-canceled.aspx). |
| 401677463 | Taylor–Defiance played November 2, 2024, 63–12. Repair missing scores and completion. [Taylor game log](https://taylor2023.prestosports.com/sports/fball/2024-25/bios/loveless_carter_825c). |
| 401773541 | Sul Ross–Angelo State played October 25, 2025, 0–62. Restore the missing observed zero. [Official box score](https://srlobos.com/sports/football/stats/2025/angelo-state/boxscore/3842). |
| 401833535 | Delta State–Kentucky State canceled November 15, 2025, despite provider `completed=true`. [Delta State schedule](https://gostatesmen.com/sports/football/schedule/2025). |

Replacement rows were matched against production season, home/away IDs, date,
and score. No old unresolved row is excluded solely because of its age. The
existing Campbell–Western Carolina replacement remains 401866625 → 401917058.

## Recovery sequence

1. Explicit-file `2026-09-06-f03-data-repair.sql` archives the two original game
   rows and frozen model tables in private `recovery.f03_row_archive`, restores
   only reviewed results, and archives/removes snapshots for 401866625. Exact
   identity, non-conflicting score, and archived-payload equality checks abort
   on drift. Locks prevent concurrent writes during the short transaction.
   The journal is outside schemas with blanket consumer grants. Raw event
   identities and dlt provenance are retained. The original game row values
   are recoverable from the journal.
2. `recover_season_projections --check` validates closure, target season, the
   frozen 2025 fit, and an actual synthetic scorer execution. `--execute` repeats
   preflight and holds fit-table SHARE locks across all rebuild subprocesses.
   It rebuilds 2026 Elo/EPA/features/predictions/outlooks and refreshes marts,
   including freshness. Dispatch uses the `daily-season-load` concurrency group.
3. `2026-09-06-f03-rebuild-verification.sql` verifies unchanged complete fit
   tables, retained raw identity pair, no superseded rows in modeled outputs,
   fresh valid scores for all pending 2026 targets, and complete 12-game
   Campbell/Western Carolina projections. It reads consumer views under actual
   caller roles and checks archive access.

The two reviewed result corrections are also applied to copies during future
games ingestion, so a later provider refresh cannot silently restore the same
known missing fields. Conflicting provider scores or identities fail explicitly.
No training or historical feature rebuild is included. Historical stored
features/predictions beyond the affected 2026 recovery remain outside this run.

## Verification before production writes

- Root suite: **2,360 passed, 449 skipped** with the documented virtualenv PATH.
- Affected Ruff lint/format and whitespace checks passed.
- PostgreSQL 16 disposable fixture executed the repair twice, verified observed
  zero/score preservation, exact archival, owner-only access, and rollback for
  conflicting scores/archive payloads. The real postflight SQL also executed
  against the fixture under anon, authenticated, and analyst_ro.
- Production execution and final results are recorded below after completion.

## Production receipts

- [Guarded data repair](https://github.com/rstover-fo/cfb-database/actions/runs/34064297499)
  succeeded September 6, 2026, 22:32 UTC. Both reviewed results were restored;
  **117** superseded-event prediction snapshots were archived and removed.
- [Derived rebuild](https://github.com/rstover-fo/cfb-database/actions/runs/34064338454)
  dispatched from `16d5724` with `recover_season_projections --execute`.
- Independent review's source-season guard and NULL-completion/latest-consumer
  verification findings were resolved. PostgreSQL exercised the added NULL
  pending-game failure case. The verification script is on `420ea8a`.

Merge PR 124 so subsequent main-branch daily runs use the reconciled lifecycle
and durable source corrections; running the recovery branch does not update
the code used by scheduled jobs on main.
