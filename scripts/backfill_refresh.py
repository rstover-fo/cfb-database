#!/usr/bin/env python3
"""Budget-capped, resumable historical refresh for per-game CFBD endpoints.

CFBD corrected historical data upstream (15k+ garbage-time reclassifications
and other cleanups). The per-game endpoints behind stats.py's
`play_stats_resource` (/plays/stats) and `advanced_game_stats_resource`
(/game/box/advanced) must be re-fetched for ~2014-2025 completed games:
~1,600 games/season x 12 seasons x up to 2 tasks is up to ~38k calls -- far
too much to spend in one run against the 125,000/month budget the daily load
(scripts/load_season.py) also consumes. This script spreads that fan-out
across many capped runs and resumes exactly where an interrupted run left
off, tracked in meta.refresh_campaigns / meta.refresh_progress
(src/schemas/migrations/051_refresh_ledger.sql).

Mechanics:
  - A "campaign" names a scope (seasons + tasks) once, via --create. Every
    later run against the same --campaign reads that scope back and never
    needs it repeated.
  - Backlog per task = completed games (both scores non-null) in the
    campaign's seasons, newest season first then newest week, MINUS the
    game_ids already recorded in meta.refresh_progress for that
    (campaign, task) -- the same set-difference shape
    src/pipelines/run.py's run_metrics_wp_pipeline uses against
    metrics.win_probability.
  - Cross-task allocation: TASK_ORDER (plays_stats, then box_advanced)
    drains completely before the next task spends any of a run's
    --max-calls budget -- see TASK_ORDER's comment for why.
  - Two independent budget guards before any call is made: the ledger-backed
    month guard (SELECT SUM(calls) FROM meta.refresh_progress WHERE
    refreshed_at >= date_trunc('month', now())), which is the one that holds
    on ephemeral CI runners, checked before starting EACH BATCH; and the
    repo's local RateLimiter (its JSON state does not survive CI runners --
    see the dlt-pipelines skill and
    docs/solutions/best-practices/2026-07-28-cfbd-api-usage-audit.md F1 --
    so it is advisory there, but authoritative on a persistent local
    machine), checked once per run. Either guard tripping is a pacing stop
    (prints a message, exits 0), never an error.
  - After each successful batch, the ledger rows are inserted in one
    statement (ON CONFLICT DO NOTHING) and committed immediately, so an
    interrupted run resumes exactly -- no batch is ever re-spent.
  - Every game-id-driven write goes through the SAME dlt pipeline identity
    (pipeline_name="cfbd_stats", dataset_name="stats") that
    src/pipelines/run.py's run_stats_pipeline uses for the normal year-driven
    stats load. This is deliberate and load-bearing: dlt's pipeline state
    (which resolves the destination dataset and dlt schema version) keys off
    pipeline_name, so a mismatch here would not error -- it would silently
    stand up a second, parallel dlt schema pointed at the same Postgres
    tables instead of merging into stats.play_stats / stats.advanced_game_stats.
    See STATS_PIPELINE_NAME/STATS_DATASET_NAME below and
    tests/test_backfill_refresh.py::test_pipeline_identity_matches_run_stats_pipeline,
    which asserts this pair against run_stats_pipeline's own source text.
  - Per-game misses are recorded honestly, not silently dropped (PR #75 F6):
    a suppressed per-game 400, and (advanced_game_stats only) an empty 200,
    are recorded in meta.refresh_progress with status='no_data' instead of
    'refreshed'. Both statuses still count as "done" for backlog purposes
    (excluded by the primary key), but 'no_data' rows can be found and
    requeued with `--requeue-no-data` once the underlying cause is fixed.
    plays_stats' empty-200 is NOT a miss -- zero player-stat associations is
    a legitimate outcome for early-era/lower-division games; see
    play_stats_resource's docstring.
  - Finalize (PR #75 F7 + F1): once a run has loaded new games, or finds
    seasons left unfinalized by a prior run, it calls finalize_campaign --
    scripts/compute_adjusted_epa.py's and scripts/compute_adjusted_epa_week.py's
    compute_seasons() (adjusted-EPA refit, season and walk-forward-weekly)
    plus scripts/refresh_marts.py's refresh_marts() -- over the seasons with
    unfinalized 'refreshed' progress. All three return failure counts and
    never raise; finalize_campaign treats ANY nonzero total as failure,
    prints, and returns False WITHOUT advancing
    meta.refresh_campaigns.last_finalized_at or marking the campaign
    complete -- so a failed refresh is retried next run instead of silently
    lost, and main() exits 1 (status "finalize_failed") so the workflow's
    failure-issue step fires. On success, last_finalized_at advances to the
    exact watermark (MAX(refresh_progress.refreshed_at) covered), not now(),
    so the next run's set-difference against that watermark is race-free.
    IMPORTANT nuance: this refit is cheap insurance, not the real
    propagation path -- compute_adjusted_epa.py/compute_adjusted_epa_week.py
    both fit from marts.play_epa <- core.plays, which this campaign never
    touches. The corrections DO need to reach
    analytics.player_game_epa_build (migration
    022_player_epa_staged_build.sql's per-season loop over stats.play_stats,
    seasons <=2025), but that rebuild stays a documented ONE-TIME manual
    rerun after the campaign fully drains
    (docs/plans/2026-08-29-cfbd-expansion-rollout.md step 7) -- NOT code in
    this script. See finalize_campaign's docstring for why.

Usage:
    # One-time: create the campaign (idempotent -- safe to repeat; a repeat
    # with different --seasons/--tasks is ignored, not an update)
    python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections \\
        --create --seasons 2014-2025 --tasks plays_stats,box_advanced

    # Subsequent runs: drain the backlog, up to --max-calls games this run
    python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections

    # Check progress without spending any calls
    python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections --status

    # Requeue 'no_data' rows (suppressed 400s / empty box-score responses)
    # back into the backlog, optionally scoped to specific tasks
    python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections \\
        --requeue-no-data [--tasks box_advanced]

    # Preview the plan. Connects to the database (read queries only -- never
    # writes meta.refresh_campaigns, meta.refresh_progress, and never calls
    # the CFBD API or refreshes marts) to compute the REAL backlog; there is
    # no DB-free dry run because the backlog is defined against core.games
    # and meta.refresh_progress.
    python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections --dry-run
"""

import argparse
import logging
import math
import sys
from collections.abc import Iterable
from datetime import datetime

import dlt

from src.pipelines.sources.stats import advanced_game_stats_resource, play_stats_resource

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Task registry -- ledger task name -> the game-id-driven dlt resource that
# refreshes it. Both resources live in stats.py, already accept explicit
# game_ids, and are deliberately excluded from stats_source's default
# per-year resource list for exactly this reason -- see their docstrings.
# ---------------------------------------------------------------------------

TASK_RESOURCES = {
    "plays_stats": play_stats_resource,
    "box_advanced": advanced_game_stats_resource,
}

# Canonical cross-task processing order: plays_stats drains completely before
# box_advanced starts spending any of a run's --max-calls budget. plays_stats
# feeds the EPA/adjusted-EPA chain (marts.team_adjusted_epa,
# features.team_week) that the rest of the compute pipeline depends on daily;
# box_advanced's columns have no downstream consumer yet
# (docs/pipeline-manifest.md row 12). A partial run should finish the
# higher-value task first.
TASK_ORDER = ("plays_stats", "box_advanced")

# dlt pipeline identity for the game-id-driven writes -- MUST match
# run_stats_pipeline in src/pipelines/run.py exactly (see module docstring).
STATS_PIPELINE_NAME = "cfbd_stats"
STATS_DATASET_NAME = "stats"


def get_db_url() -> str:
    """Get database URL from dlt secrets or environment.

    Mirrors src/pipelines/run.py's `_metrics_wp_db_url` / scripts/refresh_marts.py's
    `get_db_url` -- the repo-wide convention for a raw psycopg2 connection:
    dlt secrets first, then SUPABASE_DB_URL, then DATABASE_URL.
    """
    import os

    url = None
    try:
        creds = dlt.secrets.get("destination.postgres.credentials")
        if creds:
            url = str(creds)
    except Exception:
        pass

    if not url:
        url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")

    if not url:
        raise RuntimeError(
            "No database URL found. Set destination.postgres.credentials in "
            ".dlt/secrets.toml or SUPABASE_DB_URL/DATABASE_URL environment variable."
        )
    return url


# ---------------------------------------------------------------------------
# Pure helpers -- no DB, no network. Unit-tested directly in
# tests/test_backfill_refresh.py.
# ---------------------------------------------------------------------------


def parse_seasons(spec: str) -> list[int]:
    """Parse a --seasons value.

    "2014-2025" -> an inclusive range. "2014,2016,2018" -> an explicit list.
    A bare single year ("2020") also works (treated as a length-1 range).
    """
    spec = spec.strip()
    if not spec:
        raise ValueError("--seasons must not be empty")
    if "," in spec:
        return [int(s.strip()) for s in spec.split(",") if s.strip()]
    if "-" in spec:
        start_s, _, end_s = spec.partition("-")
        start, end = int(start_s), int(end_s)
        if end < start:
            raise ValueError(f"Invalid season range {spec!r}: end before start")
        return list(range(start, end + 1))
    return [int(spec)]


def parse_tasks(spec: str) -> list[str]:
    """Parse a --tasks value: a comma-separated list of task names, validated
    against TASK_RESOURCES."""
    tasks = [t.strip() for t in spec.split(",") if t.strip()]
    unknown = [t for t in tasks if t not in TASK_RESOURCES]
    if unknown:
        raise ValueError(f"Unknown task(s): {unknown}. Valid: {sorted(TASK_RESOURCES)}")
    return tasks


def resolve_task_order(requested_tasks: list[str]) -> list[str]:
    """`requested_tasks` reordered into TASK_ORDER's canonical drain sequence,
    regardless of the order the operator typed them in --tasks."""
    requested = set(requested_tasks)
    return [t for t in TASK_ORDER if t in requested]


def sort_games_newest_first(
    games: list[tuple[int, int, int]],
) -> list[tuple[int, int, int]]:
    """(game_id, season, week) tuples ordered newest season first, then
    newest week within season, then game_id as a final tiebreaker.

    Recency is the best available proxy for "this is where CFBD's correction
    is most likely to matter" and matches the ordering convention
    run_metrics_wp_pipeline's `_METRICS_WP_GAMES_QUERY` uses, so a capped run
    always drains the most recently affected data first.
    """
    return sorted(games, key=lambda g: (g[1], g[2], g[0]), reverse=True)


def compute_task_backlog(
    candidate_games: list[tuple[int, int, int]],
    done_ids: set[int],
) -> list[int]:
    """The remaining game_ids for one task, newest-first, that still need a
    call.

    `candidate_games`: every completed game in the campaign's seasons, as
    (game_id, season, week) tuples (order does not matter -- this function
    sorts). `done_ids`: game_ids already recorded in meta.refresh_progress
    for this (campaign, task).
    """
    ordered = sort_games_newest_first(list(candidate_games))
    return [gid for gid, _season, _week in ordered if gid not in done_ids]


def allocate_calls(
    backlogs: dict[str, list[int]],
    task_order: list[str],
    max_calls: int,
) -> dict[str, list[int]]:
    """Slice each task's backlog to fit a combined `max_calls` budget for
    this run, draining tasks in `task_order` before spending budget on later
    ones (plays_stats before box_advanced, per TASK_ORDER). One call == one
    game for both tasks, so `max_calls` is directly a game count.

    Returns {task: [game_ids allocated this run]} for every task in
    `task_order` (an empty list for a task the budget never reached).
    """
    allocation: dict[str, list[int]] = {}
    remaining = max_calls
    for task in task_order:
        games = backlogs.get(task, [])
        if remaining <= 0:
            allocation[task] = []
            continue
        take = games[:remaining]
        allocation[task] = take
        remaining -= len(take)
    return allocation


def plan_batches(
    allocation: dict[str, list[int]],
    task_order: list[str],
    batch_size: int,
) -> list[tuple[str, list[int]]]:
    """Flatten this run's per-task allocation into an ordered list of
    (task, batch_game_ids) tuples, chunked to `batch_size` games per dlt
    `pipeline.run()` call, in `task_order`."""
    batches: list[tuple[str, list[int]]] = []
    for task in task_order:
        games = allocation.get(task, [])
        for i in range(0, len(games), batch_size):
            batches.append((task, games[i : i + batch_size]))
    return batches


def would_exceed_monthly_cap(month_spend: int, batch_calls: int, monthly_cap: int) -> bool:
    """True when starting a batch of `batch_calls` calls would push this
    month's ledger-recorded spend past `monthly_cap`.

    This is a pacing stop, not an error: the caller defers the remaining
    batches and exits 0.
    """
    return month_spend + batch_calls > monthly_cap


def is_campaign_complete(backlogs: dict[str, list[int]]) -> bool:
    """True once every task's FULL backlog (not just one run's slice) is
    empty."""
    return all(len(v) == 0 for v in backlogs.values())


def eta_run_days(remaining_total: int, max_calls_per_run: int) -> float:
    """Runs needed to drain `remaining_total` games at `max_calls_per_run`
    per run -- "run-days" because the scheduled workflow fires once a day, so
    a run and a calendar day coincide on the automated path. 0 when nothing
    is left; math.inf when a positive backlog can never drain (max_calls<=0).
    """
    if remaining_total <= 0:
        return 0
    if max_calls_per_run <= 0:
        return math.inf
    return math.ceil(remaining_total / max_calls_per_run)


_LEDGER_INSERT_SQL = """
    INSERT INTO meta.refresh_progress (campaign, task, game_id, calls, status)
    VALUES %s
    ON CONFLICT (campaign, task, game_id) DO NOTHING
"""


def build_ledger_values(
    campaign: str, task: str, game_ids: list[int], misses: Iterable[int] = ()
) -> list[tuple]:
    """The row tuples `insert_ledger_rows` passes to `execute_values` --
    factored out so the idempotency shape (ON CONFLICT DO NOTHING against
    exactly (campaign, task, game_id, calls, status) tuples) is testable
    without a real cursor.

    `misses`: game_ids for which the call was spent but returned nothing
    (a suppressed 400, or -- advanced_game_stats only -- an empty 200; see
    the resources' docstrings in src/pipelines/sources/stats.py). Those rows
    get status='no_data' instead of 'refreshed' (PR #75 F6) -- both still
    count as "done" for backlog purposes (excluded by the primary key
    alone), but 'no_data' can be found and requeued via `--requeue-no-data`
    once the underlying cause (a real gap, a transient API issue, ...) is
    understood.
    """
    miss_set = set(misses)
    return [
        (campaign, task, gid, 1, "no_data" if gid in miss_set else "refreshed") for gid in game_ids
    ]


# ---------------------------------------------------------------------------
# DB-touching helpers
# ---------------------------------------------------------------------------

_CANDIDATE_GAMES_QUERY = """
    SELECT id, season, week
    FROM core.games
    WHERE completed = true
      AND home_points IS NOT NULL
      AND away_points IS NOT NULL
      AND season = ANY(%s)
"""

_DONE_IDS_QUERY = """
    SELECT game_id FROM meta.refresh_progress WHERE campaign = %s AND task = %s
"""

_MONTH_SPEND_QUERY = """
    SELECT COALESCE(SUM(calls), 0) FROM meta.refresh_progress
    WHERE refreshed_at >= date_trunc('month', now())
"""

_CAMPAIGN_SELECT_QUERY = """
    SELECT campaign, description, seasons, tasks, created_at, completed_at, last_finalized_at
    FROM meta.refresh_campaigns WHERE campaign = %s
"""

_CAMPAIGN_INSERT_SQL = """
    INSERT INTO meta.refresh_campaigns (campaign, description, seasons, tasks)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (campaign) DO NOTHING
"""

_CAMPAIGN_COMPLETE_SQL = """
    UPDATE meta.refresh_campaigns SET completed_at = now()
    WHERE campaign = %s AND completed_at IS NULL
"""

# Seasons with 'refreshed' progress rows newer than the campaign's
# last_finalized_at watermark (NULL/never-finalized reads as '-infinity', so
# a fresh campaign's entire history counts) -- what finalize_campaign still
# needs to cover, plus (via fetch_unfinalized) the MAX(refreshed_at) among
# them, which becomes the next watermark on success. status='refreshed' only
# -- 'no_data' rows never move the needle on the EPA refit/mart refresh.
_UNFINALIZED_SEASONS_QUERY = """
    SELECT g.season, MAX(p.refreshed_at)
    FROM meta.refresh_progress p
    JOIN core.games g ON g.id = p.game_id
    WHERE p.campaign = %s
      AND p.status = 'refreshed'
      AND p.refreshed_at > COALESCE(
          (SELECT last_finalized_at FROM meta.refresh_campaigns WHERE campaign = %s),
          '-infinity'
      )
    GROUP BY g.season
    ORDER BY g.season
"""

_NO_DATA_COUNTS_QUERY = """
    SELECT task, COUNT(*) FROM meta.refresh_progress
    WHERE campaign = %s AND status = 'no_data'
    GROUP BY task
"""

_REQUEUE_NO_DATA_SQL = """
    DELETE FROM meta.refresh_progress WHERE campaign = %s AND status = 'no_data'
"""

_REQUEUE_NO_DATA_SQL_WITH_TASKS = _REQUEUE_NO_DATA_SQL + " AND task = ANY(%s)"


def fetch_candidate_games(conn, seasons: list[int]) -> list[tuple[int, int, int]]:
    with conn.cursor() as cur:
        cur.execute(_CANDIDATE_GAMES_QUERY, (seasons,))
        return cur.fetchall()


def fetch_done_ids(conn, campaign: str, task: str) -> set[int]:
    with conn.cursor() as cur:
        cur.execute(_DONE_IDS_QUERY, (campaign, task))
        return {row[0] for row in cur.fetchall()}


def get_month_spend(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(_MONTH_SPEND_QUERY)
        return int(cur.fetchone()[0])


def get_campaign(conn, campaign: str) -> dict | None:
    """Fetch the campaign row, or None if it doesn't exist yet."""
    with conn.cursor() as cur:
        cur.execute(_CAMPAIGN_SELECT_QUERY, (campaign,))
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "campaign": row[0],
        "description": row[1],
        "seasons": list(row[2]),
        "tasks": list(row[3]),
        "created_at": row[4],
        "completed_at": row[5],
        "last_finalized_at": row[6],
    }


def fetch_unfinalized(conn, campaign: str) -> tuple[list[int], datetime | None]:
    """Seasons with 'refreshed' progress rows newer than `campaign`'s
    last_finalized_at watermark, plus the overall MAX(refreshed_at) among
    them -- the watermark finalize_campaign advances to on success.

    ([], None) when there's nothing pending: a fresh campaign with no
    progress yet, or one whose last successful finalize already covers
    every 'refreshed' row.
    """
    with conn.cursor() as cur:
        cur.execute(_UNFINALIZED_SEASONS_QUERY, (campaign, campaign))
        rows = cur.fetchall()
    if not rows:
        return [], None
    seasons = [row[0] for row in rows]
    watermark = max(row[1] for row in rows)
    return seasons, watermark


def fetch_no_data_counts(conn, campaign: str) -> dict[str, int]:
    """Per-task count of status='no_data' ledger rows for `campaign` --
    surfaced by --status so an operator can see how many suppressed
    400s/empty responses accumulated (and are requeue-able) per task."""
    with conn.cursor() as cur:
        cur.execute(_NO_DATA_COUNTS_QUERY, (campaign,))
        return {row[0]: row[1] for row in cur.fetchall()}


def requeue_no_data(conn, campaign: str, tasks: list[str] | None = None) -> int:
    """Delete 'no_data' ledger rows for `campaign` (optionally restricted to
    `tasks`), returning those game_ids to the backlog so the next drain run
    retries them. Returns the number of rows deleted."""
    with conn.cursor() as cur:
        if tasks:
            cur.execute(_REQUEUE_NO_DATA_SQL_WITH_TASKS, (campaign, tasks))
        else:
            cur.execute(_REQUEUE_NO_DATA_SQL, (campaign,))
        rowcount = cur.rowcount
    conn.commit()
    return rowcount


def get_or_create_campaign(
    conn,
    campaign: str,
    create: bool,
    seasons: list[int] | None,
    tasks: list[str] | None,
    description: str | None,
) -> dict | None:
    """Idempotently create the campaign row (ON CONFLICT DO NOTHING) when
    `create` is set, then return the row -- None if it still doesn't exist
    (create=False and never created before).

    A repeat --create against an EXISTING campaign with different
    --seasons/--tasks is deliberately a no-op on those columns (ON CONFLICT
    DO NOTHING): --create names a campaign once, it does not redefine one.
    """
    if create:
        if not seasons:
            raise ValueError("--create requires --seasons")
        if not tasks:
            raise ValueError("--create requires --tasks")
        with conn.cursor() as cur:
            cur.execute(_CAMPAIGN_INSERT_SQL, (campaign, description, seasons, tasks))
        conn.commit()

    row = get_campaign(conn, campaign)

    if create and row is not None and (row["seasons"] != seasons or row["tasks"] != tasks):
        print(
            f"NOTE: campaign {campaign!r} already existed with "
            f"seasons={row['seasons']}, tasks={row['tasks']} -- the --seasons/--tasks "
            "given now were ignored (--create is idempotent, not an update)."
        )

    return row


def mark_campaign_complete(conn, campaign: str) -> None:
    with conn.cursor() as cur:
        cur.execute(_CAMPAIGN_COMPLETE_SQL, (campaign,))
    conn.commit()


def insert_ledger_rows(
    conn, campaign: str, task: str, game_ids: list[int], misses: Iterable[int] = ()
) -> None:
    """Record a successfully-run batch, one row per game -- status='no_data'
    for any game_id in `misses` (a suppressed 400 or empty response; see
    build_ledger_values), 'refreshed' otherwise.

    ON CONFLICT DO NOTHING makes this safe to replay: if a batch's
    `pipeline.run()` succeeded but the process died before this commit,
    re-running the same batch inserts nothing new for games already recorded
    and simply adds the rest -- interruption resumes exactly.
    """
    from psycopg2.extras import execute_values

    values = build_ledger_values(campaign, task, game_ids, misses=misses)
    if not values:
        return
    with conn.cursor() as cur:
        execute_values(cur, _LEDGER_INSERT_SQL, values)
    conn.commit()


def build_stats_pipeline() -> dlt.Pipeline:
    """Same pipeline_name+dataset_name as run_stats_pipeline
    (src/pipelines/run.py) -- see the module docstring for why this must
    never drift."""
    return dlt.pipeline(
        pipeline_name=STATS_PIPELINE_NAME,
        destination="postgres",
        dataset_name=STATS_DATASET_NAME,
    )


def run_batch(pipeline: dlt.Pipeline, task: str, game_ids: list[int]) -> tuple[object, list[int]]:
    """Run one batch through the task's resource, returning (load_info,
    misses) -- `misses` is the list of game_ids the resource itself flagged
    as a suppressed 400 or empty response (PR #75 F6), for the caller to
    pass to insert_ledger_rows so those rows land as status='no_data'
    instead of silently 'refreshed'."""
    resource_fn = TASK_RESOURCES[task]
    misses: list[int] = []
    info = pipeline.run(resource_fn(game_ids=game_ids, misses=misses))
    return info, misses


def should_mark_complete(backlogs: dict[str, list[int]], unfinalized_seasons: list[int]) -> bool:
    """True once every task's FULL backlog is empty AND finalize has nothing
    left to cover -- the single completion predicate shared by every path
    that might mark a campaign done (the top-of-run "already fully drained"
    check, the pending-finalize-only fast path, and the end-of-run
    auto-complete check after a batch loop). Before PR #75 F7, a campaign
    could be marked complete purely on an empty per-game backlog even when a
    finalize (adjusted-EPA refit + mart refresh) was still outstanding or
    had failed -- this closes that gap.
    """
    return is_campaign_complete(backlogs) and not unfinalized_seasons


def finalize_campaign(conn, campaign: str, seasons: list[int], watermark: datetime | None) -> bool:
    """Refit adjusted EPA (season-level and walk-forward-weekly) over
    `seasons` and refresh materialized views, then advance `campaign`'s
    last_finalized_at watermark on full success.

    Why this refit is cheap insurance, not the real correction-propagation
    path (PR #75 F1): this campaign re-fetches stats.play_stats /
    stats.advanced_game_stats, but scripts/compute_adjusted_epa.py and
    scripts/compute_adjusted_epa_week.py both fit from marts.play_epa <-
    core.plays -- a table this campaign never touches. Refitting here
    guards against drift (an interrupted prior fit, coefficients that
    predate this campaign's launch) rather than carrying the campaign's
    corrections downstream.

    The corrections DO eventually need to reach
    analytics.player_game_epa_build (migration
    022_player_epa_staged_build.sql's per-season loop over
    stats.play_stats, seasons <=2025) -- but that rebuild stays a
    documented ONE-TIME manual rerun after the campaign fully drains
    (docs/plans/2026-08-29-cfbd-expansion-rollout.md step 7), not code in
    this script: 022 loops all twelve seasons in a single hardcoded DO
    block (it already timed out at 30 minutes doing exactly that, before
    this campaign existed -- see 022's header) and folding a full rerun
    into every per-run finalize would be enormously wasteful.

    `compute_seasons()` in both compute scripts and `refresh_marts()` all
    open their own DB connections (via each module's own `get_db_url()`)
    and return a failure COUNT rather than raising -- verified against
    their source 2026-08-30. finalize_campaign treats ANY nonzero total as
    failure: it prints and returns False without advancing
    last_finalized_at or committing anything, so the caller must not mark
    the campaign complete and a failed finalize is retried whole on the
    next run rather than silently skipped.

    On success, last_finalized_at is set to `watermark` itself (the MAX
    refresh_progress.refreshed_at fetch_unfinalized returned), never now()
    -- race-free against progress rows written after this finalize's
    queries ran but before its UPDATE commits.
    """
    from scripts.compute_adjusted_epa import compute_seasons as fit_adjusted_epa
    from scripts.compute_adjusted_epa_week import compute_seasons as fit_adjusted_epa_week
    from scripts.refresh_marts import refresh_marts

    print(f"Finalizing campaign {campaign!r}: season(s) {seasons}...")
    failures = fit_adjusted_epa(seasons) + fit_adjusted_epa_week(seasons)
    failures += refresh_marts(concurrently=True)

    if failures:
        print(
            f"Finalize FAILED: {failures} step(s)/season(s) reported failure. "
            "last_finalized_at NOT advanced; will retry next run."
        )
        return False

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE meta.refresh_campaigns SET last_finalized_at = %s WHERE campaign = %s",
            (watermark, campaign),
        )
    conn.commit()
    print(f"Finalize OK. last_finalized_at advanced to {watermark}.")
    return True


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def print_status(conn, campaign: str, max_calls: int = 1000) -> dict:
    """`--status`: print campaign progress and return a summary. Makes no API
    calls, no writes."""
    row = get_campaign(conn, campaign)
    print(f"\n=== Campaign status: {campaign} ===")
    if row is None:
        print("Campaign does not exist yet (pass --create --seasons ... --tasks ... to create it).")
        return {"campaign": campaign, "status": "not_created"}

    task_order = [t for t in TASK_ORDER if t in row["tasks"]]
    candidate_games = fetch_candidate_games(conn, row["seasons"])
    backlogs = {
        t: compute_task_backlog(candidate_games, fetch_done_ids(conn, campaign, t))
        for t in task_order
    }
    total = sum(len(v) for v in backlogs.values())
    month_spend = get_month_spend(conn)
    no_data_counts = fetch_no_data_counts(conn, campaign)
    unfinalized_seasons, _watermark = fetch_unfinalized(conn, campaign)

    print(f"Seasons: {row['seasons']}")
    print(f"Tasks: {row['tasks']}")
    print(f"Created: {row['created_at']}")
    print(f"Completed: {row['completed_at'] or 'not yet'}")
    print(f"Last finalized: {row['last_finalized_at'] or 'never'}")
    for t in task_order:
        no_data = no_data_counts.get(t, 0)
        suffix = f" ({no_data} no_data, requeue-able)" if no_data else ""
        print(f"  {t}: {len(backlogs[t])} game(s) remaining{suffix}")
    print(f"Total backlog: {total}")
    print(f"Month-to-date ledger spend: {month_spend}")
    if unfinalized_seasons:
        print(f"Pending finalize: season(s) {unfinalized_seasons} refreshed since last finalize")
    else:
        print("Pending finalize: none")
    eta = eta_run_days(total, max_calls)
    print(f"ETA to fully drain at --max-calls {max_calls}/run: ~{eta} run(s)")

    return {
        "campaign": campaign,
        "status": "complete" if row["completed_at"] else "in_progress",
        "backlog": backlogs,
        "total_backlog": total,
        "month_spend": month_spend,
        "no_data": no_data_counts,
        "pending_finalize_seasons": unfinalized_seasons,
        "last_finalized_at": row["last_finalized_at"],
    }


def run_campaign(
    conn,
    campaign: str,
    create: bool = False,
    seasons_spec: str | None = None,
    tasks_spec: str = "plays_stats,box_advanced",
    max_calls: int = 1000,
    monthly_cap: int = 30000,
    batch_size: int = 50,
    dry_run: bool = False,
    description: str | None = None,
) -> dict:
    """Drain up to `max_calls` games' worth of backlog for `campaign`,
    respecting the month guard and (when not a dry run) the local rate
    limiter, batching pipeline.run() calls at `batch_size` games.

    Also runs finalize_campaign (adjusted-EPA refit + mart refresh) after
    any run that loaded new games, or that finds season(s) left unfinalized
    by a prior run -- see finalize_campaign's and the module's docstrings
    for what finalize covers and what it deliberately does not (PR #75
    F1/F7). A finalize failure returns status "finalize_failed" WITHOUT
    marking the campaign complete, even when every per-game backlog is
    already empty, so main() can exit 1 and the workflow's failure-issue
    step fires instead of the run going silently green.
    """
    requested_tasks = parse_tasks(tasks_spec)
    seasons = parse_seasons(seasons_spec) if seasons_spec else None

    if create and dry_run:
        # Dry run never writes, even when --create is also given -- simulate
        # the row instead of inserting it.
        print(
            f"[DRY RUN] --create given: would create campaign {campaign!r} with "
            f"seasons={seasons}, tasks={requested_tasks} (skipped; dry run makes no writes)."
        )
        row = {
            "campaign": campaign,
            "description": description,
            "seasons": seasons,
            "tasks": requested_tasks,
            "created_at": None,
            "completed_at": None,
            "last_finalized_at": None,
        }
    else:
        row = get_or_create_campaign(
            conn,
            campaign,
            create=create,
            seasons=seasons,
            tasks=requested_tasks,
            description=description,
        )
        if row is None:
            print(
                f"Campaign {campaign!r} does not exist yet (pass --create --seasons ... "
                "--tasks ... to create it). No-op."
            )
            return {"campaign": campaign, "status": "not_created"}

    if row["completed_at"] is not None:
        print(f"Campaign {campaign!r} already complete (finished {row['completed_at']}). No-op.")
        return {"campaign": campaign, "status": "already_complete"}

    campaign_tasks = row["tasks"]
    unknown_for_campaign = set(requested_tasks) - set(campaign_tasks)
    if unknown_for_campaign:
        raise ValueError(
            f"Task(s) {sorted(unknown_for_campaign)} are not part of campaign {campaign!r} "
            f"(campaign tasks: {sorted(campaign_tasks)})"
        )

    all_task_order = [t for t in TASK_ORDER if t in campaign_tasks]
    run_task_order = resolve_task_order(requested_tasks)

    candidate_games = fetch_candidate_games(conn, row["seasons"])
    full_backlogs = {
        t: compute_task_backlog(candidate_games, fetch_done_ids(conn, campaign, t))
        for t in all_task_order
    }
    total_backlog_all = sum(len(v) for v in full_backlogs.values())

    print(f"\n=== Historical Refresh: {campaign} ===")
    print(f"Seasons: {row['seasons']}")
    print(f"Tasks (campaign): {campaign_tasks}  |  Tasks (this run): {run_task_order}")
    for t in all_task_order:
        print(f"  {t}: {len(full_backlogs[t])} game(s) remaining")
    print(f"Total backlog: {total_backlog_all}")

    unfinalized_seasons, watermark = fetch_unfinalized(conn, campaign)
    finalize_pending_before = bool(unfinalized_seasons)

    if is_campaign_complete(full_backlogs):
        if not finalize_pending_before:
            if not dry_run:
                mark_campaign_complete(conn, campaign)
            print("Backlog empty for every task in the campaign -- campaign complete.")
            return {"campaign": campaign, "status": "complete", "backlog": full_backlogs}

        print(
            f"Backlog empty for every task, but season(s) {unfinalized_seasons} are pending "
            "finalize (adjusted-EPA refit + mart refresh)."
        )
        if dry_run:
            print("[DRY RUN] Would finalize; no computation performed.")
            return {
                "campaign": campaign,
                "status": "dry_run",
                "backlog": full_backlogs,
                "pending_finalize_seasons": unfinalized_seasons,
            }

        # Backlog is already drained -- skip straight to finalize instead of
        # spending any API work.
        if not finalize_campaign(conn, campaign, unfinalized_seasons, watermark):
            print("Finalize failed -- campaign NOT marked complete; will retry next run.")
            return {"campaign": campaign, "status": "finalize_failed", "backlog": full_backlogs}

        unfinalized_seasons, _watermark = fetch_unfinalized(conn, campaign)
        if should_mark_complete(full_backlogs, unfinalized_seasons):
            mark_campaign_complete(conn, campaign)
            print("Finalize succeeded -- campaign complete.")
            return {"campaign": campaign, "status": "complete", "backlog": full_backlogs}

        print(
            "Finalize succeeded, but new unfinalized season(s) appeared in the meantime; "
            "will re-check next run."
        )
        return {"campaign": campaign, "status": "finalized", "backlog": full_backlogs}

    month_spend = get_month_spend(conn)
    print(f"Month-to-date ledger spend: {month_spend} (--monthly-cap {monthly_cap})")
    print(
        f"ETA to fully drain campaign backlog at --max-calls {max_calls}/run: "
        f"~{eta_run_days(total_backlog_all, max_calls)} run(s)"
    )
    if finalize_pending_before:
        print(f"Also pending finalize from a prior run: season(s) {unfinalized_seasons}.")

    run_backlogs = {t: full_backlogs[t] for t in run_task_order}
    allocation = allocate_calls(run_backlogs, run_task_order, max_calls)
    this_run_total = sum(len(v) for v in allocation.values())
    deferred = {t: len(run_backlogs[t]) - len(allocation.get(t, [])) for t in run_task_order}

    # Reported exactly like run_metrics_wp_pipeline: total backlog, this-run
    # slice, deferred count -- never a silent truncation.
    print(f"This run would attempt: {this_run_total} call(s) (--max-calls {max_calls})")
    for t in run_task_order:
        print(f"  {t}: {len(allocation.get(t, []))} this run, {deferred[t]} deferred")
    if sum(deferred.values()):
        print(
            "Backlog capped this run; deferred game(s) newest-first. Re-run to continue draining."
        )

    if dry_run:
        print("[DRY RUN] No API calls made, no ledger rows written, no mart refresh.")
        return {
            "campaign": campaign,
            "status": "dry_run",
            "backlog": full_backlogs,
            "allocation": allocation,
            "month_spend": month_spend,
            "pending_finalize_seasons": unfinalized_seasons,
        }

    # Local, advisory rate-limiter guard. Its JSON state does not survive
    # ephemeral CI runners (dlt-pipelines skill;
    # docs/solutions/best-practices/2026-07-28-cfbd-api-usage-audit.md F1),
    # so on GitHub Actions it always sees a fresh, empty state and basically
    # always passes -- it exists for a persistent local machine where the
    # same state file accumulates calls across days. The ledger's month
    # guard below (checked per-batch) is the check that actually holds in
    # CI.
    from src.pipelines.utils.rate_limiter import get_rate_limiter

    local_limiter = get_rate_limiter()
    if not local_limiter.check_budget(this_run_total):
        print(
            f"Local rate limiter shows only {local_limiter.remaining} call(s) remaining "
            "this month (advisory only on ephemeral CI runners); stopping before spending "
            "any. Not an error."
        )
        return {"campaign": campaign, "status": "local_budget_guard", "backlog": full_backlogs}

    batches = plan_batches(allocation, run_task_order, batch_size)
    pipeline = build_stats_pipeline()

    loaded_total = 0
    no_data_total = 0
    per_task_no_data: dict[str, int] = {}
    pacing_stop = False
    for i, (task, batch) in enumerate(batches, 1):
        if would_exceed_monthly_cap(month_spend, len(batch), monthly_cap):
            print(
                f"Month guard: {month_spend} + {len(batch)} would exceed --monthly-cap "
                f"{monthly_cap}. Stopping before batch {i}/{len(batches)} (pacing stop, not "
                "an error); resume this campaign in a later run/month."
            )
            pacing_stop = True
            break

        print(f"  [{i}/{len(batches)}] {task}: {len(batch)} game(s)")
        _info, misses = run_batch(pipeline, task, batch)
        insert_ledger_rows(conn, campaign, task, batch, misses=misses)
        month_spend += len(batch)
        loaded_total += len(batch)
        if misses:
            no_data_total += len(misses)
            per_task_no_data[task] = per_task_no_data.get(task, 0) + len(misses)

    no_data_suffix = f" ({no_data_total} no_data)" if no_data_total else ""
    print(f"\nLoaded {loaded_total} game-task call(s) this run{no_data_suffix}.")
    for t, n in per_task_no_data.items():
        print(
            f"  {t}: {n} no_data (suppressed 400 or empty response; see --status/--requeue-no-data)"
        )

    # Finalize whenever this run added new 'refreshed' progress, or a prior
    # run left seasons unfinalized (e.g. it loaded games but crashed/was
    # killed before finalizing). Skipped entirely when neither is true --
    # an all-no_data batch with nothing pending from before spends no extra
    # compute on a refit over unchanged data (F1/F7).
    if loaded_total > 0 or finalize_pending_before:
        unfinalized_seasons, watermark = fetch_unfinalized(conn, campaign)
        if unfinalized_seasons:
            print(
                "Finalizing (adjusted-EPA refit + mart refresh) -- corrected data feeds the "
                "EPA chain..."
            )
            if not finalize_campaign(conn, campaign, unfinalized_seasons, watermark):
                print("Finalize failed; will retry next run.")
                return {
                    "campaign": campaign,
                    "status": "finalize_failed",
                    "loaded": loaded_total,
                    "no_data": no_data_total,
                    "backlog": full_backlogs,
                }
        else:
            print("Nothing new to finalize (this run's loads were all no_data).")
    else:
        print("Nothing loaded this run and no season pending finalize; skipping mart refresh.")

    # End-of-run completion check: only when this run wasn't cut short by
    # the month guard and didn't have to defer any of its requested backlog
    # -- i.e. this run's --tasks are now fully drained through the point
    # finalize just covered -- recompute the FULL (all-task) backlog and
    # mark the campaign complete if it, and finalize, are both clean. Fixes
    # the "one extra run" quirk: previously a campaign whose last batch
    # drained the backlog needed a whole separate run just to notice.
    final_backlogs = full_backlogs
    if not pacing_stop and sum(deferred.values()) == 0:
        final_backlogs = {
            t: compute_task_backlog(candidate_games, fetch_done_ids(conn, campaign, t))
            for t in all_task_order
        }
        recheck_unfinalized, _watermark = fetch_unfinalized(conn, campaign)
        if should_mark_complete(final_backlogs, recheck_unfinalized):
            mark_campaign_complete(conn, campaign)
            print("Backlog empty for every task and finalize is current -- campaign complete.")
            return {
                "campaign": campaign,
                "status": "complete",
                "loaded": loaded_total,
                "no_data": no_data_total,
                "backlog": final_backlogs,
            }

    return {
        "campaign": campaign,
        "status": "ok",
        "loaded": loaded_total,
        "no_data": no_data_total,
        "backlog": final_backlogs,
    }


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Budget-capped, resumable historical refresh for per-game CFBD endpoints",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create a campaign (idempotent) and run its first batch
  python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections \\
      --create --seasons 2014-2025 --tasks plays_stats,box_advanced

  # Continue draining an existing campaign (default --max-calls 1000)
  python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections

  # Check progress without spending any calls
  python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections --status

  # Preview the plan (connects read-only; makes no API calls or writes)
  python scripts/backfill_refresh.py --campaign 2026-08-upstream-corrections --dry-run
        """,
    )
    parser.add_argument("--campaign", required=True, help="Campaign name (primary key)")
    parser.add_argument(
        "--create",
        action="store_true",
        help="Create the campaign row if it doesn't exist yet (idempotent), then proceed",
    )
    parser.add_argument(
        "--seasons",
        type=str,
        default=None,
        help="Season range ('2014-2025') or list ('2014,2016'). Required with --create.",
    )
    parser.add_argument(
        "--tasks",
        type=str,
        default="plays_stats,box_advanced",
        help="Comma-separated tasks for this invocation (and, with --create, the campaign's "
        f"declared task set). Valid: {sorted(TASK_RESOURCES)}",
    )
    parser.add_argument(
        "--description", type=str, default=None, help="Campaign description, stored on --create"
    )
    parser.add_argument(
        "--max-calls", type=int, default=1000, help="Per-run cap on API calls (one per game)"
    )
    parser.add_argument(
        "--monthly-cap",
        type=int,
        default=30000,
        help="Refuse to start a batch that would push this month's ledger spend past this",
    )
    parser.add_argument(
        "--batch-size", type=int, default=50, help="Games per dlt pipeline.run() call"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the backlog/allocation plan; connects to the DB for read queries only",
    )
    parser.add_argument(
        "--status", action="store_true", help="Print campaign progress and exit; no API calls"
    )
    parser.add_argument(
        "--requeue-no-data",
        action="store_true",
        help="Delete status='no_data' ledger rows for --campaign (optionally scoped by "
        "--tasks), returning those game_ids to the backlog for retry, then exit. No API "
        "calls; makes no other writes.",
    )
    return parser


def main() -> None:
    parser = create_parser()
    args = parser.parse_args()

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        if args.requeue_no_data:
            tasks_filter = parse_tasks(args.tasks) if args.tasks else None
            rowcount = requeue_no_data(conn, args.campaign, tasks=tasks_filter)
            print(f"Requeued {rowcount} no_data row(s) for campaign {args.campaign!r}.")
            sys.exit(0)

        if args.status:
            print_status(conn, args.campaign, max_calls=args.max_calls)
            sys.exit(0)

        result = run_campaign(
            conn,
            campaign=args.campaign,
            create=args.create,
            seasons_spec=args.seasons,
            tasks_spec=args.tasks,
            max_calls=args.max_calls,
            monthly_cap=args.monthly_cap,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            description=args.description,
        )
    finally:
        conn.close()

    # A failed finalize (adjusted-EPA refit or mart refresh) must fail the
    # run -- PR #75 F7: previously main() always exited 0, so a silently
    # broken finalize was never retried and never surfaced to the workflow's
    # failure-issue step.
    sys.exit(1 if result.get("status") == "finalize_failed" else 0)


if __name__ == "__main__":
    main()
