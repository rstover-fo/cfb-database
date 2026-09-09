#!/usr/bin/env python3
"""Verify a season load actually landed: partition, counts, coverage, freshness.

Intended to run right after scripts/load_season.py (the daily workflow does),
failing loudly so a silent bad load surfaces as a red job instead of a stale
dashboard weeks later.

Usage:
    python scripts/verify_load.py                   # Verify current season
    python scripts/verify_load.py --season 2026     # Verify a specific season
    python scripts/verify_load.py --strict          # Treat staleness as failure year-round

Checks:
    1. plays partitions have correct attachment/bounds and cover the load horizon
    2. core.games row count for the season matches the CFBD /games count (1 API call)
    3. completed FBS-involved games have game_team_stats rows (lower-division
       games are excluded -- CFBD only reliably publishes box scores for games
       with an FBS side; small tolerance for stragglers)
    4. completed FBS-involved games have plays rows (same scope + tolerance)
    5. marts.data_freshness is_stale flags -- heuristic only: the matview infers
       freshness from pg_stat vacuum/analyze timestamps, so staleness WARNs
       off-season and FAILs in-season (or with --strict)
    6. Optional house Elo receipts: publication age and exact recorded inputs;
       unknown cadence/upstream closure WARN, broken required inputs FAIL
    7. Optional season-scoped SDV source receipts: exact complete publication
       evidence and owner-declared age policy for the selected season
    8. ratings.massey_composite has a recent, full-coverage snapshot for the
       season (in-season only; WARNs if migration 041 isn't applied yet)
    9. meta.flat_file_loads has a recent successful 'availability' load
       (in-season only; never FAILs -- external conference sites are flaky)
    10. no unexpected dlt VARIANT (__v_double) twin has appeared on a
       charting source table since the mart(s) reading it were authored
       (KTD7 tripwire; see src/pipelines/utils/variant_twins.py) -- FAILs
       naming the column, since it means a metric is silently reading NULL
       downstream; a query failure (e.g. the table doesn't exist yet) WARNs
       instead of crashing the run

Pre-season semantics: with no completed games, checks 3-4 pass vacuously and
check 2 is the meaningful one (schedules publish in July, so core.games must
already have rows for the upcoming season).
"""

import argparse
import logging
import math
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Completed games allowed to lack box scores / plays before failing.
# FCS opponents and canceled-but-marked-completed games routinely have none.
MISSING_STATS_TOLERANCE = 10

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"


def is_in_season(month: int) -> bool:
    """September through January: weekly tables must be actively refreshing."""
    return month in {9, 10, 11, 12, 1}


def evaluate_missing(missing: int, tolerance: int = MISSING_STATS_TOLERANCE) -> str:
    """Grade a count of completed games missing downstream rows."""
    if missing == 0:
        return PASS
    if missing <= tolerance:
        return WARN
    return FAIL


def evaluate_game_counts(api_count: int, db_count: int) -> str:
    """Grade API-vs-DB game count reconciliation for a season."""
    if api_count > 0 and db_count == 0:
        return FAIL
    if db_count < api_count:
        return FAIL
    return PASS


def evaluate_staleness(frequency: str, in_season: bool, strict: bool) -> str:
    """Grade an is_stale row from marts.data_freshness."""
    if frequency == "weekly" and (in_season or strict):
        return FAIL
    return WARN


class Report:
    def __init__(self) -> None:
        self.failures = 0

    def record(self, status: str, name: str, detail: str) -> None:
        print(f"[{status}] {name}: {detail}")
        if status == FAIL:
            self.failures += 1


def check_partition(cur, season: int, report: Report) -> None:
    from src.pipelines.utils.partitions import PartitionStateError, inspect_play_partitions

    try:
        plan = inspect_play_partitions(cur, [season])
    except PartitionStateError as exc:
        report.record(FAIL, "plays_partition", str(exc))
        return
    report.record(
        FAIL if plan.missing_years else PASS,
        "plays_partition",
        f"validated catalog; missing seasons: {list(plan.missing_years)}",
    )


def check_game_counts(cur, season: int, report: Report) -> None:
    from src.pipelines.sources.base import make_request
    from src.pipelines.utils.api_client import get_client

    client = get_client()
    try:
        api_count = len(make_request(client, "/games", params={"year": season}))
    finally:
        client.close()

    cur.execute("SELECT COUNT(*) FROM core.games WHERE season = %s", (season,))
    db_count = cur.fetchone()[0]
    report.record(
        evaluate_game_counts(api_count, db_count),
        "game_counts",
        f"api={api_count} db={db_count}",
    )


# core.games mirrors CFBD /games, which spans every classification (FBS, FCS,
# II, III -- 3800+ rows/season), but box scores and plays are only reliably
# published for games involving an FBS team. Coverage checks therefore scope
# to FBS-involved games; a season-wide count would "miss" ~2K lower-division
# games by construction and could never clear MISSING_STATS_TOLERANCE.
FBS_INVOLVED = "(g.home_classification = 'fbs' OR g.away_classification = 'fbs')"


def check_completed_have_team_stats(cur, season: int, report: Report) -> None:
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM core.games g
        WHERE g.season = %s AND g.completed AND {FBS_INVOLVED}
          AND NOT EXISTS (SELECT 1 FROM core.game_team_stats s WHERE s.id = g.id)
        """,
        (season,),
    )
    missing = cur.fetchone()[0]
    report.record(
        evaluate_missing(missing),
        "completed_have_team_stats",
        f"{missing} completed FBS game(s) missing box scores (tolerance {MISSING_STATS_TOLERANCE})",
    )


def check_completed_have_plays(cur, season: int, report: Report) -> None:
    # p.season predicate enables partition pruning; plays has no week column
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM core.games g
        WHERE g.season = %s AND g.completed AND {FBS_INVOLVED}
          AND NOT EXISTS (
              SELECT 1 FROM core.plays p WHERE p.season = %s AND p.game_id = g.id
          )
        """,
        (season, season),
    )
    missing = cur.fetchone()[0]
    report.record(
        evaluate_missing(missing),
        "completed_have_plays",
        f"{missing} completed FBS game(s) missing plays (tolerance {MISSING_STATS_TOLERANCE})",
    )


def _current_in_season() -> bool:
    """Wrapper around is_in_season(now); a thin seam tests can monkeypatch."""
    from datetime import datetime

    return is_in_season(datetime.now().month)


def evaluate_snapshot_freshness(
    days_old: int | None, in_season: bool, warn_days: int, fail_days: int
) -> str:
    """Grade the age of the latest snapshot in a weekly-refreshed table.

    Off-season, staleness is expected and never graded. In-season, no
    snapshots at all (days_old is None) only WARNs -- the subsystem may be
    newly deployed and shouldn't fail an otherwise-healthy load.
    """
    if not in_season:
        return PASS
    if days_old is None:
        return WARN
    if days_old <= warn_days:
        return PASS
    if days_old <= fail_days:
        return WARN
    return FAIL


def evaluate_snapshot_team_count(count: int | None, in_season: bool) -> str:
    """Grade team coverage of the latest snapshot.

    A partial snapshot is worse than none -- it signals a parser/crosswalk
    problem rather than a simply-missing load -- so this is stricter than
    evaluate_snapshot_freshness about low (but present) counts.
    """
    if not in_season or count is None:
        return PASS
    if count >= 120:
        return PASS
    if count >= 100:
        return WARN
    return FAIL


def check_massey_composite(cur, season: int, report: Report) -> None:
    # Migration 041 may not be applied yet; to_regclass returns NULL for a
    # missing relation instead of raising, so no transaction-poisoning risk.
    cur.execute("SELECT to_regclass('ratings.massey_composite')")
    if cur.fetchone()[0] is None:
        report.record(WARN, "massey_composite", "table absent (migration 041 not applied)")
        return

    in_season = _current_in_season()

    cur.execute(
        "SELECT MAX(snapshot_date) FROM ratings.massey_composite WHERE season = %s", (season,)
    )
    latest = cur.fetchone()[0]

    days_old = None
    team_count = None
    if latest is not None:
        from datetime import date

        days_old = (date.today() - latest).days
        cur.execute(
            """
            SELECT COUNT(*) FROM ratings.massey_composite
            WHERE season = %s AND snapshot_date = %s
            """,
            (season, latest),
        )
        team_count = cur.fetchone()[0]

    report.record(
        evaluate_snapshot_freshness(days_old, in_season, warn_days=8, fail_days=14),
        "massey_freshness",
        f"latest snapshot {latest} ({days_old if days_old is not None else 'n/a'} days old)",
    )
    report.record(
        evaluate_snapshot_team_count(team_count, in_season),
        "massey_team_count",
        f"{team_count if team_count is not None else 'n/a'} team(s) in latest snapshot",
    )


def check_availability_archive(cur, season: int, report: Report) -> None:
    # Same missing-migration guard as check_massey_composite.
    cur.execute("SELECT to_regclass('meta.flat_file_loads')")
    if cur.fetchone()[0] is None:
        report.record(WARN, "availability_archive", "table absent (migration 041 not applied)")
        return

    in_season = _current_in_season()

    cur.execute(
        """
        SELECT MAX(loaded_at) FROM meta.flat_file_loads
        WHERE source = 'availability' AND status = 'loaded'
        """
    )
    latest = cur.fetchone()[0]

    if not in_season:
        report.record(PASS, "availability_archive", f"off-season, latest load {latest}")
        return

    if latest is None:
        report.record(WARN, "availability_archive", "no successful availability load recorded")
        return

    from datetime import UTC, datetime

    days_old = (datetime.now(UTC) - latest).days
    # Never FAIL: external conference/archive sites are flaky and staleness
    # here must not fail the whole daily load.
    status = PASS if days_old <= 8 else WARN
    report.record(
        status,
        "availability_archive",
        f"latest load {days_old} day(s) old (source=availability)",
    )


def check_fitted_coverage(cur, report: Report) -> None:
    """fitted_v1 must have a prediction for (nearly) every pending game.

    Independent of score_fitted.py's own gate on purpose: that gate only fires
    when the scoring step actually runs, and the failure this guards against --
    features.team_week never built for the upcoming season -- produced a clean
    exit-0 there for six months. Checking it from the post-load verifier means
    the shortfall surfaces even if the scoring step is skipped, reordered, or
    silently no-ops again.

    Season-independent (the pending window spans whatever seasons have
    schedules), so it takes no season argument.
    """
    from scripts.score_fitted import (
        MIN_UPCOMING_COVERAGE,
        PENDING_GAMES_WHERE,
        coverage_verdict,
    )

    cur.execute(
        f"""
        WITH pending AS (
            SELECT g.id, g.season, g.start_date
            FROM core.games g
            WHERE {PENDING_GAMES_WHERE}
        )
        SELECT p.season, COUNT(*),
               SUM(CASE WHEN EXISTS (
                   SELECT 1 FROM predictions.game_predictions gp
                   WHERE gp.game_id = p.id
                     AND gp.model_version = 'fitted_v1'
                     AND gp.evaluation_mode = 'published_forecast'
                     AND gp.published_at IS NOT NULL
                     AND (p.start_date IS NULL OR gp.published_at < p.start_date)
               ) THEN 1 ELSE 0 END)
        FROM pending p
        GROUP BY p.season
        ORDER BY p.season
        """
    )
    counts = cur.fetchall()
    if not counts:
        report.record(PASS, "fitted_coverage", "no canonical pending games")
    for season, n_pending, n_scored in counts:
        n_pending, n_scored = int(n_pending), int(n_scored)
        ok, coverage = coverage_verdict(n_pending, n_scored)
        report.record(
            PASS if ok else FAIL,
            "fitted_coverage",
            f"season={season}: fitted_v1 covers {n_scored}/{n_pending} pending game(s) "
            f"({coverage:.1%}, threshold {MIN_UPCOMING_COVERAGE:.0%})",
        )


# How old the newest predictions.model_backtest row may be before the honesty
# numbers count as stale. The daily workflow re-runs the backtest every morning,
# so anything past a couple of days means the step stopped running -- generous
# enough to absorb a single failed night without crying wolf.
MAX_BACKTEST_AGE_DAYS = 3


def check_backtest_freshness(cur, report: Report) -> None:
    """The published accuracy numbers must be current, not merely present.

    cfb-app reads api.model_backtest instead of hardcoding win MAE and the
    interval, and its staleness check is `run_date`. That only means something
    if something is actually advancing run_date -- otherwise a consumer reads a
    plausible row describing a model that no longer exists, and nothing fails.
    Precisely the shape this table was built to end.

    It is also the failure that already happened once: migration 045 shipped
    the view before anything had written to it, so api.model_backtest returned
    zero rows while the handoff said to depend on it. NO ROW is therefore a
    FAIL here, not a skip -- "never backtested" and "backtested last week" are
    different problems, but neither is a working state.
    """
    # Scope to the CANONICAL configuration, not merely to fitted_v1/fbs
    # (PR #57 review, P2). api.model_backtest deliberately keys on
    # (model_version, scope, season_start, season_end, strength_share) because
    # those are different measurements, so a one-off exploratory run -- a
    # narrower season range, a swept strength_share -- writes its own row with
    # today's date. A MAX(run_date) over all FBS rows would then be refreshed
    # by an experiment while the published row sat a month stale, which is the
    # same shape of not-quite-the-right-check this gate exists to catch.
    #
    # The bounds are IMPORTED, not restated. The daily workflow runs
    # backtest_preseason.py with no arguments, so the script defaults are the
    # canonical configuration by construction and the gate cannot drift from
    # what the workflow actually writes. (Restating them here is how the row
    # published to cfb-app ended up on --start 2019 against a DEFAULT_START of
    # 2018 -- a configuration the daily job would never reproduce.)
    from scripts.backtest_preseason import DEFAULT_END, DEFAULT_START
    from scripts.simulate_season import DEFAULT_STRENGTH_SHARE

    cur.execute(
        """
        SELECT MAX(run_date),
               (SELECT COUNT(*) FROM predictions.model_backtest)
        FROM predictions.model_backtest
        WHERE model_version = 'fitted_v1' AND scope = 'fbs'
          AND season_start = %(start)s AND season_end = %(end)s
          AND strength_share = %(share)s
        """,
        {"start": DEFAULT_START, "end": DEFAULT_END, "share": DEFAULT_STRENGTH_SHARE},
    )
    latest, total = cur.fetchone()
    if total == 0 or latest is None:
        report.record(
            FAIL,
            "backtest_freshness",
            f"predictions.model_backtest has no fitted_v1/fbs row for the canonical "
            f"{DEFAULT_START}-{DEFAULT_END} @ {DEFAULT_STRENGTH_SHARE} configuration -- "
            "api.model_backtest publishes nothing and consumers have no honesty numbers",
        )
        return
    cur.execute("SELECT ((now() AT TIME ZONE 'utc')::date - %s)", (latest,))
    age = int(cur.fetchone()[0])
    report.record(
        PASS if age <= MAX_BACKTEST_AGE_DAYS else FAIL,
        "backtest_freshness",
        f"newest canonical fitted_v1/fbs backtest ({DEFAULT_START}-{DEFAULT_END} "
        f"@ {DEFAULT_STRENGTH_SHARE}) is {age} day(s) old "
        f"(run_date {latest}, threshold {MAX_BACKTEST_AGE_DAYS})",
    )


def _safe_execute(cur, sql: str) -> None:
    """Best-effort execute for a SAVEPOINT/ROLLBACK/RELEASE bracket
    statement -- must never raise into the caller. If the statement itself
    fails (e.g. cur has no real `.execute`, as in unit tests, or the
    connection turns out not to support it), the caller just proceeds
    without savepoint protection rather than crashing the tripwire."""
    try:
        cur.execute(sql)
    except Exception:  # noqa: BLE001 - the savepoint bracket must never crash the check
        pass


def check_variant_twins(cur, report: Report) -> None:
    """KTD7 tripwire: no unexpected dlt VARIANT (__v_double) twin column.

    dlt types a metric column bigint on first load and creates a sibling
    `<col>__v_double` twin the first time a later load carries a fractional
    value; every value dlt can't fit in the bigint column from then on lands
    in the twin instead. The marts tracked in
    src/pipelines/utils/variant_twins.py's EXPECTED_VARIANT_TWINS COALESCE
    exactly the twins that existed live when they were authored (the
    rushing/passing charting allow-list also mirrors
    src/schemas/api/validation_rushing_views.sql's deploy-time check). A
    daily load that pushes a previously-clean column into VARIANT territory
    creates a twin no mart's COALESCE accounts for -- the affected metric
    goes silently NULL in the mart, its api view, and any RPC reading the
    mart directly. This check is what catches that between deploys, since
    the SQL validation file only runs at deploy time.

    Query failure (e.g. a tracked table doesn't exist on this database yet)
    WARNs rather than crashing the daily run -- this check is a tripwire,
    not a hard dependency of the load. But verify() runs every check on one
    shared, non-autocommit connection (psycopg2.connect() defaults to
    transactional mode), so a PostgreSQL-level error from the catalog
    queries below (e.g. a genuinely malformed query, not just "table
    missing" which information_schema tolerates as zero rows) would leave
    that connection's transaction in the aborted state -- every check that
    runs afterward would then fail with InFailedSqlTransaction, not just
    this one. A SAVEPOINT around the two catalog queries contains that:
    ROLLBACK TO SAVEPOINT on failure clears the abort without touching
    whatever check_partition/check_game_counts/etc. already did earlier in
    the same transaction, and RELEASE SAVEPOINT on success just drops the
    bookkeeping. SAVEPOINT is only valid inside a transaction block, so an
    autocommit connection (cur.connection.autocommit is True) skips the
    dance entirely -- there every statement is already its own transaction
    and a failure can't poison a later one.
    """
    from src.pipelines.utils.variant_twins import find_missing_twins, find_unexpected_twins

    use_savepoint = not getattr(getattr(cur, "connection", None), "autocommit", False)
    if use_savepoint:
        _safe_execute(cur, "SAVEPOINT variant_twins")

    try:
        unexpected = find_unexpected_twins(cur)
        missing = find_missing_twins(cur)
    except Exception as exc:  # noqa: BLE001 - tripwire must never crash the run
        if use_savepoint:
            _safe_execute(cur, "ROLLBACK TO SAVEPOINT variant_twins")
        report.record(WARN, "variant_twins", f"check could not run: {exc}")
        return

    if use_savepoint:
        _safe_execute(cur, "RELEASE SAVEPOINT variant_twins")

    if unexpected:
        for table_key, columns in sorted(unexpected.items()):
            report.record(
                FAIL,
                "variant_twins",
                f"{table_key}: unexpected __v_double twin(s) {columns} -- add the COALESCE to "
                "the affected mart, extend EXPECTED_VARIANT_TWINS and the matching "
                "validation_rushing_views.sql allow-list, then re-apply the mart",
            )
    else:
        report.record(PASS, "variant_twins", "no unexpected __v_double twins")

    if missing:
        for table_key, columns in sorted(missing.items()):
            report.record(
                WARN,
                "variant_twins_missing",
                f"{table_key}: expected __v_double twin(s) {columns} not present "
                "(informational -- OK if the table was recreated or hasn't loaded a "
                "fractional value for that metric yet)",
            )


def check_freshness(cur, in_season: bool, strict: bool, report: Report) -> None:
    cur.execute(
        """
        SELECT schema_name, table_name, expected_refresh_frequency, days_since_activity
        FROM marts.data_freshness
        WHERE is_stale
        ORDER BY schema_name, table_name
        """
    )
    rows = cur.fetchall()
    if not rows:
        report.record(PASS, "data_freshness", "no stale tables")
        return

    for schema_name, table_name, frequency, days in rows:
        report.record(
            evaluate_staleness(frequency, in_season, strict),
            "data_freshness",
            f"{schema_name}.{table_name} stale ({frequency}, {days} days since activity)",
        )


RECEIPT_FRESHNESS_ASSETS = ("analytics.house_elo_game", "marts.house_elo_game")
NONCURRENT_RECEIPT_OUTCOMES = {"failed", "deferred", "partial", "blocked"}


def check_receipt_freshness(cur, in_season: bool, strict: bool, report: Report) -> None:
    """Inspect optional source-wide publication evidence, not season/provider coverage."""
    cur.execute("SELECT to_regprocedure('public.get_asset_freshness()')")
    if cur.fetchone()[0] is None:
        report.record(WARN, "receipt_freshness", "receipt freshness migration is not installed")
        return
    # A permission/transport/query error must propagate as a failed verification,
    # not be swallowed or leave a silently aborted transaction for later checks.
    cur.execute("SELECT to_jsonb(f) FROM public.get_asset_freshness() f")
    rows = [row[0] for row in cur.fetchall()]
    keys = [(row.get("asset_key"), row.get("coverage_key")) for row in rows]
    expected = {(asset, "source-wide") for asset in RECEIPT_FRESHNESS_ASSETS}
    if len(keys) != len(expected) or set(keys) != expected:
        report.record(
            FAIL, "receipt_freshness", "expected exactly both source-wide house Elo assets"
        )
        return

    for row in rows:
        asset = row["asset_key"]
        if row.get("latest_outcome") in NONCURRENT_RECEIPT_OUTCOMES:
            report.record(
                WARN,
                "receipt_attempt",
                f"{asset}: latest attempt {row['latest_outcome']}; "
                "this does not replace current publication evidence",
            )
        state = row.get("publication_state")
        if state == "unrecorded" and row.get("generation_id") is None:
            report.record(WARN, "receipt_freshness", f"{asset}: optional publisher has no receipts")
            continue
        if state == "unpublished" and row.get("generation_id") is None:
            report.record(
                FAIL if strict else WARN,
                "receipt_freshness",
                f"{asset}: receipt history exists but no valid current publication",
            )
            continue
        if (
            state != "current"
            or not row.get("generation_id")
            or not row.get("published_at")
            or row.get("current_outcome") not in {"succeeded", "expected_no_data"}
            or (row.get("coverage") or {}).get("complete") is not True
        ):
            report.record(FAIL, "receipt_freshness", f"{asset}: invalid current receipt evidence")
            continue
        if (
            row.get("recorded_inputs_current") is False
            or row.get("input_closure_current") is False
            or (asset == "marts.house_elo_game" and row.get("recorded_inputs_current") is not True)
        ):
            report.record(
                FAIL, "receipt_freshness", f"{asset}: required input generation is not current"
            )
            continue
        details = [f"{asset}: current {row['current_outcome']} publication"]
        status = PASS
        if row.get("is_stale") is True:
            status = FAIL if in_season or strict else WARN
            details.append(f"age {row.get('age_seconds')}s exceeds declared publication interval")
        elif row.get("is_stale") is None or row.get("expected_refresh_interval") is None:
            status = WARN
            details.append("publication age policy is undeclared")
        if (
            row.get("input_closure_current") is not True
            or row.get("unversioned_input_assets") != []
        ):
            if status == PASS:
                status = WARN
            details.append("upstream closure unverified (core.games is unversioned)")
        report.record(status, "receipt_freshness", "; ".join(details))


SOURCE_FRESHNESS_IDENTITIES = (
    ("sdv_ratings_weekly", "ratings.sdv_ratings_weekly"),
    ("sdv_fpi_weekly", "ratings.espn_fpi_weekly"),
    ("sdv_team_xwalk", "ref.team_id_xwalk"),
    ("sdv_game_xwalk", "ref.game_id_xwalk"),
)
SOURCE_FRESHNESS_FIELDS = frozenset(
    {
        "source_name",
        "asset_key",
        "season",
        "coverage_key",
        "generation_id",
        "published_at",
        "age_seconds",
        "expected_refresh_interval",
        "is_stale",
        "publication_state",
        "current_outcome",
        "is_complete",
        "source_rows",
        "published_rows",
        "artifact_origin",
        "season_basis",
        "latest_outcome",
        "latest_recorded_at",
        "last_failure_outcome",
        "last_failure_at",
        "last_failure_category",
    }
)
_SOURCE_CURRENT_FIELDS = (
    "generation_id",
    "published_at",
    "age_seconds",
    "is_stale",
    "current_outcome",
    "is_complete",
    "source_rows",
    "published_rows",
    "artifact_origin",
    "season_basis",
)
_SOURCE_PUBLICATION_STATES = {"unrecorded", "unpublished", "invalid", "current"}
_SOURCE_TERMINAL_OUTCOMES = {
    "succeeded",
    "expected_no_data",
    *NONCURRENT_RECEIPT_OUTCOMES,
}


def _nonempty_string(value) -> bool:
    return type(value) is str and bool(value)


def _source_row_contract_error(row: dict, source_name: str) -> str | None:
    """Return why a typed source-freshness row is internally inconsistent."""
    state = row["publication_state"]
    if state not in _SOURCE_PUBLICATION_STATES:
        return "unknown publication state"

    interval = row["expected_refresh_interval"]
    if interval is not None and not _nonempty_string(interval):
        return "invalid refresh interval"
    if row["is_stale"] is not None and type(row["is_stale"]) is not bool:
        return "invalid staleness value"

    latest_outcome = row["latest_outcome"]
    latest_at = row["latest_recorded_at"]
    if state == "unrecorded":
        if latest_outcome is not None or latest_at is not None:
            return "unrecorded source has receipt history"
    elif latest_outcome not in _SOURCE_TERMINAL_OUTCOMES or not _nonempty_string(latest_at):
        return "receipt history is malformed"

    failure_outcome = row["last_failure_outcome"]
    failure_at = row["last_failure_at"]
    failure_category = row["last_failure_category"]
    if state == "unrecorded" and any(
        value is not None for value in (failure_outcome, failure_at, failure_category)
    ):
        return "unrecorded source has receipt history"
    if failure_outcome is None:
        if failure_at is not None or failure_category is not None:
            return "failure history is malformed"
    elif (
        failure_outcome not in NONCURRENT_RECEIPT_OUTCOMES
        or not _nonempty_string(failure_at)
        or failure_category not in {None, "source_publication_failed"}
    ):
        return "failure history is malformed"

    if state != "current":
        if any(row[field] is not None for field in _SOURCE_CURRENT_FIELDS):
            return "non-current source exposes current publication evidence"
        return None

    if (
        not _nonempty_string(row["generation_id"])
        or not _nonempty_string(row["published_at"])
        or row["current_outcome"] != "succeeded"
        or row["is_complete"] is not True
        or type(row["source_rows"]) is not int
        or type(row["published_rows"]) is not int
        or not 1 <= row["source_rows"] <= 100_000
        or row["published_rows"] != row["source_rows"]
        or row["artifact_origin"] not in {"registered_url", "local_file"}
    ):
        return "invalid current publication evidence"
    age = row["age_seconds"]
    if (
        isinstance(age, bool)
        or not isinstance(age, (int, float))
        or not math.isfinite(age)
        or age < 0
    ):
        return "invalid publication age"

    if source_name in {"sdv_ratings_weekly", "sdv_fpi_weekly"}:
        expected_basis = "artifact_field"
    elif row["artifact_origin"] == "registered_url":
        expected_basis = "registered_artifact_name"
    else:
        expected_basis = "caller_declared"
    if row["season_basis"] != expected_basis:
        return "invalid season evidence"
    return None


def check_source_freshness(cur, season: int, in_season: bool, strict: bool, report: Report) -> None:
    """Inspect optional complete-file publication evidence for one SDV season."""
    cur.execute("SELECT to_regprocedure('public.get_source_freshness(bigint)')")
    if cur.fetchone()[0] is None:
        report.record(
            WARN,
            "source_freshness",
            "season source freshness migration is not installed",
        )
        return

    cur.execute(
        "SELECT to_jsonb(f) FROM public.get_source_freshness(%s) f",
        (season,),
    )
    rows = [item[0] for item in cur.fetchall()]
    if any(type(row) is not dict or frozenset(row) != SOURCE_FRESHNESS_FIELDS for row in rows):
        report.record(FAIL, "source_freshness", "source freshness row shape is invalid")
        return

    coverage_key = f"season:{season}"
    expected = {
        (source_name, asset_key, season, coverage_key)
        for source_name, asset_key in SOURCE_FRESHNESS_IDENTITIES
    }
    identities = [
        (row["source_name"], row["asset_key"], row["season"], row["coverage_key"]) for row in rows
    ]
    identities_are_typed = all(
        _nonempty_string(source_name)
        and _nonempty_string(asset_key)
        and type(row_season) is int
        and _nonempty_string(row_coverage)
        for source_name, asset_key, row_season, row_coverage in identities
    )
    if not identities_are_typed or len(identities) != len(expected) or set(identities) != expected:
        report.record(
            FAIL,
            "source_freshness",
            f"expected exactly four SDV source assets for {coverage_key}",
        )
        return

    by_source = {row["source_name"]: row for row in rows}
    for source_name, asset_key in SOURCE_FRESHNESS_IDENTITIES:
        row = by_source[source_name]
        contract_error = _source_row_contract_error(row, source_name)
        if contract_error is not None:
            report.record(
                FAIL,
                "source_freshness",
                f"{asset_key}/{coverage_key}: {contract_error}",
            )
            continue

        latest_outcome = row["latest_outcome"]
        if latest_outcome in NONCURRENT_RECEIPT_OUTCOMES:
            report.record(
                WARN,
                "source_receipt_attempt",
                f"{asset_key}/{coverage_key}: latest attempt {latest_outcome}; "
                "this does not replace current publication evidence",
            )

        state = row["publication_state"]
        if state == "unrecorded":
            report.record(
                WARN,
                "source_freshness",
                f"{asset_key}/{coverage_key}: optional publisher has no receipts",
            )
            continue
        if state == "unpublished":
            report.record(
                FAIL if strict else WARN,
                "source_freshness",
                f"{asset_key}/{coverage_key}: receipt history exists but no valid "
                "current publication",
            )
            continue
        if state == "invalid":
            report.record(
                FAIL,
                "source_freshness",
                f"{asset_key}/{coverage_key}: current publication pointer is invalid",
            )
            continue

        status = PASS
        details = [f"{asset_key}/{coverage_key}: current succeeded complete publication"]
        if row["is_stale"] is True:
            status = FAIL if in_season or strict else WARN
            details.append("age exceeds declared publication interval")
        elif row["is_stale"] is None or row["expected_refresh_interval"] is None:
            status = WARN
            details.append("publication age policy is undeclared or unknown")
        report.record(status, "source_freshness", "; ".join(details))


def verify(season: int, strict: bool) -> int:
    """Run all checks. Returns the number of failures."""
    from datetime import datetime

    import psycopg2

    from scripts.refresh_marts import get_db_url

    in_season = is_in_season(datetime.now().month)
    report = Report()

    conn = psycopg2.connect(get_db_url())
    try:
        with conn.cursor() as cur:
            check_partition(cur, season, report)
            check_game_counts(cur, season, report)
            check_completed_have_team_stats(cur, season, report)
            check_completed_have_plays(cur, season, report)
            check_fitted_coverage(cur, report)
            check_backtest_freshness(cur, report)
            check_freshness(cur, in_season, strict, report)
            check_receipt_freshness(cur, in_season, strict, report)
            check_source_freshness(cur, season, in_season, strict, report)
            check_variant_twins(cur, report)
            check_massey_composite(cur, season, report)
            check_availability_archive(cur, season, report)
    finally:
        conn.close()

    if report.failures:
        logger.error(f"{report.failures} check(s) FAILED for season {season}")
    else:
        logger.info(f"All checks passed for season {season}")
    return report.failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify a season load")
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Season to verify (default: current season)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail weekly staleness off-season and missing current receipt publications",
    )
    args = parser.parse_args()

    season = args.season
    if season is None:
        from src.pipelines.config.years import get_current_season

        season = get_current_season()
        logger.info(f"No --season given; verifying current season {season}")

    sys.exit(1 if verify(season, strict=args.strict) else 0)


if __name__ == "__main__":
    main()
