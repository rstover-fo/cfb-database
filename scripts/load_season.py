#!/usr/bin/env python3
"""Load or refresh all data for a specific season.

Orchestrates pipeline sources in dependency order and refreshes materialized views.

Usage:
    python scripts/load_season.py                                   # Load current season
    python scripts/load_season.py --season 2025                     # Load everything for 2025
    python scripts/load_season.py --season 2025 --sources games,stats  # Load specific sources
    python scripts/load_season.py --season 2026 --sources stats:player_returning  # One resource
    python scripts/load_season.py --season 2025 --dry-run           # Show what would run
    python scripts/load_season.py --season 2025 --skip-refresh      # Load data, skip mart refresh
    python scripts/load_season.py --season 2025 --weekly            # game_stats week-by-week
"""

import argparse
import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Source loading order matters: dependencies first
SOURCE_ORDER = [
    "reference",  # Teams, conferences, venues (no year filter)
    "conferences",  # Conference membership: affiliations (bulk) + changes (year-driven)
    "coaches",  # Year-driven coach-season records (coach_tenures excluded, see below)
    "games",  # Game results
    "playoffs",  # CFP bracket/games/participants, 2014+
    "game_stats",  # Team and player box scores
    "plays",  # Play-by-play (largest dataset)
    "stats",  # Aggregated season stats
    "ratings",  # SP+, Elo, FPI, SRS, CORE, SRS-expanded
    "rankings",  # AP, Coaches polls
    "recruiting",  # Recruits, team composites
    "betting",  # Betting lines
    "draft",  # NFL draft picks
    "metrics",  # PPA, pregame win probability
    "metrics_wp",  # In-game win probability -- game-id-driven, see run_metrics_wp_pipeline
    "wepa",  # Year-keyed opponent-adjusted EPA metrics, 4 calls
    "rosters",  # Team rosters
]

# Estimated API calls per source per season (rough averages)
ESTIMATED_CALLS = {
    "reference": 10,
    # conference_affiliations is one bulk unfiltered call (not year-scoped --
    # counted here anyway since it costs something every source-grain run)
    # + conference_changes, one call per year.
    "conferences": 2,
    # coach_seasons only: one call per year. coach_tenures (per-team fan-out)
    # is deliberately excluded from this source's daily cost -- see
    # IMMUTABLE_ONCE_FINAL's comment and coaches.py's module docstring.
    "coaches": 1,
    "games": 15,
    # cfp_bracket + cfp_games + cfp_participants, one call each per year
    # (2014+ only; 0 calls for an earlier season -- see playoffs.py's
    # CFP_START guard).
    "playoffs": 3,
    "game_stats": 200,
    "plays": 400,
    # NOT 20. Most of the stats source's resources are one call per year, but
    # play_stats issues one /plays/stats request PER GAME (~1,640 games),
    # player_success_game walks ~20 weeks (regular 1-16 + postseason 1-4),
    # and game_advanced makes 2 calls (regular+postseason) -- the source
    # tracks the season's schedule rather than its resource count. The old
    # estimate understated a daily run by ~80x and hid this source behind
    # "plays" in every budget projection.
    "stats": 1_675,
    # sp/elo/fpi/srs/core/sp_conferences/srs_expanded: one call per year each.
    "ratings": 13,
    "rankings": 20,
    "recruiting": 15,
    "betting": 5,
    "draft": 5,
    "metrics": 30,
    # One call per completed game still missing from metrics.win_probability.
    # ~70 is a typical in-season week's worth of newly-completed FBS+FCS
    # games (the steady-state daily/weekly incremental case); a full-season
    # backfill run resolves far more missing games and costs proportionally
    # more -- this estimate is for the dry-run printout, not a hard cap.
    "metrics_wp": 70,
    "wepa": 4,
    # One call per team, and the team list is the season's schedule -- both
    # sides, so FCS visitors count. 350 for 2026, not the 150 this said when
    # it meant "FBS only".
    "rosters": 350,
}


# Sources whose data for a FINISHED season cannot change, so re-fetching them
# daily buys nothing and costs the whole budget.
#
# WHY THIS EXISTS. The daily workflow calls load_season.py --weekly with no
# --season, so get_current_season() resolves to `year - 1` until August: every
# off-season run re-ingested the entire, complete, immutable previous season.
# plays fans out to one /plays/stats call PER GAME (~1,600 for a full season)
# and rosters to one call per team, against the then-75,000/month budget -- roughly
# 2,000 calls a day for data that cannot have changed. That is what exhausted
# the quota and produced the 2026-07-25 run that spent three hours being
# rate-limited (see api_client.RateLimitExhausted).
#
# NOT on this list, deliberately:
#   reference   -- no year filter and ~10 calls; always cheap, always current.
#
#   coaches, conferences -- cheap (1-2 calls/season) AND their data mutates
#                  off-season: a coaching change or conference realignment
#                  lands well after a season is "final" by the games-completed
#                  definition below, so skipping them for a finished season
#                  would freeze coach_seasons/conference_affiliations exactly
#                  when hires and realignment announcements happen. Same
#                  reasoning as the `reference` exclusion above, just also
#                  worth spelling out since both are new sources.
#
#   metrics_wp  -- NOT skipped, though it was briefly added here. The 2026-07-26
#                  daily load showed why it looked like it belonged: it reported
#
#                      3829 completed games in [2025], 1312 already have win
#                      probability, 2517 missing -- 51 batches of up to 50
#
#                  for a season that ended in January, i.e. ~2,500 calls a day
#                  for data that cannot change. But skipping the whole source
#                  once a season is final is the wrong remedy (PR #54 review,
#                  P1): run_metrics_wp_pipeline computes missing games as
#                  completed-minus-loaded, so a backfill INTERRUPTED by the very
#                  quota exhaustion that motivated the skip leaves recoverable
#                  games in that 2,517. The unattended daily path passes no
#                  --season, so those rows would never be retried and the gap
#                  would become permanent by construction.
#
#                  The real defect was unbounded cost, not eligibility, and it
#                  is fixed where it lives: run_metrics_wp_pipeline caps games
#                  per run (MAX_GAMES_PER_RUN) and logs what it deferred, so the
#                  backlog drains at a bounded price instead of costing
#                  everything or nothing.
IMMUTABLE_ONCE_FINAL = frozenset(
    {
        "games",
        "playoffs",
        "game_stats",
        "plays",
        "stats",
        "ratings",
        "rankings",
        "recruiting",
        "betting",
        "draft",
        "metrics",
        "wepa",
        "rosters",
    }
)

# A season counts as finished on the same terms train_model.py uses for its
# refit guard -- one definition of "finished" in the codebase, not two. A
# permanently un-completed row (a cancellation) must not freeze a season as
# unfinished forever, hence a tolerance rather than requiring literal 100%.
SEASON_COMPLETE_THRESHOLD = 0.99
MIN_GAMES_FOR_FINISHED_SEASON = 100


def season_is_final(conn, season: int) -> bool:
    """True when `season` has essentially every scheduled game completed."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS n,
                   AVG(CASE WHEN COALESCE(completed, false) THEN 1.0 ELSE 0.0 END) AS pct
            FROM core.games
            WHERE season = %s
            """,
            (season,),
        )
        n, pct = cur.fetchone()
    if not n or n < MIN_GAMES_FOR_FINISHED_SEASON:
        return False
    return float(pct or 0.0) >= SEASON_COMPLETE_THRESHOLD


def sources_to_skip(active_sources, season_final: bool, allow_skip: bool):
    """Immutable sources to skip for a finished season.

    ``allow_skip`` is False whenever the caller named a season or specific
    sources explicitly. A backfill (``--season 2019 --sources plays``) targets
    a finished season BY DEFINITION, so skipping there would silently turn
    every backfill into a no-op -- the failure mode this guard must not
    introduce. Only the unattended daily path, which passes neither, is
    eligible.
    """
    if not (allow_skip and season_final):
        return []
    return [s for s in active_sources if s in IMMUTABLE_ONCE_FINAL]


# Sources that carry the UPCOMING season's preseason inputs, refreshed
# off-season alongside the schedule.
#
# WHY THIS EXISTS. Off-season the unattended path resolves `season` to
# `get_current_season()` = `year - 1`, and that season is finished, so every
# source in IMMUTABLE_ONCE_FINAL is skipped -- correctly, its data cannot
# change. But the skip left the upcoming season with no ingest at all beyond
# the games/betting schedule refresh below. CFBD publishes returning
# production (/player/returning), preseason SP+ (/ratings/sp), talent and
# team recruiting for a season during the spring and summer, and nothing in
# the daily path ever asked for them: on 2026-07-28, with the 2026 schedule
# loaded since spring, stats.player_returning, ratings.sp_ratings,
# recruiting.team_talent and recruiting.team_recruiting all had zero 2026
# rows, so marts.returning_production could not answer "returning production
# for 2026" and features.team_week had no preseason-known substrate.
#
# This refresh runs EVERY off-season day, so it is defined at resource grain,
# not source grain. The `stats` source is not uniformly priced: play_stats
# issues one /plays/stats request PER GAME (~1,640 for a season's schedule)
# while player_returning is a single call per year. Running the whole source
# daily would cost ~1,640 calls a day -- the exact fan-out that exhausted the
# quota on 2026-07-25 -- so only the resources that carry preseason inputs are
# named here. `ratings` and `recruiting` are flat single-call-per-year
# resources (six and five respectively), so they run whole.
#
# Total ~12 calls/day. An endpoint CFBD has not published yet returns an empty
# list or a 400 the source modules already log and skip, so this merges
# nothing rather than failing, and self-heals the day each one lands.
# `rosters` is deliberately NOT here -- one call per team (~150/day) and it
# does not firm up until August, when the normal in-season path picks it up.
#
# scripts/probe_offseason_availability.py reports, per endpoint, whether CFBD
# has published a given season yet; use it to tell "loader is broken" from
# "CFBD is merely early".
PRESEASON_STATS_RESOURCES = ("player_returning",)
PRESEASON_INPUT_SOURCES = ("stats", "ratings", "recruiting")

# What the off-season refresh above actually costs per day, as opposed to what
# ESTIMATED_CALLS says a full source-grain run of the same names would cost.
# `ratings` runs whole at 7 calls/year (sp, elo, fpi, srs, core,
# sp_conferences, srs_expanded); `recruiting` runs whole at 5.
PRESEASON_ESTIMATED_CALLS = len(PRESEASON_STATS_RESOURCES) + 7 + 5


# Sources whose runner accepts a resource filter. Only `stats` needs one so
# far -- it is the one source whose resources differ in cost by three orders
# of magnitude (play_stats is per game, the rest are per year).
RESOURCE_FILTERABLE = frozenset({"stats"})

# Table/mart names that are easy to reach for but are not the dlt resource
# name. `returning_production` is the MART; the resource behind it is
# `player_returning`. Mapping the near-miss beats failing an operator who
# asked for exactly the right data under the name the warehouse shows them.
RESOURCE_ALIASES = {"stats": {"returning_production": "player_returning"}}


def resolve_resource_names(source: str, names: list[str]) -> list[str]:
    """Map warehouse-facing aliases onto dlt resource names."""
    aliases = RESOURCE_ALIASES.get(source, {})
    return [aliases.get(name, name) for name in names]


def validate_resource_filters(filters: dict[str, list[str]]) -> None:
    """Reject unknown resource names BEFORE any source runs.

    The check already existed inside stats_source, but it fired mid-run: a
    2026-07-28 backfill got through ratings and recruiting before failing on
    `stats:returning_production` (the mart name, not the resource name), and
    the valid-names hint was buried under dlt's load logs. Failing at parse
    time puts it on the first line of output instead.
    """
    from src.pipelines.sources.stats import stats_source

    known = {"stats": {r.name for r in stats_source(years=[2000]).resources.values()}}
    for source, names in filters.items():
        unknown = [n for n in names if n not in known.get(source, set())]
        if unknown:
            raise ValueError(
                f"Unknown {source} resource(s): {unknown}. "
                f"Valid: {sorted(known.get(source, set()))}"
            )


def parse_source_specs(specs: list[str]) -> tuple[list[str], dict[str, list[str]]]:
    """Split "source[:res+res]" specs into source names and resource filters.

    `--sources stats:player_returning` loads returning production for a season
    without paying for play_stats' one-request-per-game fan-out -- the
    difference between ~1 call and ~1,640 for a season with a full schedule.
    Pure, so the parse is testable without a DB or an API key.
    """
    names: list[str] = []
    filters: dict[str, list[str]] = {}
    for spec in specs:
        name, _, resources = spec.partition(":")
        names.append(name)
        if not resources:
            continue
        if name not in RESOURCE_FILTERABLE:
            raise ValueError(
                f"Source {name!r} does not support a resource filter "
                f"(filterable: {sorted(RESOURCE_FILTERABLE)})"
            )
        filters[name] = resolve_resource_names(name, [r for r in resources.split("+") if r])
    return names, filters


def upcoming_schedule_season(season: int, month: int) -> int | None:
    """Return the next season to schedule-refresh during the off-season.

    Before August, get_current_season() still points at the previous season,
    but the upcoming season's schedule is already published on CFBD. Auto-mode
    loads keep it fresh via a cheap games-only pull (~15 calls) so the daily
    automation never depends on a manual dispatch to pick up the new season.
    """
    return season + 1 if month < 8 else None


def load_season(
    season: int,
    sources: list[str] | None = None,
    dry_run: bool = False,
    skip_refresh: bool = False,
    weekly: bool = False,
    upcoming_schedule: int | None = None,
    allow_skip_final: bool = False,
) -> dict:
    """Load or refresh all data for a given season.

    Args:
        season: The season year to load
        sources: Specific sources to load (None = all)
        dry_run: If True, show plan without executing
        skip_refresh: If True, skip mart refresh after loading
        weekly: If True, load game_stats week-by-week (~35K rows per merge)
        upcoming_schedule: If set, also refresh this season's schedule and
            betting lines plus its preseason inputs (PRESEASON_INPUT_SOURCES)
            after the main load

    Returns:
        Summary dict with timing and row counts
    """
    from src.pipelines.run import (
        run_betting_pipeline,
        run_coaches_pipeline,
        run_conferences_pipeline,
        run_draft_pipeline,
        run_game_stats_pipeline,
        run_game_stats_weekly,
        run_games_pipeline,
        run_metrics_pipeline,
        run_metrics_wp_pipeline,
        run_playoffs_pipeline,
        run_plays_pipeline,
        run_rankings_pipeline,
        run_ratings_pipeline,
        run_recruiting_pipeline,
        run_reference_pipeline,
        run_rosters_pipeline,
        run_stats_pipeline,
        run_wepa_pipeline,
    )
    from src.pipelines.utils.rate_limiter import get_rate_limiter

    # Determine which sources to run. A source may be narrowed to specific
    # resources with "source:res+res" -- see parse_source_spec.
    requested = sources if sources else [s for s in SOURCE_ORDER if s != "rosters"]
    active_sources, resource_filters = parse_source_specs(requested)
    if resource_filters:
        try:
            validate_resource_filters(resource_filters)
        except ValueError as e:
            logger.error(str(e))
            return {"error": str(e)}

    # Validate sources
    valid = set(SOURCE_ORDER)
    invalid = [s for s in active_sources if s not in valid]
    if invalid:
        logger.error(f"Unknown sources: {invalid}. Valid: {sorted(valid)}")
        return {"error": f"Unknown sources: {invalid}"}

    # Run in SOURCE_ORDER regardless of how the caller typed them. That list
    # is dependency order, not presentation order: `--sources rosters,games`
    # would otherwise resolve roster teams from core.games BEFORE loading the
    # schedule that populates it, failing the roster load and then loading the
    # games it needed. Sorting here rather than at parse time keeps the
    # unknown-source error reporting the operator's own spelling.
    active_sources = sorted(active_sources, key=SOURCE_ORDER.index)

    # Drop immutable sources for a finished season before estimating, so the
    # dry-run figure and the budget check reflect what will actually be
    # fetched rather than what would have been.
    skipped_final = []
    if allow_skip_final:
        import psycopg2

        from scripts.compute_predictions import get_db_url

        conn_check = psycopg2.connect(get_db_url())
        try:
            final = season_is_final(conn_check, season)
        finally:
            conn_check.close()
        skipped_final = sources_to_skip(active_sources, final, allow_skip_final)
        if skipped_final:
            saved = sum(ESTIMATED_CALLS.get(s, 50) for s in skipped_final)
            logger.info(
                "Season %d is finished; skipping %d immutable source(s) -- %s "
                "(~%d API calls saved). Data for a completed season cannot "
                "change; pass --season %d or --no-skip-final to force a reload.",
                season,
                len(skipped_final),
                ", ".join(skipped_final),
                saved,
                season,
            )
            active_sources = [s for s in active_sources if s not in skipped_final]
        elif final:
            logger.info("Season %d is finished but no immutable source was selected", season)

    # Estimate API calls. A resource-filtered source costs a call per named
    # resource per season, not the whole source's per-game fan-out.
    def estimate(src: str) -> int:
        named = resource_filters.get(src)
        return len(named) if named else ESTIMATED_CALLS.get(src, 50)

    total_est = sum(estimate(s) for s in active_sources)

    # Check rate limit budget
    rate_limiter = get_rate_limiter()
    status = rate_limiter.get_status()
    remaining = status["remaining"]

    logger.info(f"Season: {season}")
    logger.info(f"Sources: {', '.join(active_sources)}")
    logger.info(f"Estimated API calls: ~{total_est:,}")
    logger.info(f"Rate limit remaining: {remaining:,}")

    if total_est > remaining:
        logger.warning(
            f"Estimated calls ({total_est:,}) may exceed remaining budget ({remaining:,})"
        )

    if dry_run:
        print(f"\n[DRY RUN] Would load {len(active_sources)} sources for season {season}")
        for src in active_sources:
            named = resource_filters.get(src)
            label = f"{src}:{'+'.join(named)}" if named else src
            print(f"  {label:32s}  ~{estimate(src):,} API calls")
        print(f"\n  Total estimated:  ~{total_est:,} calls")
        print(f"  Budget remaining: {remaining:,} calls")
        if upcoming_schedule:
            print(
                f"  + Refresh {upcoming_schedule} schedule + betting lines "
                "(games + betting sources, ~20 calls)"
            )
            print(
                f"  + Refresh {upcoming_schedule} preseason inputs "
                f"({', '.join(PRESEASON_INPUT_SOURCES)}; stats limited to "
                f"{', '.join(PRESEASON_STATS_RESOURCES)}, "
                f"~{PRESEASON_ESTIMATED_CALLS:,} calls)"
            )
        if not skip_refresh:
            print("  + Refresh all materialized views after loading")
        return {"dry_run": True, "estimated_calls": total_est}

    # Map source names to runner functions
    game_stats_runner = (
        (lambda: run_game_stats_weekly(years=[season]))
        if weekly
        else (lambda: run_game_stats_pipeline(years=[season]))
    )
    runners = {
        "reference": lambda: run_reference_pipeline(),
        "conferences": lambda: run_conferences_pipeline(years=[season]),
        # coach_tenures (per-team fan-out) is deliberately not invoked here --
        # backfill/preseason only, see run_coach_tenures_pipeline.
        "coaches": lambda: run_coaches_pipeline(years=[season]),
        "games": lambda: run_games_pipeline(years=[season]),
        "playoffs": lambda: run_playoffs_pipeline(years=[season]),
        "game_stats": game_stats_runner,
        "plays": lambda: run_plays_pipeline(years=[season]),
        "stats": lambda: run_stats_pipeline(years=[season], only=resource_filters.get("stats")),
        "ratings": lambda: run_ratings_pipeline(years=[season]),
        "rankings": lambda: run_rankings_pipeline(years=[season]),
        "recruiting": lambda: run_recruiting_pipeline(years=[season]),
        "betting": lambda: run_betting_pipeline(years=[season]),
        "draft": lambda: run_draft_pipeline(years=[season]),
        "metrics": lambda: run_metrics_pipeline(years=[season]),
        "metrics_wp": lambda: run_metrics_wp_pipeline(seasons=[season]),
        "wepa": lambda: run_wepa_pipeline(years=[season]),
        # Excluded from the default active set above (one call per team), so
        # this only runs when an operator asks for it by name:
        # --sources rosters. The team list resolves from the season's
        # schedule -- no --teams required, unlike the `cfb-pipeline` CLI.
        "rosters": lambda: run_rosters_pipeline(years=[season]),
    }

    results = {}
    total_start = time.time()

    for src in active_sources:
        runner = runners.get(src)
        if not runner:
            logger.warning(f"No runner for source: {src} (skipping)")
            continue

        logger.info(f"Loading {src} for season {season}...")
        src_start = time.time()
        try:
            info = runner()
            elapsed = time.time() - src_start
            results[src] = {"status": "ok", "duration_s": round(elapsed, 1), "info": str(info)}
            logger.info(f"  {src} completed in {elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - src_start
            results[src] = {"status": "error", "duration_s": round(elapsed, 1), "error": str(e)}
            logger.error(f"  {src} failed after {elapsed:.1f}s: {e}")

    # Off-season: keep the upcoming season's published schedule and betting
    # lines fresh. Betting matters here because line_snapshots only records
    # pending games -- pre-August, only the upcoming season has any, so
    # skipping it would lose exactly the preseason line-movement history the
    # append-only snapshot feature exists to capture.
    if upcoming_schedule:
        preseason_runners = {
            # Resource-level: the full stats source would fan out one
            # /plays/stats call per scheduled game, every day.
            "stats": lambda: run_stats_pipeline(
                years=[upcoming_schedule], only=list(PRESEASON_STATS_RESOURCES)
            ),
            "ratings": lambda: run_ratings_pipeline(years=[upcoming_schedule]),
            "recruiting": lambda: run_recruiting_pipeline(years=[upcoming_schedule]),
        }
        upcoming_runners = {
            "games_upcoming": lambda: run_games_pipeline(years=[upcoming_schedule]),
            "betting_upcoming": lambda: run_betting_pipeline(years=[upcoming_schedule]),
            # Preseason inputs (returning production, SP+, talent, recruiting):
            # published progressively through the spring/summer, so ask daily
            # and let an unpublished endpoint no-op.
            **{f"{src}_upcoming": preseason_runners[src] for src in PRESEASON_INPUT_SOURCES},
        }
        for name, runner in upcoming_runners.items():
            logger.info(f"Refreshing upcoming season {upcoming_schedule}: {name}...")
            src_start = time.time()
            try:
                info = runner()
                elapsed = time.time() - src_start
                results[name] = {
                    "status": "ok",
                    "duration_s": round(elapsed, 1),
                    "info": str(info),
                }
            except Exception as e:
                elapsed = time.time() - src_start
                results[name] = {
                    "status": "error",
                    "duration_s": round(elapsed, 1),
                    "error": str(e),
                }
                logger.error(f"  {name} failed after {elapsed:.1f}s: {e}")

    # Refresh marts
    if not skip_refresh:
        logger.info("Refreshing materialized views...")
        from scripts.refresh_marts import refresh_marts

        refresh_start = time.time()
        failures = refresh_marts(concurrently=True)
        refresh_elapsed = time.time() - refresh_start
        results["_mart_refresh"] = {
            "status": "ok" if failures == 0 else "partial",
            "duration_s": round(refresh_elapsed, 1),
            "failures": failures,
        }

    total_elapsed = time.time() - total_start

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Season {season} Load Summary")
    print(f"{'=' * 60}")
    successes = sum(1 for r in results.values() if r["status"] == "ok")
    errors = sum(1 for r in results.values() if r["status"] == "error")
    for name, res in results.items():
        status_icon = "OK" if res["status"] == "ok" else "FAIL"
        print(f"  [{status_icon:4s}] {name:25s} {res['duration_s']:>8.1f}s")
    print(f"{'=' * 60}")
    print(f"  Total: {total_elapsed:.1f}s | {successes} succeeded, {errors} failed")

    return {
        "season": season,
        "skipped_final": skipped_final,
        "total_duration_s": round(total_elapsed, 1),
        "successes": successes,
        "errors": errors,
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Load all data for a specific season")
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Season year to load (default: current season)",
    )
    parser.add_argument(
        "--sources",
        type=str,
        default=None,
        help="Comma-separated sources to load (default: all). A source may be "
        "narrowed to specific resources with source:res+res, e.g. "
        "stats:player_returning -- which loads returning production without "
        "play_stats' one-request-per-game fan-out.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show plan without executing")
    parser.add_argument(
        "--skip-refresh", action="store_true", help="Skip mart refresh after loading"
    )
    parser.add_argument(
        "--no-skip-final",
        action="store_true",
        help="Load immutable sources even for a finished season. Only relevant "
        "on the unattended path (no --season, no --sources), where they are "
        "skipped by default because their data cannot change.",
    )
    parser.add_argument(
        "--weekly",
        action="store_true",
        help="Load game_stats week-by-week (~35K rows per merge) to avoid timeouts",
    )
    args = parser.parse_args()

    season = args.season
    upcoming = None
    if season is None:
        from datetime import datetime

        from src.pipelines.config.years import get_current_season

        season = get_current_season()
        upcoming = upcoming_schedule_season(season, datetime.now().month)
        logger.info(f"No --season given; using current season {season}")
        if upcoming:
            logger.info(f"Off-season: will also refresh the {upcoming} schedule")

    sources = args.sources.split(",") if args.sources else None

    # The daily workflow runs with neither --season nor --sources. Anything
    # more specific is a deliberate operator request -- most often a backfill,
    # which targets a finished season by definition -- so the skip is off.
    allow_skip_final = args.season is None and not args.sources and not args.no_skip_final

    summary = load_season(
        season=season,
        sources=sources,
        dry_run=args.dry_run,
        skip_refresh=args.skip_refresh,
        weekly=args.weekly,
        upcoming_schedule=upcoming,
        allow_skip_final=allow_skip_final,
    )

    # Validation failures return {"error": str} (singular) before any source
    # runs; per-source failures count up {"errors": int}. Both must exit
    # nonzero or a mistyped --sources reports success having loaded nothing.
    if summary.get("error") or summary.get("errors", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
