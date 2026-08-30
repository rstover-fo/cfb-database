#!/usr/bin/env python3
"""Probe CFBD spec v5.24.2 -> v5.25.0's new/changed endpoints before building
sources.

CFBD added or reshaped a handful of endpoints (playoff bracket, coach
tenure/season summaries, conference affiliation history, a new player
"success rate" family, an expanded SRS view, a player season overview,
questions about whether two existing endpoints still accept a bare `year`,
and -- new in v5.25.0 -- a `/passing` family: play-level passing charting,
player and team passing splits by game and by season). Before writing dlt
source modules and RESTAPIConfig entries for any of them we need real
response shapes -- field names, nesting, row grain, and which year-only
calls actually work -- not assumptions from the changelog.

This script is read-only: it never writes to the warehouse, and every
request goes through the same rate-limited `CFBDClient` / `make_request`
path the pipelines use (see the `cfbd-api` and `dlt-pipelines` skills), so
calls are recorded against the monthly budget like any other pipeline call.
It makes ~24 requests total across 18 probes (several issue a second,
conditional or comparison request -- see PROBES below) -- narrow and
logged, matching scripts/probe_offseason_availability.py and
scripts/probe_metrics_wp.py.

Each probe is fail-soft: an exception is recorded on that probe and the
sweep continues, so one dead endpoint never hides the others. Exit status
is 0 if any probe got a 200 back, 1 only if every single one failed (auth,
network, or every endpoint genuinely gone) -- i.e. when this run told us
nothing.

Auth: exactly like probe_offseason_availability.py -- `get_client()` reads
`sources.cfbd.api_key` via dlt's provider chain (`.dlt/secrets.toml`
locally, or `SOURCES__CFBD__API_KEY` in every other CI workflow in this
repo). This probe's workflow instead hands the key over as a plain
`CFBD_API_KEY` env var (simpler for a workflow_dispatch-only one-off with no
pipeline run alongside it), so `get_probe_client()` below falls back to
reading that directly when dlt has nothing configured.

Usage:
    python scripts/probe_2026_endpoints.py                  # all 18 probes
    python scripts/probe_2026_endpoints.py --only 4,9,12     # just these
    python scripts/probe_2026_endpoints.py --dry-run         # no network, no key needed
    python scripts/probe_2026_endpoints.py --out probe_output/

Output:
    stdout   -- human-readable summary (also appended to $GITHUB_STEP_SUMMARY
                by the probe-endpoints.yml workflow)
    <out>/NN_<slug>.json   -- one file per probe: path, params, status, count,
                              fields, sample (first 2 records), notes
    <out>/summary.json     -- all probes in one file
    <out>/fixtures/<slug>.json -- first 5 records of the first successful call,
                              raw material for tests/fixtures/ later
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Round-number response sizes that would suggest a server-side cap rather
# than "this is genuinely how many rows exist" -- flagged, never assumed.
SUSPECTED_CAPS = (1000, 2000, 5000, 10000)

PLAYER_ID_KEYS = ("playerId", "athleteId", "id")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def get_probe_client():
    """Build a CFBD client the way the pipelines do, with a CI-friendly
    fallback. See module docstring for why the fallback exists."""
    from src.pipelines.utils.api_client import CFBDClient, get_client

    try:
        return get_client()
    except ValueError:
        api_key = os.environ.get("CFBD_API_KEY")
        if not api_key:
            raise
        logger.info("dlt secrets not configured; using CFBD_API_KEY environment variable.")
        return CFBDClient(api_key=api_key)


# ---------------------------------------------------------------------------
# Response shape helpers
# ---------------------------------------------------------------------------


def _shape_of(value):
    """Describe one field's value one level deep -- enough to see a nested
    list/dict's own field names without recursing further."""
    if isinstance(value, dict):
        return {"_type": "dict", "keys": sorted(value.keys())}
    if isinstance(value, list):
        if not value:
            return {"_type": "list", "len": 0, "item_keys": []}
        first_item = value[0]
        if isinstance(first_item, dict):
            return {"_type": "list", "len": len(value), "item_keys": sorted(first_item.keys())}
        return {"_type": "list", "len": len(value), "item_type": type(first_item).__name__}
    return type(value).__name__


def field_names(record) -> dict | None:
    """Top-level field names of one record, with nested list/dict field names
    noted one level down (never recursing further)."""
    if not isinstance(record, dict):
        return None
    return {key: _shape_of(value) for key, value in record.items()}


def summarize(data) -> tuple[int | None, dict | None, list]:
    """(count, fields-of-first-record, up-to-5-record sample) for a raw CFBD
    response. Almost every CFBD list endpoint returns a JSON array; handled
    defensively in case one of these new endpoints returns a single object."""
    if isinstance(data, list):
        return len(data), field_names(data[0]) if data else None, data[:5]
    if isinstance(data, dict):
        return 1, field_names(data), [data]
    return None, None, [data]


def looks_capped(count: int | None) -> bool:
    """True if `count` sits on or very near a round number that smells like
    a server-side response cap rather than the true row count."""
    if not count:
        return False
    return any(count == cap or abs(count - cap) <= max(1, cap // 200) for cap in SUSPECTED_CAPS)


def extract_player_id(record) -> object | None:
    if not isinstance(record, dict):
        return None
    for key in PLAYER_ID_KEYS:
        if record.get(key) is not None:
            return record[key]
    return None


# ---------------------------------------------------------------------------
# One HTTP call, recorded
# ---------------------------------------------------------------------------


@dataclass
class CallRecord:
    path: str
    params: dict
    status: int | None
    count: int | None
    fields: dict | None
    sample_full: list  # up to 5 records, for fixtures
    error: str | None

    @property
    def sample(self) -> list:
        """First two records -- the {path, params, status, count, fields,
        sample} contract's slice."""
        return self.sample_full[:2]

    def to_json(self) -> dict:
        return {
            "path": self.path,
            "params": self.params,
            "status": self.status,
            "count": self.count,
            "fields": self.fields,
            "sample": self.sample,
            "error": self.error,
        }


def do_call(client, path: str, params: dict | None, dry_run: bool) -> CallRecord:
    """Make one request through make_request()/the rate limiter, or -- in
    dry-run mode -- just log and record the plan without touching the
    network. Never raises: a failed request is recorded on the CallRecord,
    not propagated, so one dead probe cannot abort the sweep."""
    params = dict(params or {})
    logger.info("REQUEST%s path=%s params=%s", " [dry-run]" if dry_run else "", path, params)

    if dry_run:
        return CallRecord(path, params, None, None, None, [], None)

    from src.pipelines.sources.base import make_request

    try:
        data = make_request(client, path, params=params)
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        body = exc.response.text[:500]
        logger.warning("RESPONSE path=%s status=%s body=%s", path, status, body)
        return CallRecord(path, params, status, None, None, [], body)
    except Exception as exc:  # fail-soft: record and move on
        logger.warning("RESPONSE path=%s error=%s", path, exc)
        return CallRecord(path, params, None, None, None, [], str(exc))

    count, fields, sample_full = summarize(data)
    logger.info("RESPONSE path=%s status=200 count=%s", path, count)
    return CallRecord(path, params, 200, count, fields, sample_full, None)


# ---------------------------------------------------------------------------
# The 18 probes
# ---------------------------------------------------------------------------
# Each probe function takes (client, ctx, dry_run) and returns
# (list[CallRecord], list[str] notes). `ctx` is a plain dict probes can use to
# pass state forward (probe 11 reuses probe 8's player id).


def probe_playoffs_cfp(client, ctx, dry_run):
    calls = [do_call(client, "/playoffs/cfp", {"year": 2024}, dry_run)]
    return calls, [
        "Bracket shape: look for rounds/games nesting and a CFBD game id field in the sample."
    ]


def probe_playoffs_cfp_games(client, ctx, dry_run):
    calls = [do_call(client, "/playoffs/cfp/games", {"year": 2024}, dry_run)]
    return calls, ["Check for a CFBD gameId field and the round field's name in the sample."]


def probe_playoffs_cfp_participants(client, ctx, dry_run):
    calls = [do_call(client, "/playoffs/cfp/participants", {"year": 2024}, dry_run)]
    return calls, ["Check team id and seed field names in the sample."]


def probe_coaches_seasons(client, ctx, dry_run):
    calls = [do_call(client, "/coaches/seasons", {}, dry_run)]
    first = calls[0]
    notes = []
    if dry_run or first.status != 200:
        calls.append(
            do_call(client, "/coaches/seasons", {"minYear": 2020, "maxYear": 2024}, dry_run)
        )
        if not dry_run:
            notes.append(
                f"No-params call failed (status={first.status}); retried with minYear/maxYear."
            )
    elif not dry_run:
        notes.append("No-params call worked -- bulk /coaches/seasons is supported.")
    return calls, notes


def probe_coaches_tenures(client, ctx, dry_run):
    calls = [do_call(client, "/coaches/tenures", {}, dry_run)]
    first = calls[0]
    notes = []
    if dry_run or first.status != 200:
        calls.append(do_call(client, "/coaches/tenures", {"year": 2024}, dry_run))
        if not dry_run:
            notes.append(f"No-params call failed (status={first.status}); retried with year=2024.")
    elif not dry_run:
        notes.append("No-params call worked -- bulk /coaches/tenures is supported.")
    return calls, notes


def probe_conferences_affiliations(client, ctx, dry_run):
    calls = [do_call(client, "/conferences/affiliations", {}, dry_run)]
    return calls, ["Row grain: per-season rows vs start/end-year ranges -- check the sample."]


def probe_conferences_changes(client, ctx, dry_run):
    calls = [do_call(client, "/conferences/changes", {"year": 2024}, dry_run)]
    return calls, []


def probe_stats_player_success(client, ctx, dry_run):
    call = do_call(client, "/stats/player/success", {"year": 2024}, dry_run)
    notes = ["Row grain: one row per player, or per player+stat_type -- check the sample."]
    if not dry_run and call.sample_full:
        player_id = extract_player_id(call.sample_full[0])
        if player_id is not None:
            ctx["player_id"] = player_id
            ctx["player_id_source"] = "probe_08_stats_player_success"
            notes.append(f"Captured player id {player_id!r} from the first record for probe 11.")
    return [call], notes


def probe_stats_player_success_game(client, ctx, dry_run):
    call_no_week = do_call(client, "/stats/player/success/game", {"year": 2024}, dry_run)
    call_week5 = do_call(client, "/stats/player/success/game", {"year": 2024, "week": 5}, dry_run)
    notes = []
    if not dry_run and looks_capped(call_no_week.count):
        notes.append(
            f"CAP SUSPECTED: no-week count={call_no_week.count} sits on/near a round number."
        )
    return [call_no_week, call_week5], notes


def probe_ratings_srs_expanded(client, ctx, dry_run):
    calls = [do_call(client, "/ratings/srs/expanded", {"year": 2005}, dry_run)]
    first = calls[0]
    notes = []
    empty_or_400 = dry_run or first.status == 400 or (first.status == 200 and not first.count)
    if empty_or_400:
        calls.append(do_call(client, "/ratings/srs/expanded", {"year": 2015}, dry_run))
        if not dry_run:
            reason = "400" if first.status == 400 else "empty"
            notes.append(f"year=2005 was {reason}; retried with year=2015.")
    return calls, notes


def probe_player_season_overview(client, ctx, dry_run):
    notes = []
    calls = []
    player_id = ctx.get("player_id")
    source = ctx.get("player_id_source", "probe_08_stats_player_success")

    if player_id is None:
        search_call = do_call(
            client, "/player/search", {"searchTerm": "smith", "year": 2024}, dry_run
        )
        calls.append(search_call)
        if dry_run:
            notes.append(
                "No player id captured yet (dry-run) -- would use probe 8's first record if "
                "run for real, else the id from this /player/search call."
            )
            player_id = "<player id from probe 8 or /player/search fallback>"
        else:
            source = "player_search_fallback"
            if search_call.sample_full:
                player_id = extract_player_id(search_call.sample_full[0])
            if player_id is None:
                notes.append(
                    "No playerId available from probe 8 or the /player/search fallback; "
                    "skipping /player/season/overview."
                )
                return calls, notes
            notes.append(f"Route used: {source} (playerId={player_id!r}).")
    else:
        notes.append(f"Route used: {source} (playerId={player_id!r}).")

    calls.append(
        do_call(client, "/player/season/overview", {"year": 2024, "playerId": player_id}, dry_run)
    )
    return calls, notes


def probe_game_box_advanced(client, ctx, dry_run):
    calls = [do_call(client, "/game/box/advanced", {"year": 2024}, dry_run)]
    notes = [
        "CRITICAL: is a bare year=2024 still supported (200 with rows) or 400/empty? "
        "See status/error above."
    ]
    return calls, notes


def probe_stats_game_advanced(client, ctx, dry_run):
    calls = [
        do_call(client, "/stats/game/advanced", {"year": 2024, "seasonType": "regular"}, dry_run)
    ]
    return calls, ["Check whether rows are per game-team in the sample."]


def probe_passing_plays(client, ctx, dry_run):
    # Live run 33320296004 (2024): bare year=2024 400'd "team or week is
    # required" -- runtime validation on this endpoint is stricter than the
    # spec's documented required-params list, which lists only `year`.
    # year+week and year+team both came back 200 count=0 for 2024, so we
    # sweep 2025 (charting shipped last season) and 2026 week 1 (this
    # weekend) instead of repeating the proven-empty bare-year call.
    call_2025_week5 = do_call(client, "/passing/plays", {"year": 2025, "week": 5}, dry_run)
    call_2025_team = do_call(client, "/passing/plays", {"year": 2025, "team": "Alabama"}, dry_run)
    call_2026_week1 = do_call(client, "/passing/plays", {"year": 2026, "week": 1}, dry_run)
    notes = [
        "Bare year=2024 was proven 400 ('team or week is required') on the live run -- "
        "dropped from this sweep. Volume probe: a season runs ~45-50k pass attempts, one "
        "week ~2,800 -- the 2025 week-5 count is now the key cap signal for this "
        "resource's iteration grain (week vs team)."
    ]
    if not dry_run:
        for label, call in (
            ("2025 week5", call_2025_week5),
            ("2025 team", call_2025_team),
            ("2026 week1", call_2026_week1),
        ):
            if looks_capped(call.count):
                notes.append(
                    f"CAP SUSPECTED: {label} call count={call.count} sits on/near a round number."
                )
    return [call_2025_week5, call_2025_team, call_2026_week1], notes


def probe_passing_players_games(client, ctx, dry_run):
    # Live run: bare year=2024 400'd "passerId, team, or week is required" --
    # same runtime-validation gap as passing/plays. Sweeping 2025/2026
    # instead of repeating the proven-empty bare-year call.
    call_2025_week5 = do_call(client, "/passing/players/games", {"year": 2025, "week": 5}, dry_run)
    call_2026_week1 = do_call(client, "/passing/players/games", {"year": 2026, "week": 1}, dry_run)
    notes = [
        "Bare year=2024 was proven 400 ('passerId, team, or week is required') on the "
        "live run -- dropped from this sweep."
    ]
    if not dry_run and looks_capped(call_2025_week5.count):
        notes.append(
            f"CAP SUSPECTED: 2025 week5 count={call_2025_week5.count} sits on/near a round number."
        )
    return [call_2025_week5, call_2026_week1], notes


def probe_passing_players_season(client, ctx, dry_run):
    call_2025 = do_call(client, "/passing/players/season", {"year": 2025}, dry_run)
    call_2026 = do_call(client, "/passing/players/season", {"year": 2026}, dry_run)
    notes = [
        "Historical-depth check for the whole /passing family: 2024 and 2014 already "
        "probed empty (200 count=0) on the live run. If 2025 has rows here, treat "
        "PASSING_DATA_START=2025 for the family; if both 2025 and 2026 are still empty, "
        "the family has no data yet."
    ]
    return [call_2025, call_2026], notes


def probe_passing_teams_games(client, ctx, dry_run):
    # Live run: bare year=2024 400'd "team or week is required", matching the
    # play/game-grain endpoints above.
    call_2025_week5 = do_call(client, "/passing/teams/games", {"year": 2025, "week": 5}, dry_run)
    call_2026_week1 = do_call(client, "/passing/teams/games", {"year": 2026, "week": 1}, dry_run)
    notes = [
        "Bare year=2024 was proven 400 ('team or week is required') on the live run -- "
        "dropped from this sweep. Confirm nested offense/defense dict shape in the "
        "sample (~1,600 game-team rows expected for a full season)."
    ]
    if not dry_run and looks_capped(call_2025_week5.count):
        notes.append(
            f"CAP SUSPECTED: 2025 week5 count={call_2025_week5.count} sits on/near a round number."
        )
    return [call_2025_week5, call_2026_week1], notes


def probe_passing_teams_season(client, ctx, dry_run):
    # Live run: year=2024 200'd count=0; no-params 400'd "year required when
    # team not specified" -- bulk (no params) is NOT supported, unlike some
    # sibling /season endpoints. Dropped that call; sweeping 2025/2026 instead.
    call_2025 = do_call(client, "/passing/teams/season", {"year": 2025}, dry_run)
    call_2026 = do_call(client, "/passing/teams/season", {"year": 2026}, dry_run)
    notes = [
        "No-params call was proven 400 ('year required when team not specified') on the "
        "live run -- dropped from this sweep. Bulk access requires team-or-year, not "
        "supported bare."
    ]
    return [call_2025, call_2026], notes


# (number, slug, function) -- number and slug drive both --only selection and
# output filenames (NN_<slug>.json).
PROBES: list[tuple[int, str, callable]] = [
    (1, "playoffs_cfp", probe_playoffs_cfp),
    (2, "playoffs_cfp_games", probe_playoffs_cfp_games),
    (3, "playoffs_cfp_participants", probe_playoffs_cfp_participants),
    (4, "coaches_seasons", probe_coaches_seasons),
    (5, "coaches_tenures", probe_coaches_tenures),
    (6, "conferences_affiliations", probe_conferences_affiliations),
    (7, "conferences_changes", probe_conferences_changes),
    (8, "stats_player_success", probe_stats_player_success),
    (9, "stats_player_success_game", probe_stats_player_success_game),
    (10, "ratings_srs_expanded", probe_ratings_srs_expanded),
    (11, "player_season_overview", probe_player_season_overview),
    (12, "game_box_advanced", probe_game_box_advanced),
    (13, "stats_game_advanced", probe_stats_game_advanced),
    (14, "passing_plays", probe_passing_plays),
    (15, "passing_players_games", probe_passing_players_games),
    (16, "passing_players_season", probe_passing_players_season),
    (17, "passing_teams_games", probe_passing_teams_games),
    (18, "passing_teams_season", probe_passing_teams_season),
]
PROBE_NUMBERS = {number for number, _, _ in PROBES}


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_probes(only: set[int] | None, dry_run: bool) -> list[dict]:
    ctx: dict = {}
    client = None
    if not dry_run:
        client = get_probe_client()

    results = []
    try:
        for number, slug, fn in PROBES:
            if only is not None and number not in only:
                continue
            logger.info("=== Probe %02d: %s ===", number, slug)
            try:
                calls, notes = fn(client, ctx, dry_run)
            except Exception as exc:  # a probe's own logic must not abort the sweep either
                logger.error("Probe %02d (%s) raised unexpectedly: %s", number, slug, exc)
                calls = [CallRecord("<unknown>", {}, None, None, None, [], str(exc))]
                notes = [f"Probe function raised: {exc}"]
            results.append({"number": number, "slug": slug, "calls": calls, "notes": notes})
    finally:
        if client is not None:
            client.close()

    return results


def print_summary(results: list[dict], dry_run: bool) -> None:
    print()
    print("=" * 78)
    print("CFBD 2026 endpoint probe" + (" -- DRY RUN (no network)" if dry_run else " -- summary"))
    print("=" * 78)
    for result in results:
        print(f"\n[{result['number']:02d}] {result['slug']}")
        for call in result["calls"]:
            if dry_run:
                print(f"    PLANNED  {call.path} params={call.params}")
                continue
            status = call.status if call.status is not None else "ERR"
            print(f"    status={status!s:>5}  {call.path} params={call.params}  count={call.count}")
            if call.error:
                print(f"             error: {call.error[:200]}")
        for note in result["notes"]:
            print(f"    NOTE: {note}")
    print()
    print("=" * 78)


def write_outputs(results: list[dict], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    fixtures_dir = out_dir / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    summary_probes = []
    for result in results:
        number, slug = result["number"], result["slug"]
        calls_json = [c.to_json() for c in result["calls"]]
        probe_json = {"number": number, "slug": slug, "notes": result["notes"], "calls": calls_json}
        if calls_json:
            primary = calls_json[0]
            probe_json.update(
                {k: primary[k] for k in ("path", "params", "status", "count", "fields", "sample")}
            )

        (out_dir / f"{number:02d}_{slug}.json").write_text(
            json.dumps(probe_json, indent=2, default=str)
        )

        fixture_records: list = []
        for call in result["calls"]:
            if call.status == 200 and call.sample_full:
                fixture_records = call.sample_full[:5]
                break
        (fixtures_dir / f"{slug}.json").write_text(
            json.dumps(fixture_records, indent=2, default=str)
        )

        summary_probes.append(probe_json)

    summary = {"generated_at": datetime.now(UTC).isoformat(), "probes": summary_probes}
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    logger.info("Wrote probe output to %s", out_dir)


def parse_only(raw: str | None) -> set[int] | None:
    if not raw:
        return None
    numbers = set()
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        numbers.add(int(chunk))
    return numbers


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Probe CFBD spec v5.24.2 -> v5.25.0's new/changed endpoints "
        "(read-only, ~24 calls)"
    )
    parser.add_argument(
        "--only",
        type=str,
        default=None,
        help="Comma-separated probe numbers to run, e.g. 4,9,12 (default: all 18)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("probe_output"),
        help="Output directory for JSON results and fixtures (default: probe_output/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned requests only; makes no network calls and needs no API key",
    )
    args = parser.parse_args(argv)

    only = parse_only(args.only)
    if only is not None:
        unknown = only - PROBE_NUMBERS
        if unknown:
            parser.error(
                f"Unknown probe number(s): {sorted(unknown)} (valid: {sorted(PROBE_NUMBERS)})"
            )

    logger.info(
        "Running %d probe(s)%s",
        len(only) if only is not None else len(PROBES),
        " (dry-run)" if args.dry_run else "",
    )

    results = run_probes(only=only, dry_run=args.dry_run)
    print_summary(results, dry_run=args.dry_run)

    if args.dry_run:
        return 0

    write_outputs(results, args.out)

    any_success = any(call.status == 200 for result in results for call in result["calls"])
    if not any_success:
        logger.error("Every probe failed -- could not determine anything about these endpoints.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
