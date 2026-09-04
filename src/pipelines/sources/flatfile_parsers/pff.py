"""Parsers for PFF Premium Stats manual-CSV exports (five report families).

Design: docs/brainstorms/2026-09-01-pff-plus-api.md, section 4 option A1
(manual drop). PFF's NCAA Premium Stats reports are auth-gated browser
downloads with no stable URL; the user exports each report family's CSV by
hand and feeds it to scripts/load_flat_files.py via --file. This module is
the pure parse layer -- no I/O, no DB, per the flat-file parser contract.

Column contracts (``FAMILY_COLUMNS``): the exact CSV header of each family,
verified byte-identical across the real 2023, 2024, and 2025 exports
(passing 44 cols, receiving 47, rushing 47, blocking 31, defense 55).
Manual CSVs get no benefit of the doubt: any missing or unexpected column
raises ``ParserStructureError`` -- a changed export is a new contract to be
reviewed by a human, never guessed at. The per-column kind drives casting:
``int``/``bigint`` columns accept integer literals only, ``float`` accepts
any numeric literal including PFF's scientific notation for large values
(``elusive_rating`` prints ``"1.0e3"`` at >= 1000), ``""`` becomes None
(NULL-never-0), and a non-conforming value fails loud. Kinds were derived
from every observed value across all 15 real (family x season) files, not
from vendor docs.

Season injection + fingerprint guard: the files carry NO season column and
identical download filenames across seasons -- three real upload batches had
three different browser-numbering orders, so filename order proves nothing.
``ctx.season`` (the operator's --season, made mandatory by the driver for
these sources) is injected into every row and then verified against
FBS-membership facts found in ``team_name``:

- marker teams (``SEASON_MARKERS``): KENNESAW joined FBS in 2024, DELAWARE
  and MO STATE in 2025 -- their presence in a file claimed as an earlier
  season is a provable contradiction;
- exact team counts for validated seasons (134 distinct team_names in
  2023-24, 136 in 2025 -- note 2023's 134 is 133 FBS members plus
  W GEORGIA, a Division II program PFF graded that season and which appears
  in all five real 2023 exports; it is mapped in pff.team_map);
- a wide sanity band (120-150 distinct teams) for seasons with no recorded
  count -- catches a partial/filtered export without ever failing a future
  season whose membership is simply unknown yet;
- a 2014 floor (PFF's NCAA Premium Stats coverage starts in 2014).

Only provable contradictions fail (``SeasonFingerprintError``), including
a validated season whose expected markers are MISSING (a 2023 export
claimed as 2024 has 134 teams and no KENNESAW); a future
season's file passes on markers it happens to contain, and marker ABSENCE
never fails anything (a team leaving FBS is realignment, not corruption).
When 2026's membership is settled, extend ``EXPECTED_TEAM_COUNTS`` (and
``SEASON_MARKERS`` if realignment adds a new marker) -- nothing else needs
touching for a new season.

Team names stay verbatim (PFF's ALL-CAPS abbreviations, e.g. BOWL GREEN);
the framework resolves them to CFBD school names into the ``school`` column
via pff.team_map (``xwalk_resolved_field`` on the registry specs).

Positions mix within every family (a passing file has WR/HB/P/K trick-play
rows; a blocking file has 1,400+ WRs) -- filter at query time, never here.
"""

import csv
import io
from collections.abc import Iterator

from src.pipelines.sources.flat_files import (
    ParseContext,
    ParserStructureError,
    SeasonFingerprintError,
)

# Earliest season PFF publishes NCAA Premium Stats for.
PFF_MIN_SEASON = 2014

# Team abbreviation -> first FBS season. Presence of a marker in a file
# claimed as an EARLIER season is a provable contradiction. For VALIDATED
# seasons (EXPECTED_TEAM_COUNTS) absence is checked too: a full 2024 export
# without KENNESAW is really a 2023 file -- both seasons have 134 teams, so
# the count alone cannot tell them apart. For unvalidated (future) seasons
# absence proves nothing (a marker could have left FBS) and is not checked.
SEASON_MARKERS: dict[str, int] = {
    "KENNESAW": 2024,
    "DELAWARE": 2025,
    "MO STATE": 2025,
}

# Exact FBS membership counts for seasons whose exports were validated.
# Extend as future seasons are validated; unlisted seasons fall back to the
# sanity band below.
EXPECTED_TEAM_COUNTS: dict[int, int] = {
    2023: 134,
    2024: 134,
    2025: 136,
}

# For seasons without a recorded exact count: a full FBS export must land in
# this band (FBS membership has been 120-136 for a decade; 150 leaves
# headroom for expansion without admitting position-filtered partials).
TEAM_COUNT_SANITY = (120, 150)

# Per-family column contracts: exact CSV header order, column -> kind.
# Kinds: "text" (verbatim), "int"/"bigint" (integer literals only),
# "float" (any numeric literal, incl. scientific notation).
FAMILY_COLUMNS: dict[str, dict[str, str]] = {
    "passing_summary": {
        "player": "text",
        "player_id": "bigint",
        "position": "text",
        "team_name": "text",
        "player_game_count": "int",
        "accuracy_percent": "float",
        "aimed_passes": "int",
        "attempts": "int",
        "avg_depth_of_target": "float",
        "avg_time_to_throw": "float",
        "bats": "int",
        "big_time_throws": "int",
        "btt_rate": "float",
        "completion_percent": "float",
        "completions": "int",
        "declined_penalties": "int",
        "def_gen_pressures": "int",
        "drop_rate": "float",
        "dropbacks": "int",
        "drops": "int",
        "epa": "float",
        "first_downs": "int",
        "franchise_id": "bigint",
        "grades_hands_fumble": "float",
        "grades_offense": "float",
        "grades_pass": "float",
        "grades_run": "float",
        "hit_as_threw": "int",
        "interceptions": "int",
        "passing_snaps": "int",
        "penalties": "int",
        "positive_epa_percent": "float",
        "pressure_to_sack_rate": "float",
        "qb_rating": "float",
        "sack_percent": "float",
        "sacks": "int",
        "scrambles": "int",
        "spikes": "int",
        "thrown_aways": "int",
        "touchdowns": "int",
        "turnover_worthy_plays": "int",
        "twp_rate": "float",
        "yards": "int",
        "ypa": "float",
    },
    "receiving_summary": {
        "player": "text",
        "player_id": "bigint",
        "position": "text",
        "team_name": "text",
        "player_game_count": "int",
        "avg_depth_of_target": "float",
        "avoided_tackles": "int",
        "caught_percent": "float",
        "contested_catch_rate": "float",
        "contested_receptions": "int",
        "contested_targets": "int",
        "declined_penalties": "int",
        "drop_rate": "float",
        "drops": "int",
        "epa": "float",
        "first_downs": "int",
        "franchise_id": "bigint",
        "fumbles": "int",
        "grades_hands_drop": "float",
        "grades_hands_fumble": "float",
        "grades_offense": "float",
        "grades_pass_block": "float",
        "grades_pass_route": "float",
        "inline_rate": "float",
        "inline_snaps": "int",
        "interceptions": "int",
        "longest": "int",
        "pass_block_rate": "float",
        "pass_blocks": "int",
        "pass_plays": "int",
        "penalties": "int",
        "positive_epa_percent": "float",
        "receptions": "int",
        "route_rate": "float",
        "routes": "int",
        "slot_rate": "float",
        "slot_snaps": "int",
        "targeted_qb_rating": "float",
        "targets": "int",
        "touchdowns": "int",
        "wide_rate": "float",
        "wide_snaps": "int",
        "yards": "int",
        "yards_after_catch": "int",
        "yards_after_catch_per_reception": "float",
        "yards_per_reception": "float",
        "yprr": "float",
    },
    "rushing_summary": {
        "player": "text",
        "player_id": "bigint",
        "position": "text",
        "team_name": "text",
        "player_game_count": "int",
        "attempts": "int",
        "avoided_tackles": "int",
        "breakaway_attempts": "int",
        "breakaway_percent": "float",
        "breakaway_yards": "int",
        "declined_penalties": "int",
        "designed_yards": "int",
        "drops": "int",
        "elu_recv_mtf": "int",
        "elu_rush_mtf": "int",
        "elu_yco": "int",
        "elusive_rating": "float",
        "explosive": "int",
        "first_downs": "int",
        "franchise_id": "bigint",
        "fumbles": "int",
        "gap_attempts": "int",
        "grades_hands_fumble": "float",
        "grades_offense": "float",
        "grades_offense_penalty": "float",
        "grades_pass": "float",
        "grades_pass_block": "float",
        "grades_pass_route": "float",
        "grades_run": "float",
        "grades_run_block": "float",
        "longest": "int",
        "penalties": "int",
        "rec_yards": "int",
        "receptions": "int",
        "routes": "int",
        "run_plays": "int",
        "scramble_yards": "int",
        "scrambles": "int",
        "targets": "int",
        "total_touches": "int",
        "touchdowns": "int",
        "yards": "int",
        "yards_after_contact": "int",
        "yco_attempt": "float",
        "ypa": "float",
        "yprr": "float",
        "zone_attempts": "int",
    },
    "offense_blocking": {
        "player": "text",
        "player_id": "bigint",
        "position": "text",
        "team_name": "text",
        "player_game_count": "int",
        "block_percent": "float",
        "declined_penalties": "int",
        "franchise_id": "bigint",
        "grades_offense": "float",
        "grades_pass_block": "float",
        "grades_run_block": "float",
        "hits_allowed": "int",
        "hurries_allowed": "int",
        "non_spike_pass_block": "int",
        "non_spike_pass_block_percentage": "float",
        "pass_block_percent": "float",
        "pbe": "float",
        "penalties": "int",
        "pressures_allowed": "int",
        "sacks_allowed": "int",
        "snap_counts_block": "int",
        "snap_counts_ce": "int",
        "snap_counts_lg": "int",
        "snap_counts_lt": "int",
        "snap_counts_offense": "int",
        "snap_counts_pass_block": "int",
        "snap_counts_pass_play": "int",
        "snap_counts_rg": "int",
        "snap_counts_rt": "int",
        "snap_counts_run_block": "int",
        "snap_counts_te": "int",
    },
    "defense_summary": {
        "player": "text",
        "player_id": "bigint",
        "position": "text",
        "team_name": "text",
        "player_game_count": "int",
        "assists": "int",
        "batted_passes": "int",
        "catch_rate": "float",
        "declined_penalties": "int",
        "forced_fumbles": "int",
        "franchise_id": "bigint",
        "fumble_recoveries": "int",
        "fumble_recovery_touchdowns": "int",
        "grades_coverage_defense": "float",
        "grades_defense": "float",
        "grades_defense_penalty": "float",
        "grades_pass_rush_defense": "float",
        "grades_run_defense": "float",
        "grades_tackle": "float",
        "hits": "int",
        "hurries": "int",
        "interception_touchdowns": "int",
        "interceptions": "int",
        "longest": "int",
        "missed_tackle_rate": "float",
        "missed_tackles": "int",
        "pass_break_ups": "int",
        "penalties": "int",
        "qb_rating_against": "float",
        "receptions": "int",
        "sacks": "int",
        "safeties": "int",
        "snap_counts_box": "int",
        "snap_counts_corner": "int",
        "snap_counts_coverage": "int",
        "snap_counts_defense": "int",
        "snap_counts_dl": "int",
        "snap_counts_dl_a_gap": "int",
        "snap_counts_dl_b_gap": "int",
        "snap_counts_dl_outside_t": "int",
        "snap_counts_dl_over_t": "int",
        "snap_counts_fs": "int",
        "snap_counts_offball": "int",
        "snap_counts_pass_rush": "int",
        "snap_counts_run_defense": "int",
        "snap_counts_slot": "int",
        "stops": "int",
        "tackles": "int",
        "tackles_for_loss": "int",
        "targets": "int",
        "total_pressures": "int",
        "touchdowns": "int",
        "yards": "int",
        "yards_after_catch": "int",
        "yards_per_reception": "float",
    },
}


def verify_season_fingerprint(teams: set[str], claimed_season: int, source: str) -> None:
    """Fail loud (``SeasonFingerprintError``) when the file's FBS-membership
    fingerprint provably contradicts the claimed season; pass otherwise.

    ``teams`` is the set of distinct ``team_name`` values in the file. See
    the module docstring for the rule set and the future-season posture
    (unknown memberships pass; only provable contradictions fail).
    """
    contradictions: list[str] = []

    if claimed_season < PFF_MIN_SEASON:
        contradictions.append(
            f"PFF NCAA Premium Stats coverage starts in {PFF_MIN_SEASON}; "
            f"season {claimed_season} cannot exist"
        )

    expected = EXPECTED_TEAM_COUNTS.get(claimed_season)
    for marker, first_fbs_season in SEASON_MARKERS.items():
        if marker in teams and claimed_season < first_fbs_season:
            contradictions.append(
                f"{marker} is in the file but did not join FBS until {first_fbs_season}"
            )
        elif marker not in teams and expected is not None and claimed_season >= first_fbs_season:
            # Validated season, marker should be there: an older export
            # (the reversed 2023/2024 swap) is the only way it is missing.
            contradictions.append(
                f"{marker} is missing from the file but has been FBS since "
                f"{first_fbs_season}; this looks like an earlier season's export"
            )

    count = len(teams)
    if expected is not None:
        if count != expected:
            contradictions.append(
                f"file has {count} distinct teams; a season-{claimed_season} "
                f"FBS export has exactly {expected}"
            )
    else:
        lo, hi = TEAM_COUNT_SANITY
        if not lo <= count <= hi:
            contradictions.append(
                f"file has {count} distinct teams; a full FBS export should have {lo}-{hi}"
            )

    if contradictions:
        raise SeasonFingerprintError(
            f"{source}: claimed season {claimed_season} is contradicted by the "
            f"file's contents: {'; '.join(contradictions)}. Check the "
            "file/season pairing -- PFF download filenames do not encode the "
            "season and browser numbering reflects upload order only."
        )


def _parse_family(raw: bytes, ctx: ParseContext, family: str) -> Iterator[dict]:
    """Shared parse path: header contract, casting, season injection,
    fingerprint verification. Materializes fully before yielding so every
    structural check runs before a single row reaches dlt.
    """
    columns = FAMILY_COLUMNS[family]

    if ctx.season is None:
        raise ParserStructureError(
            f"{ctx.source}: PFF exports carry no season column -- the load "
            "must supply one explicitly (--season)"
        )

    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    header = reader.fieldnames or []
    missing = [c for c in columns if c not in header]
    unexpected = [c for c in header if c not in columns]
    if missing or unexpected or len(header) != len(set(header)):
        duplicates = sorted({c for c in header if header.count(c) > 1})
        raise ParserStructureError(
            f"{ctx.source}: CSV header does not match the {family} contract "
            f"({len(columns)} columns): missing={missing} "
            f"unexpected={unexpected} duplicated={duplicates}"
        )

    rows: list[dict] = []
    for line_no, record in enumerate(reader, start=2):
        if record.get(None) is not None:
            raise ParserStructureError(
                f"{ctx.source}: line {line_no} has more fields than the header"
            )
        row: dict = {"season": ctx.season}
        for col, kind in columns.items():
            value = record[col]
            if value is None:
                raise ParserStructureError(
                    f"{ctx.source}: line {line_no} has fewer fields than the header"
                )
            value = value.strip()
            if value == "":
                row[col] = None
            elif kind == "text":
                row[col] = value
            else:
                try:
                    row[col] = int(value) if kind in ("int", "bigint") else float(value)
                except ValueError as e:
                    raise ParserStructureError(
                        f"{ctx.source}: line {line_no} column {col}: "
                        f"{value!r} is not a valid {kind}"
                    ) from e
        rows.append(row)

    if not rows:
        raise ParserStructureError(f"{ctx.source}: file has no data rows")

    teams = {row["team_name"] for row in rows if row["team_name"]}
    verify_season_fingerprint(teams, ctx.season, ctx.source)

    return iter(rows)


def parse_passing_summary(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    return _parse_family(raw, ctx, "passing_summary")


def parse_receiving_summary(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    return _parse_family(raw, ctx, "receiving_summary")


def parse_rushing_summary(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    return _parse_family(raw, ctx, "rushing_summary")


def parse_offense_blocking(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    return _parse_family(raw, ctx, "offense_blocking")


def parse_defense_summary(raw: bytes, ctx: ParseContext) -> Iterator[dict]:
    return _parse_family(raw, ctx, "defense_summary")
