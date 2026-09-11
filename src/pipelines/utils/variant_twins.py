"""Tripwire for dlt's bigint/double VARIANT twin columns (KTD7).

dlt types a metric column `bigint` on first load. The first time a later
load carries a fractional value for that column, dlt cannot widen the
already-materialized bigint column in place -- instead it creates a sibling
`<col>__v_double` column and routes every value it cannot fit into the
bigint (i.e. every fractional value, going forward) into the twin instead.
The base column keeps collecting whole-number values; the twin silently
becomes the only place a large share of that metric's real values live.

Every maintained consumer of a metric on one of these tables must therefore
read the base and twin correctly, usually with
`COALESCE(col::double precision, col__v_double)`. A twin for a metric that no
maintained consumer selects has no such consumer obligation, but it still
needs an exact, reviewed classification so a later unreviewed twin cannot pass
silently.

EXPECTED_VARIANT_TWINS is the existing recognized inventory. It primarily
records twins that maintained consumers support and retains the documented
legacy stats.game_havoc exceptions below. REVIEWED_RAW_ONLY_VARIANT_TWINS
separately records the exact passing twins whose metrics were verified to have
no maintained curated consumer. The daily check fails anything in neither
registry. The deploy-time SQL validation mirrors only the two rushing arrays in
EXPECTED_VARIANT_TWINS; tests/test_variant_twins.py keeps those arrays aligned.

Remediation when `find_unexpected_twins` reports a new column:
    1. Review maintained consumers of the source metric before choosing a
       classification.
    2. If the metric is consumed, add correct base/twin handling, register it
       in EXPECTED_VARIANT_TWINS, update an applicable deploy-time check, and
       redeploy the affected consumer.
    3. If the metric is verified raw-only, add its exact twin name to
       REVIEWED_RAW_ONLY_VARIANT_TWINS with the review rationale. Do not use a
       wildcard or infer classifications from the live catalog.

Scope: only tables a mart (or, for stats.game_havoc, a features-build
script with the same COALESCE obligation) actually reads. As of 2026-09-03
that is six tables: stats.rushing_player_season, stats.rushing_team_season,
stats.passing_player_season (the rushing/passing charting family above),
plus stats.player_returning (marts/031_returning_production.sql),
stats.player_usage (marts/032_player_usage.sql), and stats.game_havoc
(marts/005_defensive_havoc.sql, plus scripts/build_features.py -- see
below) -- three more marts that already COALESCE a twin on a source table
outside the charting family. A new twin appearing on any of those three
would pass this tripwire silently (the affected metric going NULL in
returning-production / player-usage / defensive-havoc / features.team_week
outputs) if they weren't tracked here too.

The rushing GAME-grain tables (stats.rushing_player_games,
stats.rushing_team_games) carry far more twins (39 and 68 respectively, as
of 2026-09-03) than the season-grain tables -- new ones appear routinely as
data lands -- but no mart COALESCEs any of them today, so a new twin there
hurts nothing and would just be alert noise. They are deliberately left out
of both registries; review the tracking scope if a maintained consumer starts
reading game-grain rushing data.
"""

from __future__ import annotations

# "schema.table" -> frozenset of expected "<col>__v_double" column names.
#
# stats.rushing_player_season / stats.rushing_team_season: the exact
# allow-lists from src/schemas/api/validation_rushing_views.sql group (e)
# (allowed_player_twins / allowed_team_twins), live-verified 2026-09-03.
# Read by marts/050 (2 of the 17 player twins), marts/051 (3 of the 8 team
# twins), and marts/052 (the remaining 15 player + 5 team twins, across its
# direction-season breakdown).
#
# stats.passing_player_season: the single twin marts/045 COALESCEs
# (average_yards_after_catch__v_double). marts/046 (passing_charting_target_
# season) aggregates fresh from stats.passing_plays' plain bigint columns
# and marts/047 (passing_charting_team_season) reads stats.passing_team_
# season's natively-double offense_/defense_ columns -- neither has a twin
# to track, so passing_plays and passing_team_season are not keys here.
#
# stats.player_returning: the single twin marts/031_returning_production.sql
# COALESCEs (total_receiving_ppa__v_double). Live-verified 2026-09-03
# presence check found exactly this one twin on the table -- if a future
# check reports it as a day-one FAIL (i.e. some *other* twin exists live
# that no mart COALESCEs), that is a real pre-existing gap to fix, not
# tripwire noise: go add the COALESCE, don't just widen the allow-list.
#
# stats.player_usage: the two twins marts/032_player_usage.sql COALESCEs on
# the nested "usage" object (usage__pass__v_double,
# usage__third_down__v_double). Seeded from the mart's own COALESCE calls;
# not independently re-verified against a live presence check the way
# player_returning was on 2026-09-03.
#
# stats.game_havoc: five twins live-verified 2026-09-05 (the 2026-09-04
# daily was this tripwire's first execution and caught three the
# 2026-09-03 seeding missed). marts/005_defensive_havoc.sql COALESCEs the
# original two in its game_havoc_season CTE
# (defense__total_havoc_events__v_double,
# defense__front_seven_havoc_events__v_double; defense__db_havoc_events has
# no variant column). Of the three newly-caught twins, none needed a new
# COALESCE in a mart's arithmetic:
#   - offense__total_havoc_events__v_double IS consumed, but by
#     scripts/build_features.py (features.team_week's
#     havoc_rate_offense_allowed), not by a marts/ file -- that COALESCE
#     already existed there and was already correct; it was simply
#     untracked here and in MART_TABLE_MAP below. build_features.py now
#     joins 005 as a second mapped file for this table in
#     tests/test_variant_twins.py.
#   - offense__front_seven_havoc_events__v_double sits on a column no
#     consumer reads today (mart 005's header has long noted "offense__* is
#     present but is unused here").
#   - defense__front_seven_havoc_rate__v_double sits on the CFBD-computed
#     per-game rate mart 005 explicitly does not use -- it recomputes its
#     own event-weighted season rate from the raw event/play counts instead
#     (see that file's AGGREGATION CHOICE note).
# All five are documented as LIVE-VERIFIED in marts/005_defensive_havoc.sql's
# header. Unlike the rushing/passing charting family, stats.game_havoc has
# no src/schemas/api/validation_rushing_views.sql counterpart -- that file's
# group (e) allow-lists (allowed_player_twins / allowed_team_twins) only
# cover stats.rushing_player_season / stats.rushing_team_season -- so this
# table's allow-list is not mirrored there.
EXPECTED_VARIANT_TWINS: dict[str, frozenset[str]] = {
    "stats.rushing_player_season": frozenset(
        {
            "open_field_yards__v_double",
            "power_success__v_double",
            "directions__unknown__power_success__v_double",
            "directions__middle__yards_per_carry__v_double",
            "directions__middle__success_rate__v_double",
            "directions__middle__ppa__v_double",
            "directions__middle__total_ppa__v_double",
            "directions__middle__line_yards__v_double",
            "directions__middle__line_yards_total__v_double",
            "directions__middle__second_level_yards__v_double",
            "directions__middle__open_field_yards__v_double",
            "directions__middle__stuff_rate__v_double",
            "directions__middle__explosiveness__v_double",
            "directions__left__line_yards_total__v_double",
            "directions__right__power_success__v_double",
            "directions__middle__power_success__v_double",
            "directions__left__power_success__v_double",
        }
    ),
    "stats.rushing_team_season": frozenset(
        {
            "defense__directions__right__line_yards_total__v_double",
            "defense__directions__middle__second_level_yards__v_double",
            "defense__directions__middle__power_success__v_double",
            "defense__directions__left__line_yards__v_double",
            "offense__line_yards_total__v_double",
            "offense__second_level_yards__v_double",
            "offense__open_field_yards__v_double",
            "defense__directions__right__power_success__v_double",
        }
    ),
    "stats.passing_player_season": frozenset(
        {
            "average_yards_after_catch__v_double",
        }
    ),
    "stats.player_returning": frozenset(
        {
            "total_receiving_ppa__v_double",
        }
    ),
    "stats.player_usage": frozenset(
        {
            "usage__pass__v_double",
            "usage__third_down__v_double",
        }
    ),
    "stats.game_havoc": frozenset(
        {
            "defense__total_havoc_events__v_double",
            "defense__front_seven_havoc_events__v_double",
            "defense__front_seven_havoc_rate__v_double",
            "offense__front_seven_havoc_events__v_double",
            "offense__total_havoc_events__v_double",
        }
    ),
}


# Exact variants reviewed on 2026-09-11 as raw-only for maintained curated
# consumers. The production dependency audit found that the only maintained
# metric consumer of stats.passing_player_season is
# marts.passing_charting_player_season (and its API projection). It selects the
# supported average_yards_after_catch base/twin pair above, but none of these 55
# variants or their base fields. Raw stats callers can still read these fields
# and must handle dlt variants themselves. Keep this registry exact: a new
# location field or a variant of a consumed metric must remain a failure until
# its own consumer review is complete.
REVIEWED_RAW_ONLY_VARIANT_TWINS: dict[str, frozenset[str]] = {
    "stats.passing_player_season": frozenset(
        {
            "explosiveness__v_double",
            "locations__deep_left__air_yards_per_attempt__v_double",
            "locations__deep_left__average_depth_of_target__v_double",
            "locations__deep_left__average_yards_after_catch__v_double",
            "locations__deep_left__completion_rate__v_double",
            "locations__deep_left__explosiveness__v_double",
            "locations__deep_left__ppa__v_double",
            "locations__deep_left__success_rate__v_double",
            "locations__deep_left__total_ppa__v_double",
            "locations__deep_left__yards_per_attempt__v_double",
            "locations__deep_middle__completion_rate__v_double",
            "locations__deep_middle__explosiveness__v_double",
            "locations__deep_middle__ppa__v_double",
            "locations__deep_middle__success_rate__v_double",
            "locations__deep_middle__total_ppa__v_double",
            "locations__deep_middle__yards_per_attempt__v_double",
            "locations__deep_right__air_yards_per_attempt__v_double",
            "locations__deep_right__average_depth_of_target__v_double",
            "locations__deep_right__average_yards_after_catch__v_double",
            "locations__deep_right__completion_rate__v_double",
            "locations__deep_right__explosiveness__v_double",
            "locations__deep_right__ppa__v_double",
            "locations__deep_right__success_rate__v_double",
            "locations__deep_right__total_ppa__v_double",
            "locations__deep_right__yards_per_attempt__v_double",
            "locations__short_left__air_yards_per_attempt__v_double",
            "locations__short_left__average_depth_of_target__v_double",
            "locations__short_left__average_yards_after_catch__v_double",
            "locations__short_left__completion_rate__v_double",
            "locations__short_left__explosiveness__v_double",
            "locations__short_left__ppa__v_double",
            "locations__short_left__success_rate__v_double",
            "locations__short_left__total_ppa__v_double",
            "locations__short_left__yards_per_attempt__v_double",
            "locations__short_middle__air_yards_per_attempt__v_double",
            "locations__short_middle__average_depth_of_target__v_double",
            "locations__short_middle__completion_rate__v_double",
            "locations__short_middle__explosiveness__v_double",
            "locations__short_middle__success_rate__v_double",
            "locations__short_middle__yards_per_attempt__v_double",
            "locations__short_right__air_yards_per_attempt__v_double",
            "locations__short_right__average_depth_of_target__v_double",
            "locations__short_right__average_yards_after_catch__v_double",
            "locations__short_right__completion_rate__v_double",
            "locations__short_right__explosiveness__v_double",
            "locations__short_right__success_rate__v_double",
            "locations__short_right__yards_per_attempt__v_double",
            "locations__unknown__air_yards_per_attempt__v_double",
            "locations__unknown__average_depth_of_target__v_double",
            "locations__unknown__explosiveness__v_double",
            "locations__unknown__ppa__v_double",
            "locations__unknown__success_rate__v_double",
            "locations__unknown__total_ppa__v_double",
            "locations__unknown__yards_per_attempt__v_double",
            "success_rate__v_double",
        }
    ),
}


def _split_table_key(table_key: str) -> tuple[str, str]:
    schema, _, table = table_key.partition(".")
    return schema, table


def _fetch_actual_twins(cur) -> dict[str, set[str]]:
    """Query information_schema.columns once for every tracked table.

    Returns {"schema.table": {actual __v_double column names}}. A table
    that does not exist (or has no twins) simply contributes an empty set --
    information_schema returns zero rows for it, not an error.
    """
    table_keys = list(EXPECTED_VARIANT_TWINS)
    pairs = [_split_table_key(key) for key in table_keys]

    actual: dict[str, set[str]] = {key: set() for key in table_keys}
    if not pairs:
        return actual

    cur.execute(
        """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE (table_schema, table_name) IN %s
          AND column_name LIKE '%%\\_\\_v\\_double' ESCAPE '\\'
        """,
        (tuple(pairs),),
    )
    for table_schema, table_name, column_name in cur.fetchall():
        actual[f"{table_schema}.{table_name}"].add(column_name)

    return actual


def find_unexpected_twins(cur) -> dict[str, list[str]]:
    """Return {"schema.table": [unexpected __v_double columns]} for any twin
    that exists live but is neither supported nor reviewed raw-only.

    A non-empty result means a daily load created a variant that needs consumer
    review before it can be classified -- see the module docstring for the two
    possible remedies. Tables with no unexpected columns are omitted from the
    result entirely (an empty dict means every twin has been reviewed).
    """
    actual = _fetch_actual_twins(cur)
    unexpected: dict[str, list[str]] = {}
    for table_key, expected_cols in EXPECTED_VARIANT_TWINS.items():
        reviewed_raw_only = REVIEWED_RAW_ONLY_VARIANT_TWINS.get(table_key, frozenset())
        extra = actual.get(table_key, set()) - expected_cols - reviewed_raw_only
        if extra:
            unexpected[table_key] = sorted(extra)
    return unexpected


def find_missing_twins(cur) -> dict[str, list[str]]:
    """Return {"schema.table": [expected __v_double columns not present]}.

    Informational only -- a table recreated from scratch (or one that has
    not yet received a load carrying a fractional value for that metric)
    can legitimately lack a twin that existed at allow-list authoring time.
    Callers should WARN on a non-empty result, never FAIL.
    """
    actual = _fetch_actual_twins(cur)
    missing: dict[str, list[str]] = {}
    for table_key, expected_cols in EXPECTED_VARIANT_TWINS.items():
        gone = expected_cols - actual.get(table_key, set())
        if gone:
            missing[table_key] = sorted(gone)
    return missing
