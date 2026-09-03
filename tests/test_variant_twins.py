"""Drift guard + unit tests for src/pipelines/utils/variant_twins.py (KTD7).

Three independent places encode "which __v_double twins are expected":
Python (EXPECTED_VARIANT_TWINS), the deploy-time SQL validation
(src/schemas/api/validation_rushing_views.sql group (e)), and the marts'
own COALESCE calls (005/031/032/045/050/051/052 -- the rushing/passing
charting family plus the three non-charting marts that also COALESCE a
twin: defensive_havoc, returning_production, player_usage). This file
asserts all three agree, so a change to one that isn't mirrored in the
others fails a fast, DB-free unit test instead of surfacing as a silent
NULL in production. Plus unit tests for find_unexpected_twins/
find_missing_twins against a fake cursor. No DB, no network -- pure text
parsing and in-memory fakes.

Only 045/050/051/052 are cross-checked against validation_rushing_views.sql
(TestAllowListsMatchSql) -- that SQL file's allow-lists only cover the
rushing/passing charting tables. 005/031/032 have no deploy-time SQL
counterpart, so they're covered only by the marts-vs-Python drift guard
below (TestAllowListsMatchMarts) and by MART_TABLE_MAP's exhaustiveness
check against EXPECTED_VARIANT_TWINS.
"""

import re
from pathlib import Path

import pytest

from src.pipelines.utils.variant_twins import (
    EXPECTED_VARIANT_TWINS,
    find_missing_twins,
    find_unexpected_twins,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VALIDATION_SQL = (
    PROJECT_ROOT / "src" / "schemas" / "api" / "validation_rushing_views.sql"
).read_text()

# Rushing/passing charting family -- cross-checked against the deploy-time
# SQL validation allow-lists (TestAllowListsMatchSql) as well as the
# marts-vs-Python drift guard below.
MART_FILES = [
    PROJECT_ROOT / "src" / "schemas" / "marts" / "045_passing_charting_player_season.sql",
    PROJECT_ROOT / "src" / "schemas" / "marts" / "050_rushing_charting_player_season.sql",
    PROJECT_ROOT / "src" / "schemas" / "marts" / "051_rushing_charting_team_season.sql",
    PROJECT_ROOT / "src" / "schemas" / "marts" / "052_rushing_charting_direction_season.sql",
]

# Every mart that COALESCEs a __v_double twin, mapped to the source table(s)
# it reads -- the full set the marts-vs-Python drift guard walks. Includes
# MART_FILES (the charting family) plus the three non-charting marts Finding
# 1 (KTD7 follow-up, 2026-09-03) added: defensive_havoc reads
# stats.game_havoc, returning_production reads stats.player_returning,
# player_usage reads stats.player_usage.
MART_TABLE_MAP: dict[Path, list[str]] = {
    PROJECT_ROOT / "src" / "schemas" / "marts" / "045_passing_charting_player_season.sql": [
        "stats.passing_player_season"
    ],
    PROJECT_ROOT / "src" / "schemas" / "marts" / "050_rushing_charting_player_season.sql": [
        "stats.rushing_player_season"
    ],
    PROJECT_ROOT / "src" / "schemas" / "marts" / "051_rushing_charting_team_season.sql": [
        "stats.rushing_team_season"
    ],
    PROJECT_ROOT / "src" / "schemas" / "marts" / "052_rushing_charting_direction_season.sql": [
        "stats.rushing_player_season",
        "stats.rushing_team_season",
    ],
    PROJECT_ROOT / "src" / "schemas" / "marts" / "005_defensive_havoc.sql": ["stats.game_havoc"],
    PROJECT_ROOT / "src" / "schemas" / "marts" / "031_returning_production.sql": [
        "stats.player_returning"
    ],
    PROJECT_ROOT / "src" / "schemas" / "marts" / "032_player_usage.sql": ["stats.player_usage"],
}


def _parse_sql_array(var_name: str) -> set[str]:
    """Extract the quoted string literals out of `<var_name> := ARRAY[...]`."""
    match = re.search(rf"{var_name}\s*:=\s*ARRAY\[(.*?)\];", VALIDATION_SQL, re.S)
    assert match, f"could not find `{var_name} := ARRAY[...]` in validation_rushing_views.sql"
    return set(re.findall(r"'([^']+)'", match.group(1)))


class TestAllowListsMatchSql:
    """Python and the deploy-time SQL validation must carry the identical
    allow-list for each rushing table -- KTD7's whole point is that these
    cannot drift apart silently."""

    def test_player_season_matches(self):
        sql_twins = _parse_sql_array("allowed_player_twins")
        assert sql_twins == EXPECTED_VARIANT_TWINS["stats.rushing_player_season"]

    def test_team_season_matches(self):
        sql_twins = _parse_sql_array("allowed_team_twins")
        assert sql_twins == EXPECTED_VARIANT_TWINS["stats.rushing_team_season"]

    def test_player_season_count_is_17(self):
        assert len(EXPECTED_VARIANT_TWINS["stats.rushing_player_season"]) == 17

    def test_team_season_count_is_8(self):
        assert len(EXPECTED_VARIANT_TWINS["stats.rushing_team_season"]) == 8


def _v_double_tokens(path: Path) -> set[str]:
    return set(re.findall(r"\b[a-z][a-z_]*__v_double\b", path.read_text()))


class TestAllowListsMatchMarts:
    """Every twin the Python allow-lists know about must actually be
    COALESCEd by one of the seven marts in MART_TABLE_MAP (the
    rushing/passing charting family plus defensive_havoc/
    returning_production/player_usage), and vice versa -- a twin referenced
    by a mart but missing from EXPECTED_VARIANT_TWINS would mean the daily
    check doesn't actually cover it."""

    def test_every_v_double_token_in_marts_is_expected_and_vice_versa(self):
        found: set[str] = set()
        for path in MART_TABLE_MAP:
            found |= _v_double_tokens(path)

        expected_union: set[str] = set()
        for cols in EXPECTED_VARIANT_TWINS.values():
            expected_union |= cols

        assert found == expected_union, (
            f"mart files and EXPECTED_VARIANT_TWINS disagree -- "
            f"in marts but not expected: {found - expected_union}; "
            f"expected but not in any mart: {expected_union - found}"
        )

    def test_every_expected_table_is_covered_by_a_mapped_mart(self):
        """MART_TABLE_MAP itself must not silently drop a tracked table --
        every key in EXPECTED_VARIANT_TWINS must appear as a value somewhere
        in the map, or the per-table check below would vacuously pass it."""
        mapped_tables: set[str] = set()
        for tables in MART_TABLE_MAP.values():
            mapped_tables |= set(tables)

        assert mapped_tables == set(EXPECTED_VARIANT_TWINS), (
            f"MART_TABLE_MAP and EXPECTED_VARIANT_TWINS track different tables -- "
            f"mapped but not expected: {mapped_tables - set(EXPECTED_VARIANT_TWINS)}; "
            f"expected but not mapped: {set(EXPECTED_VARIANT_TWINS) - mapped_tables}"
        )

    def test_each_tables_twins_come_from_its_own_mapped_marts(self):
        """Per-table version of the drift guard: the tokens found in the
        mart(s) mapped to a given source table must exactly match that
        table's entry in EXPECTED_VARIANT_TWINS -- catches a twin correctly
        spelled but attributed to the wrong table (e.g. copy-paste between
        005/031/032), which the pooled union check above cannot see."""
        tokens_by_table: dict[str, set[str]] = {key: set() for key in EXPECTED_VARIANT_TWINS}
        for path, tables in MART_TABLE_MAP.items():
            tokens = _v_double_tokens(path)
            for table_key in tables:
                tokens_by_table[table_key] |= tokens

        for table_key, expected_cols in EXPECTED_VARIANT_TWINS.items():
            found = tokens_by_table[table_key]
            # 052 shares its file (and hence its token pool) between two
            # tables (player-season and team-season), so require the
            # table's expected columns to be a subset of what its mapped
            # mart(s) contain rather than an exact match.
            assert expected_cols <= found, (
                f"{table_key}: EXPECTED_VARIANT_TWINS has columns not found in its "
                f"mapped mart(s): {expected_cols - found}"
            )


class _FakeCursor:
    """Cursor stub for find_unexpected_twins/find_missing_twins: records the
    single query issued and returns canned (schema, table, column) rows for
    fetchall(), the shape information_schema.columns yields."""

    def __init__(self, rows: list[tuple[str, str, str]]):
        self._rows = rows
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchall(self):
        return self._rows


def _all_expected_rows() -> list[tuple[str, str, str]]:
    rows = []
    for table_key, cols in EXPECTED_VARIANT_TWINS.items():
        schema, table = table_key.split(".")
        for col in cols:
            rows.append((schema, table, col))
    return rows


class TestFindUnexpectedTwins:
    def test_all_expected_returns_empty(self):
        cur = _FakeCursor(_all_expected_rows())
        assert find_unexpected_twins(cur) == {}

    def test_one_extra_column_is_reported_under_its_table(self):
        rows = _all_expected_rows()
        rows.append(("stats", "rushing_player_season", "totally_new__v_double"))
        cur = _FakeCursor(rows)

        result = find_unexpected_twins(cur)

        assert result == {"stats.rushing_player_season": ["totally_new__v_double"]}

    def test_extra_on_a_table_with_no_prior_twins_is_still_reported(self):
        rows = _all_expected_rows()
        rows.append(("stats", "passing_player_season", "some_other_metric__v_double"))
        cur = _FakeCursor(rows)

        result = find_unexpected_twins(cur)

        assert result == {"stats.passing_player_season": ["some_other_metric__v_double"]}

    def test_single_query_issued(self):
        cur = _FakeCursor(_all_expected_rows())
        find_unexpected_twins(cur)
        assert len(cur.queries) == 1
        sql, params = cur.queries[0]
        assert "information_schema.columns" in sql
        assert "v\\_double" in sql
        assert "ESCAPE" in sql
        assert len(params[0]) == len(EXPECTED_VARIANT_TWINS)


class TestFindMissingTwins:
    def test_nothing_missing_returns_empty(self):
        cur = _FakeCursor(_all_expected_rows())
        assert find_missing_twins(cur) == {}

    def test_expected_but_absent_column_is_reported(self):
        rows = [
            row
            for row in _all_expected_rows()
            if row != ("stats", "passing_player_season", "average_yards_after_catch__v_double")
        ]
        cur = _FakeCursor(rows)

        result = find_missing_twins(cur)

        assert result == {"stats.passing_player_season": ["average_yards_after_catch__v_double"]}

    def test_unexpected_and_missing_are_independent(self):
        """A table can simultaneously have an unexpected twin AND be missing
        a different expected one -- e.g. after a partial re-load."""
        rows = [
            row
            for row in _all_expected_rows()
            if row != ("stats", "rushing_player_season", "power_success__v_double")
        ]
        rows.append(("stats", "rushing_player_season", "brand_new__v_double"))
        cur = _FakeCursor(rows)

        assert find_unexpected_twins(cur) == {
            "stats.rushing_player_season": ["brand_new__v_double"]
        }
        assert find_missing_twins(cur) == {
            "stats.rushing_player_season": ["power_success__v_double"]
        }


class TestNoGameGrainTables:
    """The game-grain rushing tables carry far more twins than the season
    tables but no mart reads them -- they must stay out of
    EXPECTED_VARIANT_TWINS (module docstring explains why)."""

    def test_game_grain_tables_are_not_tracked(self):
        tracked = set(EXPECTED_VARIANT_TWINS)
        assert "stats.rushing_player_games" not in tracked
        assert "stats.rushing_team_games" not in tracked


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
