"""Drift guard + unit tests for src/pipelines/utils/variant_twins.py (KTD7).

Three independent places encode "which __v_double twins are expected":
Python (EXPECTED_VARIANT_TWINS), the deploy-time SQL validation
(src/schemas/api/validation_rushing_views.sql group (e)), and the marts'
own COALESCE calls (045/050/051/052). This file asserts all three agree, so
a change to one that isn't mirrored in the others fails a fast, DB-free
unit test instead of surfacing as a silent NULL in production. Plus unit
tests for find_unexpected_twins/find_missing_twins against a fake cursor.
No DB, no network -- pure text parsing and in-memory fakes.
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

MART_FILES = [
    PROJECT_ROOT / "src" / "schemas" / "marts" / "045_passing_charting_player_season.sql",
    PROJECT_ROOT / "src" / "schemas" / "marts" / "050_rushing_charting_player_season.sql",
    PROJECT_ROOT / "src" / "schemas" / "marts" / "051_rushing_charting_team_season.sql",
    PROJECT_ROOT / "src" / "schemas" / "marts" / "052_rushing_charting_direction_season.sql",
]


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


class TestAllowListsMatchMarts:
    """Every twin the Python allow-lists know about must actually be
    COALESCEd by one of marts 045/050/051/052, and vice versa -- a twin
    referenced by a mart but missing from EXPECTED_VARIANT_TWINS would mean
    the daily check doesn't actually cover it."""

    def test_every_v_double_token_in_marts_is_expected_and_vice_versa(self):
        found: set[str] = set()
        for path in MART_FILES:
            found |= set(re.findall(r"\b[a-z][a-z_]*__v_double\b", path.read_text()))

        expected_union: set[str] = set()
        for cols in EXPECTED_VARIANT_TWINS.values():
            expected_union |= cols

        assert found == expected_union, (
            f"mart files and EXPECTED_VARIANT_TWINS disagree -- "
            f"in marts but not expected: {found - expected_union}; "
            f"expected but not in any mart: {expected_union - found}"
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
