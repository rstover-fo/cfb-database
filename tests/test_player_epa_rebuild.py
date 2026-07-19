"""Tests for the Phase 3 player-EPA attribution rebuild.

marts.player_game_epa was rewritten to attribute per-play EPA via CFBD's
authoritative stats.play_stats athlete link table (keyed on athlete_id) instead
of parsing player names out of play_text. This module covers:

* a pure unit test (no DB) that parses the stat_type -> role VALUES mapping out
  of the mart SQL and asserts no stat_type is assigned to more than one role
  (which would double-credit a play);
* DB-gated tests (via the `db_conn` fixture, which skips when no Postgres
  credentials are configured) that verify the live mart has athlete_id
  populated, exposes the new 'receiving' category, and is free of duplicate
  (game_id, athlete_id, play_category) rows.
"""

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PLAYER_GAME_EPA_SQL = PROJECT_ROOT / "src" / "schemas" / "marts" / "011_player_game_epa.sql"

# The mapping tuples in the mart look like ('Completion', 'passing'). Every
# mapping row's role is one of the three known play_category values, which lets
# us pull exactly the mapping out of the file without depending on surrounding
# whitespace or CTE structure.
_MAPPING_RE = re.compile(r"\(\s*'([^']+)'\s*,\s*'(passing|rushing|receiving)'\s*\)")


def _parse_stat_type_roles() -> list[tuple[str, str]]:
    """Return the [(stat_type, role), ...] mapping declared in the mart SQL."""
    sql = PLAYER_GAME_EPA_SQL.read_text()
    return _MAPPING_RE.findall(sql)


def _fetch_one(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()


# ---------------------------------------------------------------------------
# Unit test (no DB): stat_type -> role mapping integrity
# ---------------------------------------------------------------------------


class TestStatTypeRoleMapping:
    """Parse the mapping VALUES block straight out of the mart SQL."""

    def test_mapping_is_present(self):
        mapping = _parse_stat_type_roles()
        assert mapping, "No stat_type -> role VALUES mapping found in 011_player_game_epa.sql"

    def test_no_stat_type_assigned_to_multiple_roles(self):
        """A stat_type mapped to two roles would credit one play to two
        categories via the same physical row -- a double count. Each stat_type
        must appear at most once across the whole mapping."""
        mapping = _parse_stat_type_roles()
        stat_types = [stat_type for stat_type, _role in mapping]
        duplicates = sorted({s for s in stat_types if stat_types.count(s) > 1})
        assert not duplicates, f"stat_type mapped to multiple roles: {duplicates}"

    def test_all_three_roles_present(self):
        """The rebuild adds 'receiving' alongside 'passing' and 'rushing'."""
        roles = {role for _stat_type, role in _parse_stat_type_roles()}
        assert roles == {"passing", "rushing", "receiving"}, (
            f"Expected exactly passing/rushing/receiving roles, got {roles}"
        )


# ---------------------------------------------------------------------------
# DB-gated tests: live marts.player_game_epa shape
# ---------------------------------------------------------------------------


class TestPlayerGameEpaRebuild:
    """Verify the rebuilt mart against the live database.

    Uses the module-scoped `db_conn` fixture from conftest.py, which calls
    pytest.skip() when no Postgres credentials are available.
    """

    def test_athlete_id_column_exists(self, db_conn):
        row = _fetch_one(
            db_conn,
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'marts'
              AND table_name = 'player_game_epa'
              AND column_name = 'athlete_id'
            """,
        )
        assert row is not None, "marts.player_game_epa should expose an athlete_id column"

    def test_athlete_id_mostly_non_null_2024(self, db_conn):
        total, non_null = _fetch_one(
            db_conn,
            """
            SELECT COUNT(*), COUNT(athlete_id)
            FROM marts.player_game_epa
            WHERE season = 2024
            """,
        )
        assert total > 0, "Expected 2024 rows in marts.player_game_epa"
        assert non_null > total * 0.5, (
            f"athlete_id should be non-null for >50% of 2024 rows, got {non_null}/{total}"
        )

    def test_receiving_category_present(self, db_conn):
        (count,) = _fetch_one(
            db_conn,
            "SELECT COUNT(*) FROM marts.player_game_epa WHERE play_category = 'receiving'",
        )
        assert count > 0, "Rebuilt mart should attribute a 'receiving' category"

    def test_expected_categories(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT play_category FROM marts.player_game_epa")
            categories = {r[0] for r in cur.fetchall()}
        assert {"passing", "rushing", "receiving"}.issubset(categories), (
            f"Missing expected categories, got {categories}"
        )

    def test_no_duplicate_athlete_game_category(self, db_conn):
        """One row per (game_id, athlete_id, play_category): the double-count
        guard (DISTINCT per play/athlete/role) plus the athlete_id grain must
        leave no duplicates."""
        (dupes,) = _fetch_one(
            db_conn,
            """
            SELECT COUNT(*) FROM (
                SELECT game_id, athlete_id, play_category
                FROM marts.player_game_epa
                GROUP BY game_id, athlete_id, play_category
                HAVING COUNT(*) > 1
            ) d
            """,
        )
        assert dupes == 0, f"Found {dupes} duplicate (game_id, athlete_id, play_category) groups"
