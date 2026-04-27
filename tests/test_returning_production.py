"""Tests for the returning production model marts and loaders.

Covers U2+ of docs/plans/2026-04-27-001-feat-returning-production-model-plan.md.

This file is the catch-all for cross-unit returning-production tests. Schema-shape
tests stay in tests/test_returning_schema.py. Loader and matview behavior tests
live here.
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Expected position_group values produced by the U2 loader. The plan calls for
# exactly 8 groups; this set is the authoritative one.
# ---------------------------------------------------------------------------

EXPECTED_POSITION_GROUPS = {"QB", "RB", "WR_TE", "OL", "DL", "LB", "DB", "ST"}


# ---------------------------------------------------------------------------
# U2: rp.refresh_fct_player_seasons() and rp.fct_player_seasons population
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def fct_player_seasons_loaded(db_conn):
    """Ensure the loader has run at least once for the test session.

    The function is idempotent (TRUNCATE + INSERT). Calling it once at module
    setup makes the rest of the tests deterministic regardless of prior state.
    """
    with db_conn.cursor() as cur:
        cur.execute("SELECT rp.refresh_fct_player_seasons()")
    return True


class TestFctPlayerSeasonsRowCounts:
    """Verify the loader produces sensible row counts for the seasons in scope.

    Plan acceptance ('≥250,000 for 2020-2025') was a planning estimate; actual
    core.roster rows for that range cap at ~141K (2020 was loaded with only 144
    teams). The bounds here reflect the actual data envelope.
    """

    def test_total_rows_within_envelope(self, db_conn, fct_player_seasons_loaded):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM rp.fct_player_seasons")
            total = cur.fetchone()[0]
        # Floor: known data lower bound. Ceiling: catches accidental cartesian fanout.
        assert 130_000 <= total <= 200_000, (
            f"rp.fct_player_seasons total rows = {total}; expected 130K-200K range"
        )

    def test_2025_row_count(self, db_conn, fct_player_seasons_loaded):
        """Plan gate: row count for season=2025 ≥ 25,000."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM rp.fct_player_seasons WHERE season = 2025")
            count = cur.fetchone()[0]
        assert count >= 25_000, f"season=2025 row count {count} below 25K floor"

    def test_2024_row_count(self, db_conn, fct_player_seasons_loaded):
        """2024 should have at least 20K rows (304 teams loaded, ~70 players each)."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM rp.fct_player_seasons WHERE season = 2024")
            count = cur.fetchone()[0]
        assert count >= 20_000, f"season=2024 row count {count} below 20K floor"

    def test_all_target_seasons_populated(self, db_conn, fct_player_seasons_loaded):
        """Every season in the 2020-2025 backfill window should have rows."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT season FROM rp.fct_player_seasons
                GROUP BY season ORDER BY season
                """
            )
            seasons = [row[0] for row in cur.fetchall()]
        assert seasons == [2020, 2021, 2022, 2023, 2024, 2025], (
            f"Expected seasons 2020-2025; got {seasons}"
        )

    def test_no_seasons_outside_window(self, db_conn, fct_player_seasons_loaded):
        """Loader filters to 2020-2025; no stray rows from earlier roster years."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM rp.fct_player_seasons
                WHERE season < 2020 OR season > 2025
                """
            )
            assert cur.fetchone()[0] == 0


class TestPositionCanonicalization:
    """Position raw values must canonicalize to exactly 8 position_group buckets."""

    def test_no_null_position_group(self, db_conn, fct_player_seasons_loaded):
        """Every row must have a non-null position_group (plan invariant)."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM rp.fct_player_seasons WHERE position_group IS NULL")
            assert cur.fetchone()[0] == 0

    def test_position_groups_match_expected_set(self, db_conn, fct_player_seasons_loaded):
        """Distinct position_group values must equal exactly the 8-group set."""
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT position_group FROM rp.fct_player_seasons ORDER BY position_group"
            )
            actual = {row[0] for row in cur.fetchall()}
        assert actual == EXPECTED_POSITION_GROUPS, (
            f"position_group values differ from plan. "
            f"Missing: {EXPECTED_POSITION_GROUPS - actual}. "
            f"Extra: {actual - EXPECTED_POSITION_GROUPS}"
        )

    def test_canonical_position_within_eleven_set(self, db_conn, fct_player_seasons_loaded):
        """The 11-canonical position column should only contain known values."""
        expected = {"QB", "RB", "WR", "TE", "OL", "EDGE", "DT", "LB", "CB", "S", "ST"}
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT position FROM rp.fct_player_seasons WHERE position IS NOT NULL"
            )
            actual = {row[0] for row in cur.fetchall()}
        assert actual.issubset(expected), f"Unexpected canonical positions: {actual - expected}"

    def test_position_detail_preserves_raw(self, db_conn, fct_player_seasons_loaded):
        """position_detail should hold the raw CFBD string for downstream debugging."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT position_detail FROM rp.fct_player_seasons
                WHERE position_detail IS NOT NULL
                """
            )
            distinct = {row[0] for row in cur.fetchall()}
        # Should include common raw values like 'OT', 'OG', 'DE' that don't appear
        # in the 11-canonical set -- proving raw is preserved.
        assert "OT" in distinct or "DE" in distinct or "FB" in distinct, (
            f"position_detail looks canonicalized rather than raw: {distinct}"
        )


class TestStatPivot:
    """Long-format (category, stat_type) stats must pivot to wide-format columns."""

    def test_known_qb_has_passing_stats(self, db_conn, fct_player_seasons_loaded):
        """Carson Beck (player_id=4430841) for 2024 Georgia: known passing-stat target."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT position_group, stat_pass_yards, stat_pass_attempts, stat_pass_tds
                FROM rp.fct_player_seasons
                WHERE player_id = '4430841' AND season = 2024
                """
            )
            row = cur.fetchone()
        assert row is not None, "Carson Beck (4430841) 2024 row missing"
        position_group, pass_yds, pass_att, pass_tds = row
        assert position_group == "QB"
        assert pass_yds is not None and pass_yds > 1000, (
            f"Carson Beck 2024 stat_pass_yards = {pass_yds}; expected starter-volume"
        )
        assert pass_att is not None and pass_att > 100
        assert pass_tds is not None and pass_tds > 0

    def test_qb_position_has_passing_stats(self, db_conn, fct_player_seasons_loaded):
        """At least 50 QBs across all seasons should have non-null pass yards >0."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM rp.fct_player_seasons
                WHERE position_group = 'QB'
                  AND stat_pass_yards IS NOT NULL AND stat_pass_yards > 0
                """
            )
            assert cur.fetchone()[0] >= 50

    def test_dl_with_sacks_has_no_offensive_stats(self, db_conn, fct_player_seasons_loaded):
        """DL with non-trivial sacks should have NULL or 0 in offensive stat columns."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT player_id, stat_pass_yards, stat_rush_yards, stat_rec_yards,
                       stat_sacks, stat_tfl
                FROM rp.fct_player_seasons
                WHERE season = 2024 AND position_group = 'DL'
                  AND stat_sacks IS NOT NULL AND stat_sacks > 5
                ORDER BY stat_sacks DESC LIMIT 5
                """
            )
            rows = cur.fetchall()
        assert len(rows) > 0, "no 2024 DLs with >5 sacks found; data load issue?"
        for player_id, pass_yds, rush_yds, rec_yds, sacks, tfl in rows:
            # Pivot semantics: SUM over filtered rows yields 0 (not NULL) when no
            # passing/rushing/receiving rows exist for the player. Both shapes are
            # acceptable per the plan's "NULL/0" wording.
            assert (pass_yds or 0) == 0, f"DL {player_id} has stat_pass_yards={pass_yds}"
            assert (rush_yds or 0) == 0, f"DL {player_id} has stat_rush_yards={rush_yds}"
            assert sacks > 5, f"DL {player_id} should have sacks>5 by query, got {sacks}"

    def test_roster_only_player_keeps_roster_fields(self, db_conn, fct_player_seasons_loaded):
        """Roster-side LEFT JOIN must produce rows even when no stats exist."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT player_id, team, position_group,
                       stat_pass_yards, stat_rush_yards, stat_tackles_solo
                FROM rp.fct_player_seasons
                WHERE season = 2024
                  AND stat_pass_attempts IS NULL
                  AND stat_rush_attempts IS NULL
                  AND stat_tackles_solo IS NULL
                  AND stat_rec_catches IS NULL
                LIMIT 5
                """
            )
            rows = cur.fetchall()
        assert len(rows) > 0, "no roster-only players found; LEFT JOIN may be acting as INNER JOIN"
        for player_id, team, pgroup, pass_y, rush_y, tackles in rows:
            assert team is not None, f"roster-only row {player_id} missing team"
            assert pgroup is not None, f"roster-only row {player_id} missing position_group"
            # Stat columns should all be NULL (LEFT JOIN miss), not 0.
            assert pass_y is None
            assert rush_y is None
            assert tackles is None


class TestReferentialIntegrity:
    """Every fct_player_seasons row must trace back to a core.roster row."""

    def test_every_player_id_in_roster(self, db_conn, fct_player_seasons_loaded):
        """A LEFT JOIN to core.roster must find a match for every fct row."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM rp.fct_player_seasons fps
                LEFT JOIN core.roster r
                  ON r.id::text = fps.player_id AND r.year::int = fps.season
                WHERE r.id IS NULL
                """
            )
            assert cur.fetchone()[0] == 0


class TestIdempotency:
    """The loader is TRUNCATE + INSERT; re-running yields identical row counts."""

    def test_rerun_produces_identical_count(self, db_conn, fct_player_seasons_loaded):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM rp.fct_player_seasons")
            before = cur.fetchone()[0]

            cur.execute("SELECT rp.refresh_fct_player_seasons()")

            cur.execute("SELECT COUNT(*) FROM rp.fct_player_seasons")
            after = cur.fetchone()[0]

        assert before == after, f"loader is non-idempotent: {before} -> {after} rows on rerun"

    def test_rerun_yields_same_per_season_distribution(self, db_conn, fct_player_seasons_loaded):
        """Stronger idempotency check: per-season counts unchanged across reruns."""
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT season, COUNT(*) FROM rp.fct_player_seasons GROUP BY season ORDER BY season"
            )
            before = dict(cur.fetchall())

            cur.execute("SELECT rp.refresh_fct_player_seasons()")

            cur.execute(
                "SELECT season, COUNT(*) FROM rp.fct_player_seasons GROUP BY season ORDER BY season"
            )
            after = dict(cur.fetchall())

        assert before == after


class TestAnonAccessAfterLoad:
    """Loader must not change the anon read invariant set up in U1."""

    def test_anon_can_select_loaded_rows(self, db_conn, fct_player_seasons_loaded):
        cur = db_conn.cursor()
        cur.execute("SET ROLE anon")
        try:
            cur.execute("SELECT COUNT(*) FROM rp.fct_player_seasons")
            assert cur.fetchone()[0] > 0
        finally:
            cur.execute("RESET ROLE")
            cur.close()
