"""Tests for marts.defensive_havoc after the Phase 4 game_havoc re-source.

Two concerns are covered:

- Pure unit (no DB): the mart still inlines the canonical garbage-time predicate
  byte-for-byte. This is a local, targeted mirror of the repo-wide drift guard
  in tests/test_garbage_time_consistency.py, scoped to just this mart so a Phase
  4 edit that silently reworded the predicate fails here too.
- DB-gated (conftest db_conn, skips without creds): the additive havoc-split
  columns exist, havoc_rate stays a fraction in [0, 1], and game_havoc actually
  populated the recent-season rows (>90% of 2024 non-null).
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MART_PATH = PROJECT_ROOT / "src" / "schemas" / "marts" / "005_defensive_havoc.sql"

# Prefer the canonical constant + extractor from the repo-wide drift guard so
# the two tests can never disagree; fall back to a local duplicate if that
# module ever moves or becomes unimportable.
try:
    from tests.test_garbage_time_consistency import (
        CANONICAL_PREDICATE,
        _extract_inline_predicates,
    )
except ImportError:  # pragma: no cover - defensive fallback
    import re

    CANONICAL_PREDICATE = (
        "(p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) > 28) OR "
        "(p.period >= 3 AND ABS(COALESCE(p.score_diff, 0)) > 35)"
    )

    _INLINE_PREDICATE_RE = re.compile(
        r"""
        \(\s*p\.period\s*(?:=|>=|<=|>|<)\s*\d+\s+
        AND\s+ABS\(COALESCE\(p\.score_diff,\s*0\)\)\s*>\s*\d+\s*\)
        \s*OR\s*
        \(\s*p\.period\s*(?:=|>=|<=|>|<)\s*\d+\s+
        AND\s+ABS\(COALESCE\(p\.score_diff,\s*0\)\)\s*>\s*\d+\s*\)
        """,
        re.VERBOSE | re.IGNORECASE,
    )

    def _extract_inline_predicates(sql_text: str) -> list[str]:
        return [
            re.sub(r"\s+", " ", m.group(0)).strip() for m in _INLINE_PREDICATE_RE.finditer(sql_text)
        ]


# ---------------------------------------------------------------------------
# Pure unit: garbage-time predicate still byte-identical to canonical
# ---------------------------------------------------------------------------


class TestGarbageTimePredicateUnchanged:
    """005_defensive_havoc.sql must keep the canonical inline garbage-time rule."""

    def test_mart_file_exists(self):
        assert MART_PATH.exists(), f"Mart SQL not found: {MART_PATH}"

    def test_inline_predicate_present_and_canonical(self):
        occurrences = _extract_inline_predicates(MART_PATH.read_text())
        assert occurrences, (
            "No inline garbage-time predicate found in 005_defensive_havoc.sql. "
            "Phase 4 kept the opponent-EPA family plays-derived, so the predicate "
            "must still be present."
        )
        for occurrence in occurrences:
            assert occurrence == CANONICAL_PREDICATE, (
                "005_defensive_havoc.sql garbage-time predicate drifted from canonical.\n"
                f"  found:     {occurrence}\n  canonical: {CANONICAL_PREDICATE}"
            )


# ---------------------------------------------------------------------------
# DB-gated: additive columns, rate range, recent-season coverage
# ---------------------------------------------------------------------------


class TestDefensiveHavocColumns:
    """The additive havoc-split columns must exist on the materialized view."""

    ADDITIVE_COLUMNS = {"front_seven_havoc_rate", "db_havoc_rate"}

    def test_additive_columns_exist(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'marts'
                  AND c.relname = 'defensive_havoc'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                """
            )
            actual = {row[0] for row in cur.fetchall()}
        missing = self.ADDITIVE_COLUMNS - actual
        assert not missing, f"marts.defensive_havoc missing additive columns: {missing}"


class TestDefensiveHavocRate:
    """havoc_rate must read as a fraction and be present for recent seasons."""

    def test_havoc_rate_within_unit_interval(self, db_conn):
        """Every non-null havoc_rate is a fraction in [0, 1] (CFBD havoc is a rate)."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM marts.defensive_havoc
                WHERE havoc_rate IS NOT NULL
                  AND (havoc_rate < 0 OR havoc_rate > 1)
                """
            )
            out_of_range = cur.fetchone()[0]
        assert out_of_range == 0, (
            f"{out_of_range} rows have havoc_rate outside [0, 1] -- the live "
            "game_havoc values are likely percentages (0..100), not fractions. "
            "Divide by 100 in the game_havoc_season CTE of 005_defensive_havoc.sql."
        )

    def test_havoc_rate_present_for_most_2024_rows(self, db_conn):
        """game_havoc should cover >90% of 2024 team-season rows."""
        with db_conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(havoc_rate) AS with_rate
                FROM marts.defensive_havoc
                WHERE season = 2024
                """
            )
            total, with_rate = cur.fetchone()
        assert total and total > 0, "No 2024 rows in marts.defensive_havoc"
        coverage = with_rate / total
        assert coverage > 0.90, (
            f"Only {with_rate}/{total} ({coverage:.1%}) of 2024 rows have a "
            "havoc_rate; expected >90%. stats.game_havoc coverage or the "
            "team/season join key is likely off."
        )
