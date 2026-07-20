"""Drift-guard tests for the garbage-time rule.

`public.is_garbage_time()` (src/schemas/functions/is_garbage_time.sql) is the
canonical source of truth for the garbage-time predicate. Several materialized
views in src/schemas/marts/ inline the equivalent predicate directly against
core.plays columns for performance instead of calling the function per row.

These tests guard against the two definitions silently drifting apart:

- `TestMartInlineSitesMatchCanonical` (no DB): regex-extracts every inline
  occurrence of the predicate from src/schemas/marts/*.sql and asserts it
  normalizes to the same canonical constant used by `is_garbage_time()`.
- `TestIsGarbageTimeFunctionMatchesCanonical` (DB, skips without creds): calls
  `public.is_garbage_time(period, score_diff)` over a grid of values and
  asserts it agrees with the canonical rule evaluated in Python.
"""

import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MARTS_DIR = PROJECT_ROOT / "src" / "schemas" / "marts"

# ---------------------------------------------------------------------------
# Canonical definition
# ---------------------------------------------------------------------------
# This must stay byte-identical (module whitespace normalization aside) to
# the predicate documented in src/schemas/functions/is_garbage_time.sql and
# inlined in the marts listed in KNOWN_INLINE_SITES below.

CANONICAL_PREDICATE = (
    "(p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) > 28) OR "
    "(p.period >= 3 AND ABS(COALESCE(p.score_diff, 0)) > 35)"
)

# Marts known to inline the predicate as of Phase 1 (2026-07-19 plan). Each
# must have at least one matching occurrence -- if this drops to zero for a
# listed file, either the inline site was removed (update this list and
# functions/is_garbage_time.sql's canonical-source comment) or the file's
# formatting drifted far enough that the regex below needs updating.
KNOWN_INLINE_SITES = {
    "002_game_epa_calc.sql",
    "004_situational_splits.sql",
    "005_defensive_havoc.sql",
    "010_play_epa.sql",
    "019_team_tempo_metrics.sql",
}

QUARTER_THRESHOLD = 28
SECOND_HALF_THRESHOLD = 35

# Matches the two-condition core of the predicate regardless of what wraps it
# (`NOT (...)` in most marts, `CASE WHEN ... THEN true` in 010_play_epa.sql).
# Threshold values and period comparisons are captured, not hardcoded, so a
# drifted number (e.g. 28 -> 30) is still extracted -- just normalized to a
# form that will no longer equal CANONICAL_PREDICATE.
INLINE_PREDICATE_RE = re.compile(
    r"""
    \(\s*p\.period\s*(?P<op1>=|>=|<=|>|<)\s*(?P<per1>\d+)\s+
    AND\s+ABS\(COALESCE\(p\.score_diff,\s*0\)\)\s*>\s*(?P<thr1>\d+)\s*\)
    \s*OR\s*
    \(\s*p\.period\s*(?P<op2>=|>=|<=|>|<)\s*(?P<per2>\d+)\s+
    AND\s+ABS\(COALESCE\(p\.score_diff,\s*0\)\)\s*>\s*(?P<thr2>\d+)\s*\)
    """,
    re.VERBOSE | re.IGNORECASE,
)


def _normalize(text: str) -> str:
    """Collapse all whitespace runs to a single space and strip."""
    return re.sub(r"\s+", " ", text).strip()


def _extract_inline_predicates(sql_text: str) -> list[str]:
    """Return the normalized text of every inline garbage-time predicate match."""
    return [_normalize(m.group(0)) for m in INLINE_PREDICATE_RE.finditer(sql_text)]


def _mart_files() -> list[Path]:
    return sorted(MARTS_DIR.glob("*.sql"))


class TestMartInlineSitesMatchCanonical:
    """Every inline garbage-time predicate in src/schemas/marts/ must match canonical."""

    def test_canonical_predicate_is_internally_consistent(self):
        # Sanity check that the constant itself matches the extraction regex,
        # so a typo in CANONICAL_PREDICATE can't silently pass every check.
        matches = _extract_inline_predicates(CANONICAL_PREDICATE)
        assert matches == [CANONICAL_PREDICATE]

    def test_known_inline_sites_have_at_least_one_occurrence(self):
        found_files = {f.name for f in _mart_files() if _extract_inline_predicates(f.read_text())}
        missing = KNOWN_INLINE_SITES - found_files
        assert not missing, (
            f"Expected inline garbage-time predicate not found in: {sorted(missing)}. "
            "If the predicate was intentionally removed or reworded, update "
            "KNOWN_INLINE_SITES here and the canonical-source comment in "
            "src/schemas/functions/is_garbage_time.sql."
        )

    @pytest.mark.parametrize("mart_path", _mart_files(), ids=lambda p: p.name)
    def test_inline_predicates_match_canonical(self, mart_path: Path):
        occurrences = _extract_inline_predicates(mart_path.read_text())
        for occurrence in occurrences:
            assert occurrence == CANONICAL_PREDICATE, (
                f"{mart_path.name} has a garbage-time predicate that has drifted from "
                f"the canonical rule.\n  found:     {occurrence}\n  canonical: "
                f"{CANONICAL_PREDICATE}\n"
                "Update BOTH this inline site and src/schemas/functions/is_garbage_time.sql "
                "together (see that file's canonical-source comment for the full list of "
                "inline sites)."
            )


# ---------------------------------------------------------------------------
# DB test: public.is_garbage_time() must agree with the canonical rule
# ---------------------------------------------------------------------------

PERIODS = [1, 2, 3, 4, 5]
SCORE_DIFFS = [-40, -36, -29, -28, -27, 0, 27, 28, 29, 35, 36, 40, None]


def _canonical_is_garbage_time(period: int, score_diff: int | None) -> bool:
    """Python evaluation of CANONICAL_PREDICATE, including COALESCE(..., 0)."""
    diff = abs(score_diff) if score_diff is not None else 0
    return (period == 4 and diff > QUARTER_THRESHOLD) or (
        period >= 3 and diff > SECOND_HALF_THRESHOLD
    )


class TestIsGarbageTimeFunctionMatchesCanonical:
    """public.is_garbage_time() must agree with the canonical rule for all inputs."""

    @pytest.mark.parametrize("period", PERIODS)
    @pytest.mark.parametrize("score_diff", SCORE_DIFFS)
    def test_matches_canonical_rule(self, db_conn, period, score_diff):
        expected = _canonical_is_garbage_time(period, score_diff)
        with db_conn.cursor() as cur:
            cur.execute("SELECT public.is_garbage_time(%s, %s)", (period, score_diff))
            actual = cur.fetchone()[0]
        assert actual == expected, (
            f"public.is_garbage_time({period}, {score_diff}) returned {actual}, "
            f"expected {expected} per the canonical rule"
        )
