"""Static drift-guards for the five situational-split RPCs.

Born from the 2026-07-23 get_red_zone_splits bug: its trips CTE filtered
core.drives on start_yardline >= 80 -- the ABSOLUTE yardline column (the only
reference to it in the repo) and a drive-START condition, when a red-zone
trip is a drive that REACHES yards_to_goal <= 20. Defense TDs-allowed
collapsed to ~0 and trip counts matched no real definition. These tests pin
the repaired shape so it cannot silently regress; behavioral correctness is
proven in prod by src/schemas/public/validation_situational_splits.sql.
"""

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = PROJECT_ROOT / "src" / "schemas"
PLAY_ANALYSIS = SCHEMAS_DIR / "public" / "006_play_analysis_functions.sql"
TEAM_SPLITS = SCHEMAS_DIR / "public" / "005_team_split_functions.sql"


def _strip_comments(sql: str) -> str:
    """Drop line comments so prose (e.g. bug-history notes) never trips the
    code-shape assertions."""
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _sql_files():
    return sorted(SCHEMAS_DIR.rglob("*.sql"))


class TestAbsoluteYardlineBan:
    def test_no_start_yardline_anywhere(self):
        """start_yardline (absolute, direction-dependent) is a proven
        foot-gun; every legitimate consumer uses start_yards_to_goal."""
        offenders = [
            str(f.relative_to(PROJECT_ROOT))
            for f in _sql_files()
            if "start_yardline" in _strip_comments(f.read_text())
        ]
        assert offenders == [], f"start_yardline referenced in: {offenders}"

    def test_split_functions_use_yards_to_goal_not_yardline(self):
        """Within the split-RPC files, any yardline mention must be the
        offense-relative yards_to_goal, never the absolute p.yardline."""
        for path in (PLAY_ANALYSIS, TEAM_SPLITS):
            sql = _strip_comments(path.read_text())
            bare = re.findall(r"\byardline\b", sql)
            assert bare == [], f"absolute yardline reference in {path.name}"


class TestGarbageTimeWiring:
    """Each split RPC applies the canonical garbage-time exclusion on its
    play-level stats (as a function call or the marts.play_epa boolean).
    Guards against the filter being dropped in a rewrite; the predicate's
    VALUE drift is guarded by test_garbage_time_consistency.py."""

    def _body(self, sql: str, fn: str) -> str:
        start = sql.index(f"FUNCTION public.{fn}")
        end = sql.find("CREATE OR REPLACE FUNCTION", start + 1)
        return sql[start : end if end != -1 else len(sql)]

    def test_all_five_reference_garbage_time(self):
        combined = {
            "get_down_distance_splits": PLAY_ANALYSIS,
            "get_field_position_splits": PLAY_ANALYSIS,
            "get_red_zone_splits": PLAY_ANALYSIS,
            "get_home_away_splits": TEAM_SPLITS,
            "get_conference_splits": TEAM_SPLITS,
        }
        for fn, path in combined.items():
            body = self._body(path.read_text(), fn)
            assert "is_garbage_time" in body, f"{fn} lost its garbage-time exclusion"


class TestRedZoneShape:
    def test_trips_are_play_derived_reached_the_20(self):
        body = PLAY_ANALYSIS.read_text()
        start = body.index("FUNCTION public.get_red_zone_splits")
        rz = body[start:]
        assert "yards_to_goal <= 20" in rz
        # Outcome attribution joins drives on the proven natural key.
        assert re.search(r"d\.game_id = t\.game_id AND d\.drive_number = t\.drive_number", rz)

    def test_drive_result_matching_is_casing_tolerant(self):
        body = PLAY_ANALYSIS.read_text()
        start = body.index("FUNCTION public.get_red_zone_splits")
        rz = body[start:]
        # Mirrors marts/006_scoring_opportunities.sql's sets: both the
        # uppercase (2025-era) and title-case (earlier seasons) labels.
        assert "'TD', 'Touchdown'" in rz
        assert "'FG', 'Field Goal'" in rz
