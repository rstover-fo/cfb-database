"""Static drift-guards for the penalty analytics layer (marts 041/042).

marts.penalty_log parses CFBD free-text play_text -- the parsing is
best-effort by design, and these tests pin the SHAPE that makes that
honest: the ordered infraction pattern list, the parse_ok escape hatch,
and the box mart's official-source pivot. Behavioral correctness is proven
in prod by src/schemas/api/validation_penalties.sql.
"""

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PENALTY_LOG = PROJECT_ROOT / "src" / "schemas" / "marts" / "041_penalty_log.sql"
PENALTY_BOX = PROJECT_ROOT / "src" / "schemas" / "marts" / "042_team_penalty_box.sql"


def _strip_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


class TestPenaltyLogShape:
    def test_infraction_mapping_is_ordered_values_cte(self):
        sql = _strip_comments(PENALTY_LOG.read_text())
        assert re.search(r"infractions\s*\(priority, label, pattern\)", sql)
        # First-match-wins semantics depend on the priority ordering.
        assert re.search(r"ORDER BY i\.priority\s+LIMIT 1", sql)

    def test_core_infractions_present(self):
        sql = PENALTY_LOG.read_text()
        for label in ("Holding", "False Start", "Pass Interference", "Offside", "Targeting"):
            assert f"'{label}'" in sql, f"infraction mapping lost {label!r}"

    def test_parse_ok_and_unknown_fallback(self):
        sql = _strip_comments(PENALTY_LOG.read_text())
        assert "parse_ok" in sql
        assert "'Unknown'" in sql, "unmatched infractions must classify as 'Unknown', not drop"

    def test_grain_key_includes_season(self):
        """Play ids are not guaranteed unique across seasons; the unique
        index (= REFRESH CONCURRENTLY key) must qualify by season."""
        sql = _strip_comments(PENALTY_LOG.read_text())
        assert re.search(r"CREATE UNIQUE INDEX ON marts\.penalty_log \(season, play_id\)", sql)


class TestPenaltyBoxShape:
    def test_pivots_official_category(self):
        sql = _strip_comments(PENALTY_BOX.read_text())
        assert "'totalPenaltiesYards'" in sql
        # Count-yards pair splits on '-'; both halves NULLIF-guarded.
        assert sql.count("split_part(pen_raw, '-'") == 2

    def test_unique_grain(self):
        sql = _strip_comments(PENALTY_BOX.read_text())
        assert re.search(r"CREATE UNIQUE INDEX ON marts\.team_penalty_box \(game_id, team\)", sql)
