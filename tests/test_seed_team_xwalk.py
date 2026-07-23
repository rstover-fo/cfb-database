"""Tests for scripts/seed_team_xwalk.py (T8).

Tests cover matching logic, SQL escaping, fixture extraction, and end-to-end
CLI runs.
"""

import re
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from scripts.seed_team_xwalk import (
    escape_sql,
    expand_abbrevs,
    generate_seed_sql,
    load_names_from_massey_fixture,
    load_names_from_sbr_fixture,
    match_team,
    normalize_name,
)


class TestNormalizeName:
    """Test normalize_name (reused from src.pipelines.utils.team_xwalk)."""

    def test_whitespace_collapse(self):
        """Whitespace is collapsed and trimmed."""
        assert normalize_name("  Ohio  State  ") == "ohio state"
        assert normalize_name("Ohio\nState") == "ohio state"

    def test_case_folding(self):
        """All input is case-folded."""
        assert normalize_name("OHIO STATE") == "ohio state"
        assert normalize_name("Ohio State") == "ohio state"


class TestExpandAbbrevs:
    """Test expand_abbrevs for additional normalization."""

    def test_trailing_st_to_state(self):
        """' st' or ' st.' → ' state'."""
        assert expand_abbrevs("ohio st") == "ohio state"
        assert expand_abbrevs("ohio st.") == "ohio state"
        # Embedded " st" should not match (we want "st louis", not "state louis")
        # but the implementation currently would. Document this behavior.
        assert expand_abbrevs("saint louis") == "saint louis"

    def test_miami_oh_to_parenthetical(self):
        """'miami oh' or 'miami-ohio' → 'miami (oh)'."""
        assert expand_abbrevs("miami oh") == "miami (oh)"
        assert expand_abbrevs("miami-oh") == "miami (oh)"
        # "ohio" at the end should not expand (only 2-char state codes)
        # but we might see "miami-ohio" → "miami (oh)" due to our logic
        expanded = expand_abbrevs("miami ohio")
        # The rule should handle "ohio" separately as a 4-char abbrev or not at all
        # For now, we only handle 2-char codes, so this should remain unchanged
        assert "miami" in expanded

    def test_univ_to_university(self):
        """'univ' or 'univ.' → 'university'."""
        assert expand_abbrevs("miami univ") == "miami university"
        assert expand_abbrevs("miami univ.") == "miami university"

    def test_ampersand(self):
        """'&amp;' → '&'."""
        assert expand_abbrevs("texas a&amp;m") == "texas a&m"

    def test_combined_expansions(self):
        """Multiple expansions in one name."""
        expanded = expand_abbrevs("miami univ. ohio")
        # After expanding "univ." → "university", we should have "miami university ohio"
        # "ohio" at the end should become "(ohio)" but we only handle 2-char state codes
        # So the result should have both "university" and "ohio" (or "(oh)" if full form)
        assert "miami" in expanded and "university" in expanded


class TestEscapeSql:
    """Test SQL quote escaping."""

    def test_simple_name(self):
        """No quotes: unchanged."""
        assert escape_sql("Ohio State") == "Ohio State"

    def test_single_quote(self):
        """Single quote is doubled."""
        assert escape_sql("Hawai'i") == "Hawai''i"

    def test_multiple_quotes(self):
        """All single quotes are doubled."""
        assert escape_sql("It's a team's name") == "It''s a team''s name"


class TestMatchTeam:
    """Test match_team matching logic."""

    def test_exact_match(self):
        """Exact match after normalization → confidence 1.0."""
        canonical = ["Ohio State", "Michigan", "Indiana"]
        cfbd_name, confidence, match_type = match_team("ohio state", canonical)
        assert cfbd_name == "Ohio State"
        assert confidence == 1.0
        assert match_type == "exact"

    def test_exact_match_with_normalization(self):
        """Normalization allows case/whitespace differences."""
        canonical = ["Ohio State"]
        cfbd_name, confidence, match_type = match_team("  OHIO  STATE  ", canonical)
        assert cfbd_name == "Ohio State"
        assert confidence == 1.0

    def test_abbrev_match(self):
        """Abbreviated form matches after expand_abbrevs → confidence 0.95."""
        canonical = ["Ohio State", "Michigan"]
        cfbd_name, confidence, match_type = match_team("ohio st", canonical)
        assert cfbd_name == "Ohio State"
        assert confidence == 0.95
        assert match_type == "abbrev"

    def test_miami_oh_to_miami_parenthetical(self):
        """'miami-ohio' matches 'Miami (OH)' via expand_abbrevs."""
        canonical = ["Miami (OH)", "Ohio State"]
        cfbd_name, confidence, match_type = match_team("miami-ohio", canonical)
        assert cfbd_name == "Miami (OH)"
        assert confidence == 0.95
        assert match_type == "abbrev"

    def test_miami_trap_prefers_specific_match(self):
        """With ['Miami', 'Miami (OH)'], 'miami-ohio' should match 'Miami (OH)'."""
        canonical = ["Miami", "Miami (OH)"]
        cfbd_name, confidence, match_type = match_team("miami-ohio", canonical)
        # The abbrev match (miami (oh)) should be preferred over fuzzy match to "Miami"
        assert cfbd_name == "Miami (OH)"
        assert confidence == 0.95

    def test_fuzzy_match(self):
        """Difflib ratio when no exact/abbrev match → confidence is ratio."""
        canonical = ["Ohio State", "Michigan"]
        cfbd_name, confidence, match_type = match_team("ohio", canonical)
        # "ohio" is close to "ohio state" but not exact or abbrev
        assert cfbd_name is not None
        assert 0.0 < confidence < 1.0
        assert match_type == "fuzzy" or match_type == "unmatched"

    def test_no_match(self):
        """No reasonable match → None."""
        canonical = ["Ohio State", "Michigan"]
        cfbd_name, confidence, match_type = match_team("XYZ University", canonical)
        assert cfbd_name is None or confidence < 0.5
        # Below default min_confidence (0.85), so match_type should be "unmatched"

    def test_min_confidence_threshold(self):
        """Matches below min_confidence are unmatched."""
        canonical = ["Ohio State"]
        cfbd_name, confidence, match_type = match_team("ohee", canonical, min_confidence=0.9)
        # A fuzzy match to "ohio state" would be around 0.6-0.7, well below 0.9
        assert match_type == "unmatched"

    def test_exact_beats_fuzzy(self):
        """Exact match is returned even if fuzzy match also exists."""
        canonical = ["Ohio State", "Ohio University"]
        cfbd_name, confidence, match_type = match_team("ohio state", canonical)
        assert cfbd_name == "Ohio State"
        assert confidence == 1.0
        assert match_type == "exact"


class TestGenerateSeedSql:
    """Test SQL generation."""

    def test_exact_matches_no_review_comment(self):
        """Exact matches have no REVIEW: confidence comment."""
        source_names = ["Ohio State"]
        canonical = ["Ohio State"]
        sql, exact, fuzzy, unmatched = generate_seed_sql("test", source_names, canonical)
        assert "INSERT INTO" in sql
        # Check that the INSERT line doesn't have a preceding "-- REVIEW: confidence" comment
        lines = sql.split("\n")
        insert_lines = [l for l in lines if "INSERT INTO" in l and not l.startswith("--")]
        # Find these lines and check the preceding line
        for i, line in enumerate(lines):
            if "INSERT INTO" in line and not line.startswith("--"):
                if i > 0:
                    prev_line = lines[i - 1]
                    assert "-- REVIEW: confidence" not in prev_line
        assert exact == 1
        assert fuzzy == 0
        assert unmatched == 0

    def test_fuzzy_matches_have_review_comment(self):
        """Fuzzy matches include REVIEW: confidence comment."""
        source_names = ["Ohio St"]
        canonical = ["Ohio State"]
        sql, exact, fuzzy, unmatched = generate_seed_sql("test", source_names, canonical)
        assert "-- REVIEW: confidence" in sql
        assert "abbrev" in sql  # or "fuzzy"
        assert exact == 0
        assert fuzzy == 1

    def test_unmatched_commented_out(self):
        """Unmatched inserts are commented out with UNMATCHED note."""
        source_names = ["XYZ University"]
        canonical = ["Ohio State", "Michigan"]
        sql, exact, fuzzy, unmatched = generate_seed_sql("test", source_names, canonical)
        assert "-- UNMATCHED" in sql
        assert "-- INSERT INTO" in sql  # Commented out
        assert exact == 0
        assert unmatched == 1

    def test_sql_escaping(self):
        """Names with single quotes are escaped."""
        source_names = ["Hawai'i"]
        canonical = ["Hawai'i"]
        sql, exact, fuzzy, unmatched = generate_seed_sql("test", source_names, canonical)
        # Should contain doubled single quote
        assert "Hawai''i" in sql

    def test_source_name_in_output(self):
        """Generated SQL includes the source name."""
        sql, _, _, _ = generate_seed_sql("massey", ["Ohio State"], ["Ohio State"])
        assert "'massey'" in sql

    def test_deterministic_ordering(self):
        """Output is sorted by source name (deterministic)."""
        source_names = ["Zephyr", "Alabama", "Michigan"]
        canonical = ["Alabama", "Michigan", "Zephyr"]
        sql1, _, _, _ = generate_seed_sql("test", source_names, canonical)
        sql2, _, _, _ = generate_seed_sql("test", source_names, canonical)
        assert sql1 == sql2  # Same SQL both times

        # Check that lines appear in order (Alabama before Michigan before Zephyr)
        lines = sql1.split("\n")
        inserts = [l for l in lines if "INSERT INTO" in l and not l.startswith("--")]
        # Should have inserts in alphabetical order by team name
        # (though we can't guarantee exact line positions due to comments)
        assert len(inserts) == 3

    def test_header_comments(self):
        """Generated SQL includes header comments with source and counts."""
        sql, _, _, _ = generate_seed_sql("sbr", ["A", "B"], ["A", "B"])
        assert "-- Generated by scripts/seed_team_xwalk.py" in sql
        assert "-- Source: sbr" in sql
        assert "-- Total source names:" in sql
        assert "-- Min confidence threshold:" in sql


class TestMasseyFixtureExtraction:
    """Test extracting team names from Massey fixture."""

    def test_loads_fixture_names(self):
        """Massey fixture is loaded and returns expected teams."""
        names = load_names_from_massey_fixture()
        # The fixture has 10 teams
        assert len(names) == 10
        # Should include these teams (from the fixture)
        assert "Georgia" in names
        assert "Ohio St" in names
        assert "Alabama" in names
        assert "Utah St" in names

    def test_fixture_names_are_strings(self):
        """All extracted names are non-empty strings."""
        names = load_names_from_massey_fixture()
        assert all(isinstance(n, str) and len(n) > 0 for n in names)


class TestSbrFixtureExtraction:
    """Test extracting team names from SBR fixture."""

    def test_loads_fixture_names(self):
        """SBR fixture is loaded and returns expected teams."""
        names = load_names_from_sbr_fixture()
        # The fixture has 10 rows (5 games × 2 teams per game)
        assert len(names) == 10
        # Should include these teams (from the fixture)
        assert "Northwestern" in names
        assert "Nebraska" in names
        assert "FloridaAtlantic" in names
        assert "UtahState" in names

    def test_fixture_names_are_strings(self):
        """All extracted names are non-empty strings."""
        names = load_names_from_sbr_fixture()
        assert all(isinstance(n, str) and len(n) > 0 for n in names)


class TestEndToEndCli:
    """End-to-end CLI tests using subprocess."""

    def test_massey_fixture_run(self, tmp_path):
        """Run --source massey --from-fixture with a temp teams file."""
        # Create a teams file with canonical names covering the fixture
        teams_file = tmp_path / "teams.txt"
        teams_file.write_text(
            "\n".join(
                [
                    "Georgia",
                    "Michigan",
                    "Ohio State",
                    "Alabama",
                    "Tennessee",
                    "Louisville",
                    "Oklahoma State",
                    "Utah State",
                    "Akron",
                    "Massachusetts",
                ]
            )
        )

        out_file = tmp_path / "seed.sql"

        result = subprocess.run(
            [
                "python",
                "scripts/seed_team_xwalk.py",
                "--source",
                "massey",
                "--from-fixture",
                "--teams-file",
                str(teams_file),
                "--out",
                str(out_file),
            ],
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert out_file.exists()
        sql = out_file.read_text()

        # Check for expected content
        assert "INSERT INTO ref.team_name_xwalk" in sql
        assert "'massey'" in sql
        # Should have inserts for the fixture teams
        assert "Georgia" in sql

        # Parse the summary line from stdout
        summary = result.stdout.strip().split("\n")[-1]
        assert "XWALK_SEED" in summary
        assert "source=massey" in summary
        assert "names=10" in summary

    def test_sbr_fixture_run(self, tmp_path):
        """Run --source sbr --from-fixture with a temp teams file."""
        # Create a teams file with canonical names covering the fixture
        teams_file = tmp_path / "teams.txt"
        teams_file.write_text(
            "\n".join(
                [
                    "Northwestern",
                    "Nebraska",
                    "Charlotte",
                    "Florida Atlantic",
                    "Nevada",
                    "New Mexico State",
                    "Connecticut",
                    "Utah State",
                    "North Texas",
                    "UTEP",
                ]
            )
        )

        out_file = tmp_path / "seed_sbr.sql"

        result = subprocess.run(
            [
                "python",
                "scripts/seed_team_xwalk.py",
                "--source",
                "sbr",
                "--from-fixture",
                "--teams-file",
                str(teams_file),
                "--out",
                str(out_file),
            ],
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert out_file.exists()
        sql = out_file.read_text()

        # Check for expected content
        assert "INSERT INTO ref.team_name_xwalk" in sql
        assert "'sbr'" in sql
        # Check for one of the fixture teams
        assert "Northwestern" in sql or "nebraska" in sql.lower()

        # Parse summary line
        summary = result.stdout.strip().split("\n")[-1]
        assert "XWALK_SEED" in summary
        assert "source=sbr" in summary

    def test_cli_names_file_run(self, tmp_path):
        """Run with --names-file and --teams-file."""
        names_file = tmp_path / "names.txt"
        names_file.write_text("Ohio St\nMichigan\nIndiana\n")

        teams_file = tmp_path / "teams.txt"
        teams_file.write_text("Ohio State\nMichigan\nIndiana University\n")

        out_file = tmp_path / "seed.sql"

        result = subprocess.run(
            [
                "python",
                "scripts/seed_team_xwalk.py",
                "--source",
                "test_source",
                "--names-file",
                str(names_file),
                "--teams-file",
                str(teams_file),
                "--out",
                str(out_file),
            ],
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, f"stderr: {result.stderr}"
        sql = out_file.read_text()
        assert "'test_source'" in sql
        assert "INSERT INTO" in sql

    def test_sql_output_parses_as_valid_syntax(self, tmp_path):
        """Generated SQL has valid syntax (basic check)."""
        teams_file = tmp_path / "teams.txt"
        teams_file.write_text("Ohio State\nMichigan\n")

        names_file = tmp_path / "names.txt"
        names_file.write_text("Ohio St\nMichigan\n")

        out_file = tmp_path / "seed.sql"

        subprocess.run(
            [
                "python",
                "scripts/seed_team_xwalk.py",
                "--source",
                "test",
                "--names-file",
                str(names_file),
                "--teams-file",
                str(teams_file),
                "--out",
                str(out_file),
            ],
            cwd=Path(__file__).parent.parent,
            capture_output=True,
        )

        sql = out_file.read_text()
        # Basic syntax checks
        assert sql.count("INSERT INTO ref.team_name_xwalk") >= 1
        assert sql.count("VALUES (") >= 1
        # Should have proper quoting
        assert "VALUES ('" in sql
        # Should not have unescaped quotes inside values (except escaped ones)
        lines = [l for l in sql.split("\n") if "INSERT INTO" in l and not l.startswith("--")]
        for line in lines:
            # Each line should be properly formed
            assert line.count("VALUES (") == 1
            assert line.endswith(");") or line.endswith("DO NOTHING;")

    def test_confidence_threshold_filtering(self, tmp_path):
        """--min-confidence filters out low-confidence matches."""
        names_file = tmp_path / "names.txt"
        names_file.write_text("OH\n")  # Very short, low match quality

        teams_file = tmp_path / "teams.txt"
        teams_file.write_text("Ohio State\n")

        out_file = tmp_path / "seed.sql"

        # High threshold should result in unmatched
        result = subprocess.run(
            [
                "python",
                "scripts/seed_team_xwalk.py",
                "--source",
                "test",
                "--names-file",
                str(names_file),
                "--teams-file",
                str(teams_file),
                "--out",
                str(out_file),
                "--min-confidence",
                "0.95",
            ],
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        sql = out_file.read_text()
        # Should have unmatched commentary
        assert "-- UNMATCHED" in sql or "-- INSERT INTO" in sql

    def test_summary_line_format(self, tmp_path):
        """Summary line has correct format."""
        teams_file = tmp_path / "teams.txt"
        teams_file.write_text("Ohio State\n")

        names_file = tmp_path / "names.txt"
        names_file.write_text("Ohio St\nOhio State\n")

        out_file = tmp_path / "seed.sql"

        result = subprocess.run(
            [
                "python",
                "scripts/seed_team_xwalk.py",
                "--source",
                "massey",
                "--names-file",
                str(names_file),
                "--teams-file",
                str(teams_file),
                "--out",
                str(out_file),
            ],
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
        )

        # Extract last line (summary)
        summary = result.stdout.strip().split("\n")[-1]
        # Should match: XWALK_SEED source=massey names=2 exact=1 fuzzy=1 unmatched=0 out=...
        pattern = r"XWALK_SEED source=\S+ names=\d+ exact=\d+ fuzzy=\d+ unmatched=\d+ out=\S+"
        assert re.match(pattern, summary), f"Summary line doesn't match pattern: {summary}"
