"""Unit tests for scripts/build_pff_player_xwalk.py's pure matching core:
suffix/particle normalization on both sides, last-name + first-initial +
school matching, ambiguity handling (never guess), and transfer-observation
union. No DB, no network -- the DB flow is exercised only by running the
script for real against the warehouse.
"""

import scripts.build_pff_player_xwalk as xwalk


class TestNormalization:
    def test_plain_name(self):
        assert xwalk.pff_match_key("Caden Veltkamp", "Florida Atlantic") == (
            "Florida Atlantic",
            "C",
            "VELTKAMP",
        )

    def test_suffix_with_period_stripped(self):
        assert xwalk.pff_match_key("Cameron Fox Jr.", "Toledo")[2] == "FOX"

    def test_suffix_without_period_stripped(self):
        assert xwalk.pff_match_key("Tre Williams III", "Auburn")[2] == "WILLIAMS"

    def test_stacked_suffixes_stripped(self):
        assert xwalk.pff_match_key("John Smith Jr. II", "Ohio State")[2] == "SMITH"

    def test_multi_token_surname_matches_on_final_token(self):
        # PFF "Jordan Van Buren" vs CFBD last_name "Van Buren": both sides
        # reduce to the final surname token.
        assert xwalk.pff_match_key("Jordan Van Buren", "LSU")[2] == "BUREN"
        assert xwalk.roster_match_key("Jordan", "Van Buren", "LSU")[2] == "BUREN"

    def test_hyphenated_surname_kept_whole(self):
        assert xwalk.pff_match_key("Miller Del Rio-Wilson", "Syracuse")[2] == "RIO-WILSON"
        assert xwalk.roster_match_key("Miller", "Del Rio-Wilson", "Syracuse")[2] == "RIO-WILSON"

    def test_roster_last_name_carrying_suffix_stripped(self):
        # The systematic 2025 validation miss: CFBD last_name retains the
        # suffix ("Fox Jr.") while PFF appends it to the display name.
        assert xwalk.roster_match_key("Cameron", "Fox Jr.", "Toledo") == (
            "Toledo",
            "C",
            "FOX",
        )

    def test_initials_style_first_name(self):
        assert xwalk.pff_match_key("T.J. Lockley", "West Georgia") == (
            "West Georgia",
            "T",
            "LOCKLEY",
        )

    def test_suffix_only_token_is_never_stripped_to_nothing(self):
        assert xwalk.pff_match_key("Jr.", "Nowhere State")[2] == "JR"

    def test_empty_names_return_none(self):
        assert xwalk.pff_match_key("   ", "Toledo") is None
        assert xwalk.roster_match_key("", "Smith", "Toledo") is None
        assert xwalk.roster_match_key("A", "", "Toledo") is None


class TestMatchPlayers:
    ROSTER = [
        {"athlete_id": "1", "first_name": "Caden", "last_name": "Veltkamp", "team": "FAU-full"},
        {"athlete_id": "2", "first_name": "Cameron", "last_name": "Fox Jr.", "team": "Toledo"},
        {"athlete_id": "3", "first_name": "Chris", "last_name": "Smith", "team": "Duke"},
        {"athlete_id": "4", "first_name": "Carl", "last_name": "Smith", "team": "Duke"},
    ]

    def test_unique_match(self):
        result = xwalk.match_players(
            [{"pff_player_id": 10, "player": "Caden Veltkamp", "school": "FAU-full"}],
            self.ROSTER,
        )
        assert result.matches == [
            {
                "pff_player_id": 10,
                "athlete_id": "1",
                "match_method": xwalk.MATCH_METHOD,
            }
        ]
        assert result.ambiguous == [] and result.unmatched == []

    def test_suffix_mismatch_still_matches(self):
        result = xwalk.match_players(
            [{"pff_player_id": 11, "player": "Cameron Fox Jr.", "school": "Toledo"}],
            self.ROSTER,
        )
        assert [m["athlete_id"] for m in result.matches] == ["2"]

    def test_two_candidates_is_ambiguous_never_guessed(self):
        # Chris Smith and Carl Smith, both Duke: same (school, C, SMITH) key.
        result = xwalk.match_players(
            [{"pff_player_id": 12, "player": "Chris Smith", "school": "Duke"}],
            self.ROSTER,
        )
        assert result.matches == []
        assert len(result.ambiguous) == 1
        assert result.ambiguous[0].candidates == ["3", "4"]

    def test_no_candidate_is_unmatched(self):
        result = xwalk.match_players(
            [{"pff_player_id": 13, "player": "Ghost Player", "school": "Toledo"}],
            self.ROSTER,
        )
        assert result.matches == [] and result.ambiguous == []
        assert result.unmatched[0].pff_player_id == 13

    def test_transfer_observations_union_to_one_athlete(self):
        # Same PFF id seen at two schools (transfer); the roster spans
        # seasons so the same athlete appears at both -> one candidate.
        roster = self.ROSTER + [
            {"athlete_id": "1", "first_name": "Caden", "last_name": "Veltkamp", "team": "WKU"},
        ]
        result = xwalk.match_players(
            [
                {"pff_player_id": 10, "player": "Caden Veltkamp", "school": "WKU"},
                {"pff_player_id": 10, "player": "Caden Veltkamp", "school": "FAU-full"},
            ],
            roster,
        )
        assert [m["athlete_id"] for m in result.matches] == ["1"]

    def test_transfer_observations_with_conflicting_athletes_are_ambiguous(self):
        roster = self.ROSTER + [
            {"athlete_id": "9", "first_name": "Caden", "last_name": "Veltkamp", "team": "WKU"},
        ]
        result = xwalk.match_players(
            [
                {"pff_player_id": 10, "player": "Caden Veltkamp", "school": "WKU"},
                {"pff_player_id": 10, "player": "Caden Veltkamp", "school": "FAU-full"},
            ],
            roster,
        )
        assert result.matches == []
        assert result.ambiguous[0].candidates == ["1", "9"]

    def test_results_are_deterministically_ordered(self):
        players = [
            {"pff_player_id": 20, "player": "Ghost B", "school": "Toledo"},
            {"pff_player_id": 13, "player": "Ghost A", "school": "Toledo"},
        ]
        result = xwalk.match_players(players, self.ROSTER)
        assert [u.pff_player_id for u in result.unmatched] == [13, 20]
