"""Unit tests for scripts/build_pff_player_xwalk.py's pure matching core:
suffix/particle normalization on both sides, last-name + first-initial +
school matching, season-scoped candidate tiers, the first-name tiebreak,
ambiguity handling (never guess), and transfer-observation union. No DB,
no network -- the DB flow is exercised only by running the
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


def _r(athlete_id, first, last, team, year=2024):
    return {
        "athlete_id": athlete_id,
        "first_name": first,
        "last_name": last,
        "team": team,
        "year": year,
    }


def _p(pff_id, player, school, season=2024):
    return {"pff_player_id": pff_id, "player": player, "school": school, "season": season}


class TestMatchPlayers:
    ROSTER = [
        _r("1", "Caden", "Veltkamp", "FAU-full"),
        _r("2", "Cameron", "Fox Jr.", "Toledo"),
        _r("3", "Chris", "Smith", "Duke"),
        _r("4", "Carl", "Smith", "Duke"),
    ]

    def test_unique_match(self):
        result = xwalk.match_players([_p(10, "Caden Veltkamp", "FAU-full")], self.ROSTER)
        assert result.matches == [
            {
                "pff_player_id": 10,
                "athlete_id": "1",
                "match_method": f"{xwalk.MATCH_METHOD}+season",
            }
        ]
        assert result.ambiguous == [] and result.unmatched == []

    def test_suffix_mismatch_still_matches(self):
        result = xwalk.match_players([_p(11, "Cameron Fox Jr.", "Toledo")], self.ROSTER)
        assert [m["athlete_id"] for m in result.matches] == ["2"]

    def test_two_candidates_narrow_on_first_name(self):
        # Chris Smith and Carl Smith, both Duke 2024: same (school, C, SMITH)
        # key, but the full first-name token is unique.
        result = xwalk.match_players([_p(12, "Chris Smith", "Duke")], self.ROSTER)
        assert [m["athlete_id"] for m in result.matches] == ["3"]
        assert result.matches[0]["match_method"] == f"{xwalk.MATCH_METHOD}+season+first_name"

    def test_same_first_name_stays_ambiguous_never_guessed(self):
        roster = self.ROSTER + [_r("5", "Chris", "Smith", "Duke")]
        result = xwalk.match_players([_p(12, "Chris Smith", "Duke")], roster)
        assert result.matches == []
        assert len(result.ambiguous) == 1
        assert result.ambiguous[0].candidates == ["3", "5"]

    def test_nickname_first_name_keeps_all_candidates_ambiguous(self):
        # PFF "Trey" vs legal first names: narrowing to zero survivors must
        # not silently pick anyone.
        result = xwalk.match_players([_p(12, "C.J. Smith", "Duke")], self.ROSTER)
        assert result.matches == []
        assert result.ambiguous[0].candidates == ["3", "4"]

    def test_no_candidate_is_unmatched(self):
        result = xwalk.match_players([_p(13, "Ghost Player", "Toledo")], self.ROSTER)
        assert result.matches == [] and result.ambiguous == []
        assert result.unmatched[0].pff_player_id == 13

    def test_transfer_observations_union_to_one_athlete(self):
        # Same PFF id seen at two schools (transfer); the roster spans
        # seasons so the same athlete appears at both -> one candidate.
        roster = self.ROSTER + [_r("1", "Caden", "Veltkamp", "WKU", 2025)]
        result = xwalk.match_players(
            [_p(10, "Caden Veltkamp", "WKU", 2025), _p(10, "Caden Veltkamp", "FAU-full", 2024)],
            roster,
        )
        assert [m["athlete_id"] for m in result.matches] == ["1"]

    def test_transfer_observations_with_conflicting_athletes_are_ambiguous(self):
        roster = self.ROSTER + [_r("9", "Caden", "Veltkamp", "WKU")]
        result = xwalk.match_players(
            [_p(10, "Caden Veltkamp", "WKU"), _p(10, "Caden Veltkamp", "FAU-full")],
            roster,
        )
        assert result.matches == []
        assert result.ambiguous[0].candidates == ["1", "9"]

    def test_results_are_deterministically_ordered(self):
        players = [_p(20, "Ghost B", "Toledo"), _p(13, "Ghost A", "Toledo")]
        result = xwalk.match_players(players, self.ROSTER)
        assert [u.pff_player_id for u in result.unmatched] == [13, 20]


class TestSeasonTiers:
    """The 2026-09-04 finding: matching against every roster season since
    2004 made 'J. Smith @ Kentucky' collide with two decades of J. Smiths.
    Candidates are scoped to the PFF season first and only widen when the
    tighter tier finds nobody."""

    ROSTER = [
        _r("old", "Jaden", "Smith", "Kentucky", 2009),
        _r("new", "Jaden", "Smith", "Kentucky", 2024),
    ]

    def test_same_season_candidate_beats_a_decade_old_namesake(self):
        result = xwalk.match_players([_p(1, "Jaden Smith", "Kentucky", 2024)], self.ROSTER)
        assert [m["athlete_id"] for m in result.matches] == ["new"]
        assert result.matches[0]["match_method"].endswith("+season")

    def test_adjacent_season_fills_a_roster_gap(self):
        # Rostered 2024 only, graded by PFF in 2023 (roster gap): +-1 tier.
        result = xwalk.match_players([_p(1, "Jaden Smith", "Kentucky", 2023)], self.ROSTER)
        assert [m["athlete_id"] for m in result.matches] == ["new"]
        assert result.matches[0]["match_method"].endswith("+season+-1")

    def test_any_season_is_the_last_resort(self):
        roster = [_r("only", "Jaden", "Smith", "Kentucky", 2015)]
        result = xwalk.match_players([_p(1, "Jaden Smith", "Kentucky", 2024)], roster)
        assert [m["athlete_id"] for m in result.matches] == ["only"]
        assert result.matches[0]["match_method"].endswith("+any_season")

    def test_ambiguity_at_a_tight_tier_does_not_widen(self):
        # Two same-season candidates with the same first name: stay
        # ambiguous rather than pulling in more from wider tiers.
        roster = self.ROSTER + [_r("new2", "Jaden", "Smith", "Kentucky", 2024)]
        result = xwalk.match_players([_p(1, "Jaden Smith", "Kentucky", 2024)], roster)
        assert result.matches == []
        assert result.ambiguous[0].candidates == ["new", "new2"]

    def test_each_observation_resolves_its_own_tier_before_the_union(self):
        # Codex/Greptile finding on PR #115: A/2024 hits athlete X at the
        # exact season; B/2023 finds athlete Y only via the +-1 tier. The
        # first observation's hit must not stop the second from looking, or
        # X is written while a conflicting identity (Y) goes unreported.
        roster = [
            _r("X", "Jaden", "Smith", "A", 2024),
            _r("Y", "Jaden", "Smith", "B", 2022),
        ]
        result = xwalk.match_players(
            [_p(1, "Jaden Smith", "A", 2024), _p(1, "Jaden Smith", "B", 2023)], roster
        )
        assert result.matches == []
        assert result.ambiguous[0].candidates == ["X", "Y"]

    def test_transfer_with_one_gapped_roster_still_matches_when_same_athlete(self):
        roster = [
            _r("X", "Jaden", "Smith", "A", 2024),
            _r("X", "Jaden", "Smith", "B", 2022),
        ]
        result = xwalk.match_players(
            [_p(1, "Jaden Smith", "A", 2024), _p(1, "Jaden Smith", "B", 2023)], roster
        )
        assert [m["athlete_id"] for m in result.matches] == ["X"]
        # The widest tier any observation needed is what gets recorded.
        assert result.matches[0]["match_method"].endswith("+season+-1")

    def test_multi_season_observations_scope_each_to_its_own_year(self):
        roster = [
            _r("a", "Jaden", "Smith", "Kentucky", 2023),
            _r("a", "Jaden", "Smith", "Kentucky", 2024),
        ]
        result = xwalk.match_players(
            [_p(1, "Jaden Smith", "Kentucky", 2023), _p(1, "Jaden Smith", "Kentucky", 2024)],
            roster,
        )
        assert [m["athlete_id"] for m in result.matches] == ["a"]
        assert len(result.matches[0]) == 3
