"""Tests for roster data source."""

from unittest.mock import MagicMock, patch


def test_rosters_resource_yields_players():
    """Roster endpoint should yield player records with team/year context."""
    from src.pipelines.sources.rosters import rosters_resource

    mock_response = [
        {"id": 12345, "first_name": "Jalen", "last_name": "Milroe", "position": "QB", "jersey": 4},
        {"id": 12346, "first_name": "Ryan", "last_name": "Williams", "position": "WR", "jersey": 2},
    ]

    with (
        patch("src.pipelines.sources.rosters.get_client") as mock_get_client,
        patch("src.pipelines.sources.rosters.make_request") as mock_make_request,
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = mock_response

        results = list(rosters_resource(teams=["Alabama"], years=[2024]))

        assert len(results) == 2
        assert results[0]["id"] == 12345
        assert results[0]["team"] == "Alabama"
        assert results[0]["year"] == 2024


def test_rosters_resource_iterates_teams_and_years():
    """Should call API for each team/year combination."""
    from src.pipelines.sources.rosters import rosters_resource

    with (
        patch("src.pipelines.sources.rosters.get_client") as mock_get_client,
        patch("src.pipelines.sources.rosters.make_request") as mock_make_request,
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = []

        list(rosters_resource(teams=["Alabama", "Georgia"], years=[2023, 2024]))

        # 2 teams x 2 years = 4 API calls
        assert mock_make_request.call_count == 4


def test_rosters_resource_handles_empty_response():
    """Should handle teams with no roster data gracefully."""
    from src.pipelines.sources.rosters import rosters_resource

    with (
        patch("src.pipelines.sources.rosters.get_client") as mock_get_client,
        patch("src.pipelines.sources.rosters.make_request") as mock_make_request,
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = []

        results = list(rosters_resource(teams=["Alabama"], years=[2024]))

        assert len(results) == 0


def test_rosters_source_returns_resource():
    """Rosters source should return the rosters resource."""
    from src.pipelines.sources.rosters import rosters_source

    with patch("src.pipelines.sources.rosters.get_client") as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        source = rosters_source(teams=["Alabama"], years=[2024])

        # Source should return a list containing the resource
        assert source is not None


class TestScheduledTeamResolution:
    """/roster is one request per team, so the team list IS the cost of a
    roster load. load_season's `--sources rosters` has no --teams to pass, so
    the list resolves from the season's schedule."""

    def test_teams_come_from_both_sides_of_the_schedule(self):
        """An FCS visitor plays exactly one FBS game; missing it would leave a
        hole in every roster-derived feature for that matchup."""
        from src.pipelines.run import scheduled_teams

        conn = FakeConn([("Alabama",), ("Chattanooga",), ("Oklahoma",)])
        assert scheduled_teams(conn, [2026]) == ["Alabama", "Chattanooga", "Oklahoma"]

    def test_null_team_names_are_dropped(self):
        """A NULL team would become /roster?team=None -- a wasted call."""
        from src.pipelines.run import scheduled_teams

        assert scheduled_teams(FakeConn([("Alabama",), (None,)]), [2026]) == ["Alabama"]

    def test_query_covers_home_and_away(self):
        from src.pipelines.run import _SCHEDULED_TEAMS_QUERY

        assert "home_team" in _SCHEDULED_TEAMS_QUERY
        assert "away_team" in _SCHEDULED_TEAMS_QUERY
        assert "UNION" in _SCHEDULED_TEAMS_QUERY.upper()

    def test_rosters_without_teams_or_years_is_an_error(self):
        """Refuse rather than guess: with no years there is no schedule to
        resolve against, and mode-derived years would silently load the wrong
        season."""
        import pytest

        from src.pipelines.run import run_rosters_pipeline

        with pytest.raises(ValueError, match="either an explicit team list or years"):
            run_rosters_pipeline(teams=None, years=None)


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a, **k):
        pass

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return FakeCursor(self._rows)
