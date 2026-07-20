"""Tests for betting lines and line-snapshot data sources."""

from unittest.mock import MagicMock, patch

from src.pipelines.sources.betting import betting_source


def test_betting_source_returns_all_resources():
    """betting_source should expose lines, team_ats, and line_snapshots."""
    source = betting_source(years=[2024])

    assert set(source.resources.keys()) == {"lines", "team_ats", "line_snapshots"}


def test_line_snapshots_resource_yields_only_pending_games():
    """Completed games (non-null home/away score) must not be snapshotted."""
    from src.pipelines.sources.betting import line_snapshots_resource

    mock_response = [
        {
            "id": 1,
            "season": 2024,
            "week": 1,
            "homeTeam": "Alabama",
            "awayTeam": "Georgia",
            "homeScore": 24,
            "awayScore": 17,
            "lines": [
                {
                    "provider": "consensus",
                    "spread": -3.5,
                    "formattedSpread": "Alabama -3.5",
                    "overUnder": 55.5,
                    "homeMoneyline": -160,
                    "awayMoneyline": 140,
                }
            ],
        },
        {
            "id": 2,
            "season": 2024,
            "week": 2,
            "homeTeam": "Ohio State",
            "awayTeam": "Michigan",
            "homeScore": None,
            "awayScore": None,
            "lines": [
                {
                    "provider": "consensus",
                    "spread": -6.5,
                    "formattedSpread": "Ohio State -6.5",
                    "overUnder": 48.5,
                    "homeMoneyline": -250,
                    "awayMoneyline": 210,
                }
            ],
        },
        {
            "id": 3,
            "season": 2024,
            "week": 2,
            "homeTeam": "Texas",
            "awayTeam": "Oklahoma",
            "homeScore": None,
            "awayScore": 10,  # partially scored -- treated as not pending
            "lines": [
                {
                    "provider": "consensus",
                    "spread": -2.5,
                    "formattedSpread": "Texas -2.5",
                    "overUnder": 50.5,
                    "homeMoneyline": -130,
                    "awayMoneyline": 110,
                }
            ],
        },
    ]

    with (
        patch("src.pipelines.sources.betting.get_client") as mock_get_client,
        patch("src.pipelines.sources.betting.make_request") as mock_make_request,
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = mock_response

        results = list(line_snapshots_resource(years=[2024]))

        assert len(results) == 1
        assert results[0]["game_id"] == 2
        assert results[0]["home_team"] == "Ohio State"


def test_line_snapshots_resource_stamps_single_captured_at_and_hex_hash():
    """Every row in a run shares captured_at; line_hash is 32-char hex."""
    from src.pipelines.sources.betting import line_snapshots_resource

    mock_response = [
        {
            "id": 10,
            "season": 2024,
            "week": 3,
            "homeTeam": "Oregon",
            "awayTeam": "Washington",
            "homeScore": None,
            "awayScore": None,
            "lines": [
                {
                    "provider": "consensus",
                    "spread": -7.0,
                    "formattedSpread": "Oregon -7.0",
                    "overUnder": 52.0,
                    "homeMoneyline": -300,
                    "awayMoneyline": 250,
                },
                {
                    "provider": "DraftKings",
                    "spread": -6.5,
                    "formattedSpread": "Oregon -6.5",
                    "overUnder": 51.5,
                    "homeMoneyline": -280,
                    "awayMoneyline": 230,
                },
            ],
        },
        {
            "id": 11,
            "season": 2024,
            "week": 3,
            "homeTeam": "USC",
            "awayTeam": "UCLA",
            "homeScore": None,
            "awayScore": None,
            "lines": [
                {
                    "provider": "consensus",
                    "spread": -4.0,
                    "formattedSpread": "USC -4.0",
                    "overUnder": 58.0,
                    "homeMoneyline": -180,
                    "awayMoneyline": 150,
                }
            ],
        },
    ]

    with (
        patch("src.pipelines.sources.betting.get_client") as mock_get_client,
        patch("src.pipelines.sources.betting.make_request") as mock_make_request,
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_make_request.return_value = mock_response

        results = list(line_snapshots_resource(years=[2024]))

        assert len(results) == 3

        captured_ats = {row["captured_at"] for row in results}
        assert len(captured_ats) == 1  # identical stamp across every row

        for row in results:
            assert isinstance(row["line_hash"], str)
            assert len(row["line_hash"]) == 32
            int(row["line_hash"], 16)  # valid hex


def test_line_snapshots_resource_hash_differs_on_spread_and_normalizes_none():
    """Two lines differing only in spread hash differently; None hashes stably."""
    from src.pipelines.sources.betting import line_snapshots_resource

    def make_game(game_id, spread):
        return {
            "id": game_id,
            "season": 2024,
            "week": 4,
            "homeTeam": "Clemson",
            "awayTeam": "Florida State",
            "homeScore": None,
            "awayScore": None,
            "lines": [
                {
                    "provider": "consensus",
                    "spread": spread,
                    "formattedSpread": None,
                    "overUnder": None,
                    "homeMoneyline": None,
                    "awayMoneyline": None,
                }
            ],
        }

    with (
        patch("src.pipelines.sources.betting.get_client") as mock_get_client,
        patch("src.pipelines.sources.betting.make_request") as mock_make_request,
    ):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Run 1: two games, differing only in spread.
        mock_make_request.return_value = [make_game(20, -3.0), make_game(21, -3.5)]
        results = list(line_snapshots_resource(years=[2024]))

        assert len(results) == 2
        assert results[0]["line_hash"] != results[1]["line_hash"]

        # Run 2: same None-heavy payload -- hash for game 20 must be stable
        # across runs and independent of captured_at.
        mock_make_request.return_value = [make_game(20, -3.0)]
        results_again = list(line_snapshots_resource(years=[2024]))

        assert results_again[0]["line_hash"] == results[0]["line_hash"]
