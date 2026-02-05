"""Tests for endpoint configuration validation.

Regression tests to prevent the PK bugs fixed in Sprint 1.
"""

import pytest

from src.pipelines.config.endpoints import (
    ALL_ENDPOINTS,
    BETTING_ENDPOINTS,
    CORE_ENDPOINTS,
    DRAFT_ENDPOINTS,
    METRICS_ENDPOINTS,
    RANKINGS_ENDPOINTS,
    RATINGS_ENDPOINTS,
    RECRUITING_ENDPOINTS,
    REFERENCE_ENDPOINTS,
    STATS_ENDPOINTS,
)


class TestEndpointConfigStructure:
    """Every endpoint config must have valid required fields."""

    @pytest.fixture(params=[
        (group_name, name, config)
        for group_name, group in ALL_ENDPOINTS.items()
        for name, config in group.items()
    ], ids=lambda x: f"{x[0]}.{x[1]}")
    def endpoint(self, request):
        return request.param

    def test_has_path(self, endpoint):
        _, name, config = endpoint
        assert config.path, f"{name} missing path"
        assert config.path.startswith("/"), f"{name} path must start with /"

    def test_has_table_name(self, endpoint):
        _, name, config = endpoint
        assert config.table_name, f"{name} missing table_name"

    def test_has_primary_key(self, endpoint):
        _, name, config = endpoint
        assert config.primary_key, f"{name} missing primary_key"

    def test_has_valid_write_disposition(self, endpoint):
        _, name, config = endpoint
        assert config.write_disposition in ("replace", "merge", "append"), (
            f"{name} has invalid write_disposition: {config.write_disposition}"
        )

    def test_has_schema(self, endpoint):
        _, name, config = endpoint
        assert config.schema, f"{name} missing schema"


class TestNoDuplicateTableNames:
    """No two endpoints should write to the same table."""

    def test_unique_table_names_within_group(self):
        for group_name, group in ALL_ENDPOINTS.items():
            tables = [c.table_name for c in group.values()]
            assert len(tables) == len(set(tables)), (
                f"Duplicate table names in {group_name}: {tables}"
            )


class TestPrimaryKeyIntegrity:
    """Regression tests for PK bugs fixed in Sprint 1."""

    def test_coaches_pk_no_jsonb(self):
        """coaches PK must not include 'seasons' (JSONB array)."""
        pk = REFERENCE_ENDPOINTS["coaches"].primary_key
        assert "seasons" not in pk
        assert pk == ["first_name", "last_name"]

    def test_player_season_stats_pk_includes_team(self):
        """player_season_stats needs 5-part PK including team for transfers."""
        pk = STATS_ENDPOINTS["player_season_stats"].primary_key
        assert "team" in pk, "player_season_stats PK must include team for transfer players"
        assert "stat_type" in pk
        assert "category" in pk
        assert pk == ["player_id", "season", "team", "category", "stat_type"]

    def test_transfer_portal_pk_includes_origin(self):
        """transfer_portal PK must include origin to handle name collisions."""
        pk = RECRUITING_ENDPOINTS["transfer_portal"].primary_key
        assert "player_id" not in pk
        assert "origin" in pk, "transfer_portal PK must include origin school"
        assert pk == ["first_name", "last_name", "origin", "season"]

    def test_lines_pk_is_composite(self):
        """lines flattens nested structure — needs game_id + provider."""
        pk = BETTING_ENDPOINTS["lines"].primary_key
        assert pk == ["game_id", "provider"]

    def test_draft_picks_pk(self):
        """draft_picks PK should use year + overall."""
        pk = DRAFT_ENDPOINTS["picks"].primary_key
        assert pk == ["year", "overall"]

    def test_play_stats_pk(self):
        """play_stats needs composite PK for player-play associations."""
        config = STATS_ENDPOINTS["play_stats"]
        assert config.primary_key == ["game_id", "play_id", "athlete_id", "stat_type"]

    def test_game_havoc_pk(self):
        """game_havoc needs game_id + team composite PK."""
        config = STATS_ENDPOINTS["game_havoc"]
        assert config.primary_key == ["game_id", "team"]

    def test_team_ats_pk(self):
        """team_ats needs year + team_id composite PK."""
        config = BETTING_ENDPOINTS["team_ats"]
        assert config.primary_key == ["year", "team_id"]

    def test_play_stat_types_pk(self):
        """play_stat_types is reference table with id PK."""
        config = REFERENCE_ENDPOINTS["play_stat_types"]
        assert config.primary_key == ["id"]

    def test_fg_expected_points_pk(self):
        """fg_expected_points is keyed by distance."""
        config = METRICS_ENDPOINTS["fg_expected_points"]
        assert config.primary_key == ["distance"]


class TestSyncedEndpointPKs:
    """Verify PKs for endpoints that existed but weren't in config."""

    def test_wepa_team_season_pk(self):
        """wepa_team_season keyed by year + team."""
        config = METRICS_ENDPOINTS["wepa_team_season"]
        assert config.primary_key == ["year", "team"]

    def test_wepa_players_passing_pk(self):
        """wepa_players_passing keyed by id + year."""
        config = METRICS_ENDPOINTS["wepa_players_passing"]
        assert config.primary_key == ["id", "year"]

    def test_wepa_players_rushing_pk(self):
        """wepa_players_rushing keyed by id + year."""
        config = METRICS_ENDPOINTS["wepa_players_rushing"]
        assert config.primary_key == ["id", "year"]

    def test_wepa_players_kicking_pk(self):
        """wepa_players_kicking keyed by id + year."""
        config = METRICS_ENDPOINTS["wepa_players_kicking"]
        assert config.primary_key == ["id", "year"]

    def test_ppa_teams_pk(self):
        """ppa_teams keyed by season + team."""
        config = METRICS_ENDPOINTS["ppa_teams"]
        assert config.primary_key == ["season", "team"]

    def test_pregame_win_probability_pk(self):
        """pregame_win_probability keyed by season + game_id."""
        config = METRICS_ENDPOINTS["pregame_win_probability"]
        assert config.primary_key == ["season", "game_id"]

    def test_advanced_team_stats_pk(self):
        """advanced_team_stats keyed by season + team."""
        config = STATS_ENDPOINTS["advanced_team_stats"]
        assert config.primary_key == ["season", "team"]


class TestPrimaryKeyFieldsAreStrings:
    """PK fields should be simple string column names, not nested/complex types."""

    def test_all_pk_fields_are_strings(self):
        for group_name, group in ALL_ENDPOINTS.items():
            for name, config in group.items():
                pk = config.primary_key
                if isinstance(pk, list):
                    for field in pk:
                        assert isinstance(field, str), (
                            f"{group_name}.{name} PK field {field} is not a string"
                        )
                else:
                    assert isinstance(pk, str), (
                        f"{group_name}.{name} PK {pk} is not a string"
                    )


class TestWriteDispositions:
    """All endpoints should use merge to preserve indexes and FKs."""

    def test_reference_uses_merge(self):
        """Reference endpoints use merge to preserve indexes, FKs, and triggers."""
        for name, config in REFERENCE_ENDPOINTS.items():
            assert config.write_disposition == "merge", (
                f"Reference endpoint {name} should use 'merge'"
            )

    def test_non_reference_uses_merge(self):
        non_ref = {
            "core": CORE_ENDPOINTS,
            "stats": STATS_ENDPOINTS,
            "ratings": RATINGS_ENDPOINTS,
            "recruiting": RECRUITING_ENDPOINTS,
            "betting": BETTING_ENDPOINTS,
            "draft": DRAFT_ENDPOINTS,
            "metrics": METRICS_ENDPOINTS,
            "rankings": RANKINGS_ENDPOINTS,
        }
        for group_name, group in non_ref.items():
            for name, config in group.items():
                assert config.write_disposition == "merge", (
                    f"{group_name}.{name} should use 'merge', got '{config.write_disposition}'"
                )
