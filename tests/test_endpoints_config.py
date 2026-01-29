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

    def test_transfer_portal_pk_uses_name_fields(self):
        """transfer_portal API has no player_id — use name fields."""
        pk = RECRUITING_ENDPOINTS["transfer_portal"].primary_key
        assert "player_id" not in pk
        assert pk == ["season", "first_name", "last_name"]

    def test_lines_pk_is_composite(self):
        """lines flattens nested structure — needs game_id + provider."""
        pk = BETTING_ENDPOINTS["lines"].primary_key
        assert pk == ["game_id", "provider"]

    def test_draft_picks_pk(self):
        """draft_picks PK should use year + overall."""
        pk = DRAFT_ENDPOINTS["picks"].primary_key
        assert pk == ["year", "overall"]


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
    """Reference data should use replace, everything else should use merge."""

    def test_reference_uses_replace(self):
        for name, config in REFERENCE_ENDPOINTS.items():
            assert config.write_disposition == "replace", (
                f"Reference endpoint {name} should use 'replace'"
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
