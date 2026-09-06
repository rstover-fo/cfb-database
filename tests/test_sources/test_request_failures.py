"""Failure outcomes for bounded game-stat and roster request loops."""

import json
import logging
from collections.abc import Callable
from unittest.mock import MagicMock, patch

import httpx
import pytest
from dlt.extract.exceptions import ResourceExtractionError

from src.pipelines.sources.base import RequestBudgetExhausted, make_request
from src.pipelines.sources.game_stats import (
    game_player_stats_resource,
    game_team_stats_resource,
)
from src.pipelines.sources.rosters import rosters_resource
from src.pipelines.utils.api_client import RateLimitCircuitOpen, RateLimitExhausted
from src.pipelines.utils.request_outcomes import (
    ResponseValidationError,
    SourceRequestError,
    request_failure_summary,
)

ResourceFactory = Callable[[], object]


def _game_team_resource():
    return game_team_stats_resource([2026], season_type="regular", weeks=[1, 2, 3])


def _game_player_resource():
    return game_player_stats_resource([2026], season_type="regular", weeks=[1, 2, 3])


def _roster_resource():
    return rosters_resource(teams=["Alabama", "Georgia", "Texas"], years=[2026])


RESOURCE_CASES = [
    pytest.param(
        "src.pipelines.sources.game_stats",
        _game_team_resource,
        "/games/teams",
        {"year": 2026, "seasonType": "regular", "week": 1},
        id="game-team-stats",
    ),
    pytest.param(
        "src.pipelines.sources.game_stats",
        _game_player_resource,
        "/games/players",
        {"year": 2026, "seasonType": "regular", "week": 1},
        id="game-player-stats",
    ),
    pytest.param(
        "src.pipelines.sources.rosters",
        _roster_resource,
        "/roster",
        {"team": "Alabama", "year": 2026},
        id="rosters",
    ),
]


def _http_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.test/resource")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(str(status_code), request=request, response=response)


def _failure(kind: str) -> BaseException:
    if kind.isdigit():
        return _http_error(int(kind))
    if kind == "exhausted-429":
        return RateLimitExhausted("rate limited on every attempt")
    if kind == "open-breaker":
        return RateLimitCircuitOpen("rate limit circuit is open")
    if kind == "timeout":
        return httpx.ReadTimeout(
            "read timed out",
            request=httpx.Request("GET", "https://example.test/resource"),
        )
    if kind == "invalid-json":
        return json.JSONDecodeError("invalid JSON", "<html>", 0)
    if kind == "local-budget":
        return RequestBudgetExhausted("API budget exhausted")
    raise AssertionError(f"unknown failure kind: {kind}")


def _chain(error: BaseException) -> list[BaseException]:
    causes = []
    seen = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        causes.append(current)
        current = current.__cause__ or current.__context__
    return causes


@pytest.mark.parametrize("module,resource_factory,endpoint,params", RESOURCE_CASES)
@pytest.mark.parametrize(
    "failure_kind",
    [
        "400",
        "401",
        "403",
        "404",
        "500",
        "exhausted-429",
        "open-breaker",
        "timeout",
        "invalid-json",
        "local-budget",
    ],
)
def test_request_failures_stop_the_bounded_resource_and_preserve_the_cause(
    module: str,
    resource_factory: ResourceFactory,
    endpoint: str,
    params: dict,
    failure_kind: str,
):
    client = MagicMock()
    original = _failure(failure_kind)

    with (
        patch(f"{module}.get_client", return_value=client),
        patch(f"{module}.make_request", side_effect=original) as request,
        pytest.raises(ResourceExtractionError) as exc_info,
    ):
        list(resource_factory())

    summary = request_failure_summary(exc_info.value)
    assert summary == {
        "endpoint": endpoint,
        "params": params,
        "outcome": "failed",
        "error_type": type(original).__name__,
        "counts_scope": "resource_invocation",
        "counts_unit": "requests",
        "counts": {"succeeded": 0, "expected_no_data": 0, "failed": 1, "deferred": 2},
    }
    assert original in _chain(exc_info.value)
    assert request.call_count == 1
    client.close.assert_called_once_with()
    json.dumps(summary)


@pytest.mark.parametrize("module,resource_factory,endpoint,_params", RESOURCE_CASES)
def test_successful_empty_responses_are_visible_expected_no_data(
    module: str,
    resource_factory: ResourceFactory,
    endpoint: str,
    _params: dict,
    caplog: pytest.LogCaptureFixture,
):
    client = MagicMock()
    with (
        patch(f"{module}.get_client", return_value=client),
        patch(f"{module}.make_request", return_value=[]) as request,
        caplog.at_level(logging.INFO),
    ):
        assert list(resource_factory()) == []

    assert request.call_count == 3
    assert caplog.text.count(f"outcome=expected_no_data endpoint={endpoint}") == 3
    client.close.assert_called_once_with()


@pytest.mark.parametrize("module,resource_factory,endpoint,_params", RESOURCE_CASES)
def test_success_receipts_describe_fetched_requests(
    module: str,
    resource_factory: ResourceFactory,
    endpoint: str,
    _params: dict,
    caplog: pytest.LogCaptureFixture,
):
    client = MagicMock()
    with (
        patch(f"{module}.get_client", return_value=client),
        patch(f"{module}.make_request", return_value=[{"id": 0, "optional": None}]),
        caplog.at_level(logging.INFO),
    ):
        rows = list(resource_factory())

    assert len(rows) == 3
    assert caplog.text.count(f"outcome=succeeded endpoint={endpoint}") == 3
    assert "fetched_rows=1" in caplog.text
    client.close.assert_called_once_with()


@pytest.mark.parametrize("module,resource_factory,endpoint,first_params", RESOURCE_CASES)
def test_success_then_failure_reports_prior_fetch_and_deferred_requests(
    module: str,
    resource_factory: ResourceFactory,
    endpoint: str,
    first_params: dict,
):
    client = MagicMock()
    original = _http_error(503)
    with (
        patch(f"{module}.get_client", return_value=client),
        patch(
            f"{module}.make_request",
            side_effect=[[{"id": 17, "extra": "accepted"}], original],
        ) as request,
    ):
        iterator = iter(resource_factory())
        assert next(iterator)["id"] == 17
        with pytest.raises(ResourceExtractionError) as exc_info:
            next(iterator)

    summary = request_failure_summary(exc_info.value)
    assert summary is not None
    assert summary["endpoint"] == endpoint
    assert summary["params"] != first_params
    assert summary["counts"] == {
        "succeeded": 1,
        "expected_no_data": 0,
        "failed": 1,
        "deferred": 1,
    }
    assert request.call_count == 2
    assert original in _chain(exc_info.value)
    client.close.assert_called_once_with()


@pytest.mark.parametrize("module,resource_factory,_endpoint,_params", RESOURCE_CASES)
@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"id": 1}, id="object-instead-of-list"),
        pytest.param([{"id": 1}, "bad record"], id="non-dict-record"),
        pytest.param([{"name": "missing"}], id="missing-id"),
        pytest.param([{"id": None}], id="null-id"),
    ],
)
def test_invalid_response_is_rejected_before_any_row_is_yielded(
    module: str,
    resource_factory: ResourceFactory,
    _endpoint: str,
    _params: dict,
    payload: object,
):
    client = MagicMock()
    with (
        patch(f"{module}.get_client", return_value=client),
        patch(f"{module}.make_request", return_value=payload) as request,
    ):
        iterator = iter(resource_factory())
        with pytest.raises(ResourceExtractionError) as exc_info:
            next(iterator)

    summary = request_failure_summary(exc_info.value)
    assert summary is not None
    assert summary["error_type"] == "ResponseValidationError"
    assert summary["counts"] == {
        "succeeded": 0,
        "expected_no_data": 0,
        "failed": 1,
        "deferred": 2,
    }
    assert any(isinstance(error, ResponseValidationError) for error in _chain(exc_info.value))
    assert request.call_count == 1
    client.close.assert_called_once_with()


def test_local_budget_exhaustion_has_a_stable_typed_classification():
    limiter = MagicMock()
    limiter.check_budget.return_value = False
    limiter.calls_used = 125_000
    with (
        patch("src.pipelines.sources.base.get_rate_limiter", return_value=limiter),
        pytest.raises(RequestBudgetExhausted, match="API budget exhausted"),
    ):
        make_request(MagicMock(), "/roster", {"team": "Alabama", "year": 2026})


def test_request_failure_summary_traverses_wrappers_and_guards_cycles():
    original = RateLimitExhausted("spent")
    request_error = SourceRequestError(
        "/roster",
        {"team": "Alabama", "year": 2026},
        original,
        {"succeeded": 0, "expected_no_data": 0, "failed": 1, "deferred": 1},
    )
    request_error.__cause__ = original
    outer = RuntimeError("dlt wrapper")
    outer.__cause__ = request_error
    assert request_failure_summary(outer) == request_error.to_summary()

    cycle = RuntimeError("cycle")
    cycle.__cause__ = cycle
    assert request_failure_summary(cycle) is None


def test_resource_contracts_keep_existing_merge_keys_and_roster_table():
    game_team = game_team_stats_resource([2026], weeks=[1])
    game_player = game_player_stats_resource([2026], weeks=[1])
    game_team_schema = game_team.compute_table_schema()
    game_player_schema = game_player.compute_table_schema()
    roster_schema = rosters_resource.compute_table_schema()
    assert game_team.write_disposition == "merge"
    assert game_team_schema["columns"]["id"]["primary_key"] is True
    assert game_player.write_disposition == "merge"
    assert game_player_schema["columns"]["id"]["primary_key"] is True
    assert rosters_resource.table_name == "roster"
    assert rosters_resource.write_disposition == "merge"
    assert {name for name, column in roster_schema["columns"].items() if column["primary_key"]} == {
        "id",
        "team",
        "year",
    }
