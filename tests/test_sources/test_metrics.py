"""Tests for metrics_source's default resource composition and the
predicted-PPA down/distance fan-out (R2).

metrics_wp_source / win_probability_resource have their own dedicated
tests/test_sources/test_metrics_wp.py; this file covers metrics_source's
resource list and the reworked ppa_predicted_resource.
"""

from unittest.mock import MagicMock, patch

import httpx


def _http_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.collegefootballdata.com/ppa/predicted")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


class TestMetricsSourceDefaultResources:
    def test_ppa_predicted_is_not_in_the_default_return(self):
        """R2: ppa_predicted is a 120-call static lookup, not year-scoped
        data -- it must stay out of the daily/incremental metrics_source
        path (docs/pipeline-manifest.md row 48)."""
        from src.pipelines.sources.metrics import metrics_source

        with patch("src.pipelines.sources.metrics.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            source = metrics_source(years=[2024])

            assert "ppa_predicted" not in set(source.resources.keys())

    def test_win_probability_also_stays_out(self):
        """Regression guard alongside the ppa_predicted exclusion above."""
        from src.pipelines.sources.metrics import metrics_source

        with patch("src.pipelines.sources.metrics.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            source = metrics_source(years=[2024])

            assert "win_probability" not in set(source.resources.keys())


class TestMetricsPpaPredictedSource:
    def test_returns_only_ppa_predicted(self):
        from src.pipelines.sources.metrics import metrics_ppa_predicted_source

        with patch("src.pipelines.sources.metrics.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            source = metrics_ppa_predicted_source()

            assert set(source.resources.keys()) == {"ppa_predicted"}


class TestPpaPredictedResource:
    def test_fans_out_120_down_distance_combinations(self):
        """4 downs x 30 distances = 120 calls."""
        from src.pipelines.sources.metrics import ppa_predicted_resource

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(ppa_predicted_resource())

            assert mock_make_request.call_count == 120

    def test_first_and_last_param_combos(self):
        from src.pipelines.sources.metrics import ppa_predicted_resource

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(ppa_predicted_resource())

            calls = mock_make_request.call_args_list
            assert calls[0].args[1] == "/ppa/predicted"
            assert calls[0].kwargs["params"] == {"down": 1, "distance": 1}
            assert calls[1].kwargs["params"] == {"down": 1, "distance": 2}
            assert calls[29].kwargs["params"] == {"down": 1, "distance": 30}
            assert calls[30].kwargs["params"] == {"down": 2, "distance": 1}
            assert calls[-1].kwargs["params"] == {"down": 4, "distance": 30}

    def test_stamps_down_and_distance_onto_every_row(self):
        """The response itself only carries yardLine/predictedPoints per the
        CFBD OpenAPI PredictedPointsValue schema -- down/distance are not
        echoed back and must be stamped from the request."""
        from src.pipelines.sources.metrics import ppa_predicted_resource

        response_by_params = {
            (1, 10): [
                {"yardLine": 1, "predictedPoints": -1.2},
                {"yardLine": 2, "predictedPoints": -1.1},
            ],
        }

        def side_effect(client, path, params=None):
            return response_by_params.get((params["down"], params["distance"]), [])

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = side_effect

            results = [
                r
                for r in ppa_predicted_resource()
                if r.get("down") == 1 and r.get("distance") == 10
            ]

            assert len(results) == 2
            for row in results:
                assert row["down"] == 1
                assert row["distance"] == 10
                assert "yardLine" in row

    def test_400_combo_is_skipped_and_continues(self):
        from src.pipelines.sources.metrics import ppa_predicted_resource

        call_count = {"n": 0}

        def side_effect(client, path, params=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise _http_error(400)
            return []

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = side_effect

            results = list(ppa_predicted_resource())

            assert results == []
            assert call_count["n"] == 120

    def test_merge_disposition_and_primary_key(self):
        from src.pipelines.sources.metrics import ppa_predicted_resource

        with patch("src.pipelines.sources.metrics.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = ppa_predicted_resource()

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"down", "distance", "yard_line"}
