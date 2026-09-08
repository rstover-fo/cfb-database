"""Loader and post-load verification must use the catalog before claiming success."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from scripts.verify_load import Report, check_partition
from src.pipelines import run
from src.pipelines.utils.partitions import PartitionStateError


@pytest.mark.parametrize("fails", [False, True])
def test_loader_preflight_precedes_source_and_load(fails):
    events = []
    conn = Mock()

    def preflight(connection, years, create):
        assert connection is conn
        assert years == [2027]
        assert create is True
        events.append("preflight")
        if fails:
            raise PartitionStateError("unattached partition")

    def source(**kwargs):
        events.append("source")
        assert kwargs["years"] == [2027]
        return "source"

    pipeline = Mock()
    credentials = pipeline.destination_client.return_value.config.credentials
    credentials.to_native_representation.return_value = "pipeline-scoped-target"
    pipeline.run.side_effect = lambda source: events.append("load")
    with (
        patch("psycopg2.connect", return_value=conn) as connect,
        patch.object(run, "_metrics_wp_db_url", side_effect=AssertionError("wrong target")),
        patch("src.pipelines.utils.partitions.ensure_play_partitions", side_effect=preflight),
        patch.object(run, "plays_source", side_effect=source),
        patch.object(run.dlt, "pipeline", return_value=pipeline),
    ):
        if fails:
            with pytest.raises(PartitionStateError):
                run.run_plays_pipeline([2027])
            assert events == ["preflight"]
        else:
            run.run_plays_pipeline([2027])
            assert events == ["preflight", "source", "load"]
    connect.assert_called_once_with("pipeline-scoped-target")
    conn.close.assert_called_once()


@pytest.mark.parametrize("missing", [(), (2027,)])
def test_verifier_reports_missing_catalog_coverage(missing):
    report = Report()
    with patch(
        "src.pipelines.utils.partitions.inspect_play_partitions",
        return_value=SimpleNamespace(missing_years=missing),
    ):
        check_partition(Mock(), 2027, report)
    assert report.failures == bool(missing)


def test_verifier_fails_invalid_partition():
    report = Report()
    with patch(
        "src.pipelines.utils.partitions.inspect_play_partitions",
        side_effect=PartitionStateError("wrong bounds"),
    ):
        check_partition(Mock(), 2027, report)
    assert report.failures == 1
