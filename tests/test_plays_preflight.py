"""Loader and post-load verification must use the catalog before claiming success."""

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest

from scripts.verify_load import Report, check_partition
from src.pipelines import run
from src.pipelines.utils.partitions import PartitionStateError
from src.pipelines.utils.play_indexes import PlayIndexStateError


@pytest.mark.parametrize("fails", [None, "partition", "index"])
def test_loader_preflight_precedes_source_and_load(fails):
    events = []
    conn = MagicMock()

    def preflight(connection, years, create):
        assert connection is conn
        assert years == [2027]
        assert create is True
        events.append("preflight")
        if fails == "partition":
            raise PartitionStateError("unattached partition")

    def validate(cur, index_names):
        assert index_names == ["plays_dlt_id_unique"]
        events.append("index-check")
        if fails == "index":
            raise PlayIndexStateError("wrong index owner")

    def source(**kwargs):
        events.append("source")
        assert kwargs["years"] == [2027]
        return "source"

    pipeline = Mock()
    pipeline.pipeline_name = "cfbd_plays"
    credentials = pipeline.destination.configuration.return_value.credentials
    credentials.to_native_representation.return_value = "pipeline-scoped-target"
    pipeline.run.side_effect = lambda source: events.append("load")
    with (
        patch("psycopg2.connect", return_value=conn) as connect,
        patch.object(run, "_metrics_wp_db_url", side_effect=AssertionError("wrong target")),
        patch("src.pipelines.utils.partitions.ensure_play_partitions", side_effect=preflight),
        patch("src.pipelines.utils.play_indexes.validate_play_indexes", side_effect=validate),
        patch.object(run, "plays_source", side_effect=source),
        patch.object(run.dlt, "pipeline", return_value=pipeline),
    ):
        if fails == "partition":
            with pytest.raises(PartitionStateError):
                run.run_plays_pipeline([2027])
            assert events == ["preflight"]
        elif fails == "index":
            with pytest.raises(PlayIndexStateError):
                run.run_plays_pipeline([2027])
            assert events == ["preflight", "index-check"]
        else:
            run.run_plays_pipeline([2027])
            assert events == ["preflight", "index-check", "source", "load"]
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


@pytest.mark.parametrize("pipeline_scoped", [False, True])
def test_fresh_pipeline_resolves_credentials_before_fetch(tmp_path, monkeypatch, pipeline_scoped):
    """Exercise real dlt configuration with no stored schema or destination sync."""
    import dlt
    from psycopg2.extensions import parse_dsn

    general = "postgresql://test_user:test_password@localhost:5432/general_target"
    scoped = "postgresql://test_user:test_password@localhost:5432/plays_target"
    monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS", general)
    monkeypatch.delenv("CFBD_PLAYS__DESTINATION__POSTGRES__CREDENTIALS", raising=False)
    if pipeline_scoped:
        monkeypatch.setenv("CFBD_PLAYS__DESTINATION__POSTGRES__CREDENTIALS", scoped)
    pipeline = dlt.pipeline(
        pipeline_name="cfbd_plays",
        pipelines_dir=str(tmp_path),
        destination="postgres",
        dataset_name="core",
    )
    assert pipeline.default_schema_name is None
    assert list(pipeline.schemas) == []
    events = []
    conn = MagicMock()

    def preflight(connection, years, create):
        assert connection is conn
        events.append("partition")

    def validate(cur, names):
        assert names == ["plays_dlt_id_unique"]
        events.append("index")

    def source(**kwargs):
        events.append("fetch")
        return "source"

    try:
        with (
            patch.object(run.dlt, "pipeline", return_value=pipeline),
            patch("psycopg2.connect", return_value=conn) as connect,
            patch("src.pipelines.utils.partitions.ensure_play_partitions", side_effect=preflight),
            patch("src.pipelines.utils.play_indexes.validate_play_indexes", side_effect=validate),
            patch.object(run, "plays_source", side_effect=source),
            patch.object(pipeline, "run", side_effect=lambda _: events.append("load")),
        ):
            run.run_plays_pipeline([2027])
        assert events == ["partition", "index", "fetch", "load"]
        assert parse_dsn(connect.call_args.args[0])["dbname"] == (
            "plays_target" if pipeline_scoped else "general_target"
        )
        assert pipeline.default_schema_name is None
        assert list(pipeline.schemas) == []
        conn.close.assert_called_once()
    finally:
        pipeline.deactivate()
