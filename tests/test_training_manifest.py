from copy import deepcopy

import pytest

from scripts import train_model as training
from scripts.training_manifest import (
    build_training_manifest,
    manifests_match_for_freshness,
    source_text_fingerprint,
    training_data_fingerprint,
)


def _game(game_id=1, value=1.0):
    side = dict.fromkeys(training.TEAM_WEEK_SOURCE_COLUMNS, value)
    return {
        "game_id": game_id,
        "season": 2017,
        "season_type": "regular",
        "week": 1,
        "neutral_site": False,
        "home_points": 21,
        "away_points": 14,
        "home_tw": side,
        "away_tw": {**side},
    }


def _manifest(*, games=None, revision="a" * 40):
    return build_training_manifest(
        feature_names=training.FEATURE_NAMES,
        team_week_source_columns=training.TEAM_WEEK_SOURCE_COLUMNS,
        diff_feature_columns=training.DIFF_FEATURE_COLUMNS,
        implementation={"math": source_text_fingerprint({"source": "v1"})},
        train_seasons=[2015, 2016, 2017],
        games=games or [_game()],
        ridge_alpha=training.RIDGE_ALPHA,
        winprob_ridge_alpha=training.WINPROB_ALPHA,
        runtime={"python": "3.12.1", "numpy": "2.0.0"},
        code_revision=revision,
    )


def test_training_data_hash_covers_exact_consumed_values_and_row_order():
    original = training_data_fingerprint([_game(1), _game(2)])
    same_with_reordered_dict = training_data_fingerprint(
        [dict(reversed(list(_game(1).items()))), _game(2)]
    )
    changed_value = training_data_fingerprint([_game(1, 2.0), _game(2)])
    changed_order = training_data_fingerprint([_game(2), _game(1)])
    assert original == same_with_reordered_dict
    assert original["row_count"] == 2
    assert original["digest"] != changed_value["digest"]
    assert original["digest"] != changed_order["digest"]


def test_manifest_records_contract_cutoff_runtime_calibration_and_no_archive_claim():
    manifest = _manifest()
    assert manifest["lineage"] == "known"
    assert manifest["feature_contract"]["feature_names"] == training.FEATURE_NAMES
    assert manifest["training_window"]["cutoff"] == {
        "kind": "season_end",
        "season": 2017,
        "inclusive": True,
    }
    assert manifest["training_data"]["row_count"] == 1
    assert manifest["runtime"] == {"python": "3.12.1", "numpy": "2.0.0"}
    assert manifest["calibration"]["population"] == "training_logits"
    assert "archive" not in repr(manifest).lower()


def test_freshness_ignores_unrelated_git_revision_but_not_reproducible_fields():
    stored = _manifest(revision="a" * 40)
    current = _manifest(revision="b" * 40)
    assert manifests_match_for_freshness(stored, current)

    changed = deepcopy(current)
    changed["training_data"]["digest"] = "f" * 64
    assert not manifests_match_for_freshness(stored, changed)
    changed = deepcopy(current)
    changed["runtime"]["numpy"] = "3.0.0"
    assert not manifests_match_for_freshness(stored, changed)


def test_unknown_legacy_lineage_never_counts_as_fresh():
    current = _manifest()
    legacy = deepcopy(current)
    legacy["lineage"] = "legacy_unknown"
    assert not manifests_match_for_freshness(legacy, current)


def test_source_fingerprint_changes_with_transformation_text():
    assert source_text_fingerprint({"math": "x + 1"}) != source_text_fingerprint({"math": "x + 2"})


def test_training_window_must_be_contiguous_and_ordered():
    with pytest.raises(ValueError, match="contiguous"):
        build_training_manifest(
            feature_names=[],
            team_week_source_columns=[],
            diff_feature_columns=[],
            implementation={},
            train_seasons=[2015, 2017],
            games=[],
            ridge_alpha=1,
            winprob_ridge_alpha=1,
        )


def test_real_current_manifest_fingerprints_math_query_and_feature_builder():
    manifest = training.current_training_manifest([2015, 2016, 2017], [_game()])
    assert set(manifest["implementation"]) == {
        "training_math",
        "training_query",
        "feature_builder",
        "upstream_feature_python",
        "upstream_feature_sql",
    }
    assert all(
        len(fingerprint["digest"]) == 64 for fingerprint in manifest["implementation"].values()
    )
