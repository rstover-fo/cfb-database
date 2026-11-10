"""Merge keys and dispositions pinned on the dlt resources that actually load.

A `replace` on a reference table drops the indexes, foreign keys and triggers
other schemas depend on, and a wrong merge key silently duplicates or
overwrites rows. These read the live resource definitions, not a parallel
registry that can drift from them.
"""

import pytest

from src.pipelines.sources.betting import betting_source
from src.pipelines.sources.draft import draft_source
from src.pipelines.sources.recruiting import recruiting_source
from src.pipelines.sources.reference import reference_source


def _primary_key(resource) -> set[str]:
    columns = resource.compute_table_schema()["columns"]
    return {name for name, column in columns.items() if column.get("primary_key")}


REFERENCE_KEYS = {
    "conferences": {"id"},
    "teams": {"id"},
    "teams_fbs": {"id"},
    "venues": {"id"},
    # Not `seasons`: it is a JSON array and cannot be part of a key.
    "coaches": {"first_name", "last_name"},
    "play_types": {"id"},
    "play_stat_types": {"id"},
    "draft_positions": {"name"},
    "draft_teams": {"location", "nickname"},
    "stat_categories": {"name"},
    "calendar": {"season", "week"},
}


def test_reference_source_resources_are_the_reviewed_set():
    assert set(reference_source().resources) == set(REFERENCE_KEYS)


@pytest.mark.parametrize("name", sorted(REFERENCE_KEYS))
def test_reference_resource_merges_on_its_reviewed_key(name):
    resource = reference_source().resources[name]

    assert resource.write_disposition == "merge"
    assert _primary_key(resource) == REFERENCE_KEYS[name]


@pytest.mark.parametrize(
    ("source", "name", "key"),
    [
        (
            recruiting_source,
            "transfer_portal",
            {"first_name", "last_name", "origin", "season"},
        ),
        # Flattening the lines array yields several rows per game.
        (betting_source, "lines", {"game_id", "provider"}),
        (draft_source, "draft_picks", {"year", "overall"}),
    ],
)
def test_composite_merge_keys(source, name, key):
    resource = source(years=[2024]).resources[name]

    assert resource.write_disposition == "merge"
    assert _primary_key(resource) == key
