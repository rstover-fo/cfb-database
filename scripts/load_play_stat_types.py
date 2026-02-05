"""Load just the play_stat_types reference table."""

import dlt

from src.pipelines.sources.reference import play_stat_types_resource

pipeline = dlt.pipeline(
    pipeline_name="cfbd_play_stat_types",
    destination="postgres",
    dataset_name="ref",
)

print("Loading play_stat_types...")
info = pipeline.run(play_stat_types_resource())
print(f"Load info: {info}")
