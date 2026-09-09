"""Coverage contracts for explicitly enrolled source publication adapters.

This is separate from the SQL refresh registry: a season file is a source work
unit, while its downstream materialized view still covers the whole relation.
Cadence describes fetching policy; it does not configure a publication SLA.
"""

from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True)
class SourcePublicationAsset:
    source_name: str
    asset_key: str
    coverage_grain: str
    correction_policy: str
    expected_no_data_policy: str
    watermark_rule: str
    provider: str
    admission_policy: str
    cadence: str
    parser_contract: str
    protocol: str
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]

    def coverage_key(self, season: int) -> str:
        if type(season) is not int or not 1869 <= season <= 2200:
            raise ValueError("receipt publication requires an explicit season from 1869 to 2200")
        return f"season:{season}"


SDV_RATINGS_PUBLICATION = SourcePublicationAsset(
    source_name="sdv_ratings_weekly",
    asset_key="ratings.sdv_ratings_weekly",
    coverage_grain="season",
    correction_policy="replace_selected_season_from_complete_file",
    expected_no_data_policy="never; missing file is deferred, empty file is invalid",
    watermark_rule="artifact_sha256",
    provider="sportsdataverse_public_file",
    admission_policy="not_cfbd; bounded flat-file fetch retries",
    cadence="weekly",
    parser_contract="sdv-ratings-v1",
    protocol="sdv-ratings-season-v1",
    columns=(
        "season",
        "through_week",
        "team_id",
        "adj_off_epa",
        "adj_def_epa",
        "adj_st_epa",
        "adj_net",
        "fei_off",
        "fei_def",
        "fei_net",
        "off_pace",
        "net_z",
        "games",
        "off_rank",
        "def_rank",
        "net_rank",
    ),
    primary_key=("season", "through_week", "team_id"),
)

SOURCE_PUBLICATION_ASSETS = MappingProxyType(
    {SDV_RATINGS_PUBLICATION.source_name: SDV_RATINGS_PUBLICATION}
)
