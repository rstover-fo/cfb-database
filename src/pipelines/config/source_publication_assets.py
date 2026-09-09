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

    def season_basis(self, artifact_origin: str) -> str:
        """Describe the evidence used to assign rows to the selected season."""
        if artifact_origin not in {"registered_url", "local_file"}:
            raise ValueError("artifact origin must be registered_url or local_file")
        if self.source_name in {"sdv_fpi_weekly", "sdv_ratings_weekly"}:
            return "artifact_field"
        if artifact_origin == "registered_url":
            return "registered_artifact_name"
        return "caller_declared"


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

SDV_FPI_PUBLICATION = SourcePublicationAsset(
    source_name="sdv_fpi_weekly",
    asset_key="ratings.espn_fpi_weekly",
    coverage_grain="season",
    correction_policy="replace_selected_season_from_complete_file",
    expected_no_data_policy="never; missing file is deferred, empty file is invalid",
    watermark_rule="artifact_sha256",
    provider="sportsdataverse_public_file",
    admission_policy="not_cfbd; bounded flat-file fetch retries",
    cadence="weekly",
    parser_contract="sdv-fpi-v1",
    protocol="sdv-season-file-v1",
    columns=(
        "season",
        "season_type",
        "week",
        "team_id",
        "last_updated",
        "run_date_time_key",
        "snapshot_out_of_sequence",
        "fpi",
        "fpirank",
        "projectedw",
        "projectedl",
        "projectedt",
        "projectedwpctrank",
        "probwinout",
        "probwinconf",
        "sosremainingrank",
        "accomplishment",
        "accomplishmentrank",
        "adjwins",
        "adjlosses",
        "adjwinpctrank",
        "gamecontrol",
        "gamecontrolrank",
        "adjavgingamewp",
        "adjavgingamewprank",
        "avgingamewp",
        "avgingamewprank",
        "avgsosrank",
        "topsosrank",
        "epaoffense",
        "epadefense",
        "epaspecialteams",
        "probwindiv",
        "probmakeplayoffs",
        "probmaketitlegame",
        "numwins",
        "numlosses",
        "numties",
        "probwintitle",
        "rankchange7days",
        "prob6wins",
        "rank",
        "offefficiency",
        "offefficiencyrank",
        "defefficiency",
        "defefficiencyrank",
        "stefficiency",
        "stefficiencyrank",
        "totefficiency",
        "totefficiencyrank",
        "snapshot_is_contemporaneous",
    ),
    primary_key=("season", "season_type", "week", "team_id"),
)

SDV_TEAM_XWALK_PUBLICATION = SourcePublicationAsset(
    source_name="sdv_team_xwalk",
    asset_key="ref.team_id_xwalk",
    coverage_grain="season",
    correction_policy="replace_selected_season_from_complete_file",
    expected_no_data_policy="never; missing file is deferred, empty file is invalid",
    watermark_rule="artifact_sha256",
    provider="sportsdataverse_public_file",
    admission_policy="not_cfbd; bounded flat-file fetch retries",
    cadence="weekly",
    parser_contract="sdv-team-xwalk-v1",
    protocol="sdv-season-file-v1",
    columns=(
        "season",
        "norm_key",
        "xwalk_key",
        "espn_team_id",
        "espn_team",
        "espn_abbreviation",
        "fox_team_id",
        "fox_team",
        "fox_abbreviation",
        "yahoo_team_id",
        "yahoo_team",
        "yahoo_abbreviation",
        "matched_sources",
    ),
    primary_key=("season", "xwalk_key"),
)

SDV_GAME_XWALK_PUBLICATION = SourcePublicationAsset(
    source_name="sdv_game_xwalk",
    asset_key="ref.game_id_xwalk",
    coverage_grain="season",
    correction_policy="replace_selected_season_from_complete_file",
    expected_no_data_policy="never; missing file is deferred, empty file is invalid",
    watermark_rule="artifact_sha256",
    provider="sportsdataverse_public_file",
    admission_policy="not_cfbd; bounded flat-file fetch retries",
    cadence="weekly",
    parser_contract="sdv-game-xwalk-v1",
    protocol="sdv-season-file-v1",
    columns=(
        "season",
        "matchup_key",
        "yahoo_date",
        "espn_game_id",
        "fox_game_id",
        "yahoo_game_id",
        "yahoo_global_game_id",
        "home_team",
        "away_team",
        "espn_date",
        "fox_date",
        "matched_sources",
    ),
    primary_key=("season", "matchup_key", "yahoo_date"),
)

SOURCE_PUBLICATION_ASSETS = MappingProxyType(
    {
        asset.source_name: asset
        for asset in (
            SDV_RATINGS_PUBLICATION,
            SDV_FPI_PUBLICATION,
            SDV_TEAM_XWALK_PUBLICATION,
            SDV_GAME_XWALK_PUBLICATION,
        )
    }
)
