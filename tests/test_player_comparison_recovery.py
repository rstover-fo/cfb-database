"""Regression coverage for the player-comparison metadata-drift repair."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MART_SQL = PROJECT_ROOT / "src" / "schemas" / "marts" / "020_player_comparison.sql"
CREATE_MARKER = "CREATE MATERIALIZED VIEW marts.player_comparison AS"
END_MARKER = "WITH DATA;"


def _mart_select() -> str:
    """Extract the materialized-view SELECT so fixtures exercise production SQL."""
    sql = MART_SQL.read_text()
    return sql.split(CREATE_MARKER, 1)[1].split(END_MARKER, 1)[0].strip()


def _fixture_query() -> str:
    select_sql = _mart_select()
    fixture_ctes = """
WITH fixture_player_season_stats(
    player_id, player, team, position, season, category, stat_type, stat
) AS (
    VALUES
        ('5302440', 'A. Runner', 'Alabama State', 'RB', 2026, 'rushing', 'CAR', '12'),
        ('5302440', 'Alex Runner', 'Alabama State', 'WR', 2026, 'rushing', 'YDS', '87'),
        ('5302440', 'Alex Runner', 'Alabama State', 'WR', 2026, 'rushing', 'TD', '2'),
        ('5302440', 'Alex Runner', 'Alabama State', 'WR', 2026, 'rushing', 'YPC', ''),
        ('5302440', 'Alex Runner', 'Prairie View', 'WR', 2026, 'receiving', 'REC', '4'),
        ('6000000', 'Zed Tie', 'Tie State', 'RB', 2026, 'rushing', 'CAR', '9'),
        ('6000000', 'Aaron Tie', 'Tie State', 'WR', 2026, 'rushing', 'YDS', '45')
),
fixture_roster(id, year, height, weight, jersey, home_city, home_state, team) AS (
    SELECT
        NULL::bigint, NULL::integer, NULL::integer, NULL::integer,
        NULL::integer, NULL::text, NULL::text, NULL::text
    WHERE FALSE
),
fixture_recruits(athlete_id, stars, rating, ranking, year) AS (
    SELECT NULL::bigint, NULL::integer, NULL::numeric, NULL::integer, NULL::integer
    WHERE FALSE
),
fixture_ppa(id, season, average_ppa__all, total_ppa__all) AS (
    SELECT NULL::bigint, NULL::integer, NULL::numeric, NULL::numeric
    WHERE FALSE
),
"""
    select_sql = select_sql.replace(
        "WITH position_groups AS (", fixture_ctes + "position_groups AS ("
    )
    replacements = {
        "stats.player_season_stats": "fixture_player_season_stats",
        "core.roster": "fixture_roster",
        "recruiting.recruits": "fixture_recruits",
        "metrics.ppa_players_season": "fixture_ppa",
    }
    for source, fixture in replacements.items():
        select_sql = select_sql.replace(source, fixture)
    return select_sql


def test_fixture_query_uses_mart_select_and_replaces_physical_sources():
    query = _fixture_query()

    assert "PERCENT_RANK()" in query
    assert "MODE() WITHIN GROUP" in query
    assert all(
        source not in query
        for source in (
            "stats.player_season_stats",
            "core.roster",
            "recruiting.recruits",
            "metrics.ppa_players_season",
        )
    )


def test_metadata_drift_keeps_unique_grain_and_all_stats(db_conn):
    """Execute the real mart SELECT against read-only CTE fixtures."""
    with db_conn.cursor() as cur:
        cur.execute(_fixture_query())
        columns = [description.name for description in cur.description]
        rows = [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    assert len(rows) == 3
    by_key = {(row["player_id"], row["team"]): row for row in rows}
    assert set(by_key) == {
        ("5302440", "Alabama State"),
        ("5302440", "Prairie View"),
        ("6000000", "Tie State"),
    }

    repaired = by_key[("5302440", "Alabama State")]
    assert repaired["player_id"] == "5302440"
    # The most frequent metadata pair wins and remains an observed pair,
    # rather than combining a name and position from different source rows.
    assert (repaired["name"], repaired["position"]) == ("Alex Runner", "WR")
    assert (repaired["name"], repaired["position"]) in {
        ("A. Runner", "RB"),
        ("Alex Runner", "WR"),
    }
    assert repaired["rush_car"] == 12
    assert repaired["rush_yds"] == 87
    assert repaired["rush_td"] == 2
    assert repaired["rush_ypc"] is None

    separate_team = by_key[("5302440", "Prairie View")]
    assert separate_team["rec"] == 4
    assert separate_team["rush_yds"] is None

    # Equal-frequency modes resolve to the earliest JSONB ordering.
    tied = by_key[("6000000", "Tie State")]
    assert (tied["name"], tied["position"]) == ("Aaron Tie", "WR")
    assert tied["rush_car"] == 9
    assert tied["rush_yds"] == 45
