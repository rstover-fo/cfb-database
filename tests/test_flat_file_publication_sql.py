"""Executed publication checks for the three season-file SDV sources."""

from __future__ import annotations

import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from psycopg2 import sql
from psycopg2.extensions import parse_dsn

from tests.test_asset_freshness_sql import freshness_db as _freshness_db  # noqa: F401
from tests.test_asset_publication_sql import publication_args as elo_publication_args
from tests.test_asset_publication_sql import publication_db as _publication_db  # noqa: F401
from tests.test_asset_publication_sql import publish as publish_elo
from tests.test_generation_refresh_sql import generation_db as _generation_db  # noqa: F401
from tests.test_sdv_ratings_publication_sql import SOURCE_OBJECTS as SDV_RATINGS_OBJECTS
from tests.test_sdv_ratings_publication_sql import publication_args as ratings_publication_args
from tests.test_sdv_ratings_publication_sql import publish as publish_ratings
from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_071 = ROOT / "src/schemas/migrations/071_sdv_ratings_publication.sql"
MIGRATION_072 = ROOT / "src/schemas/migrations/072_sdv_source_batch_publication.sql"
PROTOCOL = "sdv-season-file-v1"

SOURCES = {
    "sdv_fpi_weekly": {
        "asset": "ratings.espn_fpi_weekly",
        "parser": "sdv-fpi-v1",
        "season_basis": "artifact_field",
        "url": (
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_fpi_weekly/cfb_fpi_weekly_{season}.parquet"
        ),
    },
    "sdv_team_xwalk": {
        "asset": "ref.team_id_xwalk",
        "parser": "sdv-team-xwalk-v1",
        "season_basis": "registered_artifact_name",
        "url": (
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_crosswalk/cfb_teams_crosswalk_{season}.parquet"
        ),
    },
    "sdv_game_xwalk": {
        "asset": "ref.game_id_xwalk",
        "parser": "sdv-game-xwalk-v1",
        "season_basis": "registered_artifact_name",
        "url": (
            "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
            "cfb_crosswalk/cfb_schedule_crosswalk_{season}.parquet"
        ),
    },
}

FPI_DOUBLES = (
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
)

SOURCE_OBJECTS = """
CREATE SCHEMA ref;
CREATE TABLE ref.team_id_xwalk (
    season bigint NOT NULL,
    norm_key text NOT NULL,
    xwalk_key text NOT NULL,
    espn_team_id bigint,
    espn_team text,
    espn_abbreviation text,
    fox_team_id text,
    fox_team text,
    fox_abbreviation text,
    yahoo_team_id text,
    yahoo_team text,
    yahoo_abbreviation text,
    matched_sources text,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    _dlt_load_id varchar NOT NULL,
    _dlt_id varchar NOT NULL,
    PRIMARY KEY (season, xwalk_key),
    UNIQUE (_dlt_id)
);
CREATE INDEX idx_team_id_xwalk_norm_key ON ref.team_id_xwalk(season, norm_key);
CREATE INDEX idx_team_id_xwalk_espn ON ref.team_id_xwalk(espn_team_id)
    WHERE espn_team_id IS NOT NULL;

CREATE TABLE ref.game_id_xwalk (
    season bigint NOT NULL,
    matchup_key text NOT NULL,
    yahoo_date date NOT NULL,
    espn_game_id bigint,
    fox_game_id text,
    yahoo_game_id text,
    yahoo_global_game_id text,
    home_team text,
    away_team text,
    espn_date date,
    fox_date date,
    matched_sources text,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    _dlt_load_id varchar NOT NULL,
    _dlt_id varchar NOT NULL,
    PRIMARY KEY (season, matchup_key, yahoo_date),
    UNIQUE (_dlt_id)
);
CREATE INDEX idx_game_id_xwalk_espn ON ref.game_id_xwalk(espn_game_id)
    WHERE espn_game_id IS NOT NULL;
CREATE INDEX idx_game_id_xwalk_yahoo ON ref.game_id_xwalk(yahoo_game_id);

CREATE TABLE ratings.espn_fpi_weekly (
    season bigint NOT NULL,
    season_type bigint NOT NULL,
    week bigint NOT NULL,
    team_id bigint NOT NULL,
    last_updated timestamptz,
    run_date_time_key bigint,
    snapshot_out_of_sequence boolean,
    fpi double precision,
    fpirank double precision,
    projectedw double precision,
    projectedl double precision,
    projectedt double precision,
    projectedwpctrank double precision,
    probwinout double precision,
    probwinconf double precision,
    sosremainingrank double precision,
    accomplishment double precision,
    accomplishmentrank double precision,
    adjwins double precision,
    adjlosses double precision,
    adjwinpctrank double precision,
    gamecontrol double precision,
    gamecontrolrank double precision,
    adjavgingamewp double precision,
    adjavgingamewprank double precision,
    avgingamewp double precision,
    avgingamewprank double precision,
    avgsosrank double precision,
    topsosrank double precision,
    epaoffense double precision,
    epadefense double precision,
    epaspecialteams double precision,
    probwindiv double precision,
    probmakeplayoffs double precision,
    probmaketitlegame double precision,
    numwins double precision,
    numlosses double precision,
    numties double precision,
    probwintitle double precision,
    rankchange7days double precision,
    prob6wins double precision,
    rank double precision,
    offefficiency double precision,
    offefficiencyrank double precision,
    defefficiency double precision,
    defefficiencyrank double precision,
    stefficiency double precision,
    stefficiencyrank double precision,
    totefficiency double precision,
    totefficiencyrank double precision,
    snapshot_is_contemporaneous boolean,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    _dlt_load_id varchar NOT NULL,
    _dlt_id varchar NOT NULL,
    PRIMARY KEY (season, season_type, week, team_id),
    UNIQUE (_dlt_id)
);
CREATE INDEX idx_espn_fpi_weekly_season_week
    ON ratings.espn_fpi_weekly(season, week);

REVOKE ALL ON ref.team_id_xwalk, ref.game_id_xwalk,
    ratings.espn_fpi_weekly
FROM anon, authenticated, analyst_ro, publication_bystander;
REVOKE ALL ON SCHEMA ref, ratings
FROM anon, authenticated, analyst_ro, publication_bystander;
GRANT USAGE ON SCHEMA ref, ratings TO anon, authenticated, analyst_ro;
GRANT SELECT ON ref.team_id_xwalk, ref.game_id_xwalk,
    ratings.espn_fpi_weekly TO anon, authenticated, analyst_ro;
"""

PUBLISH_SQL = """
SELECT * FROM warehouse_source_batch.publish_source_load(
    %s, %s, %s, %s::jsonb, %s::jsonb
)
"""


@pytest.fixture
def batch_db(request):
    conn, target = request.getfixturevalue("_generation_db")
    query(conn, SDV_RATINGS_OBJECTS)
    query(conn, SOURCE_OBJECTS)
    query(conn, MIGRATION_071.read_text())
    query(conn, MIGRATION_072.read_text())
    query(conn, MIGRATION_072.read_text())
    return conn, target


def source_query(conn, statement, values=None, *, commit=True):
    with conn.cursor() as cur:
        cur.execute("SET LOCAL ROLE warehouse_source_publisher")
        cur.execute(statement, values)
        result = cur.fetchall() if cur.description else None
    if commit:
        conn.commit()
    return result


def plan(conn, source, season=2025):
    return source_query(
        conn,
        "SELECT warehouse_source_batch.get_source_plan(%s,%s)",
        (source, season),
    )[0][0]


def start_load(conn, load_plan, run_id=None):
    run_id = str(run_id or uuid.uuid4())
    result = source_query(
        conn,
        "SELECT warehouse_source_batch.start_source_load(%s,%s::jsonb)",
        (run_id, json.dumps(load_plan)),
    )[0][0]
    assert str(result) == run_id
    return run_id


def fpi_row(season=2025, team_id=333, *, week=0, season_type=2, suffix="a"):
    row = {name: None for name in FPI_DOUBLES}
    row.update(
        season=season,
        season_type=season_type,
        week=week,
        team_id=team_id,
        last_updated="2025-08-20T12:30:00+00:00",
        run_date_time_key=20250820123000,
        snapshot_out_of_sequence=False,
        snapshot_is_contemporaneous=True,
        fpi=18.25,
        fpirank=float(team_id),
        _dlt_load_id=f"fpi-load-{season}-{suffix}",
        _dlt_id=f"fpi-row-{season}-{season_type}-{week}-{team_id}-{suffix}",
    )
    return row


def team_row(
    season=2025,
    norm_key="alpha wolves",
    *,
    espn_team_id=101,
    fox_team_id="fox-101",
    yahoo_team_id="yahoo-101",
    suffix="a",
):
    first_id = next(
        (value for value in (espn_team_id, fox_team_id, yahoo_team_id) if value is not None),
        None,
    )
    return {
        "season": season,
        "norm_key": norm_key,
        "xwalk_key": f"{norm_key}#{first_id}",
        "espn_team_id": espn_team_id,
        "espn_team": None,
        "espn_abbreviation": None,
        "fox_team_id": fox_team_id,
        "fox_team": None,
        "fox_abbreviation": None,
        "yahoo_team_id": yahoo_team_id,
        "yahoo_team": None,
        "yahoo_abbreviation": None,
        "matched_sources": None,
        "_dlt_load_id": f"team-load-{season}-{suffix}",
        "_dlt_id": f"team-row-{season}-{suffix}",
    }


def game_row(season=2025, matchup_key="alpha|beta", *, day=None, suffix="a"):
    day = day or f"{season}-08-30"
    return {
        "season": season,
        "matchup_key": matchup_key,
        "yahoo_date": day,
        "espn_game_id": None,
        "fox_game_id": None,
        "yahoo_game_id": f"yahoo-{suffix}",
        "yahoo_global_game_id": None,
        "home_team": "Beta Bears",
        "away_team": "Alpha Wolves",
        "espn_date": None,
        "fox_date": None,
        "matched_sources": "yahoo",
        "_dlt_load_id": f"game-load-{season}-{suffix}",
        "_dlt_id": f"game-row-{season}-{suffix}",
    }


def rows_for(source, season=2025, *, suffix="a"):
    if source == "sdv_fpi_weekly":
        return [fpi_row(season, suffix=suffix), fpi_row(season, 2483, week=1, suffix=suffix)]
    if source == "sdv_team_xwalk":
        return [
            team_row(season, suffix=f"{suffix}-1"),
            team_row(
                season,
                "null ids school",
                espn_team_id=None,
                fox_team_id=None,
                yahoo_team_id=None,
                suffix=f"{suffix}-2",
            ),
        ]
    return [
        game_row(season, suffix=f"{suffix}-1"),
        game_row(season, "alpha|beta", day=f"{season}-12-06", suffix=f"{suffix}-2"),
    ]


def evidence(source, rows, *, artifact_origin="registered_url"):
    basis = (
        "artifact_field"
        if source == "sdv_fpi_weekly"
        else (
            "registered_artifact_name" if artifact_origin == "registered_url" else "caller_declared"
        )
    )
    return {
        "artifact_origin": artifact_origin,
        "stage_schema": "warehouse_source_stage",
        "parser_contract": SOURCES[source]["parser"],
        "dlt_load_ids": sorted({row["_dlt_load_id"] for row in rows}),
        "source_rows": len(rows),
        "season_basis": basis,
    }


def publication_args(
    conn,
    source,
    *,
    season=2025,
    rows=None,
    generation_id=None,
    sha=None,
    load_plan=None,
    artifact_origin="registered_url",
):
    rows = rows if rows is not None else rows_for(source, season)
    load_plan = load_plan or plan(conn, source, season)
    return (
        start_load(conn, load_plan),
        str(generation_id or uuid.uuid4()),
        sha or ("a" * 64),
        json.dumps(rows),
        json.dumps(evidence(source, rows, artifact_origin=artifact_origin)),
    )


def publish(conn, args, *, commit=True):
    return source_query(conn, PUBLISH_SQL, args, commit=commit)[0]


def current(conn, *, asset=None, season=None):
    clauses = []
    values = []
    if asset is not None:
        clauses.append("asset_key=%s")
        values.append(asset)
    if season is not None:
        clauses.append("coverage_key=%s")
        values.append(f"season:{season}")
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return query(
        conn,
        "SELECT asset_key,coverage_key,generation_id::text "
        f"FROM meta.asset_current_generations{where} ORDER BY asset_key,coverage_key",
        tuple(values) or None,
    )


def denied(conn, role, statement):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute(statement)
    finally:
        conn.rollback()


@pytest.mark.parametrize("source", SOURCES)
def test_plan_is_exact_source_and_season_contract(source, batch_db):
    conn, _ = batch_db
    assert plan(conn, source) == {
        "protocol": PROTOCOL,
        "source_name": source,
        "asset_key": SOURCES[source]["asset"],
        "coverage_key": "season:2025",
        "season": 2025,
        "expected_generation_id": None,
        "parser_contract": SOURCES[source]["parser"],
    }
    assert query(
        conn,
        "SELECT count(*) FROM meta.asset_freshness_policies WHERE asset_key=%s",
        (SOURCES[source]["asset"],),
    ) == [(0,)]


@pytest.mark.parametrize(
    "source,season",
    [
        ("sdv_ratings_weekly", 2025),
        ("unknown", 2025),
        ("sdv_fpi_weekly", 1868),
        ("sdv_fpi_weekly", 2201),
    ],
)
def test_plan_rejects_unregistered_source_and_out_of_range_season(source, season, batch_db):
    conn, _ = batch_db
    with pytest.raises(psycopg2.Error):
        plan(conn, source, season)
    conn.rollback()


@pytest.mark.parametrize("source", SOURCES)
def test_publish_preserves_source_specific_types_nulls_and_grain(source, batch_db):
    conn, _ = batch_db
    rows = rows_for(source)
    args = publication_args(conn, source, rows=rows, artifact_origin="local_file")
    assert publish(conn, args) == (args[1], False, 2)
    asset = SOURCES[source]["asset"]
    assert current(conn, asset=asset, season=2025) == [(asset, "season:2025", args[1])]

    if source == "sdv_fpi_weekly":
        result = query(
            conn,
            """
            SELECT season,season_type,week,team_id,last_updated,
                   snapshot_out_of_sequence,snapshot_is_contemporaneous,
                   projectedt,fpi
            FROM ratings.espn_fpi_weekly ORDER BY week,team_id
            """,
        )
        assert result[0][:4] == (2025, 2, 0, 333)
        assert isinstance(result[0][4], datetime) and result[0][4].tzinfo is not None
        assert result[0][5:] == (False, True, None, 18.25)
    elif source == "sdv_team_xwalk":
        assert query(
            conn,
            """
            SELECT norm_key,xwalk_key,espn_team_id,fox_team_id,yahoo_team_id
            FROM ref.team_id_xwalk ORDER BY norm_key
            """,
        ) == [
            ("alpha wolves", "alpha wolves#101", 101, "fox-101", "yahoo-101"),
            ("null ids school", "null ids school#None", None, None, None),
        ]
    else:
        assert query(
            conn,
            """
            SELECT matchup_key,yahoo_date,espn_game_id,espn_date,fox_date
            FROM ref.game_id_xwalk ORDER BY yahoo_date
            """,
        ) == [
            ("alpha|beta", date(2025, 8, 30), None, None, None),
            ("alpha|beta", date(2025, 12, 6), None, None, None),
        ]

    receipt = query(
        conn,
        """
        SELECT coverage,input_observations,source_watermark
        FROM meta.asset_receipts WHERE generation_id=%s
        """,
        (args[1],),
    )[0]
    assert receipt[0] == {
        "complete": True,
        "scope": "season",
        "season": 2025,
        "mode": "full_file_for_season",
        "source_rows": 2,
        "published_rows": 2,
    }
    assert receipt[1]["protocol"] == PROTOCOL
    assert receipt[1]["parser_contract"] == SOURCES[source]["parser"]
    assert receipt[1]["artifact_origin"] == "local_file"
    assert (
        receipt[1]["season_basis"]
        == evidence(source, rows, artifact_origin="local_file")["season_basis"]
    )
    assert receipt[2] == args[2]
    assert query(
        conn,
        "SELECT source_url,status,row_count FROM meta.flat_file_loads",
    ) == [(None, "loaded", 2)]


def test_team_all_null_provider_id_key_is_valid_and_collides_only_on_full_key(batch_db):
    conn, _ = batch_db
    rows = [
        team_row(
            norm_key="same school",
            espn_team_id=None,
            fox_team_id=None,
            yahoo_team_id=None,
            suffix="none",
        ),
        team_row(norm_key="same school", espn_team_id=777, suffix="espn"),
    ]
    args = publication_args(conn, "sdv_team_xwalk", rows=rows)
    assert publish(conn, args) == (args[1], False, 2)
    assert query(
        conn,
        "SELECT xwalk_key,espn_team_id FROM ref.team_id_xwalk ORDER BY xwalk_key",
    ) == [("same school#777", 777), ("same school#None", None)]

    duplicate = rows + [dict(rows[0], _dlt_id="another-row-id")]
    bad = publication_args(
        conn,
        "sdv_team_xwalk",
        rows=duplicate,
        sha="b" * 64,
    )
    with pytest.raises(psycopg2.Error):
        publish(conn, bad)
    conn.rollback()
    assert current(conn, asset=SOURCES["sdv_team_xwalk"]["asset"], season=2025) == [
        (SOURCES["sdv_team_xwalk"]["asset"], "season:2025", args[1])
    ]


def test_correction_removes_stale_rows_and_preserves_other_source_seasons(batch_db):
    conn, _ = batch_db
    generations = {}
    digest_chars = iter("abcdef")
    for source in SOURCES:
        for season in (2024, 2025):
            args = publication_args(
                conn,
                source,
                season=season,
                rows=rows_for(source, season, suffix="initial"),
                sha=next(digest_chars) * 64,
            )
            publish(conn, args)
            generations[(source, season)] = args[1]

    corrected = [team_row(2025, norm_key="replacement", suffix="corrected")]
    corrected_args = publication_args(
        conn,
        "sdv_team_xwalk",
        rows=corrected,
        sha="1" * 64,
    )
    assert publish(conn, corrected_args) == (corrected_args[1], False, 1)

    assert query(
        conn,
        "SELECT season,norm_key FROM ref.team_id_xwalk ORDER BY season,norm_key",
    ) == [
        (2024, "alpha wolves"),
        (2024, "null ids school"),
        (2025, "replacement"),
    ]
    assert query(
        conn,
        "SELECT season,count(*) FROM ratings.espn_fpi_weekly GROUP BY season ORDER BY season",
    ) == [(2024, 2), (2025, 2)]
    assert query(
        conn,
        "SELECT season,count(*) FROM ref.game_id_xwalk GROUP BY season ORDER BY season",
    ) == [(2024, 2), (2025, 2)]
    assert current(conn, asset=SOURCES["sdv_team_xwalk"]["asset"]) == [
        (SOURCES["sdv_team_xwalk"]["asset"], "season:2024", generations[("sdv_team_xwalk", 2024)]),
        (SOURCES["sdv_team_xwalk"]["asset"], "season:2025", corrected_args[1]),
    ]
    assert query(
        conn,
        "SELECT row_delta FROM meta.asset_receipts WHERE generation_id=%s",
        (corrected_args[1],),
    ) == [
        (
            {
                "previous_rows": 2,
                "published_rows": 1,
                "inserted_rows": 1,
                "deleted_rows": 2,
                "changed_rows": 0,
            },
        )
    ]


def test_registered_url_ledger_and_season_basis_are_source_specific(batch_db):
    conn, _ = batch_db
    for index, source in enumerate(SOURCES, start=1):
        args = publication_args(conn, source, sha=str(index) * 64)
        publish(conn, args)
    assert query(
        conn,
        """
        SELECT source,source_url FROM meta.flat_file_loads ORDER BY source
        """,
    ) == sorted(
        [
            (
                f"{source}:2025",
                SOURCES[source]["url"].format(season=2025),
            )
            for source in SOURCES
        ]
    )
    assert query(
        conn,
        """
        SELECT asset_key,input_observations->>'season_basis'
        FROM meta.asset_receipts
        WHERE asset_key = ANY(%s) ORDER BY asset_key
        """,
        ([details["asset"] for details in SOURCES.values()],),
    ) == sorted([(details["asset"], details["season_basis"]) for details in SOURCES.values()])


@pytest.mark.parametrize(
    "source,malformation",
    [
        ("sdv_fpi_weekly", "empty"),
        ("sdv_fpi_weekly", "extra"),
        ("sdv_fpi_weekly", "missing"),
        ("sdv_fpi_weekly", "wrong_season"),
        ("sdv_fpi_weekly", "duplicate_pk"),
        ("sdv_fpi_weekly", "duplicate_dlt"),
        ("sdv_fpi_weekly", "negative_week"),
        ("sdv_fpi_weekly", "string_bool"),
        ("sdv_fpi_weekly", "naive_timestamp"),
        ("sdv_fpi_weekly", "nonfinite"),
        ("sdv_team_xwalk", "wrong_xwalk_key"),
        ("sdv_team_xwalk", "blank_key"),
        ("sdv_game_xwalk", "bad_date"),
        ("sdv_game_xwalk", "blank_key"),
    ],
)
def test_malformed_rows_fail_before_data_receipt_pointer_or_ledger(source, malformation, batch_db):
    conn, _ = batch_db
    rows = rows_for(source)
    if malformation == "empty":
        rows = []
    elif malformation == "extra":
        rows[0]["unexpected"] = 1
    elif malformation == "missing":
        del rows[0]["projectedt"]
    elif malformation == "wrong_season":
        rows[0]["season"] = 2024
    elif malformation == "duplicate_pk":
        rows.append(dict(rows[0], _dlt_id="distinct-dlt-id"))
    elif malformation == "duplicate_dlt":
        rows[1]["_dlt_id"] = rows[0]["_dlt_id"]
    elif malformation == "negative_week":
        rows[0]["week"] = -1
    elif malformation == "string_bool":
        rows[0]["snapshot_is_contemporaneous"] = "false"
    elif malformation == "naive_timestamp":
        rows[0]["last_updated"] = "2025-08-20T12:30:00"
    elif malformation == "nonfinite":
        rows[0]["fpi"] = "Infinity"
    elif malformation == "wrong_xwalk_key":
        rows[0]["xwalk_key"] = "forged"
    elif malformation == "bad_date":
        rows[0]["yahoo_date"] = "2025-02-30"
    else:
        key = "norm_key" if source == "sdv_team_xwalk" else "matchup_key"
        rows[0][key] = " "
        if source == "sdv_team_xwalk":
            rows[0]["xwalk_key"] = " #101"

    args = publication_args(conn, source, rows=rows)
    if malformation == "nonfinite":
        args = (
            *args[:3],
            args[3].replace('"fpi": "Infinity"', '"fpi": 1e10000'),
            args[4],
        )
    with pytest.raises(psycopg2.Error):
        publish(conn, args)
    conn.rollback()
    assert query(conn, f"SELECT count(*) FROM {SOURCES[source]['asset']}") == [(0,)]
    assert current(conn, asset=SOURCES[source]["asset"]) == []
    assert query(
        conn,
        "SELECT count(*) FROM meta.asset_receipts WHERE asset_key=%s",
        (SOURCES[source]["asset"],),
    ) == [(0,)]
    assert query(conn, "SELECT count(*) FROM meta.flat_file_loads") == [(0,)]


@pytest.mark.parametrize(
    "source,evidence_error",
    [
        ("sdv_fpi_weekly", "extra"),
        ("sdv_fpi_weekly", "missing"),
        ("sdv_fpi_weekly", "stage_schema"),
        ("sdv_fpi_weekly", "parser"),
        ("sdv_fpi_weekly", "source_rows"),
        ("sdv_fpi_weekly", "dlt_load_ids"),
        ("sdv_fpi_weekly", "season_basis"),
        ("sdv_team_xwalk", "season_basis"),
        ("sdv_game_xwalk", "season_basis"),
    ],
)
def test_staged_evidence_must_be_exact_and_match_rows(source, evidence_error, batch_db):
    conn, _ = batch_db
    args = list(publication_args(conn, source))
    proof = json.loads(args[4])
    if evidence_error == "extra":
        proof["unexpected"] = True
    elif evidence_error == "missing":
        del proof["artifact_origin"]
    elif evidence_error == "stage_schema":
        proof["stage_schema"] = "public"
    elif evidence_error == "parser":
        proof["parser_contract"] = "wrong-v1"
    elif evidence_error == "source_rows":
        proof["source_rows"] += 1
    elif evidence_error == "dlt_load_ids":
        proof["dlt_load_ids"] = ["forged"]
    else:
        proof["season_basis"] = (
            "caller_declared" if source == "sdv_fpi_weekly" else "artifact_field"
        )
    args[4] = json.dumps(proof)

    with pytest.raises(psycopg2.Error, match="evidence|season|stage|parser|row|dlt"):
        publish(conn, tuple(args))
    conn.rollback()
    assert query(conn, f"SELECT count(*) FROM {SOURCES[source]['asset']}") == [(0,)]
    assert current(conn, asset=SOURCES[source]["asset"]) == []
    assert query(conn, "SELECT count(*) FROM meta.flat_file_loads") == [(0,)]


def test_start_rejects_forged_or_noncanonical_plan(batch_db):
    conn, _ = batch_db
    good = plan(conn, "sdv_fpi_weekly")
    for mutation in (
        {"source_name": "sdv_team_xwalk"},
        {"asset_key": "ref.team_id_xwalk"},
        {"coverage_key": "season:2024"},
        {"parser_contract": "wrong"},
        {"unexpected": True},
    ):
        forged = dict(good)
        forged.update(mutation)
        with pytest.raises(psycopg2.Error):
            start_load(conn, forged)
        conn.rollback()
    assert query(conn, "SELECT count(*) FROM meta.operation_runs") == [(0,)]


@pytest.mark.parametrize("source", SOURCES)
def test_rollback_is_atomic_for_rows_receipt_pointer_and_ledger(source, batch_db):
    conn, target = batch_db
    original = publication_args(conn, source, sha="a" * 64)
    publish(conn, original)
    replacement_rows = rows_for(source, suffix="rollback")
    replacement_rows.pop()
    replacement = publication_args(
        conn,
        source,
        rows=replacement_rows,
        sha="b" * 64,
    )
    observer = psycopg2.connect(target)
    try:
        assert publish(conn, replacement, commit=False) == (replacement[1], False, 1)
        assert current(observer, asset=SOURCES[source]["asset"], season=2025) == [
            (SOURCES[source]["asset"], "season:2025", original[1])
        ]
        conn.rollback()
        assert current(observer, asset=SOURCES[source]["asset"], season=2025) == [
            (SOURCES[source]["asset"], "season:2025", original[1])
        ]
        assert query(
            observer,
            "SELECT count(*) FROM meta.asset_receipts WHERE generation_id=%s",
            (replacement[1],),
        ) == [(0,)]
        assert query(
            observer,
            "SELECT count(*) FROM meta.flat_file_loads WHERE file_sha256=%s",
            (replacement[2],),
        ) == [(0,)]
        assert query(observer, f"SELECT count(*) FROM {SOURCES[source]['asset']}") == [(2,)]
    finally:
        observer.close()


@pytest.mark.parametrize("source", SOURCES)
def test_replay_and_same_hash_new_generation_have_distinct_semantics(source, batch_db):
    conn, _ = batch_db
    first = publication_args(conn, source, sha="a" * 64)
    assert publish(conn, first) == (first[1], False, 2)
    second = publication_args(
        conn,
        source,
        rows=rows_for(source, suffix="new"),
        sha="a" * 64,
    )
    assert publish(conn, second) == (second[1], False, 2)
    assert query(
        conn,
        "SELECT status FROM meta.flat_file_loads ORDER BY id",
    ) == [("loaded",), ("skipped",)]

    assert publish(conn, first) == (first[1], True, 2)
    assert current(conn, asset=SOURCES[source]["asset"], season=2025) == [
        (SOURCES[source]["asset"], "season:2025", second[1])
    ]
    assert query(
        conn,
        "SELECT count(*) FROM meta.asset_receipts WHERE asset_key=%s",
        (SOURCES[source]["asset"],),
    ) == [(2,)]


@pytest.mark.parametrize("source", SOURCES)
def test_compare_and_swap_allows_one_concurrent_winner(source, batch_db):
    conn, target = batch_db
    expected = plan(conn, source)
    args = [
        publication_args(
            conn,
            source,
            load_plan=expected,
            rows=rows_for(source, suffix=suffix),
            sha=char * 64,
        )
        for suffix, char in (("one", "a"), ("two", "b"))
    ]

    def contender(call_args):
        other = psycopg2.connect(target)
        try:
            try:
                return "ok", source_query(other, PUBLISH_SQL, call_args)[0]
            except psycopg2.Error as exc:
                other.rollback()
                return "error", str(exc)
        finally:
            other.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(contender, args))
    assert sorted(status for status, _ in results) == ["error", "ok"]
    assert "changed" in next(detail for status, detail in results if status == "error").lower()
    winner = next(detail for status, detail in results if status == "ok")
    assert current(conn, asset=SOURCES[source]["asset"], season=2025) == [
        (SOURCES[source]["asset"], "season:2025", str(winner[0]))
    ]
    assert query(
        conn,
        "SELECT count(*) FROM meta.asset_receipts WHERE asset_key=%s",
        (SOURCES[source]["asset"],),
    ) == [(1,)]
    assert query(conn, "SELECT count(*) FROM meta.flat_file_loads") == [(1,)]


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("outcome", ["failed", "deferred"])
def test_failure_finishes_run_without_rows_ledger_or_pointer(source, outcome, batch_db):
    conn, _ = batch_db
    good = publication_args(conn, source, sha="a" * 64)
    publish(conn, good)
    before = current(conn, asset=SOURCES[source]["asset"], season=2025)

    run_id = start_load(conn, plan(conn, source))
    generation_id = str(uuid.uuid4())
    result = source_query(
        conn,
        "SELECT warehouse_source_batch.fail_source_load(%s,%s,%s)",
        (run_id, generation_id, outcome),
    )[0][0]
    assert str(result) == generation_id
    assert current(conn, asset=SOURCES[source]["asset"], season=2025) == before
    assert query(
        conn,
        """
        SELECT r.outcome,r.published_at,r.coverage->>'complete',r.error_summary,
               o.outcome
        FROM meta.asset_receipts r
        JOIN meta.operation_runs o USING(operation_run_id)
        WHERE r.generation_id=%s
        """,
        (generation_id,),
    ) == [
        (
            outcome,
            None,
            "false",
            "source_publication_failed",
            "blocked" if outcome == "deferred" else outcome,
        )
    ]
    assert query(conn, "SELECT count(*) FROM meta.flat_file_loads") == [(1,)]


def test_publisher_has_only_four_new_rpcs_and_no_relation_or_stage_access(batch_db):
    conn, _ = batch_db
    assert query(
        conn,
        """
        SELECT rolcanlogin,rolinherit,rolsuper,rolcreatedb,rolcreaterole,
               rolreplication,rolbypassrls
        FROM pg_roles WHERE rolname='warehouse_source_publisher'
        """,
    ) == [(False, False, False, False, False, False, False)]

    signatures = (
        "warehouse_source_batch.get_source_plan(text,bigint)",
        "warehouse_source_batch.start_source_load(uuid,jsonb)",
        "warehouse_source_batch.publish_source_load(uuid,uuid,text,jsonb,jsonb)",
        "warehouse_source_batch.fail_source_load(uuid,uuid,text)",
    )
    for signature in signatures:
        privilege = query(
            conn,
            "SELECT has_function_privilege('warehouse_source_publisher',%s,'EXECUTE'),"
            "has_function_privilege('anon',%s,'EXECUTE')",
            (signature, signature),
        )
        assert privilege == [(True, False)], signature
    assert query(
        conn,
        """
        SELECT count(*)
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='warehouse_source_batch'
          AND has_function_privilege('warehouse_source_publisher',p.oid,'EXECUTE')
        """,
    ) == [(4,)]

    for role in ("anon", "authenticated", "analyst_ro", "publication_bystander"):
        denied(
            conn,
            role,
            "SELECT warehouse_source_batch.get_source_plan('sdv_fpi_weekly',2025)",
        )
        assert query(
            conn,
            "SELECT has_schema_privilege(%s,'warehouse_source_batch','USAGE'),"
            "has_schema_privilege(%s,'warehouse_source_stage','USAGE,CREATE')",
            (role, role),
        ) == [(False, False)]

    for relation in (
        "ratings.espn_fpi_weekly",
        "ref.team_id_xwalk",
        "ref.game_id_xwalk",
        "meta.flat_file_loads",
        "meta.asset_receipts",
        "meta.asset_current_generations",
        "meta.operation_runs",
    ):
        assert query(
            conn,
            "SELECT has_table_privilege('warehouse_source_publisher',%s,"
            "'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')",
            (relation,),
        ) == [(False,)]
    denied(
        conn,
        "warehouse_source_publisher",
        "UPDATE ratings.espn_fpi_weekly SET fpi=0",
    )
    denied(conn, "warehouse_source_publisher", "SELECT * FROM meta.asset_receipts")

    for role in ("anon", "authenticated"):
        for relation in (details["asset"] for details in SOURCES.values()):
            privileges = query(
                conn,
                "SELECT has_table_privilege(%s,%s,'SELECT'),"
                "has_table_privilege(%s,%s,'INSERT'),"
                "has_table_privilege(%s,%s,'UPDATE'),"
                "has_table_privilege(%s,%s,'DELETE'),"
                "has_table_privilege(%s,%s,'TRUNCATE')",
                (role, relation, role, relation, role, relation, role, relation, role, relation),
            )
            assert privileges == [(True, False, False, False, False)], (
                role,
                relation,
                privileges,
            )

    # Migration 071 remains callable through its original bounded RPC.
    assert (
        source_query(
            conn,
            "SELECT warehouse_source.get_sdv_ratings_plan(2025)",
        )[0][0]["protocol"]
        == "sdv-ratings-season-v1"
    )


@pytest.mark.parametrize("source", SOURCES)
def test_direct_row_mutation_invalidates_only_affected_source_season(source, batch_db):
    conn, _ = batch_db
    other_source = next(candidate for candidate in SOURCES if candidate != source)
    source_generations = {}
    for season, char in ((2024, "a"), (2025, "b")):
        args = publication_args(
            conn,
            source,
            season=season,
            rows=rows_for(source, season),
            sha=char * 64,
        )
        publish(conn, args)
        source_generations[season] = args[1]
    other = publication_args(conn, other_source, sha="c" * 64)
    publish(conn, other)

    table = SOURCES[source]["asset"]
    query(conn, f"UPDATE {table} SET loaded_at=loaded_at WHERE season=2025")
    assert current(conn, asset=table) == [(table, "season:2024", source_generations[2024])]
    assert current(conn, asset=SOURCES[other_source]["asset"], season=2025) == [
        (SOURCES[other_source]["asset"], "season:2025", other[1])
    ]

    query(conn, f"TRUNCATE {table}")
    assert current(conn, asset=table) == []
    assert current(conn, asset=SOURCES[other_source]["asset"], season=2025) == [
        (SOURCES[other_source]["asset"], "season:2025", other[1])
    ]


@pytest.mark.parametrize(
    "source,statement",
    [
        (
            "sdv_fpi_weekly",
            "ALTER TABLE ratings.espn_fpi_weekly RENAME TO espn_fpi_weekly_moved",
        ),
        ("sdv_team_xwalk", "DROP TABLE ref.team_id_xwalk"),
        (
            "sdv_game_xwalk",
            "DROP TRIGGER invalidate_sdv_source_batch_row_pointer ON ref.game_id_xwalk",
        ),
    ],
)
def test_new_target_ddl_does_not_block_old_sdv_or_elo_publication(source, statement, batch_db):
    conn, _ = batch_db
    new_args = {}
    for candidate, char in zip(SOURCES, "123", strict=True):
        args = publication_args(conn, candidate, sha=char * 64)
        publish(conn, args)
        new_args[candidate] = args
    old_args = ratings_publication_args(conn, sha="b" * 64)
    publish_ratings(conn, old_args)
    elo_args = elo_publication_args(conn)
    publish_elo(conn, elo_args)

    query(conn, statement)
    assert current(conn, asset=SOURCES[source]["asset"]) == []
    for candidate in SOURCES:
        if candidate != source:
            assert current(conn, asset=SOURCES[candidate]["asset"], season=2025) == [
                (
                    SOURCES[candidate]["asset"],
                    "season:2025",
                    new_args[candidate][1],
                )
            ]
    assert current(conn, asset="ratings.sdv_ratings_weekly", season=2025) == [
        ("ratings.sdv_ratings_weekly", "season:2025", old_args[1])
    ]
    assert {
        row[0]
        for row in current(conn)
        if row[0] in {"analytics.house_elo_game", "marts.house_elo_game"}
    } == {"analytics.house_elo_game", "marts.house_elo_game"}

    sibling = next(candidate for candidate in SOURCES if candidate != source)
    sibling_args = publication_args(
        conn,
        sibling,
        rows=rows_for(sibling, suffix="after-ddl"),
        sha="d" * 64,
    )
    assert publish(conn, sibling_args) == (sibling_args[1], False, 2)
    corrected_old = ratings_publication_args(
        conn,
        rows=None,
        sha="c" * 64,
    )
    assert publish_ratings(conn, corrected_old) == (corrected_old[1], False, 2)
    next_elo = elo_publication_args(conn)
    assert publish_elo(conn, next_elo)[:3] == (next_elo[1], next_elo[2], False)


def test_inherited_target_child_invalidates_and_blocks_publication(batch_db):
    conn, _ = batch_db
    first = publication_args(conn, "sdv_team_xwalk")
    publish(conn, first)
    query(
        conn,
        "CREATE TABLE ref.team_id_xwalk_child () INHERITS (ref.team_id_xwalk)",
    )
    query(
        conn,
        """
        INSERT INTO ref.team_id_xwalk_child(
            season,norm_key,xwalk_key,_dlt_load_id,_dlt_id
        ) VALUES (2025,'child','child#None','child-load','child-row')
        """,
    )
    assert current(conn, asset=SOURCES["sdv_team_xwalk"]["asset"]) == []

    retry = publication_args(
        conn,
        "sdv_team_xwalk",
        rows=rows_for("sdv_team_xwalk", suffix="retry"),
        sha="b" * 64,
    )
    with pytest.raises(psycopg2.Error, match="catalog|contract|inherit"):
        publish(conn, retry)
    conn.rollback()
    assert current(conn, asset=SOURCES["sdv_team_xwalk"]["asset"]) == []


@pytest.mark.parametrize(
    "statement",
    [
        """
        DROP TRIGGER invalidate_sdv_source_batch_row_pointer
            ON ratings.espn_fpi_weekly;
        CREATE TRIGGER invalidate_sdv_source_batch_row_pointer
            BEFORE INSERT OR UPDATE OR DELETE ON ratings.espn_fpi_weekly
            FOR EACH ROW WHEN (false)
            EXECUTE FUNCTION warehouse_source_batch.invalidate_row_pointer()
        """,
        """
        CREATE RULE suppress_source_insert AS
            ON INSERT TO ratings.espn_fpi_weekly
            DO INSTEAD NOTHING
        """,
    ],
)
def test_altered_target_guard_or_rewrite_rule_fails_closed_without_sibling_loss(
    statement, batch_db
):
    conn, _ = batch_db
    original = {}
    for source, char in zip(SOURCES, "123", strict=True):
        args = publication_args(conn, source, sha=char * 64)
        publish(conn, args)
        original[source] = args[1]

    query(conn, statement)
    assert current(conn, asset=SOURCES["sdv_fpi_weekly"]["asset"]) == []
    for sibling in ("sdv_team_xwalk", "sdv_game_xwalk"):
        assert current(conn, asset=SOURCES[sibling]["asset"], season=2025) == [
            (SOURCES[sibling]["asset"], "season:2025", original[sibling])
        ]

    retry = publication_args(
        conn,
        "sdv_fpi_weekly",
        rows=rows_for("sdv_fpi_weekly", suffix="untrusted-topology"),
        sha="4" * 64,
    )
    with pytest.raises(psycopg2.Error, match="catalog|contract|guard|trigger|rule"):
        publish(conn, retry)
    conn.rollback()
    for sibling in ("sdv_team_xwalk", "sdv_game_xwalk"):
        assert current(conn, asset=SOURCES[sibling]["asset"], season=2025) == [
            (SOURCES[sibling]["asset"], "season:2025", original[sibling])
        ]


def test_shared_function_ddl_invalidates_all_three_source_pointers(batch_db):
    conn, _ = batch_db
    for source, char in zip(SOURCES, "123", strict=True):
        publish(conn, publication_args(conn, source, sha=char * 64))
    assert (
        len(
            [
                row
                for row in current(conn)
                if row[0] in {details["asset"] for details in SOURCES.values()}
            ]
        )
        == 3
    )

    query(
        conn,
        "ALTER FUNCTION warehouse_source_batch.invalidate_row_pointer() "
        "RENAME TO invalidate_row_pointer_changed",
    )
    assert all(current(conn, asset=details["asset"]) == [] for details in SOURCES.values())


def test_disabled_event_guard_blocks_all_three_publishers(batch_db):
    conn, _ = batch_db
    original = {}
    for source, char in zip(SOURCES, "123", strict=True):
        args = publication_args(conn, source, sha=char * 64)
        publish(conn, args)
        original[source] = args[1]

    # PostgreSQL excludes commands targeting event triggers from
    # ddl_command_start/end. Runtime catalog checks must therefore fail closed.
    query(conn, "ALTER EVENT TRIGGER warehouse_source_batch_invalidate_drop DISABLE")
    for source, char in zip(SOURCES, "456", strict=True):
        retry = publication_args(
            conn,
            source,
            rows=rows_for(source, suffix="disabled-guard"),
            sha=char * 64,
        )
        with pytest.raises(psycopg2.Error, match="catalog|contract|guard"):
            publish(conn, retry)
        conn.rollback()
        assert current(conn, asset=SOURCES[source]["asset"], season=2025) == [
            (SOURCES[source]["asset"], "season:2025", original[source])
        ]


@pytest.mark.parametrize(
    "source,fixture_name",
    [
        ("sdv_fpi_weekly", "sdv_fpi_weekly_sample.parquet"),
        ("sdv_team_xwalk", "sdv_team_xwalk_sample.parquet"),
        ("sdv_game_xwalk", "sdv_game_xwalk_sample.parquet"),
    ],
)
def test_real_adapter_stages_local_parquet_and_publishes_exact_types(
    source, fixture_name, batch_db, monkeypatch, tmp_path
):
    from src.pipelines.sources.flat_files import REGISTRY
    from src.pipelines.utils import flat_file_publication as adapter

    conn, target = batch_db
    schema = pq.read_schema(ROOT / "tests/fixtures/flatfiles" / fixture_name)
    raw_row = {name: None for name in schema.names}
    if source == "sdv_fpi_weekly":
        raw_row.update(
            season=2025,
            season_type=2,
            week=0,
            team_id=333,
            last_updated="2025-08-20T12:30:00Z",
            run_date_time_key=20250820123000,
            snapshot_out_of_sequence=False,
            snapshot_is_contemporaneous=True,
            fpi=18.25,
        )
    elif source == "sdv_team_xwalk":
        raw_row.update(norm_key="all null providers")
    else:
        raw_row.update(
            matchup_key="alpha wolves|beta bears",
            yahoo_date="2026-01-02",
            home_team="Beta Bears",
            away_team="Alpha Wolves",
        )
    local_file = tmp_path / f"{source}.parquet"
    pq.write_table(pa.Table.from_pylist([raw_row], schema=schema), local_file)
    dsn = parse_dsn(target)
    target_url = (
        f"postgresql://{dsn['user']}:{dsn['password']}@{dsn['host']}:{dsn['port']}/{dsn['dbname']}"
    )
    monkeypatch.setattr(adapter, "get_db_url", lambda: target_url)

    result = adapter.run_source_publication(
        REGISTRY[source],
        file_path=str(local_file),
        season=2025,
    )
    assert result["error"] is None
    assert result["status"] == "loaded"
    assert result["rows"] == 1

    if source == "sdv_fpi_weekly":
        row = query(
            conn,
            """
            SELECT season,season_type,week,team_id,last_updated,
                   snapshot_out_of_sequence,snapshot_is_contemporaneous,
                   projectedt,fpi,_dlt_load_id,_dlt_id
            FROM ratings.espn_fpi_weekly
            """,
        )[0]
        assert row[:4] == (2025, 2, 0, 333)
        assert isinstance(row[4], datetime) and row[4].tzinfo is not None
        assert row[5:9] == (False, True, None, 18.25)
    elif source == "sdv_team_xwalk":
        row = query(
            conn,
            """
            SELECT season,norm_key,xwalk_key,espn_team_id,fox_team_id,
                   yahoo_team_id,_dlt_load_id,_dlt_id
            FROM ref.team_id_xwalk
            """,
        )[0]
        assert row[:6] == (
            2025,
            "all null providers",
            "all null providers#None",
            None,
            None,
            None,
        )
    else:
        row = query(
            conn,
            """
            SELECT season,matchup_key,yahoo_date,espn_game_id,espn_date,
                   fox_date,_dlt_load_id,_dlt_id
            FROM ref.game_id_xwalk
            """,
        )[0]
        assert row[:6] == (
            2025,
            "alpha wolves|beta bears",
            date(2026, 1, 2),
            None,
            None,
            None,
        )
    assert all(isinstance(value, str) and value for value in row[-2:])

    receipt = query(
        conn,
        """
        SELECT generation_id::text,input_observations,source_watermark
        FROM meta.asset_receipts WHERE asset_key=%s
        """,
        (SOURCES[source]["asset"],),
    )[0]
    assert receipt[0] == result["generation_id"]
    assert receipt[1]["artifact_origin"] == "local_file"
    assert receipt[1]["season_basis"] == (
        "artifact_field" if source == "sdv_fpi_weekly" else "caller_declared"
    )
    assert receipt[1]["dlt_load_ids"] == [row[-2]]
    assert receipt[2] == result["sha"]
    assert query(
        conn,
        "SELECT count(*) FROM warehouse_source_stage._dlt_loads WHERE load_id=%s",
        (row[-2],),
    ) == [(1,)]
    stage_name = adapter._stage_table_name(source, result["run_id"])
    assert query(
        conn,
        """
        SELECT pg_catalog.to_regclass(%s),
               pg_catalog.to_regnamespace('warehouse_source_stage_staging')
        """,
        (f"warehouse_source_stage.{stage_name}",),
    ) == [(None, None)]
    assert query(
        conn,
        "SELECT source_url,status,row_count FROM meta.flat_file_loads",
    ) == [(None, "loaded", 1)]
