#!/usr/bin/env python3
# ruff: noqa: E501
"""Screen candidate in-season features for ``features.team_week``.

Pre-registration: decision rule is |partial_r| >= 0.08 AND Benjamini-Hochberg FDR q = 0.10 across the six candidates, run once; outcome variable is home margin (home points minus away points); controls are the model's two dominant existing features as home-minus-away diffs — Elo pregame and adjusted-EPA net — read from features.team_week (resolve the exact column names from DIFF_FEATURE_COLUMNS in scripts/train_model.py: the elo entry and the adjusted-EPA net entry).

The screen is read-only. Candidate values are computed from completed FBS-
involved games and are strictly as-of: a team-week keyed at ``week_index=W``
can use only rows with ``week_index < W`` in the same season. The SQL query
does the warehouse scans and week bucketing; the pure helpers below keep the
leak boundary and the small-sample rules directly testable without a database.
"""

import argparse
import math
import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

from scripts.build_features import compute_week_index, leak_free_week_index
from scripts.screen_preseason_features import (
    FDR_ALPHA,
    MIN_PARTIAL_R,
    benjamini_hochberg,
    complete_cases,
    partial_corr_pvalue,
    screen_verdict,
    second_order_partial_correlation,
)
from scripts.train_model import DIFF_FEATURE_COLUMNS

DEFAULT_START_SEASON = 2015
DEFAULT_END_SEASON = 2025

CANDIDATE_COLUMNS = (
    "off_ppd",
    "def_ppd_allowed",
    "off_field_pos",
    "def_field_pos_allowed",
    "form_net_epa_last4",
    "vol_net_epa",
)

CONTROL_ELO = "elo_diff"
CONTROL_ADJ_EPA_NET = "adj_epa_net_diff"
SCREEN_CONTROLS = (CONTROL_ELO, CONTROL_ADJ_EPA_NET)


def _diff_source_column(feature_name: str) -> str:
    """Resolve a named model diff from the training feature contract."""
    matches = [column for name, column in DIFF_FEATURE_COLUMNS if name == feature_name]
    if len(matches) != 1:
        raise RuntimeError(
            f"DIFF_FEATURE_COLUMNS must contain exactly one {feature_name!r} entry; "
            f"found {matches!r}"
        )
    return matches[0]


ELO_SOURCE_COLUMN = _diff_source_column("d_elo")

# The current train_model.py intentionally excludes adj_epa_net from
# DIFF_FEATURE_COLUMNS because the deployed model uses adj_epa_off and
# adj_epa_def separately. The feature-table contract in build_features.py
# still writes the exact net column, so use that confirmed warehouse name and
# retain this mismatch as an explicit module-level fact rather than silently
# selecting one of the two component columns.
_ADJ_EPA_NET_ENTRIES = [
    column
    for name, column in DIFF_FEATURE_COLUMNS
    if name in {"d_adj_epa_net", "adj_epa_net"} or column == "adj_epa_net"
]
ADJ_EPA_NET_SOURCE_COLUMN = _ADJ_EPA_NET_ENTRIES[0] if _ADJ_EPA_NET_ENTRIES else "adj_epa_net"
ADJ_EPA_NET_ENTRY_MISSING_FROM_DIFF_CONTRACT = not bool(_ADJ_EPA_NET_ENTRIES)


def _finite(value: Any) -> bool:
    """Whether a database or test value can safely enter a numeric aggregate."""
    return value is not None and math.isfinite(float(value))


def _mean(values: Iterable[float]) -> float | None:
    values = list(values)
    return sum(values) / len(values) if values else None


def drive_points(drive: Mapping[str, Any]) -> float | None:
    """Return exact points scored during a drive from its score delta."""
    start = drive.get("start_offense_score")
    end = drive.get("end_offense_score")
    if not (_finite(start) and _finite(end)):
        return None
    return float(end) - float(start)


def _prior_rows(
    rows: Iterable[Mapping[str, Any]],
    as_of_week_index: int,
    season: int | None = None,
    sorted_by_week_index: bool = False,
) -> list[Mapping[str, Any]]:
    """Filter rows to the same season and strict pregame as-of boundary.

    Grouped buckets are sorted, so they can stop at the first current/future
    row. Raw helper callers leave the flag false and are scanned completely.
    """
    prior: list[Mapping[str, Any]] = []
    for row in rows:
        week_index = row.get("week_index")
        if week_index is None:
            continue
        week_index = int(week_index)
        if sorted_by_week_index and not leak_free_week_index(week_index, as_of_week_index):
            break
        if season is not None and row.get("season") != season:
            continue
        if leak_free_week_index(week_index, as_of_week_index):
            prior.append(row)
    return prior


def group_screen_inputs(
    drives: Iterable[Mapping[str, Any]],
    weekly_net_epa: Iterable[Mapping[str, Any]],
) -> tuple[
    dict[tuple[int, str], list[Mapping[str, Any]]],
    dict[tuple[int, str], list[Mapping[str, Any]]],
    dict[tuple[int, str], list[Mapping[str, Any]]],
]:
    """Bucket fetched rows once by ``(season, team)`` and sort each bucket.

    The offense and defense drive indexes share each source drive row, while
    the EPA index receives one row per team's weekly net EPA. Grouping keeps a
    later game lookup local to the two teams in that game instead of rescanning
    every season's raw rows for every game.
    """
    offense_by_team: defaultdict[tuple[int, str], list[Mapping[str, Any]]] = defaultdict(list)
    defense_by_team: defaultdict[tuple[int, str], list[Mapping[str, Any]]] = defaultdict(list)
    weekly_by_team: defaultdict[tuple[int, str], list[Mapping[str, Any]]] = defaultdict(list)

    for drive in drives:
        season = drive.get("season")
        week_index = drive.get("week_index")
        if season is None or week_index is None:
            continue
        if drive.get("offense") is not None:
            offense_by_team[(int(season), str(drive["offense"]))].append(drive)
        if drive.get("defense") is not None:
            defense_by_team[(int(season), str(drive["defense"]))].append(drive)

    for row in weekly_net_epa:
        season = row.get("season")
        team = row.get("team")
        if season is None or team is None or row.get("week_index") is None:
            continue
        weekly_by_team[(int(season), str(team))].append(row)

    for buckets in (offense_by_team, defense_by_team, weekly_by_team):
        for rows in buckets.values():
            rows.sort(key=lambda row: int(row["week_index"]))

    return dict(offense_by_team), dict(defense_by_team), dict(weekly_by_team)


def _aggregate_drive_sides(
    offense_drives: Iterable[Mapping[str, Any]],
    defense_drives: Iterable[Mapping[str, Any]],
) -> dict[str, float | None]:
    """Aggregate already team-bucketed offense and defense drive rows."""

    def _ppd(rows: Iterable[Mapping[str, Any]]) -> float | None:
        points = [points for row in rows if (points := drive_points(row)) is not None]
        return _mean(points)

    def _field_position(rows: Iterable[Mapping[str, Any]]) -> float | None:
        positions = [
            float(row["start_yards_to_goal"])
            for row in rows
            if _finite(row.get("start_yards_to_goal"))
        ]
        return _mean(positions)

    offense_drives = list(offense_drives)
    defense_drives = list(defense_drives)
    return {
        "off_ppd": _ppd(offense_drives) if offense_drives else None,
        "def_ppd_allowed": _ppd(defense_drives) if defense_drives else None,
        "off_field_pos": _field_position(offense_drives) if offense_drives else None,
        "def_field_pos_allowed": (_field_position(defense_drives) if defense_drives else None),
    }


def aggregate_bucketed_drive_features(
    as_of_week_index: int,
    offense_drives: Iterable[Mapping[str, Any]],
    defense_drives: Iterable[Mapping[str, Any]],
) -> dict[str, float | None]:
    """Aggregate only the two pre-grouped drive buckets for one team."""
    return _aggregate_drive_sides(
        _prior_rows(offense_drives, as_of_week_index, sorted_by_week_index=True),
        _prior_rows(defense_drives, as_of_week_index, sorted_by_week_index=True),
    )


def aggregate_drive_features(
    team: str,
    as_of_week_index: int,
    drives: Iterable[Mapping[str, Any]],
    season: int | None = None,
) -> dict[str, float | None]:
    """Aggregate the four drive candidates for one team before a week.

    The same drive can contribute to the offense and defense aggregates, but
    each side is counted only once in its corresponding points-per-drive and
    field-position mean. Missing score or field-position values are omitted
    from that metric; if the team has no prior drive rows, all four values are
    ``None`` as required by the maturity rule.
    """
    prior = _prior_rows(drives, as_of_week_index, season=season)
    offense_drives = [row for row in prior if row.get("offense") == team]
    defense_drives = [row for row in prior if row.get("defense") == team]
    return _aggregate_drive_sides(offense_drives, defense_drives)


def _form_and_volatility_from_prior(
    prior: Iterable[Mapping[str, Any]],
) -> tuple[float | None, float | None]:
    """Calculate both EPA candidates from already filtered prior rows."""
    values = [float(row["net_epa"]) for row in prior]

    form = None
    if len(values) >= 4:
        form = float(_mean(values[-4:]) - _mean(values))

    volatility = None
    if len(values) >= 2:
        mean = sum(values) / len(values)
        volatility = math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))

    return form, volatility


def compute_form_and_volatility(
    weekly_net_epa: Iterable[Mapping[str, Any]],
    as_of_week_index: int,
) -> tuple[float | None, float | None]:
    """Return last-four-minus-season-mean form and population volatility.

    The input is already at ``(team, season, week_index)`` grain. The strict
    ``week_index < as_of_week_index`` filter is applied here as a second line
    of defense so callers cannot accidentally include the current game.
    """
    prior = [
        row for row in _prior_rows(weekly_net_epa, as_of_week_index) if _finite(row.get("net_epa"))
    ]
    prior.sort(key=lambda row: int(row["week_index"]))
    return _form_and_volatility_from_prior(prior)


def compute_bucketed_form_and_volatility(
    weekly_net_epa: Iterable[Mapping[str, Any]],
    as_of_week_index: int,
) -> tuple[float | None, float | None]:
    """Calculate EPA candidates from a sorted, team-specific bucket."""
    prior = [
        row
        for row in _prior_rows(
            weekly_net_epa,
            as_of_week_index,
            sorted_by_week_index=True,
        )
        if _finite(row.get("net_epa"))
    ]
    return _form_and_volatility_from_prior(prior)


def compute_as_of_features(
    team: str,
    as_of_week_index: int,
    drives: Iterable[Mapping[str, Any]],
    weekly_net_epa: Iterable[Mapping[str, Any]],
    season: int | None = None,
) -> dict[str, float | None]:
    """Build all six candidate values for one team-week as-of row."""
    result = aggregate_drive_features(team, as_of_week_index, drives, season=season)
    form, volatility = compute_form_and_volatility(
        [row for row in weekly_net_epa if season is None or row.get("season") == season],
        as_of_week_index,
    )
    result["form_net_epa_last4"] = form
    result["vol_net_epa"] = volatility
    return result


def compute_bucketed_as_of_features(
    as_of_week_index: int,
    offense_drives: Iterable[Mapping[str, Any]],
    defense_drives: Iterable[Mapping[str, Any]],
    weekly_net_epa: Iterable[Mapping[str, Any]],
) -> dict[str, float | None]:
    """Build candidates from the three pre-grouped rows for one team."""
    result = aggregate_bucketed_drive_features(as_of_week_index, offense_drives, defense_drives)
    form, volatility = compute_bucketed_form_and_volatility(weekly_net_epa, as_of_week_index)
    result["form_net_epa_last4"] = form
    result["vol_net_epa"] = volatility
    return result


def _difference(home: float | None, away: float | None) -> float | None:
    """Home-minus-away difference that preserves candidate NULLs."""
    if home is None or away is None:
        return None
    return float(home) - float(away)


def build_screen_frame(
    games: Iterable[Mapping[str, Any]],
    drives: Iterable[Mapping[str, Any]],
    weekly_net_epa: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Join pure as-of candidates and controls into one row per game."""
    drives = list(drives)
    weekly_net_epa = list(weekly_net_epa)
    offense_by_team, defense_by_team, weekly_by_team = group_screen_inputs(drives, weekly_net_epa)
    frame: list[dict[str, Any]] = []

    for game in games:
        if not game.get("completed", True):
            continue
        if game.get("home_points") is None or game.get("away_points") is None:
            continue

        season = int(game["season"])
        as_of_week_index = compute_week_index(int(game["week"]), game["season_type"])
        home_key = (season, str(game["home_team"]))
        away_key = (season, str(game["away_team"]))
        home_features = compute_bucketed_as_of_features(
            as_of_week_index,
            offense_by_team.get(home_key, ()),
            defense_by_team.get(home_key, ()),
            weekly_by_team.get(home_key, ()),
        )
        away_features = compute_bucketed_as_of_features(
            as_of_week_index,
            offense_by_team.get(away_key, ()),
            defense_by_team.get(away_key, ()),
            weekly_by_team.get(away_key, ()),
        )

        row: dict[str, Any] = {
            "game_id": game.get("game_id"),
            "season": season,
            "home_margin": float(game["home_points"]) - float(game["away_points"]),
            CONTROL_ELO: _difference(game.get("home_elo_pregame"), game.get("away_elo_pregame")),
            CONTROL_ADJ_EPA_NET: _difference(
                game.get("home_adj_epa_net"), game.get("away_adj_epa_net")
            ),
        }
        row.update(
            {
                candidate: _difference(home_features[candidate], away_features[candidate])
                for candidate in CANDIDATE_COLUMNS
            }
        )
        frame.append(row)

    return frame


def apply_verdicts(results: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Add BH q-values, both pass flags, and an uppercase final verdict."""
    rows = [dict(row) for row in results]
    testable = [row for row in rows if row.get("p_value") is not None]
    qvalues = benjamini_hochberg([float(row["p_value"]) for row in testable])
    for row, qvalue in zip(testable, qvalues, strict=True):
        row["bh_q"] = qvalue

    for row in rows:
        row.setdefault("bh_q", None)
        partial_r = float(row.get("partial_r", 0.0))
        floor_pass = abs(partial_r) >= MIN_PARTIAL_R
        bh_pass = row["bh_q"] is not None and row["bh_q"] <= FDR_ALPHA
        row["floor_pass"] = floor_pass
        row["bh_pass"] = bh_pass
        row["verdict"] = "SHIP" if screen_verdict(partial_r, row["bh_q"]) == "ship" else "REJECT"
    return rows


def screen_frame(frame: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Screen each candidate on its own complete-case game sample."""
    raw_results: list[dict[str, Any]] = []
    needed_controls = list(SCREEN_CONTROLS)
    for candidate in CANDIDATE_COLUMNS:
        rows = complete_cases(frame, ["home_margin", *needed_controls, candidate])
        n_games = len(rows)
        if n_games < 3:
            partial_r = 0.0
            p_value = None
        else:
            x = [float(row[candidate]) for row in rows]
            y = [float(row["home_margin"]) for row in rows]
            elo = [float(row[CONTROL_ELO]) for row in rows]
            adj_epa_net = [float(row[CONTROL_ADJ_EPA_NET]) for row in rows]
            partial_r = second_order_partial_correlation(x, y, elo, adj_epa_net)
            p_value = partial_corr_pvalue(partial_r, n_games, n_controls=2)
        raw_results.append(
            {
                "candidate": candidate,
                "n_games": n_games,
                "partial_r": partial_r,
                "p_value": p_value,
            }
        )
    return apply_verdicts(raw_results)


FBS_INVOLVED = "(g.home_classification = 'fbs' OR g.away_classification = 'fbs')"

GAMES_QUERY = f"""
    SELECT g.id AS game_id, g.season, g.season_type, g.week,
           g.home_team, g.away_team, g.home_points, g.away_points,
           COALESCE(g.completed, false) AS completed,
           h.{ELO_SOURCE_COLUMN} AS home_elo_pregame,
           a.{ELO_SOURCE_COLUMN} AS away_elo_pregame,
           h.{ADJ_EPA_NET_SOURCE_COLUMN} AS home_adj_epa_net,
           a.{ADJ_EPA_NET_SOURCE_COLUMN} AS away_adj_epa_net
    FROM core.games g
    JOIN features.team_week h
      ON h.game_id = g.id AND h.team = g.home_team
    JOIN features.team_week a
      ON a.game_id = g.id AND a.team = g.away_team
    WHERE g.season BETWEEN %(start_season)s AND %(end_season)s
      AND COALESCE(g.completed, false)
      AND g.home_points IS NOT NULL
      AND g.away_points IS NOT NULL
      AND {FBS_INVOLVED}
    ORDER BY g.season, g.week, g.id
"""

DRIVES_QUERY = f"""
    SELECT g.season, d.game_id, d.offense, d.defense,
           CASE WHEN g.season_type = 'postseason' THEN 100 + g.week ELSE g.week END
               AS week_index,
           d.start_yards_to_goal, d.start_offense_score, d.end_offense_score
    FROM core.drives d
    JOIN core.games g ON g.id = d.game_id
    WHERE g.season BETWEEN %(start_season)s AND %(end_season)s
      AND COALESCE(g.completed, false)
      AND g.home_points IS NOT NULL
      AND g.away_points IS NOT NULL
      AND {FBS_INVOLVED}
"""

EPA_QUERY = f"""
    WITH plays_wi AS (
        SELECT g.season, pe.offense, pe.defense, pe.epa,
               CASE WHEN g.season_type = 'postseason' THEN 100 + g.week ELSE g.week END
                   AS week_index
        FROM marts.play_epa pe
        JOIN core.games g ON g.id = pe.game_id
        WHERE g.season BETWEEN %(start_season)s AND %(end_season)s
          AND COALESCE(g.completed, false)
          AND {FBS_INVOLVED}
          AND NOT pe.is_garbage_time
    ),
    off_week_agg AS (
        SELECT season, offense AS team, week_index,
               SUM(epa) AS sum_epa, COUNT(*) AS n_plays
        FROM plays_wi
        GROUP BY season, offense, week_index
    ),
    def_week_agg AS (
        SELECT season, defense AS team, week_index,
               SUM(epa) AS sum_epa, COUNT(*) AS n_plays
        FROM plays_wi
        GROUP BY season, defense, week_index
    )
    SELECT o.season, o.team, o.week_index,
           o.sum_epa / NULLIF(o.n_plays, 0)
               - d.sum_epa / NULLIF(d.n_plays, 0) AS net_epa
    FROM off_week_agg o
    JOIN def_week_agg d
      ON d.season = o.season
     AND d.team = o.team
     AND d.week_index = o.week_index
    ORDER BY o.season, o.team, o.week_index
"""


def get_db_url() -> str:
    """Resolve the warehouse URL using the build_features.py convention."""
    import os

    import dlt

    url = None
    try:
        creds = dlt.secrets.get("destination.postgres.credentials")
        if creds:
            url = str(creds)
    except Exception:
        pass

    if not url:
        url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")

    if not url:
        raise RuntimeError(
            "No database URL found. Set destination.postgres.credentials in "
            ".dlt/secrets.toml or SUPABASE_DB_URL environment variable."
        )
    return url


def fetch_screen_inputs(
    conn: Any, start_season: int, end_season: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Read games, drive rows, and weekly EPA rows without mutating the DB."""
    import psycopg2.extras

    params = {"start_season": start_season, "end_season": end_season}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(GAMES_QUERY, params)
        games = [dict(row) for row in cur.fetchall()]
        cur.execute(DRIVES_QUERY, params)
        drives = [dict(row) for row in cur.fetchall()]
        cur.execute(EPA_QUERY, params)
        weekly_net_epa = [dict(row) for row in cur.fetchall()]
    return games, drives, weekly_net_epa


def report(results: Iterable[Mapping[str, Any]]) -> None:
    """Print every candidate, including rejected candidates and their numbers."""
    print(
        "candidate                 n_games   partial_r   p_value     bh_q  "
        "bh_pass floor_pass verdict"
    )
    print("-" * 96)
    for row in results:
        p_value = "na" if row["p_value"] is None else f"{row['p_value']:.6g}"
        bh_q = "na" if row["bh_q"] is None else f"{row['bh_q']:.6g}"
        print(
            f"{row['candidate']:<25} {row['n_games']:>7} {row['partial_r']:>11.5f} "
            f"{p_value:>10} {bh_q:>8} {str(row['bh_pass']):>8} "
            f"{str(row['floor_pass']):>10} {row['verdict']:>7}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Screen in-season team-week features")
    parser.add_argument("--start-season", type=int, default=DEFAULT_START_SEASON)
    parser.add_argument("--end-season", type=int, default=DEFAULT_END_SEASON)
    args = parser.parse_args()
    if args.start_season > args.end_season:
        parser.error("--start-season cannot be after --end-season")

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        games, drives, weekly_net_epa = fetch_screen_inputs(
            conn, args.start_season, args.end_season
        )
        frame = build_screen_frame(games, drives, weekly_net_epa)
        report(screen_frame(frame))
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"screen_week_features: {exc}", file=sys.stderr)
        raise
