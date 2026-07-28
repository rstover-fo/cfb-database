#!/usr/bin/env python3
"""Screen candidate in-season features for ``features.team_week``.

Pre-registration: decision rule is |partial_r| >= 0.08 AND Benjamini-Hochberg
FDR q = 0.10 across the six candidates, run once; outcome variable is home
margin (home points minus away points); controls are the model's two dominant
existing features as home-minus-away diffs — Elo pregame (resolved from the
``d_elo`` entry in ``DIFF_FEATURE_COLUMNS``) and the substrate's
``adj_epa_net`` column read directly from ``features.team_week`` (the deployed
model carries adjusted EPA as separate off/def diffs, so the net column has no
``DIFF_FEATURE_COLUMNS`` entry).

Trajectory maturity is operationalized as week-row gates (>= 4 prior weeks for
form, >= 2 for volatility), not the substrate's ``MIN_TEAM_PLAYS`` play-count
gate — see the 2026-07-28 entry in the team-week design doc for the disclosed
difference.

The screen is read-only. Candidate values are computed from completed FBS-
involved games and are strictly as-of: a team-week keyed at ``week_index=W``
can use only rows with ``week_index < W`` in the same season. The SQL query
does the warehouse scans and week bucketing; the pure helpers below keep the
leak boundary and the small-sample rules directly testable without a database.
A run whose frame or per-candidate sample falls below the coverage floors
fails loudly as UNTESTABLE rather than reporting rejections.

Control modes:
  - v1 = the pre-registered 2026-07-28 run: Elo + adj_epa_net controls over
    2015-2025.
  - v2 amendment = the same |partial_r| >= 0.08 floor and the same
    Benjamini-Hochberg FDR q=0.10, run once, using a first-order partial
    correlation controlling for fitted_v1's frozen walk-forward expected
    margin; its window is restricted to seasons with a stored prior vintage.

The v2 amendment was pre-registered 2026-07-28 in response to the PR #59
review before any v2 run.
"""

import argparse
import math
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from statistics import pstdev
from typing import Any

from scripts.build_features import (
    POSTSEASON_WEEK_OFFSET,
    compute_week_index,
    leak_free_week_index,
)
from scripts.score_fitted import (
    fetch_available_train_through,
    load_fit,
    score_game,
)
from scripts.screen_preseason_features import (
    FDR_ALPHA,
    MIN_PARTIAL_R,
    benjamini_hochberg,
    complete_cases,
    partial_corr_pvalue,
    partial_correlation,
    screen_verdict,
    second_order_partial_correlation,
)
from scripts.train_model import DIFF_FEATURE_COLUMNS, TEAM_WEEK_SOURCE_COLUMNS

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
CONTROL_MODEL_MARGIN = "model_margin"
SCREEN_CONTROLS = (CONTROL_ELO, CONTROL_ADJ_EPA_NET)
CONTROL_MODE_FEATURES = "features"
CONTROL_MODE_MODEL_MARGIN = "model-margin"
CONTROL_MODES = (CONTROL_MODE_FEATURES, CONTROL_MODE_MODEL_MARGIN)


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

# train_model.py intentionally has no adj_epa_net entry in
# DIFF_FEATURE_COLUMNS: the deployed model carries adjusted EPA as separate
# adj_epa_off/adj_epa_def diffs. The feature-table contract in
# build_features.py still writes the exact net column, so the control reads
# that warehouse column by its literal name.
ADJ_EPA_NET_SOURCE_COLUMN = "adj_epa_net"


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
) -> list[Mapping[str, Any]]:
    """Filter a sorted team bucket to the strict pregame as-of boundary."""
    prior: list[Mapping[str, Any]] = []
    for row in rows:
        week_index = row.get("week_index")
        if week_index is None:
            continue
        week_index = int(week_index)
        if not leak_free_week_index(week_index, as_of_week_index):
            break
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
        _prior_rows(offense_drives, as_of_week_index),
        _prior_rows(defense_drives, as_of_week_index),
    )


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
        volatility = pstdev(values)

    return form, volatility


def compute_bucketed_form_and_volatility(
    weekly_net_epa: Iterable[Mapping[str, Any]],
    as_of_week_index: int,
) -> tuple[float | None, float | None]:
    """Calculate EPA candidates from a sorted, team-specific bucket."""
    prior = [
        row for row in _prior_rows(weekly_net_epa, as_of_week_index) if _finite(row.get("net_epa"))
    ]
    return _form_and_volatility_from_prior(prior)


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
        if CONTROL_MODEL_MARGIN in game:
            row[CONTROL_MODEL_MARGIN] = game[CONTROL_MODEL_MARGIN]
        frame.append(row)

    return frame


def build_model_margin_frame(
    games: Iterable[Mapping[str, Any]],
    drives: Iterable[Mapping[str, Any]],
    weekly_net_epa: Iterable[Mapping[str, Any]],
    fits_by_train_through: Mapping[int, Mapping[str, Any]],
    scorer: Callable[[dict[str, Any], Mapping[str, Any]], tuple[float, float]] = score_game,
) -> list[dict[str, Any]]:
    """Build the v2 frame using each season's frozen prior fitted margin.

    A game is omitted when ``season - 1`` has no stored vintage. ``scorer`` is
    injectable solely to keep this filtering/wiring path pure in unit tests;
    production passes the imported ``score_fitted.score_game`` function.
    """
    scored_games: list[dict[str, Any]] = []
    for game in games:
        train_through = int(game["season"]) - 1
        fit = fits_by_train_through.get(train_through)
        if fit is None:
            continue
        scored_game = dict(game)
        scored_game[CONTROL_MODEL_MARGIN] = float(scorer(scored_game, fit)[0])
        scored_games.append(scored_game)
    return build_screen_frame(scored_games, drives, weekly_net_epa)


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


def screen_frame(
    frame: list[dict[str, Any]], control_mode: str = CONTROL_MODE_FEATURES
) -> list[dict[str, Any]]:
    """Screen each candidate on its own complete-case game sample."""
    if control_mode not in CONTROL_MODES:
        raise ValueError(f"unknown control mode {control_mode!r}")

    controls = SCREEN_CONTROLS if control_mode == CONTROL_MODE_FEATURES else (CONTROL_MODEL_MARGIN,)
    raw_results: list[dict[str, Any]] = []
    for candidate in CANDIDATE_COLUMNS:
        rows = complete_cases(frame, ["home_margin", *controls, candidate])
        n_games = len(rows)
        if n_games < 3:
            partial_r = 0.0
            p_value = None
        else:
            x = [float(row[candidate]) for row in rows]
            y = [float(row["home_margin"]) for row in rows]
            if control_mode == CONTROL_MODE_FEATURES:
                elo = [float(row[CONTROL_ELO]) for row in rows]
                adj_epa_net = [float(row[CONTROL_ADJ_EPA_NET]) for row in rows]
                partial_r = second_order_partial_correlation(x, y, elo, adj_epa_net)
                n_controls = 2
            else:
                model_margin = [float(row[CONTROL_MODEL_MARGIN]) for row in rows]
                partial_r = partial_correlation(x, y, model_margin)
                n_controls = 1
            p_value = partial_corr_pvalue(partial_r, n_games, n_controls=n_controls)
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
_HOME_TEAM_WEEK_COLUMNS = ",\n           ".join(
    f"h.{column} AS home_{column}" for column in TEAM_WEEK_SOURCE_COLUMNS
)
_AWAY_TEAM_WEEK_COLUMNS = ",\n           ".join(
    f"a.{column} AS away_{column}" for column in TEAM_WEEK_SOURCE_COLUMNS
)

GAMES_QUERY = f"""
    SELECT g.id AS game_id, g.season, g.season_type, g.week,
           g.home_team, g.away_team, g.home_points, g.away_points,
           g.neutral_site,
           COALESCE(g.completed, false) AS completed,
           h.{ADJ_EPA_NET_SOURCE_COLUMN} AS home_adj_epa_net,
           a.{ADJ_EPA_NET_SOURCE_COLUMN} AS away_adj_epa_net,
           {_HOME_TEAM_WEEK_COLUMNS},
           {_AWAY_TEAM_WEEK_COLUMNS}
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
           CASE WHEN g.season_type = 'postseason'
                    THEN {POSTSEASON_WEEK_OFFSET} + g.week ELSE g.week END
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
               CASE WHEN g.season_type = 'postseason'
                    THEN {POSTSEASON_WEEK_OFFSET} + g.week ELSE g.week END
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


def _rows_to_screen_games(raw: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Add the nested team-week shape expected by score_fitted.score_game."""
    games: list[dict[str, Any]] = []
    for source in raw:
        row = dict(source)
        row["home_tw"] = {column: row[f"home_{column}"] for column in TEAM_WEEK_SOURCE_COLUMNS}
        row["away_tw"] = {column: row[f"away_{column}"] for column in TEAM_WEEK_SOURCE_COLUMNS}
        games.append(row)
    return games


def fetch_screen_inputs(
    conn: Any, start_season: int, end_season: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Read games, drive rows, and weekly EPA rows without mutating the DB."""
    import psycopg2.extras

    params = {"start_season": start_season, "end_season": end_season}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(GAMES_QUERY, params)
        games = _rows_to_screen_games(cur.fetchall())
        cur.execute(DRIVES_QUERY, params)
        drives = [dict(row) for row in cur.fetchall()]
        cur.execute(EPA_QUERY, params)
        weekly_net_epa = [dict(row) for row in cur.fetchall()]
    return games, drives, weekly_net_epa


def report(
    results: Iterable[Mapping[str, Any]],
    control_mode: str = CONTROL_MODE_FEATURES,
    effective_seasons: Iterable[int] = (),
) -> None:
    """Print mode/window metadata and every candidate, including rejections."""
    seasons = sorted(set(effective_seasons))
    effective_window = f"{seasons[0]}-{seasons[-1]}" if seasons else "none"
    print(
        f"CONTROL_MODE mode={control_mode} effective_window={effective_window} "
        f"seasons={','.join(str(season) for season in seasons) or 'none'}"
    )
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


def load_model_margin_fits(
    conn: Any, start_season: int, end_season: int
) -> dict[int, dict[str, Any]]:
    """Load the frozen prior vintage needed by each requested game season."""
    available = fetch_available_train_through(conn)
    train_through_seasons = sorted(
        train_through
        for train_through in available
        if start_season - 1 <= train_through <= end_season - 1
    )
    return {train_through: load_fit(conn, train_through) for train_through in train_through_seasons}


# Coverage floors: below these, a run is UNTESTABLE, never a rejection. A
# missing or partially built features.team_week would otherwise drain the
# frame and let six hollow REJECT verdicts print on a green exit.
MIN_FRAME_GAMES = 1000
MIN_CANDIDATE_GAMES = 500


def assert_screen_coverage(frame: list[dict[str, Any]], results: list[dict[str, Any]]) -> None:
    """Fail loudly when the frame or any candidate sample is too thin to screen."""
    if len(frame) < MIN_FRAME_GAMES:
        raise RuntimeError(
            f"UNTESTABLE: screen frame has {len(frame)} games "
            f"(< {MIN_FRAME_GAMES}); check features.team_week and core.games "
            "coverage before trusting any verdict"
        )
    thin = [r for r in results if r["n_games"] < MIN_CANDIDATE_GAMES]
    if thin:
        names = ", ".join(f"{r['candidate']} (n={r['n_games']})" for r in thin)
        raise RuntimeError(
            f"UNTESTABLE: candidate sample(s) below {MIN_CANDIDATE_GAMES} games: {names}; "
            "verdicts withheld"
        )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI parser separately so its defaults stay unit-testable."""
    parser = argparse.ArgumentParser(description="Screen in-season team-week features")
    parser.add_argument("--start-season", type=int, default=DEFAULT_START_SEASON)
    parser.add_argument("--end-season", type=int, default=DEFAULT_END_SEASON)
    parser.add_argument(
        "--control-mode",
        choices=CONTROL_MODES,
        default=CONTROL_MODE_FEATURES,
        help="features=pre-registered Elo/EPA controls; model-margin=v2 frozen margin control",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    if args.start_season > args.end_season:
        parser.error("--start-season cannot be after --end-season")

    import psycopg2

    conn = psycopg2.connect(get_db_url())
    try:
        games, drives, weekly_net_epa = fetch_screen_inputs(
            conn, args.start_season, args.end_season
        )
        if args.control_mode == CONTROL_MODE_MODEL_MARGIN:
            fits = load_model_margin_fits(conn, args.start_season, args.end_season)
            frame = build_model_margin_frame(games, drives, weekly_net_epa, fits)
        else:
            frame = build_screen_frame(games, drives, weekly_net_epa)
        results = screen_frame(frame, args.control_mode)
        assert_screen_coverage(frame, results)
        report(results, args.control_mode, (row["season"] for row in frame))
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"screen_week_features: {exc}", file=sys.stderr)
        raise
