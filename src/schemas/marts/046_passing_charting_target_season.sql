-- marts.passing_charting_target_season
-- =============================================================================
-- Passing charting (spec v5.25.0, 2026-08-30 expansion_views unit, task 3).
-- THE HIGHEST-VALUE UNIT IN THIS BATCH: cfb-app has no receiver-grain
-- analysis at all today (api.player_wepa_leaders carries passing/rushing/
-- kicking, no receiving category), so "who is the best receiver" is
-- currently unanswerable from the contracted surface.
--
-- Built from stats.passing_plays (PK game_id, play_id -- play-grain, not
-- pre-aggregated), aggregated here to (season, target_id, team_id).
-- season is stamped on every stats.passing_plays row by
-- passing_plays_resource (`row.setdefault("season", year)`; live-verified
-- zero NULLs 2026-08-31) and is read directly here, so charted plays never
-- depend on core.games having loaded that game -- the previous join to
-- core.games for g.season silently DROPPED any charted play whose game was
-- missing there (PR #81 review finding). offense_id (renamed team_id here)
-- is the numeric CFBD/ESPN team id per stats.passing_plays.offense_id's own
-- column COMMENT (migration 057): "Prefer this over the `offense` name
-- string when joining to ref.teams(id) -- ref.teams has 35 legitimate
-- duplicate school names, so a name-string join needs DISTINCT ON (school)
-- or accepts fanout; offense_id avoids that." ref.teams.id is that table's
-- primary key, so the LEFT JOIN below cannot fan out regardless of the
-- duplicate-school-name issue (id joins never hit it; only name joins do).
--
-- Column names (gameId/playId/targetId/target/outcome/airYards/
-- yardsAfterCatch/parseStatus/offenseId) verified against
-- src/schemas/migrations/057_passing_grants_indexes.sql and the 2026-08-30
-- probe fixture tests/fixtures/cfbd_2026/passing_plays.json -- NOT a fresh
-- pg_attribute read from this session (no DB access here). Re-verify at
-- deploy time.
--
-- Semantics:
--  - targets_charted: total charted plays where this player was the target,
--    regardless of whether air-yards/YAC charting is complete for the play
--    (i.e. this dataset's own denominator, distinct from the two finer
--    denominators below). receptions: targets_charted plays with
--    outcome = 'completion'.
--  - total_air_yards/average_depth_of_target are computed only over plays
--    where air_yards IS NOT NULL (charted); air_yards_charted_plays is that
--    count -- added beyond the literal column list cfb-app's work order
--    named, for the same charting-transparency reason task 2 required two
--    separate denominators on the player-season mart: without it, a
--    leaderboard here would rank on charting coverage, not receiving
--    volume. Same construction for total_yards_after_catch/
--    average_yards_after_catch/yards_after_catch_charted_plays. NULL means
--    not charted; 0 is a real observed value (mirrors migration 057's
--    phrasing). CONTRACT (2026-08-31, cfb-app follow-up):
--    yards_after_catch_charted_plays <= receptions by construction of the
--    game -- YAC exists only on completions (play-level verified: zero
--    YAC-charted plays with outcome <> 'completion' in 14,832) -- so the
--    YAC coverage denominator is receptions, never targets_charted;
--    air-yards coverage stays targets_charted-scoped.
--  - target_share_charted = this target's targets_charted / the SUM of
--    targets_charted across every target on that (season, team_id) --
--    i.e. a share of CHARTED attempts only, named target_share_charted (not
--    target_share) so it is never misread as a true target-share metric
--    computed from the full, uncharted play population.
--  - partial_share = fraction of this target's contributing plays with
--    parse_status = 'partial' -- per migration 057's COMMENT, 'partial' is
--    the only value observed in probing and means a play's air yards/pass
--    depth/direction/location/YAC charting fields may read NULL.
--
-- Data starts 2025 (PASSING_DATA_START in src/pipelines/sources/passing.py).
DROP MATERIALIZED VIEW IF EXISTS marts.passing_charting_target_season CASCADE;

CREATE MATERIALIZED VIEW marts.passing_charting_target_season AS
WITH plays_seasoned AS (
    SELECT
        pp.target_id,
        pp.target,
        pp.offense_id AS team_id,
        pp.season,
        pp.outcome,
        pp.parse_status,
        pp.air_yards,
        pp.yards_after_catch
    FROM stats.passing_plays pp
    WHERE pp.target_id IS NOT NULL
),
target_agg AS (
    SELECT
        season,
        target_id,
        team_id,
        mode() WITHIN GROUP (ORDER BY target) AS target,
        COUNT(*) AS targets_charted,
        COUNT(*) FILTER (WHERE outcome = 'completion') AS receptions,
        SUM(air_yards) AS total_air_yards,
        AVG(air_yards) AS average_depth_of_target,
        COUNT(*) FILTER (WHERE air_yards IS NOT NULL) AS air_yards_charted_plays,
        SUM(yards_after_catch) AS total_yards_after_catch,
        AVG(yards_after_catch) AS average_yards_after_catch,
        COUNT(*) FILTER (WHERE yards_after_catch IS NOT NULL) AS yards_after_catch_charted_plays,
        (COUNT(*) FILTER (WHERE parse_status = 'partial'))::numeric / COUNT(*) AS partial_share
    FROM plays_seasoned
    GROUP BY season, target_id, team_id
)
SELECT
    ta.target_id,
    ta.target,
    ta.season,
    ta.team_id,
    t.school AS team,
    ta.targets_charted,
    ta.receptions,
    ta.total_air_yards,
    ta.average_depth_of_target,
    ta.air_yards_charted_plays,
    ta.total_yards_after_catch,
    ta.average_yards_after_catch,
    ta.yards_after_catch_charted_plays,
    ta.targets_charted::numeric
        / NULLIF(SUM(ta.targets_charted) OVER (PARTITION BY ta.season, ta.team_id), 0)
        AS target_share_charted,
    ta.partial_share
FROM target_agg ta
LEFT JOIN ref.teams t ON t.id = ta.team_id;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.passing_charting_target_season (season, target_id, team_id);

-- Query indexes
CREATE INDEX ON marts.passing_charting_target_season (season, team_id);
CREATE INDEX ON marts.passing_charting_target_season (target_id);
