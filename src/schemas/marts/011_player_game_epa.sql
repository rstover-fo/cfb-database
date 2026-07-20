-- Player EPA attribution per game (rebuilt on stats.play_stats athlete_id)
-- =============================================================================
-- Replaces the previous play_text regex attribution
-- (TRIM(SPLIT_PART(play_text, ' rush '/' pass ', 1))) with CFBD's authoritative
-- athlete-per-play link table stats.play_stats. Each play's EPA (from
-- marts.play_epa) is credited to the athletes CFBD associates with that play,
-- bucketed into a role (passing / rushing / receiving) by stat_type.
--
-- OUTPUT GRAIN: (game_id, team, athlete_id, play_category)
-- OUTPUT COLUMNS (all previous columns preserved; ADDS athlete_id):
--   game_id, season, team, player_name, athlete_id, play_category,
--   plays, total_epa, epa_per_play, success_rate, explosive_plays, total_yards
--
-- CATEGORY NAMING (backward compatible): the PREVIOUS mart already emitted the
-- gerund values 'passing' and 'rushing' as play_category (see git history /
-- the platform-wide convention in api.player_season_leaders which uses
-- passing/rushing/receiving). Those literals -- NOT 'pass'/'rush', which are
-- marts.play_epa's *input* play_category -- are what downstream (012,
-- get_player_game_log, cfb-app) reads. We therefore KEEP 'passing'/'rushing'
-- and ADD 'receiving' for the new receiver role.
--
-- =============================================================================
-- stats.play_stats COLUMNS. The stat_type VALUE DOMAIN used in the mapping
-- below is LIVE-VERIFIED (2026-07-20 presence check against production
-- information_schema: ref.play_stat_types has exactly 26 names). The
-- remaining structural columns are still coded to the loader's yielded shape
-- (src/pipelines/sources/stats.py::play_stats_resource, PK
-- [game_id, play_id, athlete_id, stat_type]); dlt snake_cases the CFBD
-- /plays/stats (PlayStat) fields. If a CREATE fails with "column does not
-- exist", this list is the diff to check against information_schema.columns:
--   game_id      bigint            -> joins marts.play_epa.game_id
--   play_id      varchar           -> joins marts.play_epa.play_id (= core.plays.id)
--   athlete_id   varchar           -> CFBD athleteId (matched to core.roster.id::text)
--   athlete_name varchar           -> CFBD athleteName (surfaced as player_name)
--   stat_type    varchar           -> CFBD statType NAME (matches ref.play_stat_types.name;
--                                      live-verified 26-name domain, 2026-07-20)
--   team         varchar           -> CFBD team/school name (= marts.play_epa.offense
--                                      for offensive players)
--   (unused: season, week, conference, opponent, team_score, opponent_score,
--    drive_id, period, clock__*, yards_to_goal, down, distance, stat)
--
-- DATA DEPTH: stats.play_stats loads ~2014+ (loader year range), while
-- core.plays goes back to 2004. This mart therefore has NO rows before ~2014.
-- The previous regex version covered 2004+; losing pre-2014 player-EPA is the
-- accepted tradeoff (per the Tier 1 analytics-unlock plan) for authoritative,
-- non-fragile attribution.
--
-- =============================================================================
-- STAT_TYPE -> ROLE MAPPING: LIVE-VERIFIED 2026-07-20 via a presence check
-- against production information_schema. ref.play_stat_types has EXACTLY 26
-- names. The mapping below is drawn from that live 26-name catalog:
--   passing (credit the passer):  'Completion', 'Incompletion',
--                                  'Interception Thrown', 'Sack Taken'
--   rushing:                      'Rush'
--   receiving:                    'Reception', 'Target'
-- 'Interception' and 'Sack' (no qualifier) ARE live stat_type names, but the
-- presence check confirms they name the DEFENDER's event (the tackler/
-- intercepting player), not the passer's -- they are deliberately EXCLUDED
-- from this passing mapping rather than included and relied on the guard to
-- filter out.
-- There is no 'Passing Touchdown' / 'Rushing Touchdown' / 'Receiving
-- Touchdown' stat_type in the live catalog. The generic 'Touchdown' stat_type
-- IS live but is deliberately NOT mapped here: scoring plays already carry a
-- Completion/Rush/Reception row (crediting the score via that role), and
-- 'Touchdown' itself is role-ambiguous (it also fires on defensive/special-
-- teams returns), so mapping it risks crediting a return TD to an offensive
-- player.
-- The `ps.team = pe.offense` guard remains as defense-in-depth: only
-- offensive players (passer/rusher/receiver, whose play_stats.team = the
-- play's offense) are ever credited, so a defender-side Sack/Interception row
-- could never leak into an offense's passing EPA even if it were mistakenly
-- added to the VALUES list above.
-- =============================================================================

-- Fail fast rather than run unbounded: a healthy build of this mart takes
-- minutes; if the plan degenerates, die at 30 minutes so the failure is
-- visible instead of a multi-hour hang (see migrations/021_prep_player_epa_build.sql).
SET statement_timeout = '1800s';

-- Prerequisite: the staging table this matview reads history from. DDL is
-- intentionally duplicated from migrations/022_player_epa_staged_build.sql
-- (which also POPULATES it) so this file stands alone in any provisioning
-- order -- a clean-database run_marts.py pass creates the mart with empty
-- history plus the live arm, and the empty-guard below directs the operator
-- to run 022 for the historical seasons.
CREATE TABLE IF NOT EXISTS analytics.player_game_epa_build (
    game_id BIGINT NOT NULL,
    season BIGINT,
    team VARCHAR NOT NULL,
    player_name VARCHAR,
    athlete_id VARCHAR NOT NULL,
    play_category TEXT NOT NULL,
    plays BIGINT,
    total_epa NUMERIC(8, 2),
    epa_per_play NUMERIC(6, 4),
    success_rate NUMERIC(5, 3),
    explosive_plays BIGINT,
    total_yards BIGINT
);
CREATE UNIQUE INDEX IF NOT EXISTS player_game_epa_build_key
    ON analytics.player_game_epa_build (game_id, team, athlete_id, play_category);

DROP MATERIALIZED VIEW IF EXISTS marts.player_game_epa CASCADE;

-- STAGED-BUILD STRUCTURE (2026-07-20): a single-query build of the full
-- 2014+ history exceeds this compute tier (see migrations/
-- 022_player_epa_staged_build.sql, which builds seasons 2014-2025 into
-- analytics.player_game_epa_build one season at a time). This matview reads
-- that static history and computes ONLY the current season (>= 2026) live,
-- so REFRESH stays one-season-sized in perpetuity. HISTORY HORIZON: after
-- the 2026 season, fold 2026 into the staging table (extend 022's loop) and
-- bump both the <= 2025 and >= 2026 bounds here (2027-proofing follow-up).

CREATE MATERIALIZED VIEW marts.player_game_epa AS
SELECT
    game_id, season, team, player_name, athlete_id, play_category,
    plays, total_epa, epa_per_play, success_rate, explosive_plays, total_yards
FROM analytics.player_game_epa_build
WHERE season <= 2025

UNION ALL

SELECT
    game_id, season, team, player_name, athlete_id, play_category,
    plays, total_epa, epa_per_play, success_rate, explosive_plays, total_yards
FROM (
WITH stat_type_roles (stat_type, play_category) AS (
    VALUES
        -- Passer roles -> 'passing' (live-verified 2026-07-20; excludes the
        -- defender-side 'Interception'/'Sack' stat_types -- see header)
        ('Completion',           'passing'),
        ('Incompletion',         'passing'),
        ('Interception Thrown',  'passing'),
        ('Sack Taken',           'passing'),
        -- Rusher roles -> 'rushing'
        ('Rush',                 'rushing'),
        -- Receiver roles -> 'receiving'
        ('Reception',            'receiving'),
        ('Target',               'receiving')
),
role_athletes AS (
    -- One row per (game, play, athlete, team, role), deduped BEFORE the EPA
    -- join. The multiple play_stats rows a single athlete gets for ONE role on
    -- ONE play (e.g. a completed pass fires both 'Reception' and 'Target' --
    -- both receiving-role) collapse here over a narrow all-text/int key, which
    -- is far cheaper than deduping the joined set with its float metric
    -- columns. A player may still earn credit in TWO categories on the same
    -- play (passer + rusher on a trick play): distinct roles, intended.
    SELECT DISTINCT
        ps.game_id,
        ps.play_id,
        ps.athlete_id,
        ps.team,
        r.play_category
    FROM stats.play_stats ps
    JOIN stat_type_roles r ON r.stat_type = ps.stat_type
    WHERE ps.season >= 2026         -- live arm: current season only (history horizon)
),
role_plays AS (
    -- marts.play_epa is unique per play_id, so joining the pre-deduped
    -- role_athletes cannot re-introduce duplicates -- no DISTINCT needed here.
    -- The season floor matches stats.play_stats coverage (~2014+) and prunes
    -- the play_epa scan for the planner.
    SELECT
        pe.game_id,
        pe.season,
        pe.offense AS team,
        ra.athlete_id,
        ra.play_category,
        ra.play_id,
        pe.epa,
        pe.success,
        pe.explosive,
        pe.yards_gained
    FROM role_athletes ra
    JOIN marts.play_epa pe
        ON pe.game_id = ra.game_id
       AND pe.play_id = ra.play_id
    WHERE ra.team = pe.offense      -- credit only the offense (see header)
      AND NOT pe.is_garbage_time
      AND pe.season >= 2026         -- live arm: current season only (history horizon)
),
athlete_names AS (
    -- One display name per (game, athlete). play_stats.athlete_name repeats
    -- across a game; MAX() yields a single stable value and keeps this a strict
    -- 1:1 join against role_plays so it can never fan out the play counts.
    SELECT
        game_id,
        athlete_id,
        MAX(athlete_name) AS player_name
    FROM stats.play_stats
    WHERE season >= 2026            -- live arm: current season only
    GROUP BY game_id, athlete_id
)
SELECT
    rp.game_id,
    rp.season,
    rp.team,
    MAX(an.player_name) AS player_name,
    rp.athlete_id,
    rp.play_category,
    COUNT(*) AS plays,
    SUM(rp.epa)::NUMERIC(8, 2) AS total_epa,
    AVG(rp.epa)::NUMERIC(6, 4) AS epa_per_play,
    AVG(rp.success)::NUMERIC(5, 3) AS success_rate,
    SUM(rp.explosive) AS explosive_plays,
    SUM(rp.yards_gained) AS total_yards
FROM role_plays rp
LEFT JOIN athlete_names an
    ON an.game_id = rp.game_id
   AND an.athlete_id = rp.athlete_id
GROUP BY rp.game_id, rp.season, rp.team, rp.athlete_id, rp.play_category
HAVING COUNT(*) >= 3   -- Minimum 3 plays per player/game/category
) AS live_current_season;

-- Unique index: rekeyed on athlete_id (required for REFRESH CONCURRENTLY).
CREATE UNIQUE INDEX ON marts.player_game_epa (game_id, team, athlete_id, play_category);
-- Non-unique name-path index retained for backward-compatible name lookups.
CREATE INDEX ON marts.player_game_epa (player_name, season);
CREATE INDEX ON marts.player_game_epa (team, season);
CREATE INDEX ON marts.player_game_epa (total_epa DESC);

-- Empty-guard: stats.play_stats (gate table, ~2014+) backs this mart. If it is
-- absent/empty the mart materializes to zero rows; fail loudly at deploy time
-- rather than silently serving an empty player-EPA surface downstream.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.player_game_epa) = 0 THEN
        RAISE EXCEPTION 'marts.player_game_epa is empty: run migrations/022_player_epa_staged_build.sql to populate the analytics.player_game_epa_build history table (and backfill stats.play_stats first if it is empty), then refresh this mart. If both are populated, the stat_type/team assumptions in this file no longer match the live table (see header comment).';
    END IF;
END $$;
