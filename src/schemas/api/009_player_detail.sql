-- Player detail API view
-- Comprehensive player profile: roster info, recruiting, season stats (pass/rush/rec/def), and PPA
-- Exposed via PostgREST as /api/player_detail
-- Extracted from deployed Supabase database on 2026-02-06
--
-- 2026-08-30 (expansion_views unit, task 0 -- BUG FIX): the recruiting join
-- fanned out for players carrying more than one recruiting.recruits row for
-- the same athlete_id (reclassifications: a player signed in one class year
-- and re-ranked/re-classed into another keeps BOTH rows -- recruiting.recruits'
-- primary key is its own `id`, not athlete_id, per src/pipelines/sources/
-- recruiting.py). The original join (`rec.athlete_id::text = r.id::text`,
-- no year filter) matched every one of a player's recruiting rows against
-- every one of their roster-season rows, duplicating every stat column
-- verbatim. Verified live by cfb-app (player_id 5079720, season 2025: 2 rows
-- differing only in stars/recruit_rating/national_ranking/recruit_class).
-- Under 1% of players per season (2020: 39/16,419 ... 2025: 14/30,004) but
-- concentrated in blue-chip reclassification cases -- exactly who gets asked
-- about most -- and SUM(rec_yds)-style aggregation over this view silently
-- double-counts them.
--
-- Fix: `recruit_dedup` collapses recruiting.recruits to one row per
-- athlete_id via DISTINCT ON, keeping the row with the HIGHEST recruit
-- class year (a reclassification's later year is the authoritative,
-- final-status entry -- CFBD does not retract the earlier row). A player's
-- recruiting pedigree (stars/rating/ranking/class) is a property of the
-- person, not of a given playing season, so every roster-season row for
-- that player_id now joins to the SAME single recruiting row -- this is a
-- deliberate 1-to-1-on-athlete_id dedupe, not a (player_id, season, team)
-- key on the recruiting side (recruiting.recruits carries neither a
-- matching `season` nor `team` column to key on), and it is what makes the
-- final view exactly one row per (player_id, season, team) -- roster's own
-- grain -- again.
--
-- 2026-08-30 (expansion_views unit, task 6 -- additive): LEFT JOIN LATERAL a
-- compact stats.player_season_overview payload (games, usage, PPA-overview).
-- Column names LIVE-VERIFIED 2026-08-30 via information_schema.columns
-- against stats.player_season_overview, including a check for dlt VARIANT
-- (__v_double) twins on every bigint-typed column -- the same pattern
-- codified in src/schemas/009_variant_columns.sql and marts/032_player_usage.sql
-- (dlt creates a `<col>__v_double` sibling when the API returns a float for
-- a column it first inferred as bigint from an earlier integer value; the
-- float-typed rows land in the twin and the base column reads NULL for
-- them unless the two are COALESCEd). Result:
--   - games: integer by nature (a games-played count), no twin possible.
--   - usage__overall, usage__pass, ppa__average__all, ppa__total__all: all
--     natively double precision on this table -- no twin exists, read
--     directly.
--   - usage__rush: bigint base column WITH a __v_double twin -- live,
--     155/249 non-null values are stranded in usage__rush__v_double alone.
--     COALESCEd below; reading the base column alone would have silently
--     NULLed out the majority of non-null values.
--
-- The overview table's primary key is being changed to (season, id, team)
-- in a parallel diff (src/pipelines/sources/player_overview.py, same day) --
-- this join does not depend on which PK shape is live: LEFT JOIN LATERAL
-- ... ORDER BY (overview.team = player_detail.team) DESC LIMIT 1 always
-- returns at most one row (preferring a team-matched stint, falling back to
-- any stint for that player-season if team doesn't match or is NULL), so it
-- cannot fan out player_detail regardless of how many team-stints a given
-- (season, id) has on the overview table.

CREATE OR REPLACE VIEW api.player_detail AS
WITH player_passing AS (
    SELECT
        player_season_stats.player_id,
        player_season_stats.season,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'ATT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_att,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'COMPLETIONS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_cmp,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YDS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_yds,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_td,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'INT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_int,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'PCT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_pct
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'passing'::text
    GROUP BY player_season_stats.player_id, player_season_stats.season
), player_rushing AS (
    SELECT
        player_season_stats.player_id,
        player_season_stats.season,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'CAR'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rush_car,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YDS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rush_yds,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rush_td,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YPC'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rush_ypc
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'rushing'::text
    GROUP BY player_season_stats.player_id, player_season_stats.season
), player_receiving AS (
    SELECT
        player_season_stats.player_id,
        player_season_stats.season,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'REC'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rec,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YDS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rec_yds,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rec_td,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'YPR'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS rec_ypr
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'receiving'::text
    GROUP BY player_season_stats.player_id, player_season_stats.season
), player_defense AS (
    SELECT
        player_season_stats.player_id,
        player_season_stats.season,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TOT'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS tackles,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'SACKS'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS sacks,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'TFL'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS tfl,
        max(
            CASE
                WHEN player_season_stats.stat_type::text = 'PD'::text THEN player_season_stats.stat::numeric
                ELSE NULL::numeric
            END) AS pass_def
    FROM stats.player_season_stats
    WHERE player_season_stats.category::text = 'defensive'::text
    GROUP BY player_season_stats.player_id, player_season_stats.season
), recruit_dedup AS (
    -- Task 0 fix: one row per athlete_id (recruiting.recruits' PK is its own
    -- `id`, not athlete_id -- see file header). ORDER BY year DESC keeps the
    -- reclassified/authoritative entry when an athlete has more than one
    -- recruiting-class row. NULLS LAST is required: Postgres defaults DESC
    -- to NULLS FIRST, so without it a recruiting row with a NULL year would
    -- outrank a real recruit_class year instead of losing to it.
    SELECT DISTINCT ON (athlete_id)
        athlete_id,
        stars,
        rating,
        ranking,
        year
    FROM recruiting.recruits
    ORDER BY athlete_id, year DESC NULLS LAST
)
SELECT
    r.id AS player_id,
    (r.first_name::text || ' '::text) || r.last_name::text AS name,
    r.team,
    r."position",
    r.year AS season,
    r.height,
    r.weight,
    r.jersey,
    r.home_city,
    r.home_state,
    rec.stars,
    rec.rating AS recruit_rating,
    rec.ranking AS national_ranking,
    rec.year AS recruit_class,
    pp.pass_att,
    pp.pass_cmp,
    pp.pass_yds,
    pp.pass_td,
    pp.pass_int,
    pp.pass_pct,
    pr.rush_car,
    pr.rush_yds,
    pr.rush_td,
    pr.rush_ypc,
    prv.rec,
    prv.rec_yds,
    prv.rec_td,
    prv.rec_ypr,
    pd.tackles,
    pd.sacks,
    pd.tfl,
    pd.pass_def,
    ppa.average_ppa__all AS ppa_avg,
    ppa.total_ppa__all AS ppa_total,
    -- Task 6 additions (additive; see file header for join-safety rationale)
    ov.games,
    ov.usage_overall,
    ov.usage_pass,
    ov.usage_rush,
    ov.ppa_overview_avg,
    ov.ppa_overview_total
FROM core.roster r
LEFT JOIN recruit_dedup rec ON rec.athlete_id::text = r.id::text
LEFT JOIN player_passing pp ON pp.player_id::text = r.id::text AND pp.season = r.year
LEFT JOIN player_rushing pr ON pr.player_id::text = r.id::text AND pr.season = r.year
LEFT JOIN player_receiving prv ON prv.player_id::text = r.id::text AND prv.season = r.year
LEFT JOIN player_defense pd ON pd.player_id::text = r.id::text AND pd.season = r.year
LEFT JOIN metrics.ppa_players_season ppa ON ppa.id::text = r.id::text AND ppa.season = r.year
LEFT JOIN LATERAL (
    SELECT
        pso.games,
        pso.usage__overall AS usage_overall,
        pso.usage__pass AS usage_pass,
        -- usage__rush is bigint with a dlt __v_double VARIANT twin on this
        -- table (live-verified 2026-08-30: 155/249 non-null values are
        -- stranded in the twin alone) -- COALESCE per the repo's codified
        -- pattern (src/schemas/009_variant_columns.sql,
        -- marts/032_player_usage.sql:56). games/usage__overall/usage__pass/
        -- ppa__average__all/ppa__total__all have no twin (see file header)
        -- and are read directly.
        COALESCE(pso.usage__rush::double precision, pso.usage__rush__v_double) AS usage_rush,
        pso.ppa__average__all AS ppa_overview_avg,
        pso.ppa__total__all AS ppa_overview_total,
        pso.team
    FROM stats.player_season_overview pso
    WHERE pso.id::text = r.id::text
      AND pso.season = r.year
    -- Secondary tiebreaker (pso.team) makes the pick deterministic even
    -- when two stints share the same team-match outcome (both matched, or
    -- both didn't) -- without it, LIMIT 1 could return either row from run
    -- to run since ORDER BY on the boolean match alone leaves ties
    -- unordered.
    ORDER BY (pso.team = r.team) DESC, pso.team
    LIMIT 1
) ov ON true;

COMMENT ON VIEW api.player_detail IS 'Comprehensive player profile with roster info, recruiting data, season stats, PPA metrics, and a compact season-overview payload (games/usage/ppa_overview_*). One row per (player_id, season, team) -- recruiting pedigree (stars/recruit_rating/national_ranking/recruit_class) is deduped to one row per player_id, keeping the highest (most authoritative) recruit_class year for a reclassified player (fixed 2026-08-30; was fanning out on players with 2+ recruiting.recruits rows). games/usage_overall/usage_pass/usage_rush/ppa_overview_avg/ppa_overview_total are from stats.player_season_overview via a fanout-proof LATERAL join (team-matched stint preferred, falls back to any stint for that player-season) -- NULL outside that table''s coverage.';

-- Grants are part of the definition: an apply that DROPs/recreates the
-- view would otherwise leave the PostgREST roles without read access
-- (no ALTER DEFAULT PRIVILEGES for them in this database).
GRANT SELECT ON api.player_detail TO anon, authenticated;
