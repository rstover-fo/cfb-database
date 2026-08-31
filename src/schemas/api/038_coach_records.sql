-- api.coach_records
-- Coach records at coach x team (career-at-school) grain: straight-up AND
-- against-the-spread (ATS) win/loss record, so beta testers (sports
-- bettors) can rank coaches by win percentage either way.
--
-- GRAIN: one row per (coach, team) -- a coach's whole career at a single
-- school, aggregated across every season they were listed as that school's
-- head coach. This differs from api.coaching_history (marts.coaching_tenure),
-- which splits a coach's non-contiguous stints at the same school into
-- separate tenure rows -- this view collapses all stints into one career
-- record per coach-team pair.
--
-- CAVEAT -- coach attribution is whole-season: ref.coaches__seasons (CFBD's
-- /coaches endpoint) attributes an entire season's games to a single head
-- coach per school. Mid-season coaching changes (e.g. an interim coach
-- finishing out a season after a firing) are NOT splittable at this grain --
-- all of that season's games land under whichever name CFBD's season row
-- lists. Do not use this view to attribute individual games to a specific
-- coach; it is only valid at season-or-larger grain.
--
-- CAVEAT -- ATS coverage is partial: marts.team_ats_records (sourced from
-- betting.team_ats / CFBD's historical betting lines) does not extend back
-- through every coach's full career -- older seasons predate consistently
-- tracked betting lines. The season-to-team ATS join is a LEFT JOIN, so
-- ats_* columns can be NULL (no ATS data for any of the coach's seasons at
-- that team) or based on fewer seasons than the straight-up record covers.
-- Use seasons_with_ats_data to see how many of a coach's seasons at that
-- team actually have ATS data backing the ats_* aggregates.
--
-- Join key: ref.coaches__seasons.school (team name) + year, joined to
-- marts.team_ats_records.team + season -- there is no shared surrogate id
-- between coaching data and betting data.
--
-- 2026-08-30 (expansion_views unit, task 5a -- additive): coach_id, matched
-- per-season to ref.coach_seasons by (first_name, last_name, school, year)
-- via an aggregate LEFT JOIN LATERAL (an aggregate LATERAL always returns
-- exactly one row, so ON true cannot fan out) that yields the season's
-- coach__id only when exactly one distinct id matches -- a season whose
-- (name, school, year) matches MORE than one distinct coach__id
-- contributes NULL (ambiguity is never guessed away -- PR #81 Greptile P1;
-- zero such groups exist live today, this is contract hardening). Same fix
-- and same rationale as marts.coaching_tenure's coach_id (see that file's
-- header): CFBD's older /coaches bio rows share no surrogate key with the
-- newer, richer /coaches/seasons load, so cfb-app was joining
-- coaching_history to coach_records on first_name+last_name alone
-- (cfb-app/src/lib/queries/coaches.ts:170-182). Because this view's grain is
-- a coach's WHOLE CAREER at a school (not a single tenure), the per-season
-- matches are collapsed to one coach_id per (coach_name, team) only when
-- every one of that coach's MATCHED seasons at that school agrees on the
-- same coach__id -- ambiguous (>1 distinct id, e.g. a same-named-coach
-- collision) or unmatched (0) seasons yield NULL rather than a guessed
-- value. Seasons outside ref.coach_seasons' coverage (it has no pre-2014
-- depth) do NOT invalidate the match -- requiring full-span matches would
-- NULL nearly every long career. An ambiguous season poisons the entire
-- career's coach_id (NULL), while unmatched coverage-gap seasons still do
-- not -- the two NULL causes are distinguished via an explicit per-season
-- ambiguity flag (PR #81 Greptile round-2).
--
-- PostgREST usage:
--   GET /api/coach_records?team=eq.Alabama&order=win_pct.desc
--   GET /api/coach_records?coach_name=ilike.*saban*&order=ats_win_pct.desc

DROP VIEW IF EXISTS api.coach_records;

CREATE VIEW api.coach_records AS
WITH coach_seasons AS (
    -- One row per coach-team-season, via the dlt parent/child relationship
    -- (same idiom as marts.coach_record / marts.coaching_tenure).
    SELECT
        c.first_name,
        c.last_name,
        COALESCE(NULLIF(TRIM(c.first_name || ' ' || c.last_name), ''), c.last_name, c.first_name)
            AS coach_name,
        cs.school AS team,
        cs.year AS season,
        cs.games,
        cs.wins,
        cs.losses,
        cs.ties,
        match.coach_id AS season_coach_id,
        match.ambiguous AS season_match_ambiguous
    FROM ref.coaches c
    JOIN ref.coaches__seasons cs ON cs._dlt_parent_id = c._dlt_id
    LEFT JOIN LATERAL (
        SELECT
            CASE
                WHEN COUNT(DISTINCT rcs.coach__id) = 1 THEN MIN(rcs.coach__id)
            END AS coach_id,
            COUNT(DISTINCT rcs.coach__id) > 1 AS ambiguous
        FROM ref.coach_seasons rcs
        WHERE rcs.coach__first_name = c.first_name
          AND rcs.coach__last_name = c.last_name
          AND rcs.team__school = cs.school
          AND rcs.year = cs.year
    ) match ON true
    WHERE cs.school IS NOT NULL
      AND cs.year IS NOT NULL
),
coach_seasons_ats AS (
    -- LEFT JOIN so seasons/teams with no betting-line coverage still keep
    -- their straight-up record; ATS columns simply stay NULL for that season.
    SELECT
        cse.*,
        tar.ats_wins,
        tar.ats_losses,
        tar.ats_pushes,
        (tar.team IS NOT NULL) AS has_ats_data
    FROM coach_seasons cse
    LEFT JOIN marts.team_ats_records tar
        ON tar.team = cse.team
        AND tar.season = cse.season
)
SELECT
    -- One coach_id for the coach's whole career at this school only when
    -- every matched season agrees AND no season's match was ambiguous; any
    -- ambiguous season poisons the whole career (NULL), while unmatched
    -- coverage-gap seasons do not. See coach_seasons CTE above for the
    -- per-season LATERAL match and its explicit ambiguity flag.
    CASE
        WHEN COUNT(DISTINCT season_coach_id) FILTER (WHERE season_coach_id IS NOT NULL) = 1
         AND NOT BOOL_OR(season_match_ambiguous)
            THEN MIN(season_coach_id)
    END AS coach_id,
    coach_name,
    first_name,
    last_name,
    team,

    -- Career span at this school
    MIN(season) AS first_season,
    MAX(season) AS last_season,
    COUNT(DISTINCT season) AS seasons_count,

    -- Straight-up record
    SUM(COALESCE(games, 0))::int AS games,
    SUM(COALESCE(wins, 0))::int AS wins,
    SUM(COALESCE(losses, 0))::int AS losses,
    SUM(COALESCE(ties, 0))::int AS ties,
    ROUND(
        SUM(COALESCE(wins, 0))::numeric
        / NULLIF(SUM(COALESCE(wins, 0)) + SUM(COALESCE(losses, 0)), 0),
        4
    ) AS win_pct,

    -- Against-the-spread record, aggregated over whichever of the coach's
    -- seasons at this team have betting-line coverage
    SUM(COALESCE(ats_wins, 0))::int
        + SUM(COALESCE(ats_losses, 0))::int
        + SUM(COALESCE(ats_pushes, 0))::int AS ats_games,
    SUM(COALESCE(ats_wins, 0))::int AS ats_wins,
    SUM(COALESCE(ats_losses, 0))::int AS ats_losses,
    SUM(COALESCE(ats_pushes, 0))::int AS ats_pushes,
    ROUND(
        SUM(COALESCE(ats_wins, 0))::numeric
        / NULLIF(SUM(COALESCE(ats_wins, 0)) + SUM(COALESCE(ats_losses, 0)), 0),
        4
    ) AS ats_win_pct,

    -- Coverage indicator: how many of this coach's seasons at this team
    -- actually had a matching marts.team_ats_records row
    COUNT(*) FILTER (WHERE has_ats_data) AS seasons_with_ats_data

FROM coach_seasons_ats
GROUP BY coach_name, first_name, last_name, team;

COMMENT ON VIEW api.coach_records IS 'Coach career records at a school (coach x team grain), straight-up and against-the-spread. Columns: coach_id, coach_name, first_name, last_name, team, first_season, last_season, seasons_count, games, wins, losses, ties, win_pct, ats_games, ats_wins, ats_losses, ats_pushes, ats_win_pct, seasons_with_ats_data. Coach attribution is whole-season (mid-season coaching changes not splittable -- see ref.coaches__seasons). ATS columns are partial pre-lines-era coverage (LEFT JOIN to marts.team_ats_records); use seasons_with_ats_data to gauge coverage. coach_id (added 2026-08-30) is ref.coach_seasons'' coach__id, matched by (first_name, last_name, team, year) -- the single id every MATCHED season of the coach''s career at this school agrees on; NULL when any season''s match is ambiguous (more than one distinct coach__id -- one ambiguous season poisons the entire career''s coach_id), when matched seasons disagree, or when zero seasons matched. Seasons outside ref.coach_seasons'' coverage (it has no pre-2014 depth) do NOT invalidate the match. Backed by ref.coaches / ref.coaches__seasons and marts.team_ats_records.';

GRANT SELECT ON api.coach_records TO anon, authenticated;
