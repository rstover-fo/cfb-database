-- Coaching tenure: one row per coach-team-tenure span
-- Grain: Coach × Team × Tenure (contiguous seasons)
-- Uses gap detection on ref.coaches__seasons to identify separate stints
-- Includes inherited vs recruited talent comparison
--
-- 2026-08-30 (expansion_views unit, task 5a -- additive): coach_id.
-- ref.coaches (bio: first_name/last_name only, no id) and ref.coaches__seasons
-- (this mart's source) predate ref.coach_seasons (coaches.py's newer,
-- richer /coaches/seasons load, PK coach__id/year/team__id) and share no
-- surrogate key -- cfb-app was joining coaching_history to coach_records on
-- first_name+last_name for lack of anything better
-- (cfb-app/src/lib/queries/coaches.ts:170-182), with a comment
-- acknowledging that two same-named coaches would collide. Fix: match each
-- coach-season row to ref.coach_seasons by (first_name, last_name, school,
-- year) via an aggregate LEFT JOIN LATERAL (an aggregate LATERAL always
-- returns exactly one row, so ON true cannot fan out) that yields the
-- season's coach__id only when exactly one distinct id matches -- a season
-- whose (name, school, year) matches MORE than one distinct coach__id
-- contributes NULL (ambiguity is never guessed away -- PR #81 Greptile P1;
-- zero such groups exist live today, this is contract hardening). Then in
-- tenure_agg collapse a tenure's per-season matches to ONE coach_id only
-- when every matched season agrees on the same coach__id -- if two
-- different ids appear across a tenure's seasons (a same-named-coach
-- collision) or zero seasons matched, coach_id is NULL rather than a
-- guessed value. coach_id is the single id every MATCHED season agrees on:
-- seasons outside ref.coach_seasons' coverage (it has no pre-2014 depth)
-- do NOT invalidate the match -- requiring full-span matches would NULL
-- nearly every long tenure. An ambiguous season poisons the entire
-- tenure's coach_id (NULL), while unmatched coverage-gap seasons still do
-- not -- the two NULL causes are distinguished via an explicit per-season
-- ambiguity flag (PR #81 Greptile round-2). Additive only -- no existing
-- column changed.

DROP MATERIALIZED VIEW IF EXISTS marts.coaching_tenure CASCADE;

CREATE MATERIALIZED VIEW marts.coaching_tenure AS
WITH coach_seasons AS (
    SELECT
        c.first_name,
        c.last_name,
        c.first_name || ' ' || c.last_name AS coach_name,
        cs.school AS team,
        cs.year AS season,
        cs.games,
        cs.wins,
        cs.losses,
        cs.ties,
        cs.preseason_rank,
        cs.postseason_rank,
        cs.sp_overall,
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
),
gap_detect AS (
    SELECT
        *,
        season - LAG(season) OVER (
            PARTITION BY first_name, last_name, team ORDER BY season
        ) AS gap
    FROM coach_seasons
),
tenure_groups AS (
    SELECT
        *,
        SUM(CASE WHEN gap IS NULL OR gap > 1 THEN 1 ELSE 0 END)
            OVER (PARTITION BY first_name, last_name, team ORDER BY season) AS tenure_id
    FROM gap_detect
),
tenure_agg AS (
    SELECT
        coach_name,
        first_name,
        last_name,
        team,
        tenure_id,
        MIN(season) AS tenure_start,
        MAX(season) AS tenure_end,
        COUNT(DISTINCT season) AS seasons_count,
        -- One coach_id for the whole tenure only when every matched season
        -- agrees AND no season's match was ambiguous; any ambiguous season
        -- poisons the whole tenure (NULL), while unmatched coverage-gap
        -- seasons do not. See coach_seasons CTE above for the per-season
        -- LATERAL match and its explicit ambiguity flag.
        CASE
            WHEN COUNT(DISTINCT season_coach_id) FILTER (WHERE season_coach_id IS NOT NULL) = 1
             AND NOT BOOL_OR(season_match_ambiguous)
                THEN MIN(season_coach_id)
        END AS coach_id,
        SUM(COALESCE(games, 0))::int AS total_games,
        SUM(COALESCE(wins, 0))::int AS total_wins,
        SUM(COALESCE(losses, 0))::int AS total_losses,
        SUM(COALESCE(ties, 0))::int AS total_ties,
        MAX(wins) AS best_season_wins,
        MIN(wins) AS worst_season_wins,
        MIN(preseason_rank) FILTER (WHERE preseason_rank IS NOT NULL) AS best_preseason_rank,
        MIN(postseason_rank) FILTER (WHERE postseason_rank IS NOT NULL) AS best_postseason_rank,
        ROUND(AVG(sp_overall)::numeric, 2) AS avg_sp_rating,
        MAX(sp_overall) AS peak_sp_rating
    FROM tenure_groups
    GROUP BY coach_name, first_name, last_name, team, tenure_id
),
-- Bowl game counts: postseason games during the tenure
bowl_counts AS (
    SELECT
        ta.team,
        ta.first_name,
        ta.last_name,
        ta.tenure_start,
        COUNT(DISTINCT g.id) AS bowl_games,
        COUNT(DISTINCT g.id) FILTER (
            WHERE (g.home_team = ta.team AND g.home_points > g.away_points)
               OR (g.away_team = ta.team AND g.away_points > g.home_points)
        ) AS bowl_wins
    FROM tenure_agg ta
    JOIN core.games g
        ON (g.home_team = ta.team OR g.away_team = ta.team)
        AND g.season BETWEEN ta.tenure_start AND ta.tenure_end
        AND g.season_type = 'postseason'
        AND g.completed = true
    GROUP BY ta.team, ta.first_name, ta.last_name, ta.tenure_start
),
-- Inherited talent: recruiting rank in coach's first season
inherited AS (
    SELECT
        ta.first_name,
        ta.last_name,
        ta.team,
        ta.tenure_start,
        rc.national_rank AS inherited_talent_rank
    FROM tenure_agg ta
    LEFT JOIN marts.recruiting_class rc
        ON rc.team = ta.team AND rc.year = ta.tenure_start
),
-- Year 3 talent: recruiting rank in coach's 3rd season
year3 AS (
    SELECT
        ta.first_name,
        ta.last_name,
        ta.team,
        ta.tenure_start,
        rc.national_rank AS year3_talent_rank
    FROM tenure_agg ta
    LEFT JOIN marts.recruiting_class rc
        ON rc.team = ta.team AND rc.year = ta.tenure_start + 2
    WHERE ta.seasons_count >= 3
)
SELECT
    ta.coach_id,
    ta.coach_name,
    ta.first_name,
    ta.last_name,
    ta.team,
    ta.tenure_start,
    ta.tenure_end,
    ta.seasons_count,

    -- Record
    ta.total_games,
    ta.total_wins,
    ta.total_losses,
    ta.total_ties,
    ROUND(
        ta.total_wins::numeric / NULLIF(ta.total_wins + ta.total_losses, 0),
        4
    ) AS win_pct,

    -- Conference record (from team_season_summary)
    (SELECT SUM(tss.conf_wins) FROM marts.team_season_summary tss
     WHERE tss.team = ta.team AND tss.season BETWEEN ta.tenure_start AND ta.tenure_end
    )::int AS conf_wins,
    (SELECT SUM(tss.conf_losses) FROM marts.team_season_summary tss
     WHERE tss.team = ta.team AND tss.season BETWEEN ta.tenure_start AND ta.tenure_end
    )::int AS conf_losses,
    ROUND(
        (SELECT SUM(tss.conf_wins)::numeric FROM marts.team_season_summary tss
         WHERE tss.team = ta.team AND tss.season BETWEEN ta.tenure_start AND ta.tenure_end)
        / NULLIF(
            (SELECT SUM(tss.conf_wins + tss.conf_losses) FROM marts.team_season_summary tss
             WHERE tss.team = ta.team AND tss.season BETWEEN ta.tenure_start AND ta.tenure_end),
            0
        ),
        4
    ) AS conf_win_pct,

    -- Season extremes
    ta.best_season_wins,
    ta.worst_season_wins,

    -- Ratings
    ta.avg_sp_rating,
    ta.peak_sp_rating,
    ta.best_preseason_rank,
    ta.best_postseason_rank,

    -- Recruiting during tenure
    ROUND(AVG(rc.national_rank)::numeric, 1) AS avg_recruiting_rank,
    MIN(rc.national_rank) AS best_recruiting_rank,

    -- Inherited vs own talent
    ih.inherited_talent_rank,
    y3.year3_talent_rank,
    CASE WHEN ih.inherited_talent_rank IS NOT NULL AND y3.year3_talent_rank IS NOT NULL
        THEN ih.inherited_talent_rank - y3.year3_talent_rank
    END AS talent_improvement,

    -- Postseason
    COALESCE(bc.bowl_games, 0)::int AS bowl_games,
    COALESCE(bc.bowl_wins, 0)::int AS bowl_wins,

    -- Active status
    (ta.tenure_end >= (SELECT MAX(year) FROM ref.coaches__seasons) - 1) AS is_active

FROM tenure_agg ta
LEFT JOIN marts.recruiting_class rc
    ON rc.team = ta.team AND rc.year BETWEEN ta.tenure_start AND ta.tenure_end
LEFT JOIN bowl_counts bc
    ON bc.team = ta.team
    AND bc.first_name = ta.first_name
    AND bc.last_name = ta.last_name
    AND bc.tenure_start = ta.tenure_start
LEFT JOIN inherited ih
    ON ih.team = ta.team
    AND ih.first_name = ta.first_name
    AND ih.last_name = ta.last_name
    AND ih.tenure_start = ta.tenure_start
LEFT JOIN year3 y3
    ON y3.team = ta.team
    AND y3.first_name = ta.first_name
    AND y3.last_name = ta.last_name
    AND y3.tenure_start = ta.tenure_start
GROUP BY
    ta.coach_id, ta.coach_name, ta.first_name, ta.last_name, ta.team,
    ta.tenure_id, ta.tenure_start, ta.tenure_end, ta.seasons_count,
    ta.total_games, ta.total_wins, ta.total_losses, ta.total_ties,
    ta.best_season_wins, ta.worst_season_wins,
    ta.avg_sp_rating, ta.peak_sp_rating,
    ta.best_preseason_rank, ta.best_postseason_rank,
    ih.inherited_talent_rank, y3.year3_talent_rank,
    bc.bowl_games, bc.bowl_wins;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.coaching_tenure (first_name, last_name, team, tenure_start);

-- Query indexes
CREATE INDEX ON marts.coaching_tenure (team, tenure_start);
CREATE INDEX ON marts.coaching_tenure (win_pct DESC);
CREATE INDEX ON marts.coaching_tenure (seasons_count DESC, win_pct DESC);
CREATE INDEX ON marts.coaching_tenure (is_active);
