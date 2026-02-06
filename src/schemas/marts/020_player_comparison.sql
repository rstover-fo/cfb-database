-- marts.player_comparison
-- Player stats with positional percentiles for comparison.
-- Pre-computes EAV pivot + PERCENT_RANK() window functions so
-- the thin api.player_comparison view can serve fast filtered lookups.
--
-- Sources: stats.player_season_stats, core.roster, recruiting.recruits,
--          metrics.ppa_players_season
-- Unique key: (player_id, team, season)
-- Refresh layer: 1 (no mart dependencies)

DROP MATERIALIZED VIEW IF EXISTS marts.player_comparison;

CREATE MATERIALIZED VIEW marts.player_comparison AS
WITH position_groups AS (
    SELECT unnest AS position,
           CASE
               WHEN unnest IN ('QB') THEN 'QB'
               WHEN unnest IN ('RB', 'FB') THEN 'RB'
               WHEN unnest IN ('WR') THEN 'WR'
               WHEN unnest IN ('TE') THEN 'TE'
               WHEN unnest IN ('OL', 'OT', 'OG', 'C') THEN 'OL'
               WHEN unnest IN ('DL', 'DE', 'DT', 'NT', 'EDGE') THEN 'DL'
               WHEN unnest IN ('LB', 'OLB', 'ILB') THEN 'LB'
               WHEN unnest IN ('DB', 'CB', 'S', 'FS', 'SS') THEN 'DB'
               WHEN unnest IN ('K', 'P') THEN 'K/P'
               ELSE NULL
           END AS position_group
    FROM unnest(ARRAY['QB','RB','FB','WR','TE','OL','OT','OG','C','DL','DE','DT','NT','EDGE','LB','OLB','ILB','DB','CB','S','FS','SS','K','P','ATH','KR','PR','LS'])
),
pivoted_stats AS (
    SELECT
        s.player_id,
        s.player AS name,
        s.team,
        s.position,
        s.season,
        MAX(CASE WHEN s.category = 'passing' AND s.stat_type = 'ATT' THEN NULLIF(s.stat, '')::numeric END) AS pass_att,
        MAX(CASE WHEN s.category = 'passing' AND s.stat_type = 'COMPLETIONS' THEN NULLIF(s.stat, '')::numeric END) AS pass_cmp,
        MAX(CASE WHEN s.category = 'passing' AND s.stat_type = 'YDS' THEN NULLIF(s.stat, '')::numeric END) AS pass_yds,
        MAX(CASE WHEN s.category = 'passing' AND s.stat_type = 'TD' THEN NULLIF(s.stat, '')::numeric END) AS pass_td,
        MAX(CASE WHEN s.category = 'passing' AND s.stat_type = 'INT' THEN NULLIF(s.stat, '')::numeric END) AS pass_int,
        MAX(CASE WHEN s.category = 'passing' AND s.stat_type = 'PCT' THEN NULLIF(s.stat, '')::numeric END) AS pass_pct,
        MAX(CASE WHEN s.category = 'rushing' AND s.stat_type = 'CAR' THEN NULLIF(s.stat, '')::numeric END) AS rush_car,
        MAX(CASE WHEN s.category = 'rushing' AND s.stat_type = 'YDS' THEN NULLIF(s.stat, '')::numeric END) AS rush_yds,
        MAX(CASE WHEN s.category = 'rushing' AND s.stat_type = 'TD' THEN NULLIF(s.stat, '')::numeric END) AS rush_td,
        MAX(CASE WHEN s.category = 'rushing' AND s.stat_type = 'YPC' THEN NULLIF(s.stat, '')::numeric END) AS rush_ypc,
        MAX(CASE WHEN s.category = 'receiving' AND s.stat_type = 'REC' THEN NULLIF(s.stat, '')::numeric END) AS rec,
        MAX(CASE WHEN s.category = 'receiving' AND s.stat_type = 'YDS' THEN NULLIF(s.stat, '')::numeric END) AS rec_yds,
        MAX(CASE WHEN s.category = 'receiving' AND s.stat_type = 'TD' THEN NULLIF(s.stat, '')::numeric END) AS rec_td,
        MAX(CASE WHEN s.category = 'receiving' AND s.stat_type = 'YPR' THEN NULLIF(s.stat, '')::numeric END) AS rec_ypr,
        MAX(CASE WHEN s.category = 'defensive' AND s.stat_type = 'TOT' THEN NULLIF(s.stat, '')::numeric END) AS tackles,
        MAX(CASE WHEN s.category = 'defensive' AND s.stat_type = 'SACKS' THEN NULLIF(s.stat, '')::numeric END) AS sacks,
        MAX(CASE WHEN s.category = 'defensive' AND s.stat_type = 'TFL' THEN NULLIF(s.stat, '')::numeric END) AS tfl,
        MAX(CASE WHEN s.category = 'defensive' AND s.stat_type = 'PD' THEN NULLIF(s.stat, '')::numeric END) AS pass_def
    FROM stats.player_season_stats s
    GROUP BY s.player_id, s.player, s.team, s.position, s.season
),
-- Deduplicate roster: pick roster row matching stats team, else first alphabetically
roster_deduped AS (
    SELECT DISTINCT ON (id, year)
        id, year, height, weight, jersey, home_city, home_state, team
    FROM core.roster
    ORDER BY id, year, team
),
-- Deduplicate recruits: pick highest-rated record per athlete
recruits_deduped AS (
    SELECT DISTINCT ON (athlete_id)
        athlete_id, stars, rating, ranking, year
    FROM recruiting.recruits
    ORDER BY athlete_id, rating DESC NULLS LAST
),
with_extras AS (
    SELECT
        ps.player_id,
        ps.name,
        ps.team,
        ps.position,
        pg.position_group,
        ps.season,
        -- Prefer roster bio from matching team, fall back to deduped
        COALESCE(r_match.height, r_any.height) AS height,
        COALESCE(r_match.weight, r_any.weight) AS weight,
        COALESCE(r_match.jersey, r_any.jersey) AS jersey,
        COALESCE(r_match.home_city, r_any.home_city) AS home_city,
        COALESCE(r_match.home_state, r_any.home_state) AS home_state,
        rec.stars,
        rec.rating AS recruit_rating,
        rec.ranking AS national_ranking,
        rec.year AS recruit_class,
        ps.pass_att,
        ps.pass_cmp,
        ps.pass_yds,
        ps.pass_td,
        ps.pass_int,
        ps.pass_pct,
        ps.rush_car,
        ps.rush_yds,
        ps.rush_td,
        ps.rush_ypc,
        ps.rec,
        ps.rec_yds,
        ps.rec_td,
        ps.rec_ypr,
        ps.tackles,
        ps.sacks,
        ps.tfl,
        ps.pass_def,
        ppa.average_ppa__all AS ppa_avg,
        ppa.total_ppa__all AS ppa_total
    FROM pivoted_stats ps
    LEFT JOIN position_groups pg ON pg.position = ps.position
    -- Exact team+year match from roster (no fanout since stats already grouped by team)
    LEFT JOIN core.roster r_match
        ON r_match.id::text = ps.player_id::text
        AND r_match.year = ps.season
        AND r_match.team = ps.team
    -- Fallback: deduped roster for bio data when team doesn't match
    LEFT JOIN roster_deduped r_any
        ON r_any.id::text = ps.player_id::text
        AND r_any.year = ps.season
        AND r_match.id IS NULL
    LEFT JOIN recruits_deduped rec ON rec.athlete_id::text = ps.player_id::text
    LEFT JOIN metrics.ppa_players_season ppa ON ppa.id::text = ps.player_id::text AND ppa.season = ps.season
)
SELECT
    we.player_id,
    we.name,
    we.team,
    we.position,
    we.position_group,
    we.season,
    we.height,
    we.weight,
    we.jersey,
    we.home_city,
    we.home_state,
    we.stars,
    we.recruit_rating,
    we.national_ranking,
    we.recruit_class,
    we.pass_att,
    we.pass_cmp,
    we.pass_yds,
    we.pass_td,
    we.pass_int,
    we.pass_pct,
    we.rush_car,
    we.rush_yds,
    we.rush_td,
    we.rush_ypc,
    we.rec,
    we.rec_yds,
    we.rec_td,
    we.rec_ypr,
    we.tackles,
    we.sacks,
    we.tfl,
    we.pass_def,
    we.ppa_avg,
    we.ppa_total,
    -- Percentiles (partitioned by season + position_group, NULLs sort first = low rank)
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.pass_yds NULLS FIRST)
    END AS pass_yds_pctl,
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.pass_td NULLS FIRST)
    END AS pass_td_pctl,
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.pass_pct NULLS FIRST)
    END AS pass_pct_pctl,
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.rush_yds NULLS FIRST)
    END AS rush_yds_pctl,
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.rush_td NULLS FIRST)
    END AS rush_td_pctl,
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.rush_ypc NULLS FIRST)
    END AS rush_ypc_pctl,
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.rec_yds NULLS FIRST)
    END AS rec_yds_pctl,
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.rec_td NULLS FIRST)
    END AS rec_td_pctl,
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.tackles NULLS FIRST)
    END AS tackles_pctl,
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.sacks NULLS FIRST)
    END AS sacks_pctl,
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.tfl NULLS FIRST)
    END AS tfl_pctl,
    CASE WHEN we.position_group IS NOT NULL THEN
        PERCENT_RANK() OVER (PARTITION BY we.season, we.position_group ORDER BY we.ppa_avg NULLS FIRST)
    END AS ppa_avg_pctl
FROM with_extras we
WITH DATA;

-- Indexes
CREATE UNIQUE INDEX idx_player_comparison_pk ON marts.player_comparison (player_id, team, season);
CREATE INDEX idx_player_comparison_season_posgroup ON marts.player_comparison (season, position_group);
CREATE INDEX idx_player_comparison_name ON marts.player_comparison (name);
CREATE INDEX idx_player_comparison_team_season ON marts.player_comparison (team, season);
