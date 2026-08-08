-- scouting.player_mart: denormalized player profile matview (codified from live, 2026-08-08)
-- =============================================================================
-- First version-controlled DDL for this matview -- it was created ad hoc in the
-- Supabase SQL editor by cub-scout (flagged unversioned in
-- docs/plans/2026-02-05-cross-repo-analysis.md #7). Definition extracted verbatim
-- from pg_get_viewdef in production.
--
-- Applied via: python scripts/run_migrations.py --file src/schemas/scouting/002_player_mart.sql
--
-- Known quirks, preserved deliberately (this is a codification, not a redesign):
--   * Stat/PPA CTEs are hardcoded to season 2024 -- the mart was built for the 2024
--     roster year and never generalized. A refresh today would pull the CURRENT
--     max(core.roster.year) rosters but still join 2024 stats. Frozen by design
--     while the scout service is parked.
--   * DISTINCT ON tie-break orders by core.roster._dlt_load_id -- a dlt loader
--     internal. Any change to dlt load-id semantics silently changes row selection.
--   * recruiting join is athlete_id::text = id::text (types differ at source).
--
-- Scout service is parked: the daily pg_cron refresh ('refresh-player-mart',
-- 30 12 * * *) was unscheduled on 2026-08-08. Revival step, run once:
--   SELECT cron.schedule('refresh-player-mart', '30 12 * * *',
--                        $$SELECT scouting.refresh_player_mart()$$);

CREATE MATERIALIZED VIEW IF NOT EXISTS scouting.player_mart AS
WITH passing_stats AS (
    SELECT player_id,
           max(CASE WHEN stat_type = 'ATT' THEN stat::numeric END) AS pass_att,
           max(CASE WHEN stat_type = 'COMPLETIONS' THEN stat::numeric END) AS pass_cmp,
           max(CASE WHEN stat_type = 'YDS' THEN stat::numeric END) AS pass_yds,
           max(CASE WHEN stat_type = 'TD' THEN stat::numeric END) AS pass_td,
           max(CASE WHEN stat_type = 'INT' THEN stat::numeric END) AS pass_int,
           max(CASE WHEN stat_type = 'PCT' THEN stat::numeric END) AS pass_pct
    FROM stats.player_season_stats
    WHERE category = 'passing' AND season = 2024
    GROUP BY player_id
), rushing_stats AS (
    SELECT player_id,
           max(CASE WHEN stat_type = 'CAR' THEN stat::numeric END) AS rush_car,
           max(CASE WHEN stat_type = 'YDS' THEN stat::numeric END) AS rush_yds,
           max(CASE WHEN stat_type = 'TD' THEN stat::numeric END) AS rush_td,
           max(CASE WHEN stat_type = 'YPC' THEN stat::numeric END) AS rush_ypc
    FROM stats.player_season_stats
    WHERE category = 'rushing' AND season = 2024
    GROUP BY player_id
), receiving_stats AS (
    SELECT player_id,
           max(CASE WHEN stat_type = 'REC' THEN stat::numeric END) AS rec,
           max(CASE WHEN stat_type = 'YDS' THEN stat::numeric END) AS rec_yds,
           max(CASE WHEN stat_type = 'TD' THEN stat::numeric END) AS rec_td,
           max(CASE WHEN stat_type = 'YPR' THEN stat::numeric END) AS rec_ypr
    FROM stats.player_season_stats
    WHERE category = 'receiving' AND season = 2024
    GROUP BY player_id
), defensive_stats AS (
    SELECT player_id,
           max(CASE WHEN stat_type = 'TOT' THEN stat::numeric END) AS tackles,
           max(CASE WHEN stat_type = 'SACKS' THEN stat::numeric END) AS sacks,
           max(CASE WHEN stat_type = 'TFL' THEN stat::numeric END) AS tfl,
           max(CASE WHEN stat_type = 'PD' THEN stat::numeric END) AS pass_def
    FROM stats.player_season_stats
    WHERE category = 'defensive' AND season = 2024
    GROUP BY player_id
), interception_stats AS (
    SELECT player_id,
           max(CASE WHEN stat_type = 'INT' THEN stat::numeric END) AS def_int
    FROM stats.player_season_stats
    WHERE category = 'interceptions' AND season = 2024
    GROUP BY player_id
), ppa_stats AS (
    SELECT id AS player_id,
           average_ppa__all AS ppa_avg,
           total_ppa__all AS ppa_total
    FROM metrics.ppa_players_season
    WHERE season = 2024
)
SELECT DISTINCT ON (r.id)
    r.id AS player_id,
    (r.first_name::text || ' '::text) || r.last_name::text AS name,
    r.team,
    r."position",
    r.year AS roster_year,
    r.height,
    r.weight,
    r.jersey,
    r.home_city,
    r.home_state,
    rec.stars,
    rec.rating AS recruit_rating,
    rec.ranking AS national_ranking,
    rec.year AS recruit_class,
    rec.school AS high_school,
    sp.id AS scouting_id,
    sp.composite_grade,
    sp.traits,
    sp.draft_projection,
    sp.comps,
    sp.current_status AS scouting_status,
    latest_te.event_type AS portal_status,
    latest_te.to_team AS portal_destination,
    latest_te.event_date AS portal_date,
    ps.pass_att,
    ps.pass_cmp,
    ps.pass_yds,
    ps.pass_td,
    ps.pass_int,
    ps.pass_pct,
    rs.rush_car,
    rs.rush_yds,
    rs.rush_td,
    rs.rush_ypc,
    rv.rec,
    rv.rec_yds,
    rv.rec_td,
    rv.rec_ypr,
    ds.tackles,
    ds.sacks,
    ds.tfl,
    ds.pass_def,
    ints.def_int,
    ppa.ppa_avg,
    ppa.ppa_total
FROM core.roster r
LEFT JOIN recruiting.recruits rec ON rec.athlete_id::text = r.id::text
LEFT JOIN scouting.players sp ON sp.roster_player_id = r.id::bigint
LEFT JOIN LATERAL (
    SELECT te.event_type, te.to_team, te.event_date
    FROM scouting.transfer_events te
    WHERE te.player_id = sp.id
    ORDER BY te.event_date DESC
    LIMIT 1
) latest_te ON true
LEFT JOIN passing_stats ps ON ps.player_id::text = r.id::text
LEFT JOIN rushing_stats rs ON rs.player_id::text = r.id::text
LEFT JOIN receiving_stats rv ON rv.player_id::text = r.id::text
LEFT JOIN defensive_stats ds ON ds.player_id::text = r.id::text
LEFT JOIN interception_stats ints ON ints.player_id::text = r.id::text
LEFT JOIN ppa_stats ppa ON ppa.player_id::text = r.id::text
WHERE r.year = (SELECT max(roster.year) FROM core.roster)
ORDER BY r.id, r._dlt_load_id DESC;

-- Unique index required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS idx_player_mart_id ON scouting.player_mart (player_id);
CREATE INDEX IF NOT EXISTS idx_player_mart_grade ON scouting.player_mart (composite_grade DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_player_mart_position ON scouting.player_mart ("position");
CREATE INDEX IF NOT EXISTS idx_player_mart_ppa ON scouting.player_mart (ppa_avg DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_player_mart_team ON scouting.player_mart (team);
