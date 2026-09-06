-- 061: Repair player-comparison grain after DE/DL source metadata drift.
-- Explicit-file deployment: scripts/run_migrations.py --file <this file>.
-- Atomic and repeatable. No CASCADE: unexpected dependents stop the repair.
-- Canonical definition: src/schemas/marts/020_player_comparison.sql.
BEGIN;
SET LOCAL lock_timeout = '10s';
CREATE TEMP TABLE recovery_player_objects ON COMMIT DROP AS
SELECT c.oid, n.nspname, c.relname, c.relkind,
       pg_get_userbyid(c.relowner) AS owner,
       c.relacl, obj_description(c.oid, 'pg_class') AS comment,
       c.reloptions,
       CASE WHEN c.relkind = 'v' THEN pg_get_viewdef(c.oid, true) END AS definition
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid IN ('marts.player_comparison'::regclass, 'api.player_comparison'::regclass);
CREATE TEMP TABLE recovery_player_grants ON COMMIT DROP AS
SELECT o.nspname, o.relname, x.grantee, x.privilege_type, x.is_grantable
FROM recovery_player_objects o
CROSS JOIN LATERAL aclexplode(COALESCE(o.relacl, acldefault('r', o.owner::regrole))) x;
-- Column-level grants/comments would need their own preservation path.
DO $guard$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_attribute a JOIN recovery_player_objects o ON o.oid=a.attrelid
            WHERE a.attnum>0 AND (a.attacl IS NOT NULL OR col_description(a.attrelid,a.attnum) IS NOT NULL)) THEN
   RAISE EXCEPTION 'Unexpected column grants/comments; inspect before recovery';
 END IF;
 IF EXISTS (SELECT 1 FROM recovery_player_objects WHERE reloptions IS NOT NULL) THEN
   RAISE EXCEPTION 'Unexpected relation options; inspect before recovery';
 END IF;
END $guard$;
DROP VIEW api.player_comparison;

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
-- The dlt merge key excludes player/position metadata. A corrected source row
-- can therefore leave different metadata values across a player's EAV stats.
-- Aggregate stats at the mart's published key so metadata drift cannot split
-- one player-team-season into multiple rows.
stat_aggregates AS (
    SELECT
        s.player_id,
        s.team,
        s.season,
        -- mode() keeps name/position as an observed pair while selecting the
        -- pair attached to the most source stats. The JSONB order prefers
        -- populated fields and gives tied modes a stable lexical winner.
        MODE() WITHIN GROUP (
            ORDER BY jsonb_build_array(
                NULLIF(BTRIM(s.player), '') IS NULL,
                NULLIF(BTRIM(s.position), '') IS NULL,
                s.player,
                s.position
            )
        ) AS metadata,
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
    GROUP BY s.player_id, s.team, s.season
),
pivoted_stats AS (
    SELECT
        sa.player_id,
        sa.metadata->>2 AS name,
        sa.team,
        sa.metadata->>3 AS position,
        sa.season,
        sa.pass_att,
        sa.pass_cmp,
        sa.pass_yds,
        sa.pass_td,
        sa.pass_int,
        sa.pass_pct,
        sa.rush_car,
        sa.rush_yds,
        sa.rush_td,
        sa.rush_ypc,
        sa.rec,
        sa.rec_yds,
        sa.rec_td,
        sa.rec_ypr,
        sa.tackles,
        sa.sacks,
        sa.tfl,
        sa.pass_def
    FROM stat_aggregates sa
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

DO $restore$
DECLARE o record; g record; target text; recipient text;
BEGIN
 SELECT * INTO STRICT o FROM recovery_player_objects WHERE nspname='api';
 EXECUTE format('CREATE VIEW api.player_comparison AS %s', o.definition);
 FOR o IN SELECT * FROM recovery_player_objects LOOP
   target := format('%I.%I', o.nspname, o.relname);
   EXECUTE format('ALTER %s %s OWNER TO %I',
                  CASE WHEN o.relkind='m' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END,
                  target, o.owner);
   EXECUTE format('COMMENT ON %s %s IS %L',
                  CASE WHEN o.relkind='m' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END,
                  target, o.comment);
   -- Remove creation-time default grants before restoring the captured ACL.
   FOR g IN SELECT DISTINCT x.grantee FROM pg_class c,
       LATERAL aclexplode(COALESCE(c.relacl, acldefault('r',c.relowner))) x
       WHERE c.oid=target::regclass LOOP
     recipient := CASE WHEN g.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(g.grantee)) END;
     EXECUTE format('REVOKE ALL ON TABLE %s FROM %s',target,recipient);
   END LOOP;
 END LOOP;
 FOR g IN SELECT * FROM recovery_player_grants LOOP
   recipient := CASE WHEN g.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(g.grantee)) END;
   EXECUTE format('GRANT %s ON TABLE %I.%I TO %s%s',g.privilege_type,g.nspname,g.relname,
                  recipient,CASE WHEN g.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END);
 END LOOP;
END $restore$;
COMMIT;
