-- Staged build of player-game EPA history (2014-2025)
-- =============================================================================
-- The single-query build of marts.player_game_epa exceeds what the current
-- Supabase compute tier can execute in one statement: it timed out at 30
-- minutes even lock-free, ANALYZEd, and work_mem-tuned (deploy runs 7-10,
-- 2026-07-20). History is therefore built once, ONE SEASON PER ITERATION,
-- into this internal staging table; marts.player_game_epa (011) reads history
-- from here and computes only the current season live at refresh time.
--
-- analytics.* is contract-internal (docs/SCHEMA_CONTRACT.md) -- downstream
-- consumers must keep reading marts.player_game_epa, never this table.
--
-- Idempotent: each season is DELETE + INSERT. Re-run freely.
-- History horizon: seasons <= 2025 live here; 2026+ is computed live by the
-- matview. After the 2026 season ends, fold it in by extending the loop
-- bounds and bumping the horizon in marts/011 (noted in the 2027-proofing
-- follow-up list).

-- NOTE: this CREATE TABLE is intentionally duplicated in
-- marts/011_player_game_epa.sql so the mart file stands alone in any
-- provisioning order; keep the two definitions in sync.
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

DO $$
DECLARE
    yr INT;
BEGIN
    FOR yr IN 2014..2025 LOOP
        RAISE NOTICE 'building player_game_epa season %', yr;

        EXECUTE format('DELETE FROM analytics.player_game_epa_build WHERE season = %s', yr);

        -- Same pipeline as marts/011_player_game_epa.sql's live arm, bounded
        -- to one season via EXECUTE-injected literals so the planner prunes.
        EXECUTE format($q$
            INSERT INTO analytics.player_game_epa_build
            WITH stat_type_roles (stat_type, play_category) AS (
                VALUES
                    ('Completion',          'passing'),
                    ('Incompletion',        'passing'),
                    ('Interception Thrown', 'passing'),
                    ('Sack Taken',          'passing'),
                    ('Rush',                'rushing'),
                    ('Reception',           'receiving'),
                    ('Target',              'receiving')
            ),
            role_athletes AS (
                SELECT DISTINCT
                    ps.game_id, ps.play_id, ps.athlete_id, ps.team, r.play_category
                FROM stats.play_stats ps
                JOIN stat_type_roles r ON r.stat_type = ps.stat_type
                WHERE ps.season = %s
            ),
            role_plays AS (
                SELECT
                    pe.game_id, pe.season, pe.offense AS team,
                    ra.athlete_id, ra.play_category, ra.play_id,
                    pe.epa, pe.success, pe.explosive, pe.yards_gained
                FROM role_athletes ra
                JOIN marts.play_epa pe
                    ON pe.game_id = ra.game_id AND pe.play_id = ra.play_id
                WHERE ra.team = pe.offense
                  AND NOT pe.is_garbage_time
                  AND pe.season = %s
            ),
            athlete_names AS (
                SELECT game_id, athlete_id, MAX(athlete_name) AS player_name
                FROM stats.play_stats
                WHERE season = %s
                GROUP BY game_id, athlete_id
            )
            SELECT
                rp.game_id, rp.season, rp.team,
                MAX(an.player_name) AS player_name,
                rp.athlete_id, rp.play_category,
                COUNT(*) AS plays,
                SUM(rp.epa)::NUMERIC(8, 2) AS total_epa,
                AVG(rp.epa)::NUMERIC(6, 4) AS epa_per_play,
                AVG(rp.success)::NUMERIC(5, 3) AS success_rate,
                SUM(rp.explosive) AS explosive_plays,
                SUM(rp.yards_gained) AS total_yards
            FROM role_plays rp
            LEFT JOIN athlete_names an
                ON an.game_id = rp.game_id AND an.athlete_id = rp.athlete_id
            GROUP BY rp.game_id, rp.season, rp.team, rp.athlete_id, rp.play_category
            HAVING COUNT(*) >= 3
        $q$, yr, yr, yr);
    END LOOP;
END $$;

ANALYZE analytics.player_game_epa_build;
