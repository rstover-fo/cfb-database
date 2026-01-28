-- Sprint 2B.1: Partition core.plays by season
-- This is a major migration on a 3.4M row / 1.8GB table.
--
-- Strategy:
--   1. Create new partitioned table with identical schema
--   2. Create partitions for each season (2004-2025)
--   3. Copy data in batches by season
--   4. Verify row counts match
--   5. Swap tables via rename
--   6. Recreate indexes on partitioned table
--   7. Keep old table until dlt load is verified
--
-- IMPORTANT: Run this during a maintenance window. No pipeline loads during migration.

-- Step 1: Create the partitioned table
CREATE TABLE IF NOT EXISTS core.plays_partitioned (
    game_id             bigint,
    drive_id            character varying,
    id                  character varying NOT NULL,
    drive_number        bigint,
    play_number         bigint,
    offense             character varying,
    offense_conference  character varying,
    offense_score       bigint,
    defense             character varying,
    defense_conference  character varying,
    defense_score       bigint,
    home                character varying,
    away                character varying,
    period              bigint,
    clock__minutes      bigint,
    clock__seconds      bigint,
    offense_timeouts    bigint,
    defense_timeouts    bigint,
    yardline            bigint,
    yards_to_goal       bigint,
    down                bigint,
    distance            bigint,
    yards_gained        bigint,
    scoring             boolean,
    play_type           character varying,
    play_text           character varying,
    ppa                 double precision,
    wallclock           timestamp with time zone,
    season              bigint,
    _dlt_load_id        character varying NOT NULL,
    _dlt_id             character varying NOT NULL
) PARTITION BY LIST (season);

-- Step 2: Create partitions for each season (2004-2025)
DO $$
DECLARE
    yr INT;
BEGIN
    FOR yr IN 2004..2025 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS core.plays_y%s PARTITION OF core.plays_partitioned FOR VALUES IN (%s)',
            yr, yr
        );
    END LOOP;
    -- Future partition for 2026
    EXECUTE 'CREATE TABLE IF NOT EXISTS core.plays_y2026 PARTITION OF core.plays_partitioned FOR VALUES IN (2026)';
END $$;

-- Step 3: Copy data (all at once — Postgres handles partition routing)
INSERT INTO core.plays_partitioned
SELECT * FROM core.plays;

-- Step 4: Verify row counts
DO $$
DECLARE
    old_count BIGINT;
    new_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO old_count FROM core.plays;
    SELECT COUNT(*) INTO new_count FROM core.plays_partitioned;

    IF old_count != new_count THEN
        RAISE EXCEPTION 'Row count mismatch! Old: %, New: %', old_count, new_count;
    END IF;

    RAISE NOTICE 'Row counts match: % rows', old_count;
END $$;

-- Step 5: Swap tables
ALTER TABLE core.plays RENAME TO plays_old;
ALTER TABLE core.plays_partitioned RENAME TO plays;

-- Also rename partitions to use clean names (optional but nice)
-- Skipping partition rename — they keep their plays_y20XX names which is fine

-- Step 6: Recreate indexes on partitioned table
-- Postgres will automatically create per-partition indexes
-- NOTE: Unique indexes on partitioned tables MUST include the partition key (season)
CREATE UNIQUE INDEX IF NOT EXISTS plays_dlt_id_unique ON core.plays(_dlt_id, season);
CREATE INDEX IF NOT EXISTS idx_plays_game_id ON core.plays(game_id);
CREATE INDEX IF NOT EXISTS idx_plays_drive_id ON core.plays(drive_id);
CREATE INDEX IF NOT EXISTS idx_plays_season ON core.plays(season);
CREATE INDEX IF NOT EXISTS idx_plays_offense ON core.plays(offense);
CREATE INDEX IF NOT EXISTS idx_plays_defense ON core.plays(defense);
CREATE INDEX IF NOT EXISTS idx_plays_play_type ON core.plays(play_type);
CREATE INDEX IF NOT EXISTS idx_plays_down ON core.plays(down);
CREATE INDEX IF NOT EXISTS idx_plays_scoring ON core.plays(scoring) WHERE scoring = true;
CREATE INDEX IF NOT EXISTS idx_plays_game_id_drive_id ON core.plays(game_id, drive_id);
CREATE INDEX IF NOT EXISTS idx_plays_season_offense ON core.plays(season, offense);
CREATE INDEX IF NOT EXISTS idx_plays_season_play_type ON core.plays(season, play_type);
CREATE INDEX IF NOT EXISTS idx_plays_game_id_play_number ON core.plays(game_id, play_number);

-- BRIN not needed on partitioned table — partition pruning replaces it

-- Step 7: ANALYZE the new partitioned table
ANALYZE core.plays;

-- NOTE: core.plays_old is kept as a safety net.
-- After verifying a dlt pipeline load works against the new partitioned table,
-- drop it with: DROP TABLE core.plays_old;
