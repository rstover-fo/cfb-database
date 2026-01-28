-- Add score_diff as a generated column for garbage time filtering
-- This column is computed from offense_score - defense_score

-- First check if column already exists before adding
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core'
          AND table_name = 'plays'
          AND column_name = 'score_diff'
    ) THEN
        ALTER TABLE core.plays
        ADD COLUMN score_diff integer
        GENERATED ALWAYS AS (offense_score - defense_score) STORED;
    END IF;
END $$;

-- Index for garbage time queries
-- Uses partial index for efficient filtering on close games
CREATE INDEX IF NOT EXISTS idx_plays_score_diff ON core.plays (score_diff);

-- Index for non-garbage time plays (absolute value <= 28)
-- This covers the majority of competitive plays
CREATE INDEX IF NOT EXISTS idx_plays_competitive
ON core.plays (game_id, period)
WHERE ABS(offense_score - defense_score) <= 28;

COMMENT ON COLUMN core.plays.score_diff IS 'Score differential from offense perspective (offense_score - defense_score). Positive = offense winning.';
