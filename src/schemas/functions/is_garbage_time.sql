-- Function to detect garbage time plays
-- Garbage time: margin > 28 in 4th quarter, or > 35 in 3rd+
-- Used to filter out non-competitive plays in EPA calculations
--
-- CANONICAL SOURCE OF TRUTH
-- -------------------------
-- This function is the single source of truth for the garbage-time rule.
-- For performance, several materialized views inline the equivalent predicate
-- directly against core.plays columns (p.period, p.score_diff) rather than
-- calling this function per row. The exact inline predicate is:
--
--   (p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) > 28) OR (p.period >= 3 AND ABS(COALESCE(p.score_diff, 0)) > 35)
--
-- Any change to the garbage-time rule (thresholds or periods) MUST be applied
-- in lockstep to BOTH this function AND every inline site below, or the marts
-- and the canonical function/RPCs will silently disagree with each other:
--   - src/schemas/marts/002_game_epa_calc.sql
--   - src/schemas/marts/004_situational_splits.sql
--   - src/schemas/marts/005_defensive_havoc.sql
--   - src/schemas/marts/010_play_epa.sql
--   - src/schemas/marts/019_team_tempo_metrics.sql
--
-- tests/test_garbage_time_consistency.py enforces this: it regex-extracts the
-- inline predicate from every file in src/schemas/marts/ and fails the build
-- if any occurrence drifts from the canonical constant above.

CREATE OR REPLACE FUNCTION public.is_garbage_time(
    period integer,
    score_diff integer
) RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
SET search_path = ''
AS $$
BEGIN
    RETURN (
        (period = 4 AND ABS(COALESCE(score_diff, 0)) > 28) OR
        (period >= 3 AND ABS(COALESCE(score_diff, 0)) > 35)
    );
END;
$$;

COMMENT ON FUNCTION public.is_garbage_time IS 'Returns true if play occurred in garbage time (blowout situations where score diff > 28 in Q4 or > 35 in Q3+)';
