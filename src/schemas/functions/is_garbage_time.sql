-- Function to detect garbage time plays
-- Garbage time: margin > 28 in 4th quarter, or > 35 in 3rd+
-- Used to filter out non-competitive plays in EPA calculations

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
