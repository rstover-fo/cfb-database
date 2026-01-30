-- Aggregate drive data for visualization
-- Returns bucketed start/end positions with outcome counts
CREATE OR REPLACE FUNCTION get_drive_patterns(
  p_team TEXT,
  p_season INT
)
RETURNS TABLE (
  start_yard INT,
  end_yard INT,
  outcome TEXT,
  count BIGINT,
  avg_plays NUMERIC,
  avg_yards NUMERIC
) AS $$
BEGIN
  RETURN QUERY
  WITH drive_outcomes AS (
    SELECT
      -- Convert yards_to_goal to own-side yard line (0 = own goal, 100 = opponent goal)
      (100 - d.start_yards_to_goal)::INT AS start_yard,
      -- End yard = start + yards gained (capped at 100)
      LEAST(100, (100 - d.start_yards_to_goal + d.yards))::INT AS end_yard,
      CASE
        WHEN d.drive_result IN ('TD', 'TOUCHDOWN', 'Touchdown') THEN 'touchdown'
        WHEN d.drive_result IN ('FG', 'FIELD GOAL', 'FG GOOD', 'Field Goal') THEN 'field_goal'
        WHEN d.drive_result IN ('PUNT', 'Punt') THEN 'punt'
        WHEN d.drive_result IN ('INT', 'INTERCEPTION', 'FUMBLE', 'FUMBLE LOST', 'INT TD', 'FUMBLE TD', 'Interception', 'Fumble', 'Fumble Lost', 'Interception Return') THEN 'turnover'
        WHEN d.drive_result IN ('END OF HALF', 'END OF GAME', 'END OF 4TH QUARTER', 'End of Half', 'End of Game') THEN 'end_of_half'
        WHEN d.drive_result IN ('DOWNS', 'TURNOVER ON DOWNS', 'Downs', 'Turnover on Downs') THEN 'downs'
        ELSE 'other'
      END AS outcome,
      d.plays,
      d.yards
    FROM core.drives d
    WHERE d.offense = p_team
      AND d.season = p_season
      AND d.start_yards_to_goal IS NOT NULL
  )
  SELECT
    -- Bucket start yards into 10-yard zones
    (FLOOR(drv.start_yard / 10.0) * 10)::INT AS start_yard,
    -- Bucket end yards into 10-yard zones
    (FLOOR(drv.end_yard / 10.0) * 10)::INT AS end_yard,
    drv.outcome,
    COUNT(*)::BIGINT AS count,
    ROUND(AVG(drv.plays), 1) AS avg_plays,
    ROUND(AVG(drv.yards), 1) AS avg_yards
  FROM drive_outcomes drv
  WHERE drv.outcome != 'other'
  GROUP BY 1, 2, drv.outcome
  HAVING COUNT(*) >= 2  -- At least 2 drives for this pattern
  ORDER BY drv.outcome, 1, 2;
END;
$$ LANGUAGE plpgsql STABLE;
