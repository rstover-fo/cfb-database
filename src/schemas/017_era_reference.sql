-- Era definitions for historical analysis
-- Defines major CFB eras for trend analysis and benchmarking

DROP TABLE IF EXISTS ref.eras CASCADE;

CREATE TABLE ref.eras (
    era_code VARCHAR(20) PRIMARY KEY,
    era_name VARCHAR(50) NOT NULL,
    start_year INT NOT NULL,
    end_year INT,  -- NULL means ongoing
    description TEXT
);

INSERT INTO ref.eras (era_code, era_name, start_year, end_year, description) VALUES
    ('BCS', 'BCS Era', 2004, 2013, 'Bowl Championship Series, pre-playoff'),
    ('PLAYOFF_V1', 'Playoff V1', 2014, 2023, '4-team playoff, conference championship emphasis'),
    ('PORTAL_NIL', 'Portal/NIL Era', 2021, NULL, 'Transfer portal explosion, NIL deals reshape rosters'),
    ('PLAYOFF_V2', 'Playoff V2', 2024, NULL, '12-team playoff, expanded access');

-- Helper function to get era(s) for a given year
-- Note: Years can belong to multiple eras (e.g., 2021+ is both PORTAL_NIL and PLAYOFF_V1/V2)
CREATE OR REPLACE FUNCTION ref.get_era(p_year INT)
RETURNS TABLE(era_code VARCHAR, era_name VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT e.era_code, e.era_name
    FROM ref.eras e
    WHERE p_year >= e.start_year
      AND (e.end_year IS NULL OR p_year <= e.end_year);
END;
$$ LANGUAGE plpgsql STABLE;

-- Index for year range lookups
CREATE INDEX IF NOT EXISTS idx_eras_years ON ref.eras (start_year, end_year);
