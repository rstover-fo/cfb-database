-- Get era information for a given year
-- Returns the era_code and era_name from ref.eras for the specified year

CREATE OR REPLACE FUNCTION ref.get_era(p_year integer)
RETURNS TABLE(era_code character varying, era_name character varying)
LANGUAGE plpgsql
STABLE
SET search_path = ''
AS $function$
BEGIN
    RETURN QUERY
    SELECT e.era_code, e.era_name
    FROM ref.eras e
    WHERE p_year >= e.start_year
      AND (e.end_year IS NULL OR p_year <= e.end_year);
END;
$function$;
