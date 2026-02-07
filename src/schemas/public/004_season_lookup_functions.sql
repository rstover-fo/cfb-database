-- Season/week lookup functions
-- Used by the app to populate dropdowns and navigation.
-- Created ad-hoc in Supabase; now tracked in version control.

CREATE OR REPLACE FUNCTION public.get_available_seasons()
RETURNS INT[]
LANGUAGE sql
STABLE
SET search_path = ''
AS $$
    SELECT ARRAY_AGG(DISTINCT season ORDER BY season DESC)
    FROM core.games
    WHERE completed = true;
$$;

CREATE OR REPLACE FUNCTION public.get_available_weeks(p_season INT)
RETURNS INT[]
LANGUAGE sql
STABLE
SET search_path = ''
AS $$
    SELECT ARRAY_AGG(DISTINCT week ORDER BY week)
    FROM core.games
    WHERE season = p_season AND completed = true;
$$;
