-- Refresh the scouting player mart materialized view

CREATE OR REPLACE FUNCTION scouting.refresh_player_mart()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY scouting.player_mart;
END;
$function$;
