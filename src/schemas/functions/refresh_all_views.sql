-- Refresh all materialized views in the analytics schema

CREATE OR REPLACE FUNCTION analytics.refresh_all_views()
RETURNS void
LANGUAGE plpgsql
SET search_path = ''
AS $function$
BEGIN
    RAISE NOTICE 'Refreshing analytics.team_season_summary...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.team_season_summary;

    RAISE NOTICE 'Refreshing analytics.player_career_stats...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.player_career_stats;

    RAISE NOTICE 'Refreshing analytics.conference_standings...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.conference_standings;

    RAISE NOTICE 'Refreshing analytics.team_recruiting_trend...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.team_recruiting_trend;

    RAISE NOTICE 'Refreshing analytics.game_results...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.game_results;

    RAISE NOTICE 'All analytics views refreshed.';
END;
$function$;
