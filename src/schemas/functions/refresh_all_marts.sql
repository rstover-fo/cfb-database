-- Refresh all materialized views in the marts schema in dependency order.
--
-- Layers:
--   1: _game_epa_calc, play_epa, player_comparison, conference_head_to_head (no mart dependencies)
--   2: team_epa_season, team_season_summary, player_game_epa, defensive_havoc, scoring_opportunities,
--      team_playcalling_tendencies, team_situational_success
--   3: situational_splits, player_season_epa, coach_record, matchup_history, recruiting_class,
--      team_talent_composite, team_tempo_metrics, transfer_portal_impact
--   4: team_season_trajectory, conference_era_summary, team_style_profile,
--      coaching_tenure, recruiting_roi, conference_comparison
--   5: matchup_edges
--
-- Usage:
--   SELECT * FROM marts.refresh_all();

CREATE OR REPLACE FUNCTION marts.refresh_all()
RETURNS TABLE(view_name text, duration_ms bigint, status text)
LANGUAGE plpgsql
SET statement_timeout = 0
SET search_path = ''
AS $$
DECLARE
    v_views text[];
    v_name text;
    v_start timestamptz;
    v_elapsed bigint;
    v_layer int;
BEGIN
    -- Layer 1: No dependencies on other marts
    v_views := ARRAY[
        '_game_epa_calc',
        'play_epa',
        'player_comparison',
        'conference_head_to_head'
    ];
    v_layer := 1;

    FOREACH v_name IN ARRAY v_views LOOP
        v_start := clock_timestamp();
        BEGIN
            EXECUTE format('REFRESH MATERIALIZED VIEW marts.%I', v_name);
            v_elapsed := EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::bigint;
            view_name := v_name;
            duration_ms := v_elapsed;
            status := format('OK (layer %s)', v_layer);
            RETURN NEXT;
        EXCEPTION WHEN OTHERS THEN
            v_elapsed := EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::bigint;
            view_name := v_name;
            duration_ms := v_elapsed;
            status := format('ERROR (layer %s): %s', v_layer, SQLERRM);
            RETURN NEXT;
        END;
    END LOOP;

    -- Layer 2: Depends on Layer 1
    v_views := ARRAY[
        'team_epa_season',
        'team_season_summary',
        'player_game_epa',
        'defensive_havoc',
        'scoring_opportunities',
        'team_playcalling_tendencies',
        'team_situational_success'
    ];
    v_layer := 2;

    FOREACH v_name IN ARRAY v_views LOOP
        v_start := clock_timestamp();
        BEGIN
            EXECUTE format('REFRESH MATERIALIZED VIEW marts.%I', v_name);
            v_elapsed := EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::bigint;
            view_name := v_name;
            duration_ms := v_elapsed;
            status := format('OK (layer %s)', v_layer);
            RETURN NEXT;
        EXCEPTION WHEN OTHERS THEN
            v_elapsed := EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::bigint;
            view_name := v_name;
            duration_ms := v_elapsed;
            status := format('ERROR (layer %s): %s', v_layer, SQLERRM);
            RETURN NEXT;
        END;
    END LOOP;

    -- Layer 3: Depends on Layer 2
    v_views := ARRAY[
        'situational_splits',
        'player_season_epa',
        'coach_record',
        'matchup_history',
        'recruiting_class',
        'team_talent_composite',
        'team_tempo_metrics',
        'transfer_portal_impact'
    ];
    v_layer := 3;

    FOREACH v_name IN ARRAY v_views LOOP
        v_start := clock_timestamp();
        BEGIN
            EXECUTE format('REFRESH MATERIALIZED VIEW marts.%I', v_name);
            v_elapsed := EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::bigint;
            view_name := v_name;
            duration_ms := v_elapsed;
            status := format('OK (layer %s)', v_layer);
            RETURN NEXT;
        EXCEPTION WHEN OTHERS THEN
            v_elapsed := EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::bigint;
            view_name := v_name;
            duration_ms := v_elapsed;
            status := format('ERROR (layer %s): %s', v_layer, SQLERRM);
            RETURN NEXT;
        END;
    END LOOP;

    -- Layer 4: Depends on Layer 3
    v_views := ARRAY[
        'team_season_trajectory',
        'conference_era_summary',
        'team_style_profile',
        'coaching_tenure',
        'recruiting_roi',
        'conference_comparison'
    ];
    v_layer := 4;

    FOREACH v_name IN ARRAY v_views LOOP
        v_start := clock_timestamp();
        BEGIN
            EXECUTE format('REFRESH MATERIALIZED VIEW marts.%I', v_name);
            v_elapsed := EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::bigint;
            view_name := v_name;
            duration_ms := v_elapsed;
            status := format('OK (layer %s)', v_layer);
            RETURN NEXT;
        EXCEPTION WHEN OTHERS THEN
            v_elapsed := EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::bigint;
            view_name := v_name;
            duration_ms := v_elapsed;
            status := format('ERROR (layer %s): %s', v_layer, SQLERRM);
            RETURN NEXT;
        END;
    END LOOP;

    -- Layer 5: Depends on Layer 4 + standalone
    v_views := ARRAY[
        'matchup_edges',
        'data_freshness'
    ];
    v_layer := 5;

    FOREACH v_name IN ARRAY v_views LOOP
        v_start := clock_timestamp();
        BEGIN
            EXECUTE format('REFRESH MATERIALIZED VIEW marts.%I', v_name);
            v_elapsed := EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::bigint;
            view_name := v_name;
            duration_ms := v_elapsed;
            status := format('OK (layer %s)', v_layer);
            RETURN NEXT;
        EXCEPTION WHEN OTHERS THEN
            v_elapsed := EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::bigint;
            view_name := v_name;
            duration_ms := v_elapsed;
            status := format('ERROR (layer %s): %s', v_layer, SQLERRM);
            RETURN NEXT;
        END;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION marts.refresh_all IS
'Refreshes all 28 materialized views in the marts schema in dependency order (5 layers). '
'Returns timing and status for each view. Errors are caught per-view so one failure does not abort the rest.';
