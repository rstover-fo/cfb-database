-- scouting.fn_evaluate_portal_value: automated portal surveillance (re-adopted 2026-08-08)
-- =============================================================================
-- Re-adopts the function from the removed migration 017_portal_surveillance_cron.sql,
-- preserved verbatim in docs/handoffs/2026-07-19-portal-surveillance-cron-to-cfb-scout.md.
-- That handoff pushed the SQL out to cfb-scout because the scouting schema was owned
-- there; the 2026-08 monorepo merge brings ownership (and this function) back.
--
-- The function does NOT exist in production as of 2026-08-08 (the removed migration
-- was never applied live) -- first application creates it. It only reads/writes
-- scouting.* tables, all of which 001_tables.sql defines.
--
-- Deviation from the handoff original: SET search_path = '' added per this repo's
-- SECURITY DEFINER rules (all table references in the body are schema-qualified).
--
-- PARKED: the cron scheduling from the original is intentionally NOT executed here.
-- Revival step, run once (pg_cron is already enabled on the project):
--   SELECT cron.schedule('daily-portal-surveillance', '0 21 * * *',
--                        $$SELECT scouting.fn_evaluate_portal_value()$$);
--
-- Applied via: python scripts/run_migrations.py --file src/schemas/scouting/004_portal_surveillance.sql

CREATE OR REPLACE FUNCTION scouting.fn_evaluate_portal_value()
RETURNS TABLE (
    player_id int,
    player_name text,
    value_score decimal,
    alert_fired boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $$
DECLARE
    entry record;
    v_score decimal;
    v_alert_id int;
    v_threshold decimal := 80.0;
BEGIN
    -- Iterate through players who entered in the last 24h
    FOR entry IN
        SELECT DISTINCT p.id, p.name, p.composite_grade, te.from_team
        FROM scouting.players p
        JOIN scouting.transfer_events te ON p.id = te.player_id
        WHERE te.event_type = 'entered'
          AND te.created_at >= now() - interval '24 hours'
    LOOP
        -- Score: (composite grade * 50%) + (latest PFF overall grade * 50%)
        SELECT
            round(
                (coalesce(entry.composite_grade, 0) * 0.5) +
                (coalesce(
                    (SELECT overall_grade
                     FROM scouting.pff_grades
                     WHERE pff_grades.player_id = entry.id
                     ORDER BY season DESC, week DESC NULLS FIRST LIMIT 1
                    ), 0) * 0.5),
            1) INTO v_score;

        IF v_score >= v_threshold THEN
            -- Find or create the alert definition for this player
            INSERT INTO scouting.alerts (user_id, name, alert_type, player_id, threshold)
            VALUES ('system', 'portal-value-' || entry.id, 'portal_entry', entry.id,
                    jsonb_build_object('min_score', v_threshold))
            ON CONFLICT (user_id, name) DO UPDATE SET last_checked_at = now()
            RETURNING id INTO v_alert_id;

            INSERT INTO scouting.alert_history (alert_id, trigger_data, message)
            VALUES (
                v_alert_id,
                jsonb_build_object(
                    'score', v_score,
                    'composite', entry.composite_grade,
                    'from_team', entry.from_team
                ),
                'High-value portal entrant: ' || entry.name || ' (Score: ' || v_score || ')'
            );

            player_id := entry.id;
            player_name := entry.name;
            value_score := v_score;
            alert_fired := true;
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION scouting.fn_evaluate_portal_value() IS
    'Automated scouter that identifies high-value portal entrants and fires system alerts.';
