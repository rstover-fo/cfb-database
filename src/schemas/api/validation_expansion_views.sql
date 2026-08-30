-- Behavioral validation for the 2026-08-30 expansion_views unit (cfb-app
-- work order P1 items: passing charting, coaching, player_detail dedupe/
-- extension, refresh-campaign status). Applied as its OWN file/transaction
-- (after the marts + api files listed in
-- deploys/expansion_views-manifest.json) so a failed assertion reports
-- without rolling back the DDL. Read-only; safe to re-run any time as a
-- health check. Style follows src/schemas/api/validation_penalties.sql.
--
-- Five assertion groups:
--   (a) each new view exists and returns rows, EXCEPT api.coach_tenures and
--       api.refresh_campaign_status, which are legitimately empty until
--       their respective backfills/campaigns exist (NOTICE only, no
--       failure -- see each view's own header for why).
--   (b) api.player_detail has zero (player_id, season, team) dupes for
--       season 2025 -- the view's own declared grain, checked at the finer
--       grain than the work order's literal (player_id, season) phrasing
--       because a mid-season transfer legitimately produces two
--       (player_id, season) rows under different team values; that is not
--       the reclassification bug task 0 fixed. A targeted spot-check on the
--       specific reported case (player_id 5079720) is included alongside.
--   (c) api.passing_charting_target_season's target_share_charted and
--       partial_share are both within [0, 1] (NULLs ignored).
--   (d) api.coach_tenures' classification non-null rate is sane, SKIPPED
--       (not failed) if the table is empty (backfill-gated -- see (a)).
--   (e) grant tripwire: anon/authenticated/analyst_ro kept SELECT on every
--       new/changed view in this unit.

DO $$
DECLARE
    n_player_season BIGINT;
    n_target_season BIGINT;
    n_team_season BIGINT;
    n_coach_tenures BIGINT;
    n_refresh_status BIGINT;
    n_dupes BIGINT;
    n_smith_rows BIGINT;
    min_target_share NUMERIC;
    max_target_share NUMERIC;
    min_partial_share NUMERIC;
    max_partial_share NUMERIC;
    n_tenures_total BIGINT;
    n_tenures_classified BIGINT;
    classified_rate NUMERIC;
    role_name TEXT;
    view_name TEXT;
BEGIN
    -- (a) existence + rows (or legitimately-empty)
    SELECT COUNT(*) INTO n_player_season FROM api.passing_charting_player_season;
    SELECT COUNT(*) INTO n_target_season FROM api.passing_charting_target_season;
    SELECT COUNT(*) INTO n_team_season FROM api.passing_charting_team_season;
    SELECT COUNT(*) INTO n_coach_tenures FROM api.coach_tenures;
    SELECT COUNT(*) INTO n_refresh_status FROM api.refresh_campaign_status;

    RAISE NOTICE 'passing_charting_player_season: % rows', n_player_season;
    RAISE NOTICE 'passing_charting_target_season: % rows', n_target_season;
    RAISE NOTICE 'passing_charting_team_season: % rows', n_team_season;
    RAISE NOTICE 'coach_tenures: % rows (0 is OK -- backfill-gated, see view header)', n_coach_tenures;
    RAISE NOTICE 'refresh_campaign_status: % rows (0 is OK -- no active correction campaign row yet)', n_refresh_status;

    IF n_player_season = 0 THEN
        RAISE EXCEPTION 'passing_charting_player_season: empty -- expected 2025+ passing charting rows (source stats.passing_player_season reported ~820 player-seasons for 2025 per src/pipelines/sources/passing.py)';
    END IF;
    IF n_target_season = 0 THEN
        RAISE EXCEPTION 'passing_charting_target_season: empty -- expected rows aggregated from stats.passing_plays for 2025+';
    END IF;
    IF n_team_season = 0 THEN
        RAISE EXCEPTION 'passing_charting_team_season: empty -- expected ~136 team-seasons for 2025 per src/pipelines/sources/passing.py';
    END IF;

    -- (b) player_detail dedupe (task 0). Finer grain than the work order's
    -- literal (player_id, season) -- see header note.
    SELECT COUNT(*) INTO n_dupes
    FROM (
        SELECT player_id, season, team
        FROM api.player_detail
        WHERE season = 2025
        GROUP BY player_id, season, team
        HAVING COUNT(*) > 1
    ) dupes;

    RAISE NOTICE 'player_detail 2025: % (player_id, season, team) dupe group(s)', n_dupes;
    IF n_dupes > 0 THEN
        RAISE EXCEPTION 'player_detail: % (player_id, season, team) dupe group(s) found for season 2025 -- task 0 dedupe regressed', n_dupes;
    END IF;

    -- Targeted spot-check: the reported reclassification case
    -- (player_id 5079720, season 2025, two recruiting.recruits rows
    -- pre-fix) must now return exactly one row.
    SELECT COUNT(*) INTO n_smith_rows
    FROM api.player_detail
    WHERE player_id::text = '5079720' AND season = 2025;

    RAISE NOTICE 'player_detail 5079720/2025: % row(s) (expected 1)', n_smith_rows;
    IF n_smith_rows <> 1 THEN
        RAISE EXCEPTION 'player_detail: expected exactly 1 row for player_id 5079720, season 2025, got % -- reclassification dedupe regressed or roster/recruiting data changed shape', n_smith_rows;
    END IF;

    -- (c) target-season shares in [0, 1] (NULLs ignored by MIN/MAX)
    SELECT MIN(target_share_charted), MAX(target_share_charted),
           MIN(partial_share), MAX(partial_share)
    INTO min_target_share, max_target_share, min_partial_share, max_partial_share
    FROM api.passing_charting_target_season;

    RAISE NOTICE 'target_share_charted range: [%, %]; partial_share range: [%, %]',
        min_target_share, max_target_share, min_partial_share, max_partial_share;

    IF min_target_share IS NOT NULL AND (min_target_share < 0 OR max_target_share > 1) THEN
        RAISE EXCEPTION 'passing_charting_target_season: target_share_charted out of [0,1] range (min=%, max=%)',
            min_target_share, max_target_share;
    END IF;
    IF min_partial_share IS NOT NULL AND (min_partial_share < 0 OR max_partial_share > 1) THEN
        RAISE EXCEPTION 'passing_charting_target_season: partial_share out of [0,1] range (min=%, max=%)',
            min_partial_share, max_partial_share;
    END IF;

    -- (d) coach_tenures classification non-null rate, skipped if empty
    SELECT COUNT(*), COUNT(*) FILTER (WHERE classification IS NOT NULL)
    INTO n_tenures_total, n_tenures_classified
    FROM api.coach_tenures;

    IF n_tenures_total = 0 THEN
        RAISE NOTICE 'coach_tenures: 0 rows, skipping classification-coverage check (backfill has not run)';
    ELSE
        classified_rate := n_tenures_classified::numeric / n_tenures_total;
        RAISE NOTICE 'coach_tenures classification coverage: %/% (% pct)',
            n_tenures_classified, n_tenures_total, round(100.0 * classified_rate, 1);
        IF classified_rate < 0.5 THEN
            RAISE EXCEPTION 'coach_tenures: classification non-null rate below 50%% (%/%) -- team_id join to ref.teams likely broken',
                n_tenures_classified, n_tenures_total;
        END IF;
    END IF;

    -- (e) grant tripwire: anon/authenticated/analyst_ro kept SELECT on
    -- every new/changed view in this unit
    FOREACH view_name IN ARRAY ARRAY[
        'api.passing_charting_player_season',
        'api.passing_charting_target_season',
        'api.passing_charting_team_season',
        'api.coach_tenures',
        'api.refresh_campaign_status',
        'api.player_detail',
        'api.coaching_history',
        'api.coach_records'
    ] LOOP
        FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated', 'analyst_ro'] LOOP
            IF NOT has_table_privilege(role_name, view_name, 'SELECT') THEN
                RAISE EXCEPTION '%: role % lost SELECT', view_name, role_name;
            END IF;
        END LOOP;
    END LOOP;

    RAISE NOTICE 'expansion_views validation passed';
END $$;
