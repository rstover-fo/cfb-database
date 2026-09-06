-- Explicit-file cleanup for the stale F03 2026 fitted_v1 outlook aliases.
--
-- Evidence: Deploy Schema diagnostic run 34065108610 reported exactly these
-- 30 pre-recovery rows, each with zero current canonical regular-season games.
-- They are obsolete provider/team labels, not evidence for a rename mapping;
-- this script intentionally makes no alias or team-name normalization guess.
--
-- Run transactionally through run_migrations.py only after review.  The
-- private recovery.f03_row_archive journal was created by f03-data-repair.sql.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

-- Keep the schedule fact and the append-only snapshots stable between the
-- preflight, archive-equality check, and delete.
LOCK TABLE core.games IN SHARE MODE;
LOCK TABLE predictions.season_projections IN SHARE ROW EXCLUSIVE MODE;

DO $outlook_cleanup$
DECLARE
    allowed_teams text[] := ARRAY[
        'Albany State GA', 'Anderson (Sc)', 'Benedictine University',
        'California Lutheran University', 'Castleton', 'Central State (OH)',
        'College Of New Jersey', 'Concord University', 'Concordia',
        'Concordia University Chicago', 'Concordia-Wisconsin', 'Dickinson (PA)',
        'Eureka College', 'Greeneville', 'Lewis & Clark College', 'Linfield College',
        'Little Rock', 'Louisiana College', 'Miles College', 'Morehouse College',
        'NEWBERG', 'Saint Xavier (IL)', 'Savannah St', 'St Francis Illinois',
        'Thomas More College', 'Virginia St', 'Virginia University Of Lynchburg',
        'Western Connecticut St', 'Winston-Salem', 'Wisconsin-Lutheran'
    ];
    cutoff timestamptz := '2026-09-06 22:41:07+00'::timestamptz;
    blocked_teams text[];
    archived_count bigint;
    deleted_count bigint;
BEGIN
    IF cardinality(allowed_teams) <> 30 THEN
        RAISE EXCEPTION 'Expected exactly 30 reviewed stale outlook aliases';
    END IF;

    -- A now-canonical schedule or a fresh snapshot changes the diagnosis.
    -- Stop rather than deleting a row whose team string may now be active.
    SELECT array_agg(DISTINCT t.team ORDER BY t.team)
    INTO blocked_teams
    FROM unnest(allowed_teams) AS t(team)
    JOIN core.games AS g
      ON g.season = 2026
     AND g.season_type = 'regular'
     AND g.id <> 401866625
     AND (g.home_team = t.team OR g.away_team = t.team);
    IF blocked_teams IS NOT NULL THEN
        RAISE EXCEPTION 'Reviewed stale outlook aliases now have canonical games: %', blocked_teams;
    END IF;

    SELECT array_agg(DISTINCT p.team ORDER BY p.team)
    INTO blocked_teams
    FROM predictions.season_projections AS p
    WHERE p.season = 2026
      AND p.model_version = 'fitted_v1'
      AND p.team = ANY(allowed_teams)
      AND p.computed_at >= cutoff;
    IF blocked_teams IS NOT NULL THEN
        RAISE EXCEPTION 'Reviewed stale outlook aliases now have recent projections: %', blocked_teams;
    END IF;

    INSERT INTO recovery.f03_row_archive(source_table, row_id, payload)
    SELECT 'predictions.season_projections', p.projection_id, to_jsonb(p)
    FROM predictions.season_projections AS p
    WHERE p.season = 2026
      AND p.model_version = 'fitted_v1'
      AND p.team = ANY(allowed_teams)
      AND p.computed_at < cutoff
      AND NOT EXISTS (
          SELECT 1
          FROM core.games AS g
          WHERE g.season = 2026
            AND g.season_type = 'regular'
            AND g.id <> 401866625
            AND (g.home_team = p.team OR g.away_team = p.team)
      )
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS archived_count = ROW_COUNT;

    -- A prior archive key is acceptable only when it has an equal JSONB
    -- payload to the row about to be removed.
    IF EXISTS (
        SELECT 1
        FROM predictions.season_projections AS p
        LEFT JOIN recovery.f03_row_archive AS a
          ON a.source_table = 'predictions.season_projections'
         AND a.row_id = p.projection_id
        WHERE p.season = 2026
          AND p.model_version = 'fitted_v1'
          AND p.team = ANY(allowed_teams)
          AND p.computed_at < cutoff
          AND NOT EXISTS (
              SELECT 1
              FROM core.games AS g
              WHERE g.season = 2026
                AND g.season_type = 'regular'
                AND g.id <> 401866625
                AND (g.home_team = p.team OR g.away_team = p.team)
          )
          AND a.payload IS DISTINCT FROM to_jsonb(p)
    ) THEN
        RAISE EXCEPTION 'Outlook archive mismatch; no deletion permitted';
    END IF;

    DELETE FROM predictions.season_projections AS p
    USING recovery.f03_row_archive AS a
    WHERE a.source_table = 'predictions.season_projections'
      AND a.row_id = p.projection_id
      AND a.payload = to_jsonb(p)
      AND p.season = 2026
      AND p.model_version = 'fitted_v1'
      AND p.team = ANY(allowed_teams)
      AND p.computed_at < cutoff
      AND NOT EXISTS (
          SELECT 1
          FROM core.games AS g
          WHERE g.season = 2026
            AND g.season_type = 'regular'
            AND g.id <> 401866625
            AND (g.home_team = p.team OR g.away_team = p.team)
      );
    GET DIAGNOSTICS deleted_count = ROW_COUNT;

    RAISE NOTICE 'Archived % stale F03 outlook snapshots', archived_count;
    RAISE NOTICE 'Deleted % stale F03 outlook snapshots', deleted_count;
END $outlook_cleanup$;
