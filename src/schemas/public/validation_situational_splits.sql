-- Behavioral validation for public.get_red_zone_splits -- applied as its OWN
-- file/transaction (after 006_play_analysis_functions.sql) so a failed
-- assertion reports without rolling back the function DDL. Read-only; safe
-- to re-run any time as a health check.
--
-- Guards the 2026-07-23 fix: the old version filtered core.drives on the
-- absolute start_yardline (>= 80), undercounting trips on both sides and
-- reporting ~0 defensive red-zone TDs allowed. Assertions recompute the
-- correct definition inline (trip = drive with any snap at
-- yards_to_goal <= 20; outcome = that drive's drive_result) over two frozen
-- team-seasons and require the RPC to match exactly, plus structural
-- sanity floors that the bug class would violate.

DO $$
DECLARE
    exp_trips BIGINT;
    exp_td BIGINT;
    got RECORD;
    fixture RECORD;
BEGIN
    FOR fixture IN
        SELECT * FROM (VALUES
            ('Oklahoma',   2025, 'offense'),
            ('Oklahoma',   2025, 'defense'),
            ('Ohio State', 2024, 'offense'),
            ('Ohio State', 2024, 'defense')
        ) AS f(team, season, side)
    LOOP
        WITH trips AS (
            SELECT DISTINCT p.game_id, p.drive_number
            FROM core.plays p
            JOIN core.games g ON p.game_id = g.id
            WHERE g.season = fixture.season
              AND p.yards_to_goal <= 20
              AND p.drive_number IS NOT NULL
              AND CASE WHEN fixture.side = 'offense'
                       THEN p.offense = fixture.team
                       ELSE p.defense = fixture.team END
        )
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE d.drive_result IN ('TD', 'Touchdown'))
        INTO exp_trips, exp_td
        FROM trips t
        LEFT JOIN core.drives d
          ON d.game_id = t.game_id AND d.drive_number = t.drive_number;

        SELECT r.trips, r.touchdowns INTO got
        FROM public.get_red_zone_splits(fixture.team, fixture.season) r
        WHERE r.side = fixture.side;

        IF got.trips IS DISTINCT FROM exp_trips
           OR got.touchdowns IS DISTINCT FROM exp_td THEN
            RAISE EXCEPTION '% % %: RPC trips/TD = %/%, expected %/%',
                fixture.team, fixture.season, fixture.side,
                got.trips, got.touchdowns, exp_trips, exp_td;
        END IF;

        -- Structural floors the old bug violated: a full season has real
        -- red-zone volume and nonzero TDs on BOTH sides.
        IF exp_trips < 15 OR exp_td < 1 THEN
            RAISE EXCEPTION '% % %: implausible ground truth (trips=%, td=%)',
                fixture.team, fixture.season, fixture.side, exp_trips, exp_td;
        END IF;
    END LOOP;

    RAISE NOTICE 'situational splits validation passed';
END $$;
