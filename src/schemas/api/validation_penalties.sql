-- Behavioral validation for the penalty analytics layer -- applied as its
-- OWN file/transaction (after marts 041/042 + api 039/040) so a failed
-- assertion reports without rolling back the DDL. Read-only; safe to re-run
-- any time as a health check.
--
-- Four assertion groups:
--   (a) parse coverage floors on marts.penalty_log for recent seasons
--       (infraction extraction, team attribution) -- play_text parsing is
--       best-effort by design, so floors are deliberately below 100%;
--       actual coverage is reported via NOTICE for the log.
--   (b) cross-source consistency: mart 042 (game box scores) season sums vs
--       stats.team_season_stats (independent CFBD endpoint) within 15%.
--   (c) the question that motivated the layer is answerable: holding calls
--       against Oklahoma 2025 opponents > 0 via api.penalty_log.
--   (d) grant tripwire: anon/authenticated/analyst_ro kept SELECT on both
--       new api views.

DO $$
DECLARE
    total BIGINT;
    with_infraction BIGINT;
    with_team BIGINT;
    box_pen BIGINT;
    box_yards BIGINT;
    season_pen BIGINT;
    season_yards BIGINT;
    holding BIGINT;
    fixture RECORD;
    role_name TEXT;
    view_name TEXT;
BEGIN
    -- (a) parse coverage, penalty-type plays, seasons >= 2022
    SELECT COUNT(*),
           COUNT(*) FILTER (WHERE infraction <> 'Unknown'),
           COUNT(*) FILTER (WHERE penalized_team IS NOT NULL)
    INTO total, with_infraction, with_team
    FROM marts.penalty_log
    WHERE season >= 2022 AND is_penalty_play_type;

    RAISE NOTICE 'penalty_log 2022+: % penalty plays, infraction coverage % pct, attribution coverage % pct',
        total,
        round(100.0 * with_infraction / NULLIF(total, 0), 1),
        round(100.0 * with_team / NULLIF(total, 0), 1);

    IF total < 10000 THEN
        RAISE EXCEPTION 'penalty_log: implausibly few 2022+ penalty plays (%)', total;
    END IF;
    IF with_infraction < total * 0.90 THEN
        RAISE EXCEPTION 'penalty_log: infraction coverage below 90%% (%/%)', with_infraction, total;
    END IF;
    IF with_team < total * 0.50 THEN
        RAISE EXCEPTION 'penalty_log: team attribution below 50%% (%/%)', with_team, total;
    END IF;

    -- (b) box sums vs season stats (independent endpoints; 15% tolerance
    --     covers postseason-inclusion and provider differences)
    FOR fixture IN
        SELECT * FROM (VALUES ('Oklahoma', 2024), ('Ohio State', 2024)) f(team, season)
    LOOP
        SELECT SUM(penalties), SUM(penalty_yards) INTO box_pen, box_yards
        FROM marts.team_penalty_box
        WHERE team = fixture.team AND season = fixture.season;

        SELECT MAX(CASE WHEN stat_name = 'penalties' THEN stat_value END),
               MAX(CASE WHEN stat_name = 'penaltyYards' THEN stat_value END)
        INTO season_pen, season_yards
        FROM stats.team_season_stats
        WHERE team = fixture.team AND season = fixture.season;

        RAISE NOTICE '% %: box %/% yds vs season-stats %/% yds',
            fixture.team, fixture.season, box_pen, box_yards, season_pen, season_yards;

        IF box_pen IS NULL OR season_pen IS NULL THEN
            RAISE EXCEPTION '% %: missing penalty totals (box=%, season=%)',
                fixture.team, fixture.season, box_pen, season_pen;
        END IF;
        IF abs(box_pen - season_pen) > season_pen * 0.15
           OR abs(box_yards - season_yards) > season_yards * 0.15 THEN
            RAISE EXCEPTION '% %: box vs season-stats penalty totals diverge >15%% (%/% vs %/%)',
                fixture.team, fixture.season, box_pen, box_yards, season_pen, season_yards;
        END IF;
    END LOOP;

    -- (c) the motivating question is answerable
    SELECT COUNT(*) INTO holding
    FROM api.penalty_log pl
    WHERE pl.season = 2025
      AND pl.infraction = 'Holding'
      AND pl.penalized_team IS NOT NULL
      AND pl.penalized_team <> 'Oklahoma'
      AND (pl.offense = 'Oklahoma' OR pl.defense = 'Oklahoma');

    RAISE NOTICE 'holding calls against Oklahoma 2025 opponents (attributed): %', holding;
    IF holding = 0 THEN
        RAISE EXCEPTION 'penalty_log: no attributed holding calls against Oklahoma 2025 opponents -- parsing or attribution broke';
    END IF;

    -- (d) grants survived the DROP/CREATE
    FOREACH view_name IN ARRAY ARRAY['api.penalty_log', 'api.team_penalties'] LOOP
        FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated', 'analyst_ro'] LOOP
            IF NOT has_table_privilege(role_name, view_name, 'SELECT') THEN
                RAISE EXCEPTION '%: role % lost SELECT after re-apply', view_name, role_name;
            END IF;
        END LOOP;
    END LOOP;

    RAISE NOTICE 'penalty layer validation passed';
END $$;
