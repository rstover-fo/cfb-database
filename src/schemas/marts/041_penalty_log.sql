-- marts.penalty_log
-- =============================================================================
-- Play-derived penalty event log (2026-07-23 penalty analytics layer).
-- Grain: one row per play carrying penalty text -- play_type = 'Penalty'
-- (the flag WAS the play) plus plays of any other type whose play_text
-- mentions a penalty (declined / offsetting / tacked onto a completed play;
-- distinguish via is_penalty_play_type).
--
-- CFBD play_text is free text spanning four provider formats (verified by
-- probe, deploy run 30043634618):
--   2005-2010  "OKLAHOMA penalty 9 yard holding accepted, no play."
--   2015-2024  "Toledo Penalty, Unsportsmanlike Conduct (player) to the TOL 20"
--   2015-2025  "... PENALTY ECU Holding (Moorer,Parker) 10 yards from ... NO PLAY."
--              (team appears as abbreviation or even nickname: "PENALTY Owls ...")
--   2024-      "PENALTY on ILL-T.Cox, Unsportsmanlike Conduct / Defense, ..."
-- Parsing is therefore BEST-EFFORT by design: `infraction` comes from an
-- ordered ILIKE pattern list (unmatched -> 'Unknown'), `penalized_team` from
-- matching the game's two teams' school / 'St'-abbreviated school /
-- abbreviation / mascot against the text (NULL when neither or both match).
-- parse_ok marks rows where both succeeded; consumers needing exact officiating
-- data should treat 'Unknown'/NULL rows as unclassified, not absent.
-- Coverage floors are enforced in prod by
-- src/schemas/api/validation_penalties.sql.

DROP MATERIALIZED VIEW IF EXISTS marts.penalty_log CASCADE;

CREATE MATERIALIZED VIEW marts.penalty_log AS
WITH infractions (priority, label, pattern) AS (
    -- Ordered: specific phrases before generic substrings (Face Mask before
    -- Personal Foul; Pass Interference before Holding is moot but explicit).
    VALUES
        (1,  'Pass Interference',       '%pass interference%'),
        (2,  'Kick Catch Interference', '%kick catch%'),
        (3,  'Roughing the Passer',     '%roughing%passer%'),
        (4,  'Roughing the Kicker',     '%roughing%kicker%'),
        (5,  'Roughing the Snapper',    '%roughing%snapper%'),
        (6,  'Targeting',               '%targeting%'),
        (7,  'Horse Collar',            '%horse collar%'),
        (8,  'Face Mask',               '%face mask%'),
        (9,  'Chop Block',              '%chop block%'),
        (10, 'Clipping',                '%clipping%'),
        (11, 'Intentional Grounding',   '%intentional grounding%'),
        (12, 'False Start',             '%false start%'),
        (13, 'Delay of Game',           '%delay of game%'),
        (14, 'Encroachment',            '%encroachment%'),
        (15, 'Offside',                 '%offside%'),
        (16, 'Holding',                 '%holding%'),
        (17, 'Illegal Block',           '%illegal block%'),
        (18, 'Illegal Formation',       '%illegal formation%'),
        (19, 'Illegal Motion',          '%illegal motion%'),
        (20, 'Illegal Shift',           '%illegal shift%'),
        (21, 'Illegal Substitution',    '%substitution%'),
        (22, 'Illegal Touching',        '%illegal touch%'),
        (23, 'Illegal Forward Pass',    '%illegal forward pass%'),
        (24, 'Illegal Snap',            '%illegal snap%'),
        (25, 'Ineligible Downfield',    '%ineligible%'),
        (26, 'Unsportsmanlike Conduct', '%unsportsmanlike%'),
        (27, 'Personal Foul',           '%personal foul%'),
        (28, 'Sideline Interference',   '%sideline interference%'),
        (29, 'Too Many Men',            '%12 men%'),
        (30, 'Too Many Men',            '%too many%'),
        (31, 'Disqualification',        '%disqualification%')
),
team_idents AS (
    -- ref.teams can carry duplicate school rows; dedupe like
    -- api.team_playcalling_profile does.
    SELECT DISTINCT ON (school)
        school,
        replace(school, 'State', 'St') AS school_st,  -- "KENT ST Penalty" era
        abbreviation,
        mascot
    FROM ref.teams
    ORDER BY school, classification NULLS LAST
),
penalty_plays AS (
    SELECT
        p.id AS play_id,
        p.game_id,
        p.season,
        g.week,
        g.season_type,
        p.period,
        p.down,
        p.distance,
        p.offense,
        p.defense,
        p.home,
        p.away,
        p.play_type,
        p.play_type = 'Penalty' AS is_penalty_play_type,
        p.yards_gained,
        p.ppa,
        p.play_text
    FROM core.plays p
    JOIN core.games g ON p.game_id = g.id AND g.season = p.season
    WHERE p.play_type = 'Penalty'
       OR p.play_text ~* 'penalt'
),
attributed AS (
    SELECT
        pp.*,
        CASE
            WHEN m.matches_home AND NOT m.matches_away THEN pp.home
            WHEN m.matches_away AND NOT m.matches_home THEN pp.away
        END AS penalized_team
    FROM penalty_plays pp
    LEFT JOIN team_idents ht ON ht.school = pp.home
    LEFT JOIN team_idents awy ON awy.school = pp.away
    CROSS JOIN LATERAL (
        SELECT
            (pp.play_text ILIKE '%' || pp.home || ' penalty%'
             OR pp.play_text ILIKE '%penalty ' || pp.home || ' %'
             OR (ht.school_st <> pp.home
                 AND (pp.play_text ILIKE '%' || ht.school_st || ' penalty%'
                      OR pp.play_text ILIKE '%penalty ' || ht.school_st || ' %'))
             OR (ht.abbreviation IS NOT NULL
                 AND (pp.play_text ILIKE '%penalty ' || ht.abbreviation || ' %'
                      OR pp.play_text ILIKE '%penalty on ' || ht.abbreviation || '-%'))
             OR (ht.mascot IS NOT NULL
                 AND pp.play_text ILIKE '%penalty ' || ht.mascot || ' %')
            ) AS matches_home,
            (pp.play_text ILIKE '%' || pp.away || ' penalty%'
             OR pp.play_text ILIKE '%penalty ' || pp.away || ' %'
             OR (awy.school_st <> pp.away
                 AND (pp.play_text ILIKE '%' || awy.school_st || ' penalty%'
                      OR pp.play_text ILIKE '%penalty ' || awy.school_st || ' %'))
             OR (awy.abbreviation IS NOT NULL
                 AND (pp.play_text ILIKE '%penalty ' || awy.abbreviation || ' %'
                      OR pp.play_text ILIKE '%penalty on ' || awy.abbreviation || '-%'))
             OR (awy.mascot IS NOT NULL
                 AND pp.play_text ILIKE '%penalty ' || awy.mascot || ' %')
            ) AS matches_away
    ) m
)
SELECT
    a.play_id,
    a.game_id,
    a.season,
    a.week,
    a.season_type,
    a.period,
    a.down,
    a.distance,
    a.offense,
    a.defense,
    a.play_type,
    a.is_penalty_play_type,
    a.penalized_team,
    CASE
        WHEN a.penalized_team = a.home THEN a.away
        WHEN a.penalized_team = a.away THEN a.home
    END AS benefiting_team,
    COALESCE(inf.label, 'Unknown') AS infraction,
    (substring(a.play_text FROM '(\d+) [Yy]ard'))::int AS penalty_yards,
    a.play_text ~* 'declined' AS declined,
    a.play_text ~* 'offsett' AS offsetting,
    a.play_text ~* 'no play' AS no_play,
    (length(lower(a.play_text)) - length(replace(lower(a.play_text), 'penalt', ''))) > 6
        AS multi_penalty,
    a.yards_gained,
    a.ppa,
    a.play_text,
    (inf.label IS NOT NULL AND a.penalized_team IS NOT NULL) AS parse_ok
FROM attributed a
LEFT JOIN LATERAL (
    SELECT i.label
    FROM infractions i
    WHERE a.play_text ILIKE i.pattern
    ORDER BY i.priority
    LIMIT 1
) inf ON true;

-- Required for REFRESH CONCURRENTLY; play ids are not guaranteed unique
-- across seasons in older provider feeds, so season is part of the grain key.
CREATE UNIQUE INDEX ON marts.penalty_log (season, play_id);

-- Query indexes
CREATE INDEX ON marts.penalty_log (season, penalized_team);
CREATE INDEX ON marts.penalty_log (infraction);
CREATE INDEX ON marts.penalty_log (game_id);

-- Empty-guard (house convention, cf. marts/040): core.plays is backfilled
-- 2004+, so an empty build means the source scan silently broke.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.penalty_log) = 0 THEN
        RAISE EXCEPTION 'marts.penalty_log is empty: core.plays has no penalty-text rows, which contradicts the 2004+ backfill. Investigate before serving downstream.';
    END IF;
END $$;
