-- marts.team_ats_records
-- Team against-the-spread (ATS) records by season: cover record and average
-- cover margin. Passthrough of betting.team_ats plus a computed ats_win_pct.
-- Grain: (season, team_id) -- one row per team per season
-- Source: betting.team_ats (CFBD GET /teams/ats)
--
-- ASSUMED SOURCE COLUMNS -- confidence: HIGH (verified against the live CFBD
-- OpenAPI spec at api.collegefootballdata.com/api-docs.json, response model
-- "TeamATS"), but NOT verified against the actual populated betting.team_ats
-- table -- the live DB was unreachable this session.
-- The loader (src/pipelines/sources/betting.py::team_ats_resource, ~L99-122)
-- sets team["year"] = year and yields the raw API dict unmodified -- no
-- flattening/renaming in Python. The response model is FLAT (no nested
-- "spreadRecord"/"atsCovers" sub-object), so no double-underscore columns
-- are expected -- unlike stats.player_usage's nested "usage" object:
--   year               (int)     -- primary key component; exposed as season
--   team_id            (int)     -- primary key component  <- teamId
--   team               (text)
--   conference         (text, nullable)
--   games              (int)
--   ats_wins           (int)     <- atsWins
--   ats_losses         (int)     <- atsLosses
--   ats_pushes         (int)     <- atsPushes
--   avg_cover_margin   (double, nullable)  <- avgCoverMargin
-- If deploy fails with "column ... does not exist", check
-- information_schema.columns for betting.team_ats and fix names above --
-- in particular re-check whether the API response nests the win/loss/push
-- counts (which would surface as double-underscore columns instead of the
-- flat ats_wins/ats_losses/ats_pushes assumed here).

DROP MATERIALIZED VIEW IF EXISTS marts.team_ats_records CASCADE;

CREATE MATERIALIZED VIEW marts.team_ats_records AS
SELECT
    t.year AS season,
    t.team_id,
    t.team,
    t.conference,
    t.games,
    t.ats_wins,
    t.ats_losses,
    t.ats_pushes,
    ROUND(t.avg_cover_margin::numeric, 2) AS avg_cover_margin,

    -- ATS win percentage, excluding pushes from the denominator; NULL when
    -- the team has no decided ATS games yet.
    ROUND(
        t.ats_wins::numeric / NULLIF(t.ats_wins + t.ats_losses, 0),
        4
    ) AS ats_win_pct

FROM betting.team_ats t;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_ats_records (team_id, season);

-- Query indexes
CREATE INDEX ON marts.team_ats_records (season);
CREATE INDEX ON marts.team_ats_records (season, ats_win_pct);

-- Empty-guard: betting.team_ats backs this mart. It is one of the Phase 0
-- gate tables (docs/db-snapshot-current.json predates it) -- if it refreshes to
-- zero rows, fail loudly at deploy time instead of silently serving an empty
-- mart downstream.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.team_ats_records) = 0 THEN
        RAISE EXCEPTION 'marts.team_ats_records is empty: betting.team_ats has no rows. Run the betting backfill (deploy/tier1-backfill, action=backfill, sources=betting) and refresh this mart before use.';
    END IF;
END $$;
