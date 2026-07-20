-- api.poll_rankings
-- Weekly poll rankings (AP Top 25, Coaches Poll, CFP rankings, etc).
--
-- Source: core.rankings (dlt-loaded from CFBD /rankings).
--
-- DATA-INTEGRITY HISTORY (Phase 0 Lane D, resolved 2026-07-20):
-- The pipeline originally merged on [season, week, poll, rank], which lost
-- rows two ways -- both CONFIRMED against production via a read-only
-- diagnostic (deploy run 29763830257):
--   1. Rank ties: polls list tied teams at the same rank and skip the next
--      one (AP 2024 week 10 had two teams at #11 and no #12), so a rank-keyed
--      merge kept only one of the tied teams. 55 AP poll weeks were short of
--      25 rows; 2024 week 7 was missing three teams.
--   2. Postseason collision: CFBD reports the final (postseason) poll as
--      "week": 1, the same key as the regular-season week 1 poll, so one
--      silently overwrote the other.
-- The fix (src/pipelines/sources/rankings.py): the merge key is now
-- [season, season_type, week, poll, school] -- a team appears at most once
-- per poll per week, so school cannot collide -- and season_type is captured
-- on every row. core.rankings was reset
-- (src/schemas/migrations/023_rankings_rekey_reset.sql) and history reloaded
-- 2000-2025 under the new key.
--
-- Weekly-poll consumers should filter season_type = 'regular'; the final
-- poll for a season is season_type = 'postseason' (reported as week 1).
--
-- This file DROPs and re-CREATEs (rather than CREATE OR REPLACE) because
-- season_type sits mid-column-list, which OR REPLACE cannot do to the
-- previously deployed shape.
--
-- PostgREST usage:
--   GET /api/poll_rankings?season=eq.2024&poll=eq.AP Top 25&season_type=eq.regular&order=week.asc,rank.asc

DROP VIEW IF EXISTS api.poll_rankings;

CREATE VIEW api.poll_rankings AS
SELECT
    r.season,
    r.season_type,
    r.week,
    r.poll,
    r.rank,
    r.school,
    r.conference,
    r.first_place_votes,
    r.points
FROM core.rankings r;

GRANT SELECT ON api.poll_rankings TO anon, authenticated;

COMMENT ON VIEW api.poll_rankings IS 'Weekly poll rankings (AP Top 25, Coaches Poll, CFP, etc). Columns: season, season_type, week, poll, rank, school, conference, first_place_votes, points. Filter season_type=regular for weekly polls; the final poll is season_type=postseason (week 1). Tied teams share a rank (the next rank is skipped), so a week can legitimately contain duplicate rank values.';
