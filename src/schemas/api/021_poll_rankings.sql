-- api.poll_rankings
-- Weekly poll rankings (AP Top 25, Coaches Poll, CFP rankings, etc).
--
-- Source: core.rankings (dlt-loaded from CFBD /rankings).
--
-- KNOWN DATA ISSUE -- postseason/week collision (Phase 0 Lane D, 2026-07-20):
-- The dlt resource src/pipelines/sources/rankings.py merges on primary key
-- [season, week, poll, rank] and captures no season_type/seasonType column.
-- Regular-season weeks are loaded first (weeks 1-15), then a second pass loads
-- postseason rankings via `seasonType: "postseason"`; the yielded week number
-- for that pass is `week_data.get("week", "postseason")` -- i.e. whatever week
-- number CFBD's API puts in the postseason payload, falling back to the string
-- "postseason" only if CFBD omits the field. CFBD's /rankings endpoint
-- historically reports postseason (final) rankings under "week": 1, which is
-- the same primary-key week value as the actual week-1 regular-season poll.
-- If that holds, the merge would upsert one over the other by (season, week,
-- poll, rank) with no way to distinguish them downstream -- likely silently
-- overwriting week-1 regular-season rankings with final postseason rankings
-- (postseason loads after regular season within a single pipeline run), or
-- vice versa depending on load order across runs.
--
-- This could NOT be confirmed against live data in this environment: the
-- Supabase REST endpoint (VITE_SUPABASE_URL) was unreachable through the
-- outbound proxy (curl exit 56 / gateway 502 on CONNECT to
-- uvzwxwfjiunyceplmiru.supabase.co) when checking
--   GET /rankings?select=season,week,poll,rank&season=eq.2024&poll=eq.AP Top 25&order=week.asc
-- for duplicate (week, rank) pairs or a distinct postseason week number.
-- Follow-up: re-run that check with DB access and, if confirmed, add a
-- season_type column to core.rankings (extend the resource to capture
-- CFBD's seasonType/week_data source) and widen the merge primary key before
-- trusting week=1 rows in api.poll_rankings for "final" postseason context.
--
-- PostgREST usage:
--   GET /api/poll_rankings?season=eq.2024&poll=eq.AP Top 25&order=week.asc,rank.asc

CREATE OR REPLACE VIEW api.poll_rankings AS
SELECT
    r.season,
    r.week,
    r.poll,
    r.rank,
    r.school,
    r.conference,
    r.first_place_votes,
    r.points
FROM core.rankings r;

GRANT SELECT ON api.poll_rankings TO anon, authenticated;

COMMENT ON VIEW api.poll_rankings IS 'Weekly poll rankings (AP Top 25, Coaches Poll, CFP, etc). Columns: season, week, poll, rank, school, conference, first_place_votes, points. NOTE: core.rankings has no season_type column -- postseason rows may collide with regular-season week 1 on the underlying merge key (season, week, poll, rank); see file header comment.';
