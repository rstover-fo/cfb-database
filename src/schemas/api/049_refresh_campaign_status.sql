-- api.refresh_campaign_status
-- Owner-rights view over meta.refresh_campaigns + meta.refresh_progress
-- (2026-08-30 expansion_views unit, task 7). Lets downstream consumers
-- (the cfb-app Discord bot's long-term memory / prediction ledger)
-- scope-invalidate cached historical answers while a corrections campaign
-- (e.g. the 2014-2025 upstream play-stats/advanced-box-score refresh,
-- src/schemas/migrations/051_refresh_ledger.sql) is still draining --
-- rather than distrusting an entire season range, a consumer can check
-- whether the specific season it already answered a question about has
-- finished refreshing.
--
-- Grain: (campaign, season) -- meta.refresh_campaigns.seasons is an int[]
-- (the campaign's declared scope), unnested one row per season here.
-- games_refreshed/games_no_data count DISTINCT game_ids from
-- meta.refresh_progress with status='refreshed'/'no_data' respectively,
-- summed ACROSS every task the campaign declared (meta.refresh_campaigns.tasks)
-- -- not broken out per task. A game can appear in both counts if different
-- tasks landed different statuses for it (e.g. play_stats refreshed but
-- advanced_game_stats came back no_data for the same game) -- query
-- meta.refresh_progress directly (it is also granted SELECT to
-- anon/authenticated) for a per-task breakdown.
--
-- meta.refresh_progress has no season column of its own (its grain is
-- game_id); season comes from a join to core.games on game_id, same
-- join-to-core.games-for-season idiom used elsewhere in this unit
-- (marts.passing_charting_target_season) and in marts.penalty_log/
-- team_penalty_box.
--
-- completed_at / last_finalized_at are passed through from
-- meta.refresh_campaigns as-is: completed_at is set once every declared
-- task's backlog has drained to empty across all declared seasons;
-- last_finalized_at is the watermark of the last successful post-drain
-- finalize (adjusted-EPA refit + mart refresh) and is NULL if the campaign
-- has never been finalized.
--
-- Plain (non-materialized) view by design -- this is queried rarely and
-- meta.refresh_progress is small, so no mart is warranted.
--
-- PostgREST usage:
--   GET /api/refresh_campaign_status?campaign=eq.2026-08-upstream-corrections&season=eq.2024

DROP VIEW IF EXISTS api.refresh_campaign_status;

CREATE VIEW api.refresh_campaign_status AS
WITH campaign_seasons AS (
    SELECT
        c.campaign,
        c.completed_at,
        c.last_finalized_at,
        unnest(c.seasons) AS season
    FROM meta.refresh_campaigns c
),
progress_seasoned AS (
    SELECT
        rp.campaign,
        rp.game_id,
        rp.status,
        g.season
    FROM meta.refresh_progress rp
    JOIN core.games g ON g.id = rp.game_id
)
SELECT
    cs.campaign,
    cs.season,
    COUNT(DISTINCT ps.game_id) FILTER (WHERE ps.status = 'refreshed') AS games_refreshed,
    COUNT(DISTINCT ps.game_id) FILTER (WHERE ps.status = 'no_data') AS games_no_data,
    cs.completed_at,
    cs.last_finalized_at
FROM campaign_seasons cs
LEFT JOIN progress_seasoned ps
    ON ps.campaign = cs.campaign
    AND ps.season = cs.season
GROUP BY cs.campaign, cs.season, cs.completed_at, cs.last_finalized_at;

COMMENT ON VIEW api.refresh_campaign_status IS 'Per (campaign, season) progress against meta.refresh_campaigns'' declared scope: games_refreshed/games_no_data are distinct game counts from meta.refresh_progress (status=''refreshed''/''no_data'' respectively), summed across every task the campaign declared -- not a per-task breakdown (query meta.refresh_progress directly for that). completed_at is set once the campaign''s full backlog has drained; last_finalized_at is the watermark of the last successful post-drain finalize (NULL = never finalized). Lets a downstream consumer scope-invalidate a cached historical answer for one season instead of distrusting an entire in-progress correction campaign''s range. Backed by meta.refresh_campaigns + meta.refresh_progress joined to core.games for season.';

GRANT SELECT ON api.refresh_campaign_status TO anon, authenticated;
