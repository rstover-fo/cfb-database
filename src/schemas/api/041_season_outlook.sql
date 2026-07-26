-- api.season_outlook
-- Latest Monte Carlo season projection per (season, team, model): projected
-- wins with a distribution, schedule strength, and conference title odds.
-- Thin latest-snapshot view over predictions.season_projections (preseason
-- outlook plan, docs/plans/2026-07-25-preseason-outlook-model-plan.md,
-- Phase 5).
--
-- WHY THIS VIEW EXISTS. Two reasons, and the second is not cosmetic.
--
-- 1. A season had no readable surface. predictions.game_predictions holds
--    per-game point estimates; nothing composed them into a win total with an
--    interval, so "what is this team's 2026 outlook" degraded to prose.
--    Migration 043 materialized that composition; this exposes it.
--
-- 2. The api schema is the ONLY surface downstream consumers can reach.
--    public.run_analyst_query drops to the analyst_ro NOLOGIN role, which
--    holds USAGE+SELECT on `api` and deliberately nothing else -- see
--    public/validation_run_analyst_query.sql, which FAILS if analyst_ro can
--    reach core or marts. So a query against predictions.season_projections
--    returns "permission denied for schema predictions" by design, and the
--    projections stayed invisible to cfb-app, cfb-scout and the MCP surface
--    until this view existed. That is what it unblocks.
--
-- api views are owner-rights (NOT security_invoker -- see
-- public/012_run_analyst_query.sql), so reading through this view does not
-- require the caller to hold grants on the predictions schema. That is the
-- same mechanism api.game_predictions already relies on.
--
-- LATEST SNAPSHOT ONLY. predictions.season_projections is append-only across
-- days (one immutable row per season/team/model_version/UTC projection_date,
-- migration 043), so DISTINCT ON picks the most recent projection_date per
-- (season, team, model_version). Query the base table directly for the
-- day-by-day history -- that history is the point of the append-only design
-- (it shows how the model's read on a season moved as games resolved), and
-- this view deliberately does not expose it.
--
-- KNOWN LIMITATION -- FCS/D2 playoff games count toward the slate. Phase 4
-- excludes the postseason by filtering season_type = 'regular', which is
-- correct for FBS (bowls and CFP games are 'postseason', while conference
-- championship games are 'regular' and correctly remain). CFBD does NOT
-- apply that convention below FBS: FCS and D2 **playoff bracket** games are
-- labelled 'regular', so a deep playoff run inflates games_scheduled --
-- verified 2026-07-26, 14 teams in completed 2025 at 14-15 games
-- (Illinois State 15, Ferris State 14-0, Montana, Villanova, ...), all
-- MVFC/Big Sky/CAA/GLIAC/PSAC/UAC/Southland.
--
-- Bounded, not corrupting: every affected row is a COMPLETED season where
-- projected_wins equals actual_wins exactly, so nothing is being projected.
-- No forward-looking row is affected -- 2026 tops out at 13 games with zero
-- teams above it. The consequence for consumers is comparability: an FCS
-- team's projected_wins may span a playoff run while an FBS team's never
-- does, so do not rank the two against each other on wins alone.
--
-- NO MART. The plan sketched a marts.season_outlook alongside this; a
-- materialized view is not warranted. The grain is ~350 teams per season per
-- model, DISTINCT ON rides season_projections_daily_key, and a mart would add
-- a refresh dependency between simulate_season.py and every read. This
-- follows api.game_predictions (032), which is a thin view over the same
-- append-only table for the same reason.
--
-- PostgREST usage:
--   GET /api/season_outlook?season=eq.2026&team=eq.Oklahoma
--   GET /api/season_outlook?season=eq.2026&order=projected_wins.desc
--   GET /api/season_outlook?season=eq.2026&schedule_complete=is.true

CREATE OR REPLACE VIEW api.season_outlook AS
SELECT DISTINCT ON (season, team, model_version)
    projection_id,
    computed_at,
    projection_date,
    model_version,
    season,
    team,
    conference,

    -- Schedule state
    games_scheduled,
    games_simulated,
    -- Surfaced rather than left to the caller: when a pending game has no
    -- prediction for this model it is excluded from the simulation entirely,
    -- and every projected quantity below is computed over games_simulated.
    -- A reader comparing projected_wins to games_scheduled without this
    -- column would silently read the difference as losses -- the exact
    -- defect fixed in migration 043's games_simulated.
    games_scheduled - games_simulated AS games_unscored,
    games_completed,
    actual_wins,
    schedule_complete,

    -- Central tendency
    projected_wins,
    projected_losses,
    median_wins,

    -- Distribution. The reason this is not a single projected_wins column:
    -- a 9-3 projection spanning [7, 11] and one spanning [8.5, 9.5] are
    -- different claims and only the spread separates them.
    wins_p10,
    wins_p25,
    wins_p75,
    wins_p90,
    p_win_dist,
    p_bowl_eligible,
    p_ten_plus,

    -- Schedule strength
    sos_rating,
    sos_rank,

    -- v1-crude: highest conference win percentage per simulation, ties split
    -- evenly. Real tiebreakers and championship-game formats are not modeled.
    conf_title_prob,
    -- NULL in v1 by design; the 12-team format's autobids and seeding are
    -- their own rules-modeling project.
    playoff_prob,

    -- Provenance. residual_sigma travels with the row so a projection always
    -- carries the assumption that produced it.
    n_sims,
    residual_sigma,
    -- Appended LAST deliberately: CREATE OR REPLACE VIEW cannot insert a
    -- column mid-list, so a new field has to go on the end or the apply
    -- fails against the deployed view (same constraint as marts 5922610).
    -- v1.1 provenance -- the correlation assumption the spread was drawn
    -- under. NULL for rows written before v1.1. This does NOT move
    -- projected_wins: total per-game variance is held at residual_sigma^2 by
    -- construction, so it changes the WIDTH of the distribution only.
    strength_share
FROM predictions.season_projections
ORDER BY season, team, model_version, projection_date DESC;

-- Grants are part of the definition: an apply that DROPs/recreates the view
-- would otherwise leave the PostgREST roles without read access (no ALTER
-- DEFAULT PRIVILEGES for them in this database).
GRANT SELECT ON api.season_outlook TO anon, authenticated;

-- analyst_ro is covered by ALTER DEFAULT PRIVILEGES IN SCHEMA api
-- (public/012_run_analyst_query.sql), but only when the view is created by
-- the role that set those defaults. Granting explicitly removes that
-- dependency -- and since unblocking analyst_ro is half the point of this
-- view, inheriting the grant silently is not good enough. Guarded so a
-- database that has not applied 012 yet still applies this file.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_ro') THEN
        -- EXECUTE rather than a bare GRANT: the unambiguously portable way to
        -- run conditional DDL from PL/pgSQL.
        EXECUTE 'GRANT SELECT ON api.season_outlook TO analyst_ro';
    ELSE
        RAISE NOTICE 'analyst_ro not present; skipping grant (apply public/012_run_analyst_query.sql first)';
    END IF;
END
$$;

COMMENT ON VIEW api.season_outlook IS 'Latest Monte Carlo season projection per (season, team, model_version), from the append-only predictions.season_projections log. Columns: projection_id, computed_at, projection_date, model_version, season, team, conference, games_scheduled, games_simulated, games_unscored, games_completed, actual_wins, schedule_complete, projected_wins, projected_losses, median_wins, wins_p10/p25/p75/p90, p_win_dist, p_bowl_eligible, p_ten_plus, sos_rating, sos_rank, conf_title_prob, playoff_prob, n_sims, residual_sigma, strength_share. DISTINCT ON (season, team, model_version) ORDER BY projection_date DESC selects the most recent snapshot; query predictions.season_projections directly for day-by-day history. From v1.1 each simulation draws one season-strength offset per team (strength_share) so the tails are no longer understated the way independent per-game draws made them; offsets remain independent ACROSS teams. Projections cover only games actually on the schedule -- check schedule_complete and games_unscored before comparing projected_wins to a full slate. KNOWN LIMITATION: CFBD labels FCS/D2 playoff bracket games season_type=''regular'', so games_scheduled for a non-FBS team can include a playoff run (completed seasons only; no forward-looking row is affected). Do not rank FCS and FBS teams against each other on projected_wins alone.';
