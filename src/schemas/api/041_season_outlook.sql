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
-- does, so do not rank the two against each other on wins alone. That is now
-- filterable rather than merely documented -- see CLASSIFICATION below.
--
-- NO MART. The plan sketched a marts.season_outlook alongside this; a
-- materialized view is not warranted. The grain is ~350 teams per season per
-- model, DISTINCT ON rides season_projections_daily_key, and a mart would add
-- a refresh dependency between simulate_season.py and every read. This
-- follows api.game_predictions (032), which is a thin view over the same
-- append-only table for the same reason.
--
-- CLASSIFICATION (added 2026-07-26, cfb-app review). The view held 350 rows
-- for 2026 across 49 conferences plus 13 with a NULL conference -- FBS, FCS,
-- DII and DIII in one relation -- and nothing said which. An unfiltered
-- `ORDER BY projected_wins DESC` therefore compared teams playing entirely
-- different schedules, and the only workaround available downstream was a
-- conference-name allowlist, which cfb-app bans by repo rule after one leaked
-- FCS schools into production. So the division is a real column now.
--
-- SEASON-ACCURATE, not current membership -- the same derivation
-- api.leaderboard_teams and public.team_epa_season use (see the 2026-07-22
-- contract entry). ref.teams mirrors CFBD /teams, which reports where a team
-- plays TODAY: North Dakota State moved FCS -> FBS for 2026, so a plain
-- ref.teams join would stamp `fbs` on its 2025 row -- the FCS season it
-- actually played, and one of the 699 rows this view already holds.
-- team_season_class derives the division each team carried in its OWN games
-- that season from core.games.home_classification / away_classification;
-- ref.teams is the fallback only.
--
-- LEFT JOIN on both, deliberately: an unmatched team must yield NULL, not
-- vanish. Both sides are unique on their join key (GROUP BY (team, season);
-- DISTINCT ON (school)), so neither can multiply rows.
--
-- IS_PROJECTION (added 2026-07-26, same review). A completed season sitting in
-- this view is hindsight wearing projection column names: for 2025 every row
-- has projected_wins = actual_wins, wins_p10 = wins_p90 (the band collapsed
-- because there was nothing left to simulate), and conf_title_prob values like
-- an exact 0.2500 shared by four SEC teams -- which is not a title race, it is
-- conference_title_probs splitting a tie evenly among teams that finished
-- level. A consumer defaulting to "the current season" gets all of that
-- labelled as a forecast.
--
-- Defined per ROW as games_simulated > games_completed, i.e. "this row's
-- distribution contains at least one game that was drawn rather than played".
-- games_simulated always counts every completed game, so the difference is
-- exactly the pending-and-scored games and the flag can never go negative.
-- Two alternatives were rejected. A window over the base table
-- (bool_or(...) OVER (PARTITION BY season)) is wrong outright: window functions
-- run BEFORE DISTINCT ON, so it would see every superseded daily snapshot and
-- mark a finished season a projection on the strength of a July row. A
-- season-level flag over the latest snapshot is defensible but coarser -- it
-- would call a team whose season ended in November a forecast for the week its
-- conference is still playing a championship game, when that team's own row is
-- already pure record. Season-level remains one aggregate away:
-- bool_or(is_projection) per season.
--
-- One interaction to know: a row in an ONGOING season whose pending games all
-- lack a prediction for this model is also false. That is correct -- nothing
-- about it was projected, and projected_wins is again just the record -- but
-- it is not the same situation as a finished season. games_unscored is what
-- separates them: > 0 there, 0 for a season that is genuinely over.
--
-- PostgREST usage:
--   GET /api/season_outlook?season=eq.2026&team=eq.Oklahoma
--   GET /api/season_outlook?season=eq.2026&classification=eq.fbs&order=projected_wins.desc
--   GET /api/season_outlook?season=eq.2026&is_projection=is.true

CREATE OR REPLACE VIEW api.season_outlook AS
WITH projection_seasons AS (
    -- Scopes the core.games aggregate below to the two or three seasons this
    -- view actually holds. api.leaderboard_teams aggregates all of core.games
    -- (~90K rows) because it spans every season; this view does not, and
    -- rescanning 1869-2026 to classify ~1,050 rows would be pure waste.
    SELECT DISTINCT season FROM predictions.season_projections
),
team_season_class AS (
    SELECT
        team,
        season,
        mode() WITHIN GROUP (ORDER BY classification) AS classification
    FROM (
        SELECT g.home_team AS team, g.season, g.home_classification AS classification
        FROM core.games g
        JOIN projection_seasons ps ON ps.season = g.season
        WHERE g.home_classification IS NOT NULL
        UNION ALL
        SELECT g.away_team, g.season, g.away_classification
        FROM core.games g
        JOIN projection_seasons ps ON ps.season = g.season
        WHERE g.away_classification IS NOT NULL
    ) x
    GROUP BY team, season
),
teams_deduped AS (
    -- ref.teams has ~35 duplicate school names; 'fbs' sorts first, so this
    -- picks the FBS row when a name collides. Fallback only -- current
    -- membership, used when core.games has no classified game for the
    -- team-season.
    SELECT DISTINCT ON (school)
        school, classification
    FROM ref.teams
    ORDER BY school, classification NULLS LAST
),
latest AS (
    -- DISTINCT ON moved into a CTE now that the select list joins: leaving it
    -- outside would make the latest-snapshot guarantee depend on those joins
    -- staying 1:1, an invariant a future edit could break silently.
    SELECT DISTINCT ON (season, team, model_version) *
    FROM predictions.season_projections
    ORDER BY season, team, model_version, projection_date DESC
)
SELECT
    l.projection_id,
    l.computed_at,
    l.projection_date,
    l.model_version,
    l.season,
    l.team,
    l.conference,

    -- Schedule state
    l.games_scheduled,
    l.games_simulated,
    -- Surfaced rather than left to the caller: when a pending game has no
    -- prediction for this model it is excluded from the simulation entirely,
    -- and every projected quantity below is computed over games_simulated.
    -- A reader comparing projected_wins to games_scheduled without this
    -- column would silently read the difference as losses -- the exact
    -- defect fixed in migration 043's games_simulated.
    l.games_scheduled - l.games_simulated AS games_unscored,
    l.games_completed,
    l.actual_wins,
    -- STORED, not derived here: the threshold is division-aware as of
    -- 2026-07-26 (an Ivy League team's 10-game slate is a whole season, not a
    -- short one), and that rule lives in scripts/simulate_season.py. Rows
    -- written before that re-run still carry the flat 11-game FBS threshold.
    l.schedule_complete,

    -- Central tendency
    l.projected_wins,
    l.projected_losses,
    l.median_wins,

    -- Distribution. The reason this is not a single projected_wins column:
    -- a 9-3 projection spanning [7, 11] and one spanning [8.5, 9.5] are
    -- different claims and only the spread separates them.
    l.wins_p10,
    l.wins_p25,
    l.wins_p75,
    l.wins_p90,
    l.p_win_dist,
    -- NULL outside FBS as of the 2026-07-26 re-run: bowl eligibility is an
    -- FBS rule and P(6+ wins) for a DIII team is a number about nothing.
    -- Rows written before that re-run carry it for every division.
    l.p_bowl_eligible,
    l.p_ten_plus,

    -- Schedule strength
    l.sos_rating,
    l.sos_rank,

    -- v1-crude: highest conference win percentage per simulation, ties split
    -- evenly. Real tiebreakers and championship-game formats are not modeled.
    l.conf_title_prob,
    -- NULL in v1 by design; the 12-team format's autobids and seeding are
    -- their own rules-modeling project.
    l.playoff_prob,

    -- Provenance. residual_sigma travels with the row so a projection always
    -- carries the assumption that produced it.
    l.n_sims,
    l.residual_sigma,
    -- Everything from here down is APPENDED, in the order it was added:
    -- CREATE OR REPLACE VIEW cannot insert a column mid-list, so a new field
    -- has to go on the end or the apply fails against the deployed view (same
    -- constraint as marts 5922610). Do not re-sort this tail into a tidier
    -- grouping -- that is the same failure with better intentions.
    --
    -- v1.1 provenance -- the correlation assumption the spread was drawn
    -- under. NULL for rows written before v1.1. This does NOT move
    -- projected_wins: total per-game variance is held at residual_sigma^2 by
    -- construction, so it changes the WIDTH of the distribution only.
    l.strength_share,
    -- Division, season-accurate (see header). NULL means neither core.games
    -- nor ref.teams could place the team -- unknown, not FBS.
    COALESCE(tsc.classification, td.classification) AS classification,
    -- False = a settled record wearing projection column names (see header).
    (l.games_simulated > l.games_completed) AS is_projection
FROM latest l
LEFT JOIN team_season_class tsc ON tsc.season = l.season AND tsc.team = l.team
LEFT JOIN teams_deduped td ON td.school = l.team;

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

COMMENT ON VIEW api.season_outlook IS 'Latest Monte Carlo season projection per (season, team, model_version), from the append-only predictions.season_projections log. Columns: projection_id, computed_at, projection_date, model_version, season, team, conference, games_scheduled, games_simulated, games_unscored, games_completed, actual_wins, schedule_complete, projected_wins, projected_losses, median_wins, wins_p10/p25/p75/p90, p_win_dist, p_bowl_eligible, p_ten_plus, sos_rating, sos_rank, conf_title_prob, playoff_prob, n_sims, residual_sigma, strength_share, classification, is_projection. DISTINCT ON (season, team, model_version) ORDER BY projection_date DESC selects the most recent snapshot; query predictions.season_projections directly for day-by-day history. FILTER BY classification BEFORE RANKING: this view mixes FBS, FCS, DII and DIII (350 teams across 49 conferences in 2026), so an unfiltered ORDER BY projected_wins compares teams playing entirely different schedules. classification is season-accurate (derived from core.games.home_classification/away_classification, ref.teams as fallback), so a realignment team keeps the division it actually played in; NULL means unplaceable, not FBS. is_projection is false when games_simulated = games_completed -- the row contains no simulated game, so projected_wins equals actual_wins, wins_p10 equals wins_p90 and conf_title_prob is a tie split among teams that finished level, not a forecast; bool_or(is_projection) per season answers "is this season still being projected". From v1.1 each simulation draws one season-strength offset per team (strength_share) so the tails are no longer understated the way independent per-game draws made them; offsets remain independent ACROSS teams. Projections cover only games actually on the schedule -- check schedule_complete and games_unscored before comparing projected_wins to a full slate. KNOWN LIMITATION: CFBD labels FCS/D2 playoff bracket games season_type=''regular'', so games_scheduled for a non-FBS team can include a playoff run (completed seasons only; no forward-looking row is affected).';
