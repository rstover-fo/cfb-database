-- api.adjusted_epa_week
-- Walk-forward ridge-adjusted-EPA coefficients per (team, season,
-- week_index) -- the raw as-of fit underlying api.team_week_features'
-- adj_epa_* columns, exposed independently for transparency/audit.
-- Thin passthrough of marts.adjusted_epa_week (Tier 3 analytics,
-- docs/plans/2026-07-21-tier3-analytics-plan.md, Pillar A/C).
--
-- SIGN CONVENTION: off_coef HIGHER = better offense (more EPA/play above
-- average); def_coef LOWER / more negative = better defense (EPA *allowed*
-- above average -- a stingier defense pulls this further negative).
--
-- WEEK_INDEX: each row holds the coefficients ENTERING that week_index --
-- fit on that season's plays with week_index < WI only, so it is safe to
-- join onto that week's game without leaking the game's own result into its
-- own pregame rating. week_index = week for season_type = 'regular', 100 +
-- week for 'postseason' (CFBD restarts week numbering at 1 for bowls; the
-- postseason row's state is the full regular season).
--
-- PostgREST usage:
--   GET /api/adjusted_epa_week?team=eq.Ohio State&season=eq.2024&order=week_index.asc
--   GET /api/adjusted_epa_week?season=eq.2026&week_index=eq.8

CREATE OR REPLACE VIEW api.adjusted_epa_week AS
SELECT
    team,
    season,
    week_index,
    off_coef,
    def_coef,
    hfa_coef,
    mu,
    plays,
    lambda,
    n_teams
FROM marts.adjusted_epa_week;

GRANT SELECT ON api.adjusted_epa_week TO anon, authenticated;

COMMENT ON VIEW api.adjusted_epa_week IS 'Walk-forward ridge-adjusted-EPA coefficients entering each (team, season, week_index). Columns: team, season, week_index, off_coef, def_coef, hfa_coef, mu, plays, lambda, n_teams. off_coef higher = better offense; def_coef lower/more negative = better defense. week_index = week for season_type=''regular'', 100 + week for ''postseason''. Backed by marts.adjusted_epa_week.';
