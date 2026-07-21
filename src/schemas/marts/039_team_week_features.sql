-- marts.team_week_features
-- =============================================================================
-- Tier 3 analytics (docs/plans/2026-07-21-tier3-analytics-plan.md), Pillar C
-- (features.team_week), Phase 3.
--
-- Thin denormalization surface: SELECT * FROM features.team_week, the as-of
-- feature vector entering each team's game (grain: one row per (season,
-- season_type, week, team) -- a team plays <=1 game/week, both sides of every
-- core.games row get a row -- see migration 027 and
-- docs/brainstorms/2026-07-21-team-week-feature-design.md section 1 for the
-- full column contract). This mart exists so downstream reads go through
-- `marts` per repo convention: `features.*` is contract-internal
-- (docs/SCHEMA_CONTRACT.md -- "raw table access is internal"), the same
-- treatment as `analytics.*`. Consumers should read marts.team_week_features
-- / api.team_week_features, never features.team_week directly.
--
-- AS-OF / LEAK-FREE (design doc section 0, shared with marts.adjusted_epa_week
-- / migration 026): a row keyed to week_index = WI uses only data with
-- week_index < WI within the same season, plus explicitly leak-free preseason
-- constants and prior-season (S-1) fallbacks known before the season starts.
--   week_index = week            when season_type = 'regular'
--   week_index = 100 + week      when season_type = 'postseason'
--
-- SIGN CONVENTION (adj_epa_* columns, carried over from migrations 026/027):
-- adj_epa_off HIGHER = better offense (more EPA/play above average);
-- adj_epa_def LOWER / more negative = better defense (EPA *allowed* above
-- average -- a stingier defense pulls this further negative). adj_epa_net =
-- adj_epa_off - adj_epa_def, so HIGHER = better team overall, matching
-- adj_epa_off's own direction. adj_epa_source ('week' | 'prior_season' |
-- NULL) is the provenance flag recording which fit resolved the row (see
-- design doc section 1c's fallback predicate).
--
-- Written by scripts/build_features.py, backfilled 2015+, daily cadence in
-- season.

DROP MATERIALIZED VIEW IF EXISTS marts.team_week_features CASCADE;

CREATE MATERIALIZED VIEW marts.team_week_features AS
SELECT * FROM features.team_week;

-- Required for REFRESH CONCURRENTLY; also the natural grain key (matches
-- features.team_week's own UNIQUE (season, season_type, week, team)).
CREATE UNIQUE INDEX ON marts.team_week_features (season, season_type, week, team);

-- Query indexes
CREATE INDEX ON marts.team_week_features (team, season);
CREATE INDEX ON marts.team_week_features (season, week_index);

-- Empty-guard: features.team_week is backed by scripts/build_features.py's
-- 2015+ backfill. If that has not run yet (or ever refreshes to zero rows),
-- fail loudly at deploy time instead of silently serving an empty mart
-- downstream -- same convention as marts/036_team_adjusted_epa.sql.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.team_week_features) = 0 THEN
        RAISE EXCEPTION 'marts.team_week_features is empty: features.team_week has no rows. Run scripts/build_features.py --from 2015 to build the historical feature backfill, then refresh this mart before use.';
    END IF;
END $$;
