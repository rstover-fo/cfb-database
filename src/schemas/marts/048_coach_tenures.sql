-- marts.coach_tenures
-- =============================================================================
-- Coach tenure history from CFBD's /coaches/tenures (2026-08-30
-- expansion_views unit, task 5b). Grain: one row per (coach_id, team_id,
-- tenure_start). Distinct from the EXISTING marts.coaching_tenure (gap-
-- detected from ref.coaches__seasons, no is_interim/classification): this
-- mart is CFBD's own pre-computed continuous-tenure record from
-- ref.coach_tenures, and retires two hacks documented in the cfb-app work
-- order:
--   - is_interim replaces cfb-app's DEFAULT_MIN_GAMES = 24 heuristic
--     (coaches.ts:59-61), which exists only to keep an interim's one-game
--     record off win% leaderboards and also silently drops legitimate short
--     tenures.
--   - classification replaces pushing the FBS filter through
--     .in('team', <~130 name strings>) (coaches.ts:56-58), needed only
--     because api.coach_records has no classification column.
--
-- Column names verified against the CFBD OpenAPI spec's CoachTenure /
-- CoachReference / CoachTeamReference / CoachRecord schemas (fetched
-- 2026-08-30 from api.collegefootballdata.com/api-docs.json) -- NOT a live
-- pg_attribute read from this session (no DB access here). dlt flattens:
-- coach {id, firstName, lastName} -> coach__id/coach__first_name/
-- coach__last_name; team {id, school} -> team__id/team__school; record
-- {games, wins, losses, ties, winPercentage} -> record__games/record__wins/
-- record__losses/record__ties/record__win_percentage; hireDate -> hire_date
-- (nullable string, not a real date type per the spec); startYear/endYear ->
-- start_year/end_year (endYear nullable = active tenure); isInterim ->
-- is_interim. Re-verify at deploy time per this repo's column-contract
-- convention.
--
-- ref.coach_tenures is populated by a PER-TEAM FAN-OUT resource
-- (coaches.py's coach_tenures_resource, source cfbd_coach_tenures) that is
-- deliberately NOT part of the daily/incremental path -- it requires an
-- explicit backfill (`--source coach_tenures`, run.py::run_coach_tenures_pipeline).
-- If that backfill has not run yet in a given environment, this mart is
-- legitimately empty -- see the row-count guard note below (no hard
-- empty-guard here, unlike marts/041_penalty_log.sql and
-- marts/042_team_penalty_box.sql, whose sources are always-populated
-- 2004+ tables; this source is backfill-gated and empty-until-run is an
-- expected state, not a break).
--
-- team_id (team__id) is joined to ref.teams(id) -- that table's own primary
-- key -- for classification, so this cannot fan out on ref.teams' 35
-- duplicate school-name rows (id joins never hit that; only name-string
-- joins do).
DROP MATERIALIZED VIEW IF EXISTS marts.coach_tenures CASCADE;

CREATE MATERIALIZED VIEW marts.coach_tenures AS
SELECT
    ct.coach__id AS coach_id,
    (ct.coach__first_name || ' ' || ct.coach__last_name) AS coach_name,
    ct.team__id AS team_id,
    ct.team__school AS team,
    ct.start_year AS tenure_start,
    ct.end_year AS tenure_end,
    ct.hire_date,
    ct.is_interim,
    ct.record__games AS record_games,
    ct.record__wins AS record_wins,
    ct.record__losses AS record_losses,
    ct.record__ties AS record_ties,
    ct.record__win_percentage AS record_win_percentage,
    t.classification
FROM ref.coach_tenures ct
LEFT JOIN ref.teams t ON t.id = ct.team__id;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.coach_tenures (coach_id, team_id, tenure_start);

-- Query indexes
CREATE INDEX ON marts.coach_tenures (team_id);
CREATE INDEX ON marts.coach_tenures (coach_id);

-- Soft row-count notice, deliberately NOT a hard RAISE EXCEPTION: unlike
-- marts/041/042's always-populated core.plays/core.game_team_stats sources,
-- ref.coach_tenures depends on an explicit, currently-optional per-team
-- backfill (see header). A hard empty-guard here would fail this file's
-- very first apply in any environment where that backfill has not yet run,
-- which is the expected state right after this file is authored. Flag for
-- the orchestrator: confirm the coach_tenures backfill has actually run
-- before relying on this mart being non-empty.
DO $$
DECLARE
    n bigint;
BEGIN
    SELECT count(*) INTO n FROM marts.coach_tenures;
    RAISE NOTICE 'marts.coach_tenures: % row(s) after build (0 is expected if the coach_tenures backfill has not run yet -- see file header)', n;
END $$;
