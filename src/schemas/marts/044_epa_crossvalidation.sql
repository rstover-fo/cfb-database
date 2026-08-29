-- marts.epa_crossvalidation
-- =============================================================================
-- OUTPUT-PLAUSIBILITY HARNESS for the 2014-2025 historical play-stats refresh
-- campaign (scripts/backfill_refresh.py, ledger meta.refresh_progress,
-- migration 051). CFBD reclassified 15k+ garbage-time plays upstream; the
-- campaign re-fetches per-game play stats season by season, which rebuilds
-- marts._game_epa_calc -> marts.play_epa -> the house ridge fits. This mart
-- answers exactly one question:
--
--     Did the refreshed play data move the house EPA chain TOWARD or AWAY
--     FROM independent external estimates of the same team-seasons?
--
-- Grain: (season, external_system, team) -- one row per house team-season per
-- external rating system, for team-seasons present in BOTH sides.
--
-- -----------------------------------------------------------------------------
-- WHAT THIS IS NOT
-- -----------------------------------------------------------------------------
-- Descriptive only. It is NOT a model input, NOT a feature, and NOT a gate.
--   * Nothing here may be joined into features.team_week or any fitted vector.
--     Every row mixes SEASON-FINAL information (the house full-season ridge
--     fit, the external season-final rating) into one team-season row, so
--     using it as an as-of feature would be a textbook leak. It is deliberately
--     absent from DIFF_FEATURE_COLUMNS and from
--     docs/brainstorms/2026-07-21-team-week-feature-design.md's column spec.
--   * A correlation moving the "right" way is NOT evidence a model change is
--     good and cannot ship one. Model changes ship only through the
--     walk-forward no-regression gate on per-game marts.prediction_accuracy
--     (held-out margin MAE improves; neither Brier nor ATS degrades).
--   * This harness catches DATA-quality regressions (a bad refresh batch, a
--     silent join failure, a sign flip), not model quality.
--
-- -----------------------------------------------------------------------------
-- SURFACES COMPARED
-- -----------------------------------------------------------------------------
-- House (season grain, both rebuilt by the refresh):
--   house_net_adj_epa    marts.team_adjusted_epa.net_adj_epa -- ridge
--                        opponent-adjusted (off_coef - def_coef), the primary
--                        series. HIGHER = better (see marts/036 header).
--   house_epa_per_play   marts.team_epa_season.epa_per_play -- raw
--                        NON-GARBAGE offensive EPA/play. Diagnostic secondary:
--                        this is the series the garbage-time reclassification
--                        touches most directly, so it moves first. HIGHER =
--                        better.
--   house_def_adj_epa    carried for eyeballing only. LOWER = better. It is
--                        deliberately NOT ranked or correlated here -- ranking
--                        it needs the inverted order and would invite exactly
--                        the sign error this harness exists to catch.
--
-- External (season-final snapshot per team, all HIGHER = better):
--   'cfbd_fpi_season'   ratings.fpi_ratings.fpi (CFBD /ratings/fpi,
--                       season grain, joined on CFBD team NAME). Has data
--                       today, so the mart is useful the moment it deploys.
--   'sdv_adj_net'       ratings.sdv_ratings_weekly.adj_net at each team's
--                       greatest through_week (sportsdataverse adjusted-EPA /
--                       FEI system; joined on team_id = ref.teams.id).
--   'espn_fpi_weekly'   ratings.espn_fpi_weekly.fpi at each team's greatest
--                       (season_type, week) (joined on team_id = ref.teams.id).
--
-- The two weekly tables land with the sportsdataverse flat-file sources
-- (migration 052) and are EMPTY until those sources first run. That is
-- expected and handled: the joins are inner, so an empty external table
-- contributes zero rows rather than erroring, and the post-create block below
-- RAISEs a WARNING (not an EXCEPTION) naming the dark systems. This is a
-- deliberate deviation from the empty-guard convention in marts/036, 040 and
-- 043 -- those back onto tables that must have rows; here two of three
-- external anchors are legitimately dark on day one.
--
-- -----------------------------------------------------------------------------
-- SCALE CAVEAT (read before comparing anything)
-- -----------------------------------------------------------------------------
-- The house series are EPA/play (roughly -0.6 .. +0.6). FPI is in points
-- (roughly -30 .. +30). sdv adj_net is on its own EPA-ish scale. NEVER
-- subtract an external_value from a house value and never average the two --
-- the number would be meaningless. Every comparison in this mart is
-- rank-based or z-scored WITHIN (season, external_system), which is the only
-- cross-scale-safe form. rank_delta and the *_z columns are the per-team
-- comparison; the *_corr columns are the per-season summary.
--
-- -----------------------------------------------------------------------------
-- METHODOLOGY: what "toward or away" means operationally
-- -----------------------------------------------------------------------------
-- All statistics are computed WITHIN each (season, external_system) partition
-- and over the MATCHED SET ONLY (teams present on both sides), so a team the
-- external system does not rate never distorts a rank.
--
--   spearman_net_adj_epa    Spearman rank correlation between house_net_rank
--                           and external_rank. THE HEADLINE NUMBER. Pearson
--                           on ranks == Spearman; ranks make it immune to the
--                           scale mismatch above.
--   spearman_epa_per_play   same, for the raw non-garbage EPA/play rank.
--                           Moves earlier and harder than the adjusted series
--                           when garbage-time classification changes.
--   pearson_net_adj_epa     Pearson on the raw values. Secondary: sensitive to
--                           linearity and outliers, reported so a change that
--                           is purely rank-order-preserving is still visible.
--   mean_abs_rank_delta     mean |external_rank - house_net_rank| over the
--                           matched set, in rank units. More interpretable
--                           than a correlation delta for small movements.
--   source_rank_agreement   SELF-CHECK, not a quality signal. Correlation
--                           between external_rank (derived here by ordering
--                           external_value DESC) and external_rank_source (the
--                           rank the source itself publishes). The two are NOT
--                           expected to be EQUAL -- external_rank is over the
--                           matched set, external_rank_source is over the
--                           source's own full panel -- only their correlation
--                           is meaningful, and it should be very near +1.0.
--                           If it is near -1.0 for a system, the
--                           higher-is-better assumption on that system's value
--                           column is WRONG -- fix the ORDER BY in this file
--                           before trusting any other number for that system.
--                           This exists because sdv_ratings_weekly's adj_net
--                           sign convention is documented nowhere upstream and
--                           could not be verified against live rows (the table
--                           is empty until the flat-file source first runs).
--   coverage_pct            matched_teams / external_teams. A COVERAGE check,
--                           not a quality one: a "clean" correlation computed
--                           on 40% of the expected panel is a join failure,
--                           not a finding. Read this column FIRST.
--   corr_pairs              rows actually entering the correlations (matched
--                           rows with a non-NULL house rank). If it is 0 the
--                           season is UNTESTABLE for that system -- not a
--                           null result.
--
-- OPERATOR PROTOCOL (before / during / after the refresh campaign)
--
--  1. BEFORE the first scripts/backfill_refresh.py run, refresh this mart and
--     snapshot it. Do this ONCE; skipping it makes the campaign unmeasurable,
--     because the pre-refresh play data cannot be reconstructed afterwards:
--
--       python scripts/refresh_marts.py --views marts.epa_crossvalidation
--       CREATE TABLE analytics.epa_crossvalidation_baseline AS
--           SELECT now() AS captured_at, * FROM marts.epa_crossvalidation;
--
--  2. AFTER each campaign batch, rebuild the chain and refresh, then diff:
--
--       python scripts/compute_adjusted_epa.py --from 2014
--       python scripts/compute_adjusted_epa_week.py --incremental
--       python scripts/refresh_marts.py
--
--       SELECT c.season, c.external_system,
--              b.spearman_net_adj_epa  AS before_rho,
--              c.spearman_net_adj_epa  AS after_rho,
--              c.spearman_net_adj_epa - b.spearman_net_adj_epa AS d_rho,
--              c.mean_abs_rank_delta - b.mean_abs_rank_delta   AS d_rank_err,
--              b.matched_teams AS before_n, c.matched_teams AS after_n
--       FROM (SELECT DISTINCT season, external_system, spearman_net_adj_epa,
--                    mean_abs_rank_delta, matched_teams
--             FROM marts.epa_crossvalidation) c
--       JOIN (SELECT DISTINCT season, external_system, spearman_net_adj_epa,
--                    mean_abs_rank_delta, matched_teams
--             FROM analytics.epa_crossvalidation_baseline) b
--         USING (season, external_system)
--       ORDER BY 1, 2;
--
--  3. READ IT (decision rule, fixed here in advance so a disappointing batch
--     cannot be reinterpreted after the fact):
--       * |d_rho| <= 0.01 -> UNCHANGED. At ~130 matched teams per season that
--         is inside sampling noise; do not call it an improvement.
--       * d_rho > +0.01 on a majority of refreshed seasons AND d_rank_err <= 0
--         -> moved TOWARD independent estimates. Continue the campaign.
--       * d_rho < -0.01 on a majority of refreshed seasons, or any single
--         season worse than -0.05 -> moved AWAY. STOP the campaign and
--         investigate that season's batch before refreshing more seasons.
--       * before_n vs after_n differing by more than a couple of teams is a
--         COVERAGE change, not a quality signal. Reconcile coverage first;
--         a correlation computed on a different panel is not comparable.
--       * A season with 0 rows, or corr_pairs = 0, is UNTESTABLE. It is not a
--         negative result and must not be reported as one.
--
--  4. Per-team triage when a season regresses: order that (season,
--     external_system) by ABS(rank_delta) DESC. A playoff-caliber program with
--     a large negative rank_delta (house rates it far worse than every
--     external system) is the row to open the play data on. Sanity floor:
--     house_plays and house_games should track the schedule -- a team-season
--     whose house_plays collapsed after a batch is a failed re-fetch, not a
--     team that got worse.
--
-- -----------------------------------------------------------------------------
-- DEFERRED: as-of week grain
-- -----------------------------------------------------------------------------
-- Season grain only, deliberately. A week-grain twin against
-- marts.adjusted_epa_week is a reasonable follow-up and the alignment rule is
-- recorded here so it does not have to be rediscovered: sdv's through_week = W
-- INCLUDES weeks 1..W, while the house as-of fit at week_index = WI is fit on
-- weeks < WI. The like-for-like predicate is therefore
--     adjusted_epa_week.week_index = sdv_ratings_weekly.through_week + 1
-- (regular season; postseason rows are week + 100 on the house side and have
-- no sdv counterpart). Getting that off by one silently compares a fit against
-- a rating that already saw the game the fit did not.
--
-- -----------------------------------------------------------------------------
-- DEPLOY PREREQUISITE
-- -----------------------------------------------------------------------------
-- migration 052 (ratings.sdv_ratings_weekly, ratings.espn_fpi_weekly) must be
-- applied BEFORE this file. scripts/deploy_schema.py runs run_marts.py BEFORE
-- its --files migrations, so 052 must land in an EARLIER deploy run than this
-- mart -- not the same manifest. The presence check below fails with that
-- message instead of a bare "relation does not exist".

DO $$
BEGIN
    IF to_regclass('ratings.sdv_ratings_weekly') IS NULL
        OR to_regclass('ratings.espn_fpi_weekly') IS NULL THEN
        RAISE EXCEPTION 'marts.epa_crossvalidation requires migration 052 (ratings.sdv_ratings_weekly, ratings.espn_fpi_weekly). Apply it first: run_migrations.py --file src/schemas/migrations/052_sportsdataverse_xwalk_ratings.sql. Note deploy_schema.py runs run_marts BEFORE --files, so 052 must land in an earlier deploy run than this mart.';
    END IF;
END $$;

DROP MATERIALIZED VIEW IF EXISTS marts.epa_crossvalidation CASCADE;

CREATE MATERIALIZED VIEW marts.epa_crossvalidation AS
WITH house AS (
    -- One row per (season, team): marts.team_adjusted_epa is unique on
    -- (team, season) and marts.team_epa_season likewise, so this LEFT JOIN
    -- cannot fan out. team_epa_season is LEFT-joined because a team-season
    -- can have a ridge coefficient without a play-EPA aggregate row.
    SELECT
        a.season,
        a.team,
        a.net_adj_epa,
        a.off_adj_epa,
        a.def_adj_epa,
        a.plays        AS house_plays,
        e.epa_per_play AS house_epa_per_play,
        e.games_played AS house_games
    FROM marts.team_adjusted_epa a
    LEFT JOIN marts.team_epa_season e
        ON e.team = a.team
        AND e.season = a.season
),
house_counts AS (
    SELECT season, COUNT(*)::bigint AS house_teams
    FROM house
    GROUP BY season
),
team_names AS (
    -- id -> CFBD school name. ref.teams.id is the PK, so this direction is
    -- exactly one row per id. The REVERSE direction (school -> id) is NOT
    -- unique -- ref.teams carries ~35 duplicate school names -- which is why
    -- each external branch below de-duplicates per (season, school) with
    -- DISTINCT ON rather than trusting the name to be a key.
    SELECT t.id, t.school
    FROM ref.teams t
    WHERE t.school IS NOT NULL
),
sdv_final AS (
    -- Each team's LAST published weekly snapshot of the season. through_week
    -- is carried out to the mart so a ragged panel (some teams frozen at an
    -- earlier week) is visible rather than silent.
    SELECT DISTINCT ON (r.season, n.school)
        r.season,
        n.school                    AS team,
        r.team_id                   AS external_team_id,
        r.through_week              AS external_through_week,
        r.adj_net::double precision AS external_value,
        r.net_rank                  AS external_rank_source
    FROM ratings.sdv_ratings_weekly r
    JOIN team_names n ON n.id = r.team_id
    WHERE r.adj_net IS NOT NULL
    ORDER BY r.season, n.school, r.through_week DESC, r.games DESC NULLS LAST, r.team_id
),
espn_final AS (
    -- season_type DESC first: 3 = postseason ranks above 2 = regular season,
    -- matching the CFBD week-restart convention documented on the table.
    -- external_through_week uses the repo's week_index convention (+100 for
    -- postseason) so a final postseason snapshot reads as 101, not a
    -- deceptive 1 that looks like a week-1 rating.
    SELECT DISTINCT ON (f.season, n.school)
        f.season,
        n.school                AS team,
        f.team_id               AS external_team_id,
        CASE
            WHEN f.season_type >= 3 THEN 100 + f.week
            ELSE f.week
        END                     AS external_through_week,
        f.fpi::double precision AS external_value,
        f.fpirank::bigint       AS external_rank_source
    FROM ratings.espn_fpi_weekly f
    JOIN team_names n ON n.id = f.team_id
    WHERE f.fpi IS NOT NULL
    ORDER BY f.season, n.school, f.season_type DESC, f.week DESC, f.team_id
),
cfbd_fpi_final AS (
    -- Season grain already; DISTINCT ON is belt-and-braces against a duplicate
    -- (year, team) surviving a dlt merge, which would otherwise break this
    -- mart's unique index at refresh time instead of here.
    SELECT DISTINCT ON (r.year, r.team)
        r.year                    AS season,
        r.team                    AS team,
        NULL::bigint              AS external_team_id,
        NULL::bigint              AS external_through_week,
        r.fpi::double precision   AS external_value,
        r.resume_ranks__fpi       AS external_rank_source
    FROM ratings.fpi_ratings r
    WHERE r.fpi IS NOT NULL
    ORDER BY r.year, r.team, r.fpi DESC
),
external_all AS (
    SELECT
        'sdv_adj_net'::text AS external_system,
        'adj_net'::text     AS external_metric,
        s.season,
        s.team::varchar     AS team,
        s.external_team_id,
        s.external_through_week,
        s.external_value,
        s.external_rank_source
    FROM sdv_final s
    UNION ALL
    SELECT
        'espn_fpi_weekly'::text,
        'fpi'::text,
        e.season,
        e.team::varchar,
        e.external_team_id,
        e.external_through_week,
        e.external_value,
        e.external_rank_source
    FROM espn_final e
    UNION ALL
    SELECT
        'cfbd_fpi_season'::text,
        'fpi'::text,
        c.season,
        c.team::varchar,
        c.external_team_id,
        c.external_through_week,
        c.external_value,
        c.external_rank_source
    FROM cfbd_fpi_final c
),
external_counts AS (
    SELECT external_system, season, COUNT(*)::bigint AS external_teams
    FROM external_all
    GROUP BY external_system, season
),
matched AS (
    -- INNER JOIN: the (season, external_system) partition IS the matched set,
    -- so every rank, z-score and correlation below is computed over exactly
    -- the teams both sides rate. Unmatched teams are counted (house_teams /
    -- external_teams) but never ranked.
    SELECT
        h.season,
        x.external_system,
        h.team,
        x.external_metric,
        x.external_team_id,
        x.external_through_week,
        h.net_adj_epa AS house_net_adj_epa,
        h.off_adj_epa AS house_off_adj_epa,
        h.def_adj_epa AS house_def_adj_epa,
        h.house_epa_per_play,
        h.house_plays,
        h.house_games,
        x.external_value,
        x.external_rank_source,
        hc.house_teams,
        xc.external_teams
    FROM house h
    JOIN external_all x
        ON x.season = h.season
        AND x.team = h.team
    LEFT JOIN house_counts hc
        ON hc.season = h.season
    LEFT JOIN external_counts xc
        ON xc.season = h.season
        AND xc.external_system = x.external_system
),
ranked AS (
    SELECT
        m.*,
        -- CASE-wrapped so a NULL house value yields a NULL rank rather than a
        -- positional number from NULLS LAST, which would silently enter the
        -- correlations as a real observation.
        CASE
            WHEN m.house_net_adj_epa IS NOT NULL THEN RANK() OVER (
                PARTITION BY m.season, m.external_system
                ORDER BY m.house_net_adj_epa DESC NULLS LAST
            )
        END AS house_net_rank,
        CASE
            WHEN m.house_epa_per_play IS NOT NULL THEN RANK() OVER (
                PARTITION BY m.season, m.external_system
                ORDER BY m.house_epa_per_play DESC NULLS LAST
            )
        END AS house_epa_rank,
        RANK() OVER (
            PARTITION BY m.season, m.external_system
            ORDER BY m.external_value DESC
        ) AS external_rank,
        (
            m.house_net_adj_epa::double precision
            - AVG(m.house_net_adj_epa::double precision) OVER w
        ) / NULLIF(STDDEV_SAMP(m.house_net_adj_epa::double precision) OVER w, 0) AS house_net_z,
        (
            m.external_value - AVG(m.external_value) OVER w
        ) / NULLIF(STDDEV_SAMP(m.external_value) OVER w, 0) AS external_z
    FROM matched m
    WINDOW w AS (PARTITION BY m.season, m.external_system)
)
SELECT
    r.season,
    r.external_system,
    r.team,
    r.external_metric,
    r.external_team_id,
    r.external_through_week,

    -- House series (EPA/play scale -- see SCALE CAVEAT)
    r.house_net_adj_epa,
    r.house_off_adj_epa,
    r.house_def_adj_epa,          -- LOWER = better; not ranked here
    r.house_epa_per_play,
    r.house_plays,
    r.house_games,

    -- External series (each system's own scale -- never differenced vs house)
    ROUND(r.external_value::numeric, 4) AS external_value,
    r.external_rank_source,

    -- Per-team comparison, within (season, external_system) matched set
    r.house_net_rank,
    r.house_epa_rank,
    r.external_rank,
    ROUND(r.house_net_z::numeric, 4) AS house_net_z,
    ROUND(r.external_z::numeric, 4)  AS external_z,
    -- Positive = house ranks this team BETTER than the external system does.
    (r.external_rank - r.house_net_rank) AS rank_delta,

    -- Per-(season, external_system) harness statistics, repeated on every row
    COUNT(*) OVER w                     AS matched_teams,
    COUNT(r.house_net_rank) OVER w      AS corr_pairs,
    r.house_teams,
    r.external_teams,
    ROUND(100.0 * COUNT(*) OVER w / NULLIF(r.external_teams, 0), 1) AS coverage_pct,
    ROUND(
        (CORR(r.house_net_rank::double precision, r.external_rank::double precision) OVER w)::numeric,
        4
    ) AS spearman_net_adj_epa,
    ROUND(
        (CORR(r.house_epa_rank::double precision, r.external_rank::double precision) OVER w)::numeric,
        4
    ) AS spearman_epa_per_play,
    ROUND(
        (CORR(r.house_net_adj_epa::double precision, r.external_value) OVER w)::numeric,
        4
    ) AS pearson_net_adj_epa,
    ROUND(AVG(ABS(r.external_rank - r.house_net_rank)) OVER w, 2) AS mean_abs_rank_delta,
    -- Self-check, expect ~ +1.0. See METHODOLOGY.
    ROUND(
        (CORR(r.external_rank::double precision, r.external_rank_source::double precision) OVER w)::numeric,
        4
    ) AS source_rank_agreement
FROM ranked r
WINDOW w AS (PARTITION BY r.season, r.external_system);

-- Required for REFRESH CONCURRENTLY; also the natural grain key. Unique by
-- construction: `house` is one row per (season, team) and each external branch
-- is DISTINCT ON (season, school) under a distinct external_system label.
CREATE UNIQUE INDEX ON marts.epa_crossvalidation (season, external_system, team);

-- Query indexes: the campaign reads one system's season series at a time, then
-- drills into that season's biggest movers.
CREATE INDEX ON marts.epa_crossvalidation (external_system, season);
CREATE INDEX ON marts.epa_crossvalidation (season, external_system, rank_delta);

COMMENT ON MATERIALIZED VIEW marts.epa_crossvalidation IS
    'Descriptive plausibility harness for the historical play-stats refresh '
    'campaign: house adjusted EPA vs independent external ratings, compared by '
    'rank (Spearman) and z-score within each (season, external_system) matched '
    'set. Internal ops surface -- NOT a downstream contract, NOT a model input '
    '(it mixes season-final information and would leak if used as-of), and NOT '
    'a shipping gate (that is the walk-forward no-regression gate on '
    'marts.prediction_accuracy). See src/schemas/marts/044_epa_crossvalidation.sql '
    'for the before/after operator protocol and its fixed decision rule.';

-- Re-grant on every apply: this file DROPs the matview, which discards its
-- grants, and the marts schema has no ALTER DEFAULT PRIVILEGES for the
-- PostgREST roles. Same reasoning as marts/028_data_freshness.sql -- do NOT
-- repair a lost grant by re-running
-- migrations/grant_read_access_for_security_invoker.sql, whose blanket grant
-- over the analytics schema would undo deliberate revokes elsewhere.
GRANT SELECT ON marts.epa_crossvalidation TO anon, authenticated;

-- Coverage guard, WARNING not EXCEPTION (deliberate deviation from
-- marts/036, 040 and 043). ratings.sdv_ratings_weekly and
-- ratings.espn_fpi_weekly are legitimately empty until the sportsdataverse
-- flat-file sources first run, so an empty system is expected on day one and
-- must not fail the deploy. It must also not pass silently: a dark system is
-- an UNTESTABLE anchor, never a null finding.
DO $$
DECLARE
    v_missing text;
BEGIN
    SELECT string_agg(s, ', ' ORDER BY s)
    INTO v_missing
    FROM unnest(ARRAY['cfbd_fpi_season', 'espn_fpi_weekly', 'sdv_adj_net']) AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM marts.epa_crossvalidation x WHERE x.external_system = s
    );

    IF v_missing IS NOT NULL THEN
        RAISE WARNING 'marts.epa_crossvalidation: zero matched rows for external system(s): %. Expected while the sportsdataverse flat-file sources (sdv_ratings_weekly, sdv_fpi_weekly) have not run yet -- but cfbd_fpi_season should NEVER be dark (ratings.fpi_ratings is loaded historically); if it is, the house<->fpi_ratings team-name join is broken, not empty. These systems are UNTESTABLE, not negative results.', v_missing;
    END IF;
END $$;
