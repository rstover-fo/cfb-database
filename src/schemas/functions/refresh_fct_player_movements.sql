-- rp.refresh_fct_player_movements() -- U3 of the returning production plan.
-- Builds the movement-event grain by unioning three sources:
--   1. Roster continuity (returners on same team across consecutive seasons,
--      with HC-change detection via marts.coaching_tenure)
--   2. Portal events (recruiting.transfer_portal with 3-tier name matching:
--      exact, fuzzy levenshtein <= 2, synthetic id for unmatched)
--   3. Recruit class (recruiting.recruits, classified by stars)
--
-- Idempotent: TRUNCATE rp.fct_player_movements + TRUNCATE rp.unmatched_portal_log,
-- then INSERT. Re-running yields identical state.
-- SECURITY DEFINER + SET search_path='' per 2026-02-07 hardening.
--
-- KNOWN LIMITATIONS:
-- * Conference classification uses ref.teams (current conference, not historical).
--   Realignment (Pac-12 dissolution, USC/UCLA -> Big Ten in 2024) is reflected
--   only as of the current ref.teams snapshot. Acceptable for v1; backtest
--   accuracy can be improved by joining against a season-aware conference table.
-- * portal_juco_to_fbs will produce ~0 rows because CFBD's /player/portal does
--   not include JUCO origins. The dim row is kept for forward compatibility.
-- * P5 set is hand-curated (SEC, Big Ten, ACC, Big 12, Pac-12). Pac-12 is treated
--   as P5 even though only 2 schools remained in 2025; pre-realignment seasons
--   benefit from this. G5 is the catchall for everything else FBS.
-- * Multi-team portal entries with same (first, last, origin, season) are
--   deduped via DISTINCT ON deterministically; the dropped match is silent.

CREATE OR REPLACE FUNCTION rp.refresh_fct_player_movements()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
BEGIN
  TRUNCATE rp.fct_player_movements;
  TRUNCATE rp.unmatched_portal_log;

  INSERT INTO rp.fct_player_movements (
    player_id, transition_season, movement_type,
    source_team, source_conference, destination_team, destination_conference,
    match_confidence, match_method,
    source_first_name, source_last_name, source_url, source_date
  )
  WITH
  -- Team -> conference + classification lookup. ref.teams has duplicate `school`
  -- rows (memory 2026-02-05); DISTINCT ON dedupes, preferring populated classification.
  team_meta AS (
    SELECT DISTINCT ON (school)
      school AS team,
      conference,
      classification
    FROM ref.teams
    ORDER BY school, classification NULLS LAST, conference NULLS LAST
  ),
  -- HC for (team, season): expand each coaching_tenure span via generate_series so
  -- the join becomes a simple equality match. tenure_end IS NULL means active --
  -- treat as ongoing through 2026 (the upper bound of our scope).
  hc_lookup AS (
    SELECT
      ct.team,
      gs::int AS season,
      ct.coach_name AS hc
    FROM marts.coaching_tenure ct,
         LATERAL generate_series(
           ct.tenure_start::int,
           COALESCE(ct.tenure_end, 2026)::int
         ) AS gs
    WHERE COALESCE(ct.tenure_end, 2026) >= 2020
  ),
  -- Returners: same player, same team, consecutive seasons. HC change drives
  -- the 2-tier continuity (returning_same_hc / returning_new_hc).
  returners AS (
    SELECT
      curr.player_id,
      curr.season AS transition_season,
      prior.team AS source_team,
      prior.conference AS source_conference,
      curr.team AS destination_team,
      curr.conference AS destination_conference,
      CASE
        -- IS NOT DISTINCT FROM treats NULL=NULL as match (defensive default
        -- when coaching_tenure is missing for either side)
        WHEN hc_curr.hc IS NOT DISTINCT FROM hc_prior.hc THEN 'returning_same_hc'
        ELSE 'returning_new_hc'
      END AS movement_type,
      1.00::numeric(3,2) AS match_confidence,
      'roster_continuity'::varchar AS match_method,
      NULL::varchar AS source_first_name,
      NULL::varchar AS source_last_name,
      NULL::varchar AS source_url,
      NULL::date AS source_date
    FROM rp.fct_player_seasons curr
    JOIN rp.fct_player_seasons prior
      ON prior.player_id = curr.player_id
      AND prior.season = curr.season - 1
      AND prior.team = curr.team
    LEFT JOIN hc_lookup hc_curr
      ON hc_curr.team = curr.team AND hc_curr.season = curr.season
    LEFT JOIN hc_lookup hc_prior
      ON hc_prior.team = prior.team AND hc_prior.season = prior.season
    WHERE curr.season BETWEEN 2021 AND 2026
  ),
  -- Portal exact-match: case-insensitive (first_name, last_name) + origin team
  -- + prior season's roster row. DISTINCT ON dedupes ambiguous matches
  -- (same name on roster, rare) by smallest roster.id.
  portal_exact AS (
    SELECT DISTINCT ON (p.first_name, p.last_name, p.origin, p.season)
      p.first_name, p.last_name,
      p.origin, p.destination,
      p.season::int AS transition_season,
      p.stars, p.rating, p.position, p.transfer_date::date AS transfer_date,
      r.id::text AS player_id
    FROM recruiting.transfer_portal p
    JOIN core.roster r
      ON lower(r.first_name) = lower(p.first_name)
      AND lower(r.last_name) = lower(p.last_name)
      AND r.team = p.origin
      AND r.year = p.season - 1
    WHERE p.season BETWEEN 2021 AND 2026
    ORDER BY p.first_name, p.last_name, p.origin, p.season, r.id
  ),
  -- Portal fuzzy-match: levenshtein(full_name) <= 2 against same (origin, prior year).
  -- Excludes entries already matched exactly. fuzzystrmatch.levenshtein is in public schema.
  portal_fuzzy AS (
    SELECT DISTINCT ON (p.first_name, p.last_name, p.origin, p.season)
      p.first_name, p.last_name,
      p.origin, p.destination,
      p.season::int AS transition_season,
      p.stars, p.rating, p.position, p.transfer_date::date AS transfer_date,
      r.id::text AS player_id
    FROM recruiting.transfer_portal p
    JOIN core.roster r
      ON r.team = p.origin
      AND r.year = p.season - 1
      -- levenshtein_less_equal short-circuits at distance > max, bounding the
      -- O(m*n) scan against pathological CFBD payloads.
      AND public.levenshtein_less_equal(
        lower(coalesce(p.first_name, '') || coalesce(p.last_name, '')),
        lower(coalesce(r.first_name, '') || coalesce(r.last_name, '')),
        2
      ) <= 2
    WHERE p.season BETWEEN 2021 AND 2026
      AND NOT EXISTS (
        SELECT 1 FROM portal_exact pe
        WHERE pe.first_name = p.first_name
          AND pe.last_name = p.last_name
          AND pe.origin = p.origin
          AND pe.transition_season = p.season::int
      )
    ORDER BY p.first_name, p.last_name, p.origin, p.season,
      public.levenshtein_less_equal(
        lower(coalesce(p.first_name, '') || coalesce(p.last_name, '')),
        lower(coalesce(r.first_name, '') || coalesce(r.last_name, '')),
        2
      ),
      r.id
  ),
  -- Portal unmatched: synthetic player_id derived from md5 of all available
  -- distinguishing fields. Including destination + transfer_date avoids collision
  -- when (first, last, origin, season) collapse to a single bucket -- e.g. two
  -- NULL-name CFBD rows in the same season, or two same-name players from the
  -- same school going to different destinations. Stable across reruns; logged
  -- separately in unmatched_portal_log for audit.
  portal_unmatched AS (
    SELECT
      p.first_name, p.last_name,
      p.origin, p.destination,
      p.season::int AS transition_season,
      p.stars, p.rating, p.position, p.transfer_date::date AS transfer_date,
      'portal:' || md5(
        coalesce(p.first_name, '')   || '|' ||
        coalesce(p.last_name, '')    || '|' ||
        coalesce(p.origin, '')       || '|' ||
        coalesce(p.destination, '')  || '|' ||
        coalesce(p.transfer_date::text, '') || '|' ||
        p.season::text
      ) AS player_id
    FROM recruiting.transfer_portal p
    WHERE p.season BETWEEN 2021 AND 2026
      AND NOT EXISTS (
        SELECT 1 FROM portal_exact pe
        WHERE pe.first_name = p.first_name AND pe.last_name = p.last_name
          AND pe.origin = p.origin AND pe.transition_season = p.season::int
      )
      AND NOT EXISTS (
        SELECT 1 FROM portal_fuzzy pf
        WHERE pf.first_name = p.first_name AND pf.last_name = p.last_name
          AND pf.origin = p.origin AND pf.transition_season = p.season::int
      )
  ),
  -- Combined portal entries with conference/classification lookups for movement_type
  portal_combined AS (
    SELECT *, 1.00::numeric(3,2) AS match_confidence, 'portal_exact'::varchar AS match_method FROM portal_exact
    UNION ALL
    SELECT *, 0.80::numeric(3,2), 'portal_fuzzy'::varchar FROM portal_fuzzy
    UNION ALL
    SELECT *, 0.00::numeric(3,2), 'unmatched'::varchar FROM portal_unmatched
  ),
  portal_classified AS (
    SELECT
      pc.player_id,
      pc.transition_season,
      pc.first_name AS source_first_name,
      pc.last_name AS source_last_name,
      pc.origin AS source_team,
      src.conference AS source_conference,
      pc.destination AS destination_team,
      dst.conference AS destination_conference,
      pc.match_confidence,
      pc.match_method,
      NULL::varchar AS source_url,
      pc.transfer_date AS source_date,
      CASE
        -- FCS or non-NCAA-D1 origin -> FBS dest: portal_fcs_to_fbs (translation discount)
        WHEN COALESCE(src.classification, 'unknown') IN ('fcs', 'ii', 'iii', 'unknown')
             AND dst.classification = 'fbs'
          THEN 'portal_fcs_to_fbs'
        -- Both FBS: classify by P5/G5 of source and destination conferences
        WHEN src.classification = 'fbs' AND dst.classification = 'fbs' THEN
          CASE
            WHEN src.conference IN ('SEC', 'Big Ten', 'ACC', 'Big 12', 'Pac-12')
              AND dst.conference IN ('SEC', 'Big Ten', 'ACC', 'Big 12', 'Pac-12')
              THEN 'portal_p5_to_p5'
            WHEN src.conference IN ('SEC', 'Big Ten', 'ACC', 'Big 12', 'Pac-12')
              THEN 'portal_p5_to_g5'
            WHEN dst.conference IN ('SEC', 'Big Ten', 'ACC', 'Big 12', 'Pac-12')
              THEN 'portal_g5_to_p5'
            ELSE 'portal_g5_to_g5'
          END
        -- FBS -> non-FBS or both non-FBS: bucket as portal_g5_to_g5 (low-fidelity catchall)
        ELSE 'portal_g5_to_g5'
      END AS movement_type
    FROM portal_combined pc
    LEFT JOIN team_meta src ON src.team = pc.origin
    LEFT JOIN team_meta dst ON dst.team = pc.destination
  ),
  -- Recruits: dedupe on (athlete_id, year). Reclassifiers can have multiple rows;
  -- prefer highest stars per athlete-year tuple.
  recruits_dedup AS (
    SELECT DISTINCT ON (athlete_id, year)
      athlete_id::text AS player_id,
      year::int AS transition_season,
      committed_to AS destination_team,
      stars,
      name
    FROM recruiting.recruits
    WHERE year BETWEEN 2021 AND 2026
      AND athlete_id IS NOT NULL
      AND committed_to IS NOT NULL
    ORDER BY athlete_id, year, stars DESC NULLS LAST
  ),
  recruits AS (
    SELECT
      rd.player_id,
      rd.transition_season,
      NULL::varchar AS source_team,
      NULL::varchar AS source_conference,
      rd.destination_team,
      tm.conference AS destination_conference,
      CASE
        WHEN rd.stars >= 5 THEN 'recruit_5star'
        WHEN rd.stars = 4 THEN 'recruit_4star'
        WHEN rd.stars = 3 THEN 'recruit_3star'
        ELSE 'recruit_unrated'
      END AS movement_type,
      1.00::numeric(3,2) AS match_confidence,
      'recruit'::varchar AS match_method,
      -- Recruits store full name in 'name'; split is best-effort
      split_part(rd.name, ' ', 1) AS source_first_name,
      NULLIF(regexp_replace(rd.name, '^\S+\s+', ''), rd.name) AS source_last_name,
      NULL::varchar AS source_url,
      NULL::date AS source_date
    FROM recruits_dedup rd
    LEFT JOIN team_meta tm ON tm.team = rd.destination_team
  ),
  -- Combine all three sources with a priority ranking. PK is (player_id, transition_season);
  -- if a player_id appears in multiple sources for the same transition (rare but possible
  -- e.g., portal-matched player who's also in returners due to multi-team season quirk),
  -- prefer portal > returner > recruit. Lower priority value = higher precedence.
  all_movements AS (
    SELECT
      player_id, transition_season, movement_type,
      source_team, source_conference, destination_team, destination_conference,
      match_confidence, match_method,
      source_first_name, source_last_name, source_url, source_date,
      1 AS priority
    FROM portal_classified
    UNION ALL
    SELECT
      player_id, transition_season, movement_type,
      source_team, source_conference, destination_team, destination_conference,
      match_confidence, match_method,
      source_first_name, source_last_name, source_url, source_date,
      2 AS priority
    FROM returners
    UNION ALL
    SELECT
      player_id, transition_season, movement_type,
      source_team, source_conference, destination_team, destination_conference,
      match_confidence, match_method,
      source_first_name, source_last_name, source_url, source_date,
      3 AS priority
    FROM recruits
  )
  SELECT DISTINCT ON (player_id, transition_season)
    player_id, transition_season, movement_type,
    source_team, source_conference, destination_team, destination_conference,
    match_confidence, match_method,
    source_first_name, source_last_name, source_url, source_date
  FROM all_movements
  ORDER BY player_id, transition_season, priority;

  -- Audit log: every portal entry that did not exact- or fuzzy-match a prior-season
  -- roster row. The match conditions duplicate the portal_unmatched CTE above; if
  -- the levenshtein threshold or join keys change, update both in lockstep.
  INSERT INTO rp.unmatched_portal_log (
    transition_season, first_name, last_name, origin, destination,
    stars, rating, position, transfer_date, reason
  )
  SELECT
    p.season::int AS transition_season,
    p.first_name, p.last_name, p.origin, p.destination,
    p.stars::int, p.rating::numeric(5,4),
    p.position, p.transfer_date::date,
    CASE
      WHEN NOT EXISTS (
        SELECT 1 FROM core.roster r
        WHERE r.team = p.origin AND r.year = p.season - 1
      ) THEN 'no_roster_for_origin_season'
      ELSE 'no_name_match'
    END AS reason
  FROM recruiting.transfer_portal p
  WHERE p.season BETWEEN 2021 AND 2026
    AND NOT EXISTS (
      SELECT 1 FROM core.roster r
      WHERE lower(r.first_name) = lower(p.first_name)
        AND lower(r.last_name) = lower(p.last_name)
        AND r.team = p.origin
        AND r.year = p.season - 1
    )
    AND NOT EXISTS (
      SELECT 1 FROM core.roster r
      WHERE r.team = p.origin
        AND r.year = p.season - 1
        AND public.levenshtein_less_equal(
          lower(coalesce(p.first_name, '') || coalesce(p.last_name, '')),
          lower(coalesce(r.first_name, '') || coalesce(r.last_name, '')),
          2
        ) <= 2
    );

  ANALYZE rp.fct_player_movements;
  ANALYZE rp.unmatched_portal_log;
END;
$function$;

COMMENT ON FUNCTION rp.refresh_fct_player_movements() IS
  'U3 loader. Populates rp.fct_player_movements with three movement sources: '
  'roster continuity (returners), portal events (3-tier name match), recruit class. '
  'Idempotent (TRUNCATE + INSERT). Also writes audit rows for unmatched portal '
  'entries to rp.unmatched_portal_log.';

-- SECURITY DEFINER + REVOKE EXECUTE FROM PUBLIC: prevents anon from calling
-- this loader via PostgREST. See refresh_fct_player_seasons.sql for the same
-- rationale.
REVOKE EXECUTE ON FUNCTION rp.refresh_fct_player_movements() FROM PUBLIC;
