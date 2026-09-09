-- F19 additive SDV season-source freshness. Managed forward migration after 072.
-- This is a query-time compatibility surface over publication receipts; it
-- declares no source cadence and does not change get_asset_freshness().

DO $policy_preflight$
DECLARE policy_oid oid := pg_catalog.to_regclass('meta.asset_freshness_policies');
BEGIN
    IF policy_oid IS NULL OR NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        WHERE c.oid = policy_oid
            AND c.relkind = 'r'
            AND c.relowner = (
                SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname = current_user)
            AND c.relrowsecurity
            AND NOT c.relforcerowsecurity
            AND NOT c.relhasrules
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_inherits i
        WHERE i.inhrelid = policy_oid OR i.inhparent = policy_oid
    ) THEN
        RAISE EXCEPTION
            'asset freshness policy must be a trusted migration-owned private table';
    END IF;
END
$policy_preflight$;

ALTER TABLE meta.asset_freshness_policies
    DROP CONSTRAINT IF EXISTS asset_freshness_policies_asset_key_check;
ALTER TABLE meta.asset_freshness_policies
    ADD CONSTRAINT asset_freshness_policies_asset_key_check CHECK (asset_key IN (
        'analytics.house_elo_game',
        'marts.house_elo_game',
        'ratings.sdv_ratings_weekly',
        'ratings.espn_fpi_weekly',
        'ref.team_id_xwalk',
        'ref.game_id_xwalk'
    ));

ALTER TABLE meta.asset_freshness_policies
    DROP CONSTRAINT IF EXISTS asset_freshness_policies_coverage_key_check;
ALTER TABLE meta.asset_freshness_policies
    ADD CONSTRAINT asset_freshness_policies_coverage_key_check CHECK (
        (asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game')
            AND coverage_key = 'source-wide')
        OR
        (asset_key IN (
                'ratings.sdv_ratings_weekly', 'ratings.espn_fpi_weekly',
                'ref.team_id_xwalk', 'ref.game_id_xwalk'
            )
            AND coverage_key ~
                '^season:(18(69|[7-9][0-9])|19[0-9]{2}|20[0-9]{2}|21[0-9]{2}|2200)$')
    );

CREATE OR REPLACE FUNCTION public.get_source_freshness(p_season bigint)
RETURNS TABLE (
    source_name text,
    asset_key text,
    season bigint,
    coverage_key text,
    generation_id uuid,
    published_at timestamptz,
    age_seconds numeric,
    expected_refresh_interval interval,
    is_stale boolean,
    publication_state text,
    current_outcome text,
    is_complete boolean,
    source_rows bigint,
    published_rows bigint,
    artifact_origin text,
    season_basis text,
    latest_outcome text,
    latest_recorded_at timestamptz,
    last_failure_outcome text,
    last_failure_at timestamptz,
    last_failure_category text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = ''
AS $function$
BEGIN
    IF p_season IS NULL OR p_season < 1869 OR p_season > 2200 THEN
        RAISE EXCEPTION 'SDV source season must be between 1869 and 2200'
            USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    WITH sources(source_order, source_name, asset_key, expected_protocol,
            expected_parser) AS (
        VALUES
            (1, 'sdv_ratings_weekly'::text, 'ratings.sdv_ratings_weekly'::text,
                'sdv-ratings-season-v1'::text, 'sdv-ratings-v1'::text),
            (2, 'sdv_fpi_weekly'::text, 'ratings.espn_fpi_weekly'::text,
                'sdv-season-file-v1'::text, 'sdv-fpi-v1'::text),
            (3, 'sdv_team_xwalk'::text, 'ref.team_id_xwalk'::text,
                'sdv-season-file-v1'::text, 'sdv-team-xwalk-v1'::text),
            (4, 'sdv_game_xwalk'::text, 'ref.game_id_xwalk'::text,
                'sdv-season-file-v1'::text, 'sdv-game-xwalk-v1'::text)
    ), requested AS (
        SELECT s.*, p_season AS season, 'season:' || p_season::text AS coverage_key
        FROM sources s
    ), receipt_evidence AS (
        SELECT a.*, pointer.generation_id AS pointer_generation_id,
            receipt.generation_id AS receipt_generation_id,
            receipt.outcome AS receipt_outcome,
            receipt.coverage,
            receipt.input_observations,
            receipt.row_delta,
            receipt.published_at AS receipt_published_at,
            policy.expected_refresh_interval,
            latest.outcome AS latest_outcome,
            latest.recorded_at AS latest_recorded_at,
            failure.outcome AS last_failure_outcome,
            failure.recorded_at AS last_failure_at,
            CASE WHEN failure.error_summary = 'source_publication_failed'
                THEN 'source_publication_failed'::text ELSE NULL::text
                END AS last_failure_category
        FROM requested a
        LEFT JOIN meta.asset_current_generations pointer
            ON pointer.asset_key = a.asset_key
            AND pointer.coverage_key = a.coverage_key
        LEFT JOIN meta.asset_receipts receipt
            ON receipt.asset_key = pointer.asset_key
            AND receipt.coverage_key = pointer.coverage_key
            AND receipt.generation_id = pointer.generation_id
        LEFT JOIN meta.asset_freshness_policies policy
            ON policy.asset_key = a.asset_key
            AND policy.coverage_key = a.coverage_key
        LEFT JOIN LATERAL (
            SELECT r.outcome, r.recorded_at
            FROM meta.asset_receipts r
            WHERE r.asset_key = a.asset_key
                AND r.coverage_key = a.coverage_key
            ORDER BY r.recorded_at DESC, r.generation_id DESC
            LIMIT 1
        ) latest ON true
        LEFT JOIN LATERAL (
            SELECT r.outcome, r.recorded_at, r.error_summary
            FROM meta.asset_receipts r
            WHERE r.asset_key = a.asset_key
                AND r.coverage_key = a.coverage_key
                AND r.outcome IN ('failed', 'partial', 'deferred', 'blocked')
            ORDER BY r.recorded_at DESC, r.generation_id DESC
            LIMIT 1
        ) failure ON true
    ), normalized AS (
        SELECT e.*,
            CASE
                WHEN pg_catalog.jsonb_typeof(e.coverage->'season') = 'number'
                    AND e.coverage->>'season' ~ '^(0|[1-9][0-9]*)$'
                THEN (e.coverage->>'season')::numeric
                END AS evidence_season,
            CASE
                WHEN pg_catalog.jsonb_typeof(e.coverage->'source_rows') = 'number'
                    AND e.coverage->>'source_rows' ~ '^[1-9][0-9]{0,5}$'
                THEN (e.coverage->>'source_rows')::numeric
                END AS evidence_source_rows,
            CASE
                WHEN pg_catalog.jsonb_typeof(e.coverage->'published_rows') = 'number'
                    AND e.coverage->>'published_rows' ~ '^[1-9][0-9]{0,5}$'
                THEN (e.coverage->>'published_rows')::numeric
                END AS evidence_published_rows,
            CASE
                WHEN pg_catalog.jsonb_typeof(e.row_delta->'published_rows') = 'number'
                    AND e.row_delta->>'published_rows' ~ '^[1-9][0-9]{0,5}$'
                THEN (e.row_delta->>'published_rows')::numeric
                END AS delta_published_rows
        FROM receipt_evidence e
    ), checked AS (
        SELECT e.*,
            e.receipt_generation_id IS NOT NULL
            AND e.receipt_outcome = 'succeeded'
            AND e.receipt_published_at IS NOT NULL
            AND pg_catalog.isfinite(e.receipt_published_at)
            AND e.receipt_published_at <= pg_catalog.statement_timestamp()
            AND pg_catalog.jsonb_typeof(e.coverage) = 'object'
            AND pg_catalog.jsonb_typeof(e.coverage->'complete') = 'boolean'
            AND e.coverage->'complete' = 'true'::jsonb
            AND pg_catalog.jsonb_typeof(e.coverage->'scope') = 'string'
            AND e.coverage->>'scope' = 'season'
            AND pg_catalog.jsonb_typeof(e.coverage->'mode') = 'string'
            AND e.coverage->>'mode' = 'full_file_for_season'
            AND e.evidence_season = e.season
            AND e.evidence_source_rows BETWEEN 1 AND 100000
            AND e.evidence_published_rows = e.evidence_source_rows
            AND pg_catalog.jsonb_typeof(e.row_delta) = 'object'
            AND e.delta_published_rows = e.evidence_published_rows
            AND pg_catalog.jsonb_typeof(e.input_observations) = 'object'
            AND pg_catalog.jsonb_typeof(e.input_observations->'publisher') = 'string'
            AND e.input_observations->>'publisher' = 'load_flat_files'
            AND pg_catalog.jsonb_typeof(e.input_observations->'protocol') = 'string'
            AND e.input_observations->>'protocol' = e.expected_protocol
            AND pg_catalog.jsonb_typeof(e.input_observations->'parser_contract') = 'string'
            AND e.input_observations->>'parser_contract' = e.expected_parser
            AND pg_catalog.jsonb_typeof(e.input_observations->'stage_schema') = 'string'
            AND e.input_observations->>'stage_schema' = 'warehouse_source_stage'
            AND pg_catalog.jsonb_typeof(e.input_observations->'artifact_origin') = 'string'
            AND e.input_observations->>'artifact_origin' IN ('registered_url', 'local_file')
            AND (
                e.source_name = 'sdv_ratings_weekly'
                OR (
                    pg_catalog.jsonb_typeof(
                        e.input_observations->'source_name') = 'string'
                    AND e.input_observations->>'source_name' = e.source_name
                    AND pg_catalog.jsonb_typeof(
                        e.input_observations->'season_basis') = 'string'
                    AND e.input_observations->>'season_basis' = CASE
                        WHEN e.source_name = 'sdv_fpi_weekly'
                            THEN 'artifact_field'
                        WHEN e.input_observations->>'artifact_origin' = 'registered_url'
                            THEN 'registered_artifact_name'
                        ELSE 'caller_declared'
                    END
                )
            ) AS current_is_valid
        FROM normalized e
    )
    SELECT c.source_name, c.asset_key, c.season, c.coverage_key,
        CASE WHEN c.current_is_valid THEN c.receipt_generation_id END AS generation_id,
        CASE WHEN c.current_is_valid THEN c.receipt_published_at END AS published_at,
        CASE WHEN c.current_is_valid THEN
            EXTRACT(EPOCH FROM (
                pg_catalog.statement_timestamp() - c.receipt_published_at))
            END AS age_seconds,
        c.expected_refresh_interval,
        CASE WHEN c.current_is_valid AND c.expected_refresh_interval IS NOT NULL
            THEN pg_catalog.statement_timestamp() - c.receipt_published_at
                > c.expected_refresh_interval
            ELSE NULL::boolean END AS is_stale,
        CASE WHEN c.current_is_valid THEN 'current'::text
            WHEN c.pointer_generation_id IS NOT NULL THEN 'invalid'::text
            WHEN c.latest_outcome IS NOT NULL THEN 'unpublished'::text
            ELSE 'unrecorded'::text END AS publication_state,
        CASE WHEN c.current_is_valid THEN c.receipt_outcome END AS current_outcome,
        CASE WHEN c.current_is_valid THEN true ELSE NULL::boolean END AS is_complete,
        CASE WHEN c.current_is_valid
            THEN c.evidence_source_rows::bigint END AS source_rows,
        CASE WHEN c.current_is_valid
            THEN c.evidence_published_rows::bigint END AS published_rows,
        CASE WHEN c.current_is_valid
            THEN c.input_observations->>'artifact_origin' END AS artifact_origin,
        CASE WHEN c.current_is_valid THEN
            CASE WHEN c.source_name = 'sdv_ratings_weekly' THEN 'artifact_field'::text
                ELSE c.input_observations->>'season_basis' END
            END AS season_basis,
        c.latest_outcome, c.latest_recorded_at,
        c.last_failure_outcome, c.last_failure_at, c.last_failure_category
    FROM checked c
    ORDER BY c.source_order;
END
$function$;

COMMENT ON FUNCTION public.get_source_freshness(bigint) IS
'Safe query-time publication evidence for four fixed SDV season sources. Missing cadence and unusable evidence produce unknown staleness; season provenance is allowlisted.';

-- Remove inherited default grants and expose only this bounded owner-rights RPC.
DO $access$
DECLARE
    rpc record;
    acl_entry record;
    owner_id oid;
    recipient text;
BEGIN
    SELECT oid INTO owner_id FROM pg_catalog.pg_roles WHERE rolname = current_user;
    SELECT p.oid, p.proowner, p.proacl INTO rpc
    FROM pg_catalog.pg_proc p
    WHERE p.oid = 'public.get_source_freshness(bigint)'::regprocedure;
    IF NOT FOUND OR rpc.proowner <> owner_id THEN
        RAISE EXCEPTION 'source freshness RPC must be migration-owned';
    END IF;

    FOR acl_entry IN
        SELECT DISTINCT a.grantee
        FROM pg_catalog.aclexplode(
            COALESCE(rpc.proacl, pg_catalog.acldefault('f', rpc.proowner))) a
        WHERE a.grantee <> owner_id
    LOOP
        recipient := CASE WHEN acl_entry.grantee = 0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(
                pg_catalog.pg_get_userbyid(acl_entry.grantee)) END;
        EXECUTE pg_catalog.format(
            'REVOKE ALL ON FUNCTION public.get_source_freshness(bigint) FROM %s',
            recipient);
    END LOOP;
END
$access$;

GRANT USAGE ON SCHEMA public TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_source_freshness(bigint) TO anon, authenticated;
