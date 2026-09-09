-- F19 additive receipt freshness. Managed forward migration after 068.
-- Prepared, not a production rollout. The existing get_data_freshness() and
-- its maintenance-statistics matview remain unchanged for existing consumers.

CREATE TABLE IF NOT EXISTS meta.asset_freshness_policies (
    asset_key text NOT NULL REFERENCES meta.asset_publication_locks(asset_key),
    coverage_key text NOT NULL DEFAULT 'source-wide' CHECK (coverage_key = 'source-wide'),
    expected_refresh_interval interval CHECK (expected_refresh_interval > interval '0 seconds'),
    PRIMARY KEY (asset_key, coverage_key),
    CHECK (asset_key IN ('analytics.house_elo_game', 'marts.house_elo_game'))
);
-- meta can retain CREATE for existing warehouse writers. Reject a precreated
-- relation before INSERT could fire its untrusted triggers as the migration role.
DO $policy_owner$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c
        WHERE c.oid = 'meta.asset_freshness_policies'::regclass
            AND c.relkind = 'r'
            AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)
    ) THEN
        RAISE EXCEPTION 'freshness policy must be a migration-owned ordinary table';
    END IF;
END
$policy_owner$;
ALTER TABLE meta.asset_freshness_policies ENABLE ROW LEVEL SECURITY;

-- The optional full publisher has no declared SLA yet. Reapply preserves an
-- owner's later policy declaration; missing policy never removes an asset.
INSERT INTO meta.asset_freshness_policies(asset_key)
VALUES ('analytics.house_elo_game'), ('marts.house_elo_game')
ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS asset_receipts_latest_idx
    ON meta.asset_receipts(asset_key, coverage_key, recorded_at DESC, generation_id DESC);
CREATE INDEX IF NOT EXISTS asset_receipts_failure_idx
    ON meta.asset_receipts(asset_key, coverage_key, recorded_at DESC, generation_id DESC)
    WHERE outcome IN ('failed', 'partial', 'deferred', 'blocked');

-- Deliberately owner-rights: consumers read this safe projection, not private
-- receipt/operation/policy records. No security_invoker setting is introduced.
CREATE OR REPLACE VIEW marts.asset_receipt_freshness AS
WITH assets(asset_key, coverage_key) AS (
    VALUES ('analytics.house_elo_game'::text, 'source-wide'::text),
           ('marts.house_elo_game'::text, 'source-wide'::text)
), evidence AS (
    SELECT a.asset_key, a.coverage_key, r.generation_id, r.published_at,
        EXTRACT(EPOCH FROM (pg_catalog.statement_timestamp() - r.published_at)) AS age_seconds,
        policy.expected_refresh_interval,
        CASE WHEN r.generation_id IS NULL OR policy.expected_refresh_interval IS NULL
            THEN NULL::boolean
            ELSE pg_catalog.statement_timestamp() - r.published_at > policy.expected_refresh_interval
        END AS is_stale,
        CASE WHEN r.generation_id IS NOT NULL THEN 'current'
             WHEN latest.generation_id IS NOT NULL THEN 'unpublished'
             ELSE 'unrecorded' END AS publication_state,
        r.outcome AS current_outcome, r.coverage, r.source_watermark, r.row_delta,
        COALESCE(inputs.generations, '[]'::jsonb) AS required_input_generations,
        CASE WHEN r.generation_id IS NULL OR a.asset_key = 'analytics.house_elo_game'
            THEN NULL::boolean
            ELSE COALESCE(inputs.input_count = 1 AND inputs.expected_input_current, false)
        END AS recorded_inputs_current,
        latest.outcome AS latest_outcome, latest.recorded_at AS latest_recorded_at,
        failure.recorded_at AS last_failure_at, failure.outcome AS last_failure_outcome,
        CASE WHEN failure.error_summary = 'publication_failed'
             THEN 'publication_failed'::text ELSE NULL::text END AS last_failure_category
    FROM assets a
    LEFT JOIN meta.asset_freshness_policies policy USING (asset_key, coverage_key)
    LEFT JOIN meta.asset_current_generations pointer USING (asset_key, coverage_key)
    LEFT JOIN meta.asset_receipts r
        ON r.asset_key = pointer.asset_key AND r.coverage_key = pointer.coverage_key
        AND r.generation_id = pointer.generation_id
        AND r.outcome IN ('succeeded', 'expected_no_data')
        AND r.coverage->'complete' = 'true'::jsonb AND r.published_at IS NOT NULL
    LEFT JOIN LATERAL (
        SELECT receipt.generation_id, receipt.outcome, receipt.recorded_at
        FROM meta.asset_receipts receipt
        WHERE receipt.asset_key = a.asset_key AND receipt.coverage_key = a.coverage_key
        ORDER BY receipt.recorded_at DESC, receipt.generation_id DESC LIMIT 1
    ) latest ON true
    LEFT JOIN LATERAL (
        SELECT receipt.recorded_at, receipt.outcome, receipt.error_summary
        FROM meta.asset_receipts receipt
        WHERE receipt.asset_key = a.asset_key AND receipt.coverage_key = a.coverage_key
            AND receipt.outcome IN ('failed', 'partial', 'deferred', 'blocked')
        ORDER BY receipt.recorded_at DESC, receipt.generation_id DESC LIMIT 1
    ) failure ON true
    LEFT JOIN LATERAL (
        SELECT count(*) AS input_count,
            bool_and(edge.input_asset_key = 'analytics.house_elo_game'
                AND edge.input_coverage_key = 'source-wide'
                AND current_input.generation_id IS NOT NULL
                AND current_input.generation_id = edge.input_generation_id) AS expected_input_current,
            pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                'asset_key', edge.input_asset_key, 'coverage_key', edge.input_coverage_key,
                'generation_id', edge.input_generation_id,
                'current_generation_id', current_input.generation_id,
                'is_current', current_input.generation_id IS NOT NULL
                    AND current_input.generation_id = edge.input_generation_id
            ) ORDER BY edge.input_asset_key, edge.input_coverage_key) AS generations
        FROM meta.asset_receipt_inputs edge
        LEFT JOIN meta.asset_current_generations current_input
            ON current_input.asset_key = edge.input_asset_key
            AND current_input.coverage_key = edge.input_coverage_key
        WHERE edge.generation_id = r.generation_id
    ) inputs ON true
)
SELECT asset_key, coverage_key, generation_id, published_at, age_seconds,
    expected_refresh_interval, is_stale, publication_state, current_outcome,
    coverage, source_watermark, row_delta, required_input_generations,
    recorded_inputs_current,
    CASE WHEN recorded_inputs_current = false THEN false ELSE NULL::boolean END AS input_closure_current,
    '["core.games"]'::jsonb AS unversioned_input_assets,
    latest_outcome, latest_recorded_at, last_failure_at, last_failure_outcome, last_failure_category
FROM evidence;

CREATE OR REPLACE FUNCTION public.get_asset_freshness()
RETURNS SETOF marts.asset_receipt_freshness
LANGUAGE sql STABLE SECURITY INVOKER SET search_path = ''
AS $function$
    SELECT * FROM marts.asset_receipt_freshness ORDER BY asset_key, coverage_key;
$function$;

COMMENT ON TABLE meta.asset_freshness_policies IS
'Private owner-declared publication cadence. NULL is undeclared; intervals measure publication age, not provider freshness.';
COMMENT ON VIEW marts.asset_receipt_freshness IS
'Query-time current publication evidence for the two controlled Elo assets. core.games remains unversioned; source_watermark is an opaque input digest, not a provider timestamp.';
COMMENT ON FUNCTION public.get_asset_freshness() IS
'Additive safe receipt evidence; ages advance each SQL statement. Does not replace legacy get_data_freshness or prove complete upstream freshness.';

-- Normalize only these new objects, including hostile inherited default grants.
-- No broad schema revokes or grants may alter existing public consumers.
DO $access$
DECLARE obj record; grantee record; owner_id oid;
BEGIN
    SELECT oid INTO owner_id FROM pg_catalog.pg_roles WHERE rolname = current_user;
    FOR obj IN SELECT c.oid, n.nspname, c.relname, c.relowner, c.relacl
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE (n.nspname, c.relname) IN (
            ('meta', 'asset_freshness_policies'), ('marts', 'asset_receipt_freshness')
        ) LOOP
        IF obj.relowner <> owner_id THEN
            RAISE EXCEPTION 'freshness object %.% must be migration-owned', obj.nspname, obj.relname;
        END IF;
        EXECUTE pg_catalog.format('REVOKE ALL ON TABLE %I.%I FROM PUBLIC', obj.nspname, obj.relname);
        FOR grantee IN SELECT DISTINCT role.rolname FROM pg_catalog.aclexplode(obj.relacl) acl
            JOIN pg_catalog.pg_roles role ON role.oid = acl.grantee WHERE acl.grantee <> owner_id
        LOOP
            EXECUTE pg_catalog.format('REVOKE ALL ON TABLE %I.%I FROM %I',
                obj.nspname, obj.relname, grantee.rolname);
        END LOOP;
    END LOOP;
    SELECT p.proowner, p.proacl INTO obj FROM pg_catalog.pg_proc p
        WHERE p.oid = 'public.get_asset_freshness()'::regprocedure;
    IF obj.proowner <> owner_id THEN
        RAISE EXCEPTION 'freshness RPC must be migration-owned';
    END IF;
    REVOKE ALL ON FUNCTION public.get_asset_freshness() FROM PUBLIC;
    FOR grantee IN SELECT DISTINCT role.rolname FROM pg_catalog.aclexplode(obj.proacl) acl
        JOIN pg_catalog.pg_roles role ON role.oid = acl.grantee WHERE acl.grantee <> owner_id
    LOOP
        EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION public.get_asset_freshness() FROM %I', grantee.rolname);
    END LOOP;
END
$access$;

GRANT USAGE ON SCHEMA public, marts TO anon, authenticated;
GRANT SELECT ON marts.asset_receipt_freshness TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_asset_freshness() TO anon, authenticated;
