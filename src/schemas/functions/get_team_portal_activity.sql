-- Get transfer portal activity for a team and season
-- Returns summary stats + detailed transfer in/out lists as JSONB

CREATE OR REPLACE FUNCTION public.get_team_portal_activity(p_team text, p_season integer)
RETURNS jsonb
LANGUAGE sql
STABLE
SET search_path = ''
AS $function$
  SELECT jsonb_build_object(
    'summary', (
      SELECT to_jsonb(sub) FROM (
        SELECT
          t.team::text,
          t.season::int,
          t.transfers_in,
          t.transfers_out,
          t.net_transfers,
          t.avg_incoming_stars,
          t.avg_incoming_rating,
          t.incoming_high_stars,
          t.win_delta,
          t.portal_dependency,
          t.net_transfers_pctl,
          t.win_delta_pctl,
          t.portal_dependency_pctl
        FROM api.transfer_portal_impact t
        WHERE t.team = p_team AND t.season = p_season
      ) sub
    ),
    'transfers_in', COALESCE((
      SELECT jsonb_agg(to_jsonb(sub) ORDER BY sub.transfer_date DESC)
      FROM (
        SELECT
          tp.season::int,
          tp.first_name::text,
          tp.last_name::text,
          tp.position::text,
          tp.origin::text,
          tp.destination::text,
          tp.transfer_date::text,
          tp.stars::int,
          tp.rating::numeric,
          tp.eligibility::text
        FROM recruiting.transfer_portal tp
        WHERE tp.destination = p_team AND tp.season = p_season
      ) sub
    ), '[]'::jsonb),
    'transfers_out', COALESCE((
      SELECT jsonb_agg(to_jsonb(sub) ORDER BY sub.transfer_date DESC)
      FROM (
        SELECT
          tp.season::int,
          tp.first_name::text,
          tp.last_name::text,
          tp.position::text,
          tp.origin::text,
          tp.destination::text,
          tp.transfer_date::text,
          tp.stars::int,
          tp.rating::numeric,
          tp.eligibility::text
        FROM recruiting.transfer_portal tp
        WHERE tp.origin = p_team AND tp.season = p_season
      ) sub
    ), '[]'::jsonb)
  );
$function$;
