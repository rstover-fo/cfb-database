-- api.rushing_charting_direction_season
-- Rushing charting direction splits (2025+), one row per
-- (season, entity_type, entity_id, team, side, direction). Thin passthrough
-- of marts.rushing_charting_direction_season -- the tall companion to
-- api.rushing_charting_player_season and api.rushing_charting_team_season
-- (product decision: direction splits as tall rows in one view, not wide
-- columns on the headline views).
--
-- entity_type is 'player' or 'team'; side is 'offense' or 'defense'
-- (players are offense only). direction is exactly one of
-- left/middle/right/unknown -- 'unknown' is the UNRESOLVED remainder
-- (direction_eligible_attempts - direction_available_attempts), read
-- directly off the source rather than subtracted here, but not itself a
-- charted direction; it is always present as its own row so direction
-- coverage stays visible instead of silently dropped.
--
-- FIXED FOUR-ROW GUARANTEE: every (entity, side) has exactly 4 rows -- one
-- per direction -- even when every metric on a row is NULL (not charted).
-- A player-season row always melts to 4 rows (side = ''offense'' only); a
-- team-season row always melts to 8 rows (4 offense + 4 defense).
--
-- NULL on a metric means the carries behind it were not charted; 0 is a
-- real observed value. direction_eligible_attempts and
-- direction_available_attempts ride on every row of an (entity, side)
-- block -- constant across its 4 direction rows -- as the direction-
-- coverage denominators; available <= eligible because eligible -
-- available IS the unknown row's carries -- the unresolved remainder, not
-- a fourth charted direction. They are DIFFERENT counts from each other
-- and from the yardage-tier/touchdown denominators on
-- api.rushing_charting_player_season/api.rushing_charting_team_season, and
-- must never be merged or aliased together.
--
-- SHARES ARE THE CONSUMER''S TO COMPUTE: this view does not pre-compute a
-- direction-share column. A direction''s share of a side''s RESOLVED
-- carries (left/middle/right only, sums to 1) is that row''s
-- carries::numeric / NULLIF(direction_available_attempts, 0). A
-- direction''s share of a side''s ELIGIBLE carries, including the
-- unresolved remainder (all four rows, sums to 1), is
-- carries::numeric / NULLIF(direction_eligible_attempts, 0). Never divide
-- unknown by available -- that yields >100%; unknown / eligible is the
-- direction-coverage GAP, not a share. Both denominators are already on
-- the row. The fixed four-row melt guarantees rows where a denominator is
-- 0 (no charted direction coverage for that entity/side); NULLIF turns
-- those into NULL instead of a division-by-zero error, and NULL is the
-- correct answer -- there is no share to report.
--
-- CONTRACT (R10, non-reconciliation): summing a player entity''s carries
-- across its 4 offense rows for a team will NOT equal that team entity''s
-- carries summed across its 4 offense rows for the same (season, team) --
-- same upstream attribution gap documented on
-- api.rushing_charting_player_season/api.rushing_charting_team_season.
--
-- team_id is numeric (joins ref.teams(id)), derived from the
-- (season, offense-name -> offense_id) mapping in stats.rushing_plays;
-- applied to both player and team rows (a player row''s team_id is that of
-- the team it carried for); NULL when that mapping is ambiguous
-- (never-guess). entity_id is text on every row: player rows carry the
-- CFBD athlete id string as-is (never NULL); team rows carry
-- COALESCE(team_id::text, team) so entity_id is never NULL even when
-- team_id itself is.
--
-- PostgREST usage:
--   GET /api/rushing_charting_direction_season?season=eq.2025&entity_type=eq.team&team=eq.Michigan&side=eq.offense

DROP VIEW IF EXISTS api.rushing_charting_direction_season;

CREATE VIEW api.rushing_charting_direction_season AS
SELECT *
FROM marts.rushing_charting_direction_season;

COMMENT ON VIEW api.rushing_charting_direction_season IS 'Rushing charting direction splits (2025+): season, entity_type (player|team), entity_id (text), team, team_id (numeric, nullable on ambiguous name mapping), side (offense|defense; players are offense only), direction (left|middle|right|unknown), carries, yards, yards_per_carry, success_rate, ppa, total_ppa, line_yards, line_yards_total, second_level_yards, second_level_yards_total, open_field_yards, open_field_yards_total, stuff_rate, power_success, explosiveness, direction_eligible_attempts, direction_available_attempts. FIXED FOUR-ROW GUARANTEE: every (entity, side) has exactly 4 rows, one per direction, even when every metric is NULL -- a player-season row always melts to 4 offense rows, a team-season row always melts to 8 rows (4 offense + 4 defense). unknown is the UNRESOLVED remainder (direction_eligible_attempts - direction_available_attempts), read directly off the source rather than subtracted here, not a fourth charted direction. NULL on a metric means those carries were not charted, 0 is a real observed value; direction_eligible_attempts/direction_available_attempts are the direction-coverage denominators, constant across an (entity, side) block''s 4 rows, and are never interchangeable with the yardage-tier/touchdown denominators on the player-season/team-season views (available <= eligible because eligible - available IS unknown''s carries). Shares are the consumer''s to compute: a direction''s share of RESOLVED carries (left/middle/right only, sums to 1) is carries::numeric / NULLIF(direction_available_attempts, 0); a direction''s share of ELIGIBLE carries including the unresolved remainder (all four rows, sums to 1) is carries::numeric / NULLIF(direction_eligible_attempts, 0); never divide unknown by available (yields >100%) -- unknown / eligible is the coverage gap, not a share. NULL means no charted direction coverage for that entity/side, not an error. CONTRACT (R10): player-row carries never reconcile to team-row carries for the same (season, team, offense) -- see api.rushing_charting_player_season/api.rushing_charting_team_season. Backed by marts.rushing_charting_direction_season.';

GRANT SELECT ON api.rushing_charting_direction_season TO anon, authenticated;
