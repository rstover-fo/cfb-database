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
-- left/middle/right/unknown -- 'unknown' is CFBD's own charted bucket for a
-- carry whose direction could not be determined, never derived by
-- subtraction, and is always present as its own row.
--
-- FIXED FOUR-ROW GUARANTEE: every (entity, side) has exactly 4 rows -- one
-- per direction -- even when every metric on a row is NULL (not charted).
-- A player-season row always melts to 4 rows (side = ''offense'' only); a
-- team-season row always melts to 8 rows (4 offense + 4 defense).
--
-- NULL on a metric means the carries behind it were not charted; 0 is a
-- real observed value. direction_eligible_attempts and
-- direction_available_attempts (available <= eligible) ride on every row
-- of an (entity, side) block -- constant across its 4 direction rows -- as
-- the direction-coverage denominators; they are DIFFERENT counts from each
-- other and from the yardage-tier/touchdown denominators on
-- api.rushing_charting_player_season/api.rushing_charting_team_season, and
-- must never be merged or aliased together.
--
-- SHARES ARE THE CONSUMER''S TO COMPUTE: this view does not pre-compute a
-- direction-share column. A direction''s share of a side''s charted carries
-- is that row''s carries / direction_available_attempts (or
-- / direction_eligible_attempts for the eligible-population version) --
-- both denominators are already on the row.
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

COMMENT ON VIEW api.rushing_charting_direction_season IS 'Rushing charting direction splits (2025+): season, entity_type (player|team), entity_id (text), team, team_id (numeric, nullable on ambiguous name mapping), side (offense|defense; players are offense only), direction (left|middle|right|unknown), carries, yards, yards_per_carry, success_rate, ppa, total_ppa, line_yards, line_yards_total, second_level_yards, second_level_yards_total, open_field_yards, open_field_yards_total, stuff_rate, power_success, explosiveness, direction_eligible_attempts, direction_available_attempts. FIXED FOUR-ROW GUARANTEE: every (entity, side) has exactly 4 rows, one per direction, even when every metric is NULL -- a player-season row always melts to 4 offense rows, a team-season row always melts to 8 rows (4 offense + 4 defense). unknown is CFBD''s own charted bucket, never derived by subtraction. NULL on a metric means those carries were not charted, 0 is a real observed value; direction_eligible_attempts/direction_available_attempts (available <= eligible) are the direction-coverage denominators, constant across an (entity, side) block''s 4 rows, and are never interchangeable with the yardage-tier/touchdown denominators on the player-season/team-season views. Shares are the consumer''s to compute: carries / direction_available_attempts (or / direction_eligible_attempts). CONTRACT (R10): player-row carries never reconcile to team-row carries for the same (season, team, offense) -- see api.rushing_charting_player_season/api.rushing_charting_team_season. Backed by marts.rushing_charting_direction_season.';

GRANT SELECT ON api.rushing_charting_direction_season TO anon, authenticated;
