-- api.rushing_charting_player_season
-- Rushing charting by player-season (2025+): direction-eligible carries,
-- yardage tiers (line/second-level/open-field), success rate, PPA, stuff
-- rate, power success, explosiveness. Thin passthrough of
-- marts.rushing_charting_player_season. Direction splits are NOT on this
-- view -- see api.rushing_charting_direction_season for the per-direction
-- (left/middle/right/unknown) breakdown of the same player-seasons.
--
-- NULL on a rate/yardage-tier metric means the carries behind it were not
-- charted; 0 is a real observed value. rushing_yards_available is the
-- coverage denominator for the yardage-tier metrics; direction_eligible_
-- attempts and direction_available_attempts are the coverage denominators
-- for direction splits -- all three are DIFFERENT counts and must never be
-- merged or aliased into one column. Data starts 2025.
--
-- CONTRACT (R10, non-reconciliation): this player-season's `attempts` will
-- NOT sum, across all players on a team, to that team's offense_attempts in
-- api.rushing_charting_team_season -- CFBD attributes some carries to
-- team-only/multi-carrier/unresolved buckets that never attach to an
-- individual player. Do not build a team-share metric from these two views
-- without accounting for that gap.
--
-- position is joined from core.roster on its own primary key (id, team,
-- year) -- never a name-string join -- so it can be legitimately NULL for a
-- player-season with no matching roster row; conference comes straight off
-- stats.rushing_player_season, no join.
--
-- PostgREST usage:
--   GET /api/rushing_charting_player_season?season=eq.2025&order=total_rushing_yards.desc

DROP VIEW IF EXISTS api.rushing_charting_player_season;

CREATE VIEW api.rushing_charting_player_season AS
SELECT *
FROM marts.rushing_charting_player_season;

COMMENT ON VIEW api.rushing_charting_player_season IS 'Rushing charting by player-season (2025+): season, player_id, player, team, conference, position, attempts, rushing_yards_available, individual_attempts, unattributed_attempts, sacks, kneels, team_rushes, multi_carrier_attempts, direction_eligible_attempts, direction_available_attempts, total_rushing_yards, yards_per_carry, success_rate, ppa, total_ppa, line_yards, line_yards_total, second_level_yards, second_level_yards_total, open_field_yards, open_field_yards_total, stuff_rate, power_success, explosiveness. NULL on a rate/yardage-tier metric means those carries were not charted (0 is a real value); rushing_yards_available (yardage tiers) and direction_eligible_attempts/direction_available_attempts (direction splits) are the coverage denominators and are never interchangeable. CONTRACT (R10): player totals never reconcile to team totals in api.rushing_charting_team_season -- CFBD attributes some carries to team-only/multi-carrier/unresolved buckets that never attach to a player. Per-direction splits for these same player-seasons live in api.rushing_charting_direction_season. Backed by marts.rushing_charting_player_season.';

GRANT SELECT ON api.rushing_charting_player_season TO anon, authenticated;
