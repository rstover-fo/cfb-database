-- api.rushing_charting_team_season
-- Team-season rushing charting, offense and defense sides (2025+): direction-
-- eligible carries, yardage tiers, success rate, PPA, stuff rate, power
-- success, explosiveness, rushing touchdowns. Thin passthrough of
-- marts.rushing_charting_team_season, which flattens the raw dlt
-- offense__*/defense__* dunder shape to offense_*/defense_* -- per this
-- repo's Contract Rule 4, raw dlt column shapes must never reach the api
-- layer. Direction splits are NOT on this view -- see
-- api.rushing_charting_direction_season for the per-direction breakdown of
-- these same team-seasons (4 offense rows + 4 defense rows per team-season).
--
-- CRITICAL: defense_* is THIS TEAM'S RUN DEFENSE -- what opposing offenses
-- did against them on the ground -- NOT the opponent's own offensive row.
-- NULL on a metric means the carries behind it were not charted; 0 is a
-- real observed value. offense_rushing_yards_available/
-- defense_rushing_yards_available (yardage tiers), offense_direction_
-- eligible_attempts/offense_direction_available_attempts (and the defense_
-- equivalents, direction splits), and offense_touchdown_status_available/
-- defense_touchdown_status_available (rushing touchdowns) are the coverage
-- denominators for their respective metric families per side and must
-- never be merged or aliased together. Data starts 2025.
--
-- CONTRACT (R10, non-reconciliation): offense_attempts will NOT equal the
-- sum of `attempts` across every player on that team in
-- api.rushing_charting_player_season -- see that view's comment for why
-- (team-only/multi-carrier/unattributed attempts never attach to a player).
--
-- team_id is numeric (joins ref.teams(id)), derived from the
-- (season, offense-name -> offense_id) mapping in stats.rushing_plays; NULL
-- when that mapping is ambiguous (never-guess).
--
-- PostgREST usage:
--   GET /api/rushing_charting_team_season?season=eq.2025&order=offense_total_rushing_yards.desc

DROP VIEW IF EXISTS api.rushing_charting_team_season;

CREATE VIEW api.rushing_charting_team_season AS
SELECT *
FROM marts.rushing_charting_team_season;

COMMENT ON VIEW api.rushing_charting_team_season IS 'Team-season rushing charting, offense and defense sides (2025+): season, team_id, team, conference, offense_attempts, offense_rushing_yards_available, offense_individual_attempts, offense_unattributed_attempts, offense_sacks, offense_kneels, offense_team_rushes, offense_multi_carrier_attempts, offense_direction_eligible_attempts, offense_direction_available_attempts, offense_total_rushing_yards, offense_yards_per_carry, offense_success_rate, offense_ppa, offense_total_ppa, offense_line_yards, offense_line_yards_total, offense_second_level_yards, offense_second_level_yards_total, offense_open_field_yards, offense_open_field_yards_total, offense_stuff_rate, offense_power_success, offense_explosiveness, offense_touchdown_status_available, offense_rushing_touchdowns, and the defense_ equivalents. defense_* is this team''s run DEFENSE (what opposing offenses did against them), not the opponent''s offensive row. NULL on a metric means those carries were not charted, 0 is a real observed value; the *_rushing_yards_available/*_direction_eligible_attempts/*_direction_available_attempts/*_touchdown_status_available columns per side are the charting-coverage denominators for their respective metric families. CONTRACT (R10): team totals never reconcile to the sum of api.rushing_charting_player_season rows for that team -- CFBD attributes some carries to team-only/multi-carrier/unresolved buckets that never attach to a player. Per-direction splits for these same team-seasons live in api.rushing_charting_direction_season (4 offense + 4 defense rows per team-season). team_id is numeric (joins ref.teams(id)), derived from the (season, offense-name -> offense_id) mapping in stats.rushing_plays; NULL when that mapping is ambiguous (never-guess). Backed by marts.rushing_charting_team_season.';

GRANT SELECT ON api.rushing_charting_team_season TO anon, authenticated;
