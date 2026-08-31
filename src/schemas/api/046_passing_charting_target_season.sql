-- api.passing_charting_target_season
-- Receiver-grain (target) passing charting by season (2025+): the highest-
-- value unit in the 2026-08-30 expansion -- cfb-app has no receiver-grain
-- analysis today (api.player_wepa_leaders covers passing/rushing/kicking,
-- no receiving). Thin passthrough of marts.passing_charting_target_season.
--
-- target_share_charted is a share of CHARTED targets only (this target's
-- targets_charted / the team-season's total targets_charted) -- it is NOT a
-- true target share over every offensive snap and must never be presented
-- as one. partial_share is the fraction of this target's plays with
-- parse_status = 'partial' (charting fields on those plays may read NULL).
-- air_yards_charted_plays / yards_after_catch_charted_plays are the
-- charting-coverage denominators for the air-yards and YAC metric pairs
-- respectively -- NULL on a metric means those plays were not charted, 0 is
-- a real observed value. Data starts 2025.
--
-- PostgREST usage:
--   GET /api/passing_charting_target_season?season=eq.2025&order=receptions.desc

DROP VIEW IF EXISTS api.passing_charting_target_season;

CREATE VIEW api.passing_charting_target_season AS
SELECT *
FROM marts.passing_charting_target_season;

COMMENT ON VIEW api.passing_charting_target_season IS 'Receiver-grain (target) passing charting by season (2025+): target_id, target, season, team_id, team, targets_charted, receptions, total_air_yards, average_depth_of_target, air_yards_charted_plays, total_yards_after_catch, average_yards_after_catch, yards_after_catch_charted_plays, target_share_charted, partial_share. target_share_charted is a share of CHARTED targets only -- never present as a true target share. partial_share flags plays whose charting is incomplete (parse_status=''partial''); charting metrics read NULL when not charted, 0 is a real value. CONTRACT: yards_after_catch_charted_plays <= receptions -- YAC exists only on completions (play-level verified 2026-08-31: 0 violations in 14,832 YAC-charted plays), so the YAC coverage denominator is receptions, never targets_charted; receptions already counts completions within the charted-play universe and doubles as that denominator. The air-yards denominator remains targets_charted-scoped (air_yards_charted_plays). NOT A CONTRACT: air_yards_charted_plays = targets_charted * (1 - partial_share) happens to hold exactly today (air_yards is currently perfectly aligned with parse_status) but is incidental -- sibling charting fields already decouple from parse_status in both directions (86 partial plays carry charted fields; 664 complete plays miss some; live 2026-08-31), so derive air-yards coverage from air_yards_charted_plays / targets_charted, never from partial_share. Backed by marts.passing_charting_target_season.';

GRANT SELECT ON api.passing_charting_target_season TO anon, authenticated;
