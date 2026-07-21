-- api.game_recaps
-- Nightly LLM-generated game recap for a single game. Backed by
-- analytics.game_recaps (src/schemas/migrations/027_game_recaps.sql),
-- written by scripts/generate_recaps.py from warehouse facts only.
--
-- Content is LLM-generated, not CFBD-sourced: headline/recap are prose
-- written by scripts/generate_recaps.py's Anthropic call, constrained to the
-- facts already in this warehouse (scores, EPA plays, win-probability
-- swings, box-score leaders, betting line result) -- never invented context.
-- Regenerated only when analytics.game_recaps.regenerate is flipped true by
-- an operator; otherwise a game's recap is generated once and left as-is.
-- Rows only exist for completed FBS games (season >= 2014) that have been
-- processed by the nightly job -- a missing row means "not yet generated",
-- not "no recap available."
--
-- PostgREST usage:
--   GET /api/game_recaps?game_id=eq.401628455

CREATE OR REPLACE VIEW api.game_recaps AS
SELECT
    r.game_id,
    r.season,
    r.week,
    r.headline,
    r.recap,
    r.wp_available,
    r.model,
    r.generated_at
FROM analytics.game_recaps r
WHERE r.recap IS NOT NULL;

GRANT SELECT ON api.game_recaps TO anon, authenticated;

COMMENT ON VIEW api.game_recaps IS 'Nightly LLM-generated game recap. Content is LLM-generated from warehouse facts (not raw CFBD data) -- see analytics.game_recaps for provenance columns (model, prompt_version, input_hash). Columns: game_id, season, week, headline, recap, wp_available, model, generated_at. Only rows with a non-null recap are exposed (a missing game_id means not yet generated).';
