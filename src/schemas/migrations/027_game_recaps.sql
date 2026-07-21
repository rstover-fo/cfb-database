-- Migration: 027_game_recaps
--
-- analytics.game_recaps: nightly LLM-generated game recaps (P3.3 Lane D).
-- One row per game_id, written by scripts/generate_recaps.py, which selects
-- completed FBS games (season >= 2014) that have no recap yet (or whose
-- `regenerate` flag was set) and asks Claude to write a short recap from
-- warehouse facts only (final/quarter scores, top-EPA plays, win-probability
-- swings when available, box-score leaders, betting line result). The LLM
-- never sees anything the warehouse doesn't already know -- see
-- scripts/generate_recaps.py's module docstring for the prompt-injection
-- mitigation (play_text is CFBD free text and is treated as untrusted data,
-- never as instructions).
--
-- `regenerate` is an operator-settable flag (default false): flip a row to
-- true via direct SQL to force scripts/generate_recaps.py to rewrite that
-- game's recap on its next run; the writer resets it to false once the
-- rewrite completes. `input_hash` is md5() of the canonical facts JSON used
-- to build the prompt, so a future run can detect whether the underlying
-- facts actually changed (e.g. a late stat correction) versus a no-op rerun.
-- `wp_available` records whether metrics.win_probability had rows for this
-- game at generation time (the pipeline's per-game win-probability backfill,
-- P3.2, is not guaranteed complete for every historical game) -- when false,
-- the recap's momentum/swing framing came from the EPA-only fallback in
-- scripts/generate_recaps.py, not from real win-probability data.
--
-- Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/027_game_recaps.sql

CREATE TABLE IF NOT EXISTS analytics.game_recaps (
    game_id         BIGINT PRIMARY KEY,
    season          INT NOT NULL,
    week            INT,
    headline        TEXT,
    recap           TEXT,
    wp_available    BOOLEAN NOT NULL DEFAULT false,
    model           TEXT,
    prompt_version  INT,
    input_hash      TEXT,
    input_tokens    INT,
    output_tokens   INT,
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    regenerate      BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS game_recaps_season_week_idx
    ON analytics.game_recaps (season, week);

CREATE INDEX IF NOT EXISTS game_recaps_regenerate_idx
    ON analytics.game_recaps (regenerate) WHERE regenerate;

-- analytics already has USAGE granted to anon/authenticated
-- (grant_read_access_for_security_invoker.sql), but that grant only covered
-- tables that existed at the time -- new tables need their own SELECT grant.
GRANT SELECT ON analytics.game_recaps TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON analytics.game_recaps FROM anon, authenticated;

COMMENT ON TABLE analytics.game_recaps IS
    'Nightly LLM-generated game recaps, one row per game_id. Written by scripts/generate_recaps.py from warehouse facts only (no external context). Prefer api.game_recaps for the downstream-facing contract surface.';
COMMENT ON COLUMN analytics.game_recaps.game_id IS 'core.games.id this recap covers.';
COMMENT ON COLUMN analytics.game_recaps.season IS 'Season year, copied from core.games at generation time.';
COMMENT ON COLUMN analytics.game_recaps.week IS 'Week number, copied from core.games at generation time.';
COMMENT ON COLUMN analytics.game_recaps.headline IS 'Single-line LLM-generated headline.';
COMMENT ON COLUMN analytics.game_recaps.recap IS '150-220 word LLM-generated recap. NULL until the first successful generation.';
COMMENT ON COLUMN analytics.game_recaps.wp_available IS 'Whether metrics.win_probability had rows for this game at generation time; false means the recap''s swing framing came from the EPA-only fallback, not real win-probability data.';
COMMENT ON COLUMN analytics.game_recaps.model IS 'Anthropic model ID used to generate this recap (see scripts/generate_recaps.py::MODEL_ID).';
COMMENT ON COLUMN analytics.game_recaps.prompt_version IS 'scripts/generate_recaps.py::PROMPT_VERSION at generation time, for auditing prompt changes across the table''s history.';
COMMENT ON COLUMN analytics.game_recaps.input_hash IS 'md5() of the canonical (sorted-key) JSON facts block used to build the prompt -- lets a future run detect whether the underlying facts changed since the last generation.';
COMMENT ON COLUMN analytics.game_recaps.input_tokens IS 'Prompt tokens billed for the generating request (response.usage.input_tokens).';
COMMENT ON COLUMN analytics.game_recaps.output_tokens IS 'Completion tokens billed for the generating request (response.usage.output_tokens).';
COMMENT ON COLUMN analytics.game_recaps.generated_at IS 'When this row was last (re)generated.';
COMMENT ON COLUMN analytics.game_recaps.regenerate IS 'Operator-settable flag: set true to force scripts/generate_recaps.py to rewrite this game''s recap on its next run. Reset to false by the writer once the rewrite completes.';
