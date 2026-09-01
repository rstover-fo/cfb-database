-- Migration: 056_fanout_miss_ledger
--
-- meta.fanout_misses: shared ledger of fan-out misses -- terminal (400/404)
-- responses, plus 5xx failures that survived api_client's full retry budget
-- (recorded by player_overview.py so one flaky gateway response cannot kill a
-- multi-thousand-call dispatch) -- from the per-entity fan-out drainers in
-- coaches.py (coach_profiles_resource) and player_overview.py
-- (player_season_overview_resource) -- PR #75 review finding A (P1 x2, unit
-- R2, 2026-08-30). The shared 30-day window means a 5xx skip is deferred,
-- never blacklisted: it ages back into eligibility like a late-published id.
--
-- Without this, run.py's set-difference drainers (run_coach_profiles_pipeline
-- ~line 661, run_player_overview_pipeline ~line 1193) re-select the SAME ids
-- every run forever: a 400/404 means CFBD has no data for that id, but
-- neither drainer persisted that fact anywhere, so the id never left the
-- "missing" set computed from ref.coach_profiles / stats.player_season_overview
-- alone. That burns one wasted API call per stale miss per run, indefinitely,
-- and eventually wedges the 200/250-cap slices -- the same handful of
-- terminal misses keep re-occupying batch slots, deferring real work behind
-- them run after run.
--
-- 30-day re-eligibility (run.py::FANOUT_MISS_RETRY_DAYS, enforced in run.py,
-- not here) lets late-published data self-heal: a coach profile or player
-- overview CFBD has not published YET looks identical, from this table's
-- point of view, to one it will never publish. Misses are excluded from a
-- drainer's candidate set only while last_attempt_at is within the retry
-- window, then age back into eligibility.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-055. Idempotent (CREATE TABLE/SCHEMA IF NOT EXISTS;
-- GRANT/REVOKE are naturally re-appliable).
--
-- No secondary index: both call sites (run.py::_fetch_recent_fanout_misses)
-- read with `WHERE source = %s AND last_attempt_at > now() - make_interval(
-- days => %s)` -- a lookup on the primary key's leading column (source) plus
-- a range filter on a column the PK doesn't cover. The table is a bounded
-- backlog of terminal misses across two sources, not an ever-growing log, so
-- a plan that narrows to one source's rows via the PK and filters
-- last_attempt_at in-memory is fine without a dedicated composite index.
--
-- meta is not a SCHEMA_CONTRACT surface (docs/SCHEMA_CONTRACT.md) -- no
-- contract edit needed for this table.
--
-- Apply via:
--   python scripts/run_migrations.py --file src/schemas/migrations/056_fanout_miss_ledger.sql

CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.fanout_misses (
    source text NOT NULL,
    key text NOT NULL,
    status_code int NOT NULL,
    attempts int NOT NULL DEFAULT 1,
    last_attempt_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, key)
);

COMMENT ON TABLE meta.fanout_misses IS
    'Misses from per-entity fan-out drainers: terminal (400/404) responses, '
    'plus 5xx failures that survived api_client''s full retry budget '
    '(player_overview.py records these so one flaky gateway response cannot '
    'kill a multi-thousand-call dispatch). Keyed by (source, key) so a '
    'set-difference drainer (src/pipelines/run.py) can exclude a '
    'recently-attempted miss instead of re-spending its API call every run. '
    'source: ''coach_profiles'' | ''player_season_overview''. '
    'key: str(coach_id) for coach_profiles, ''{season}:{player_id}'' for '
    'player_season_overview. 30-day re-eligibility is enforced in run.py '
    '(FANOUT_MISS_RETRY_DAYS), not here, so late-published CFBD data -- and '
    'a transient 5xx skip -- self-heals instead of being excluded forever. '
    'PR #75 review finding A.';

COMMENT ON COLUMN meta.fanout_misses.attempts IS
    'Incremented (not overwritten) on every re-encountered miss via the '
    'ON CONFLICT (source, key) DO UPDATE upsert in '
    'run.py::_record_fanout_misses -- a rising count is the operator signal '
    'that a given id keeps 400/404ing rather than having been hit once.';

GRANT USAGE ON SCHEMA meta TO anon, authenticated;
GRANT SELECT ON meta.fanout_misses TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON meta.fanout_misses FROM anon, authenticated;
