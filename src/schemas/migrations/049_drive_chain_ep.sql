-- Drive-state Markov chain: transitions + expected-points states
-- =============================================================================
-- Design: docs/plans/2026-08-08-drive-chain-ep-model-plan.md (P1).
--
-- Two league-grain tables written by scripts/compute_drive_chain.py:
--
--   analytics.drive_chain_transitions -- observed scrimmage-snapshot
--     transitions per era: (era, from_state, to_state, n, p_raw, p_shrunk).
--     to_state is either a transient state key or an absorbing outcome
--     (TD, FG, MISSED_FG, PUNT, TURNOVER, TURNOVER_TD, DOWNS, SAFETY,
--     END_OF_HALF). State key format: "d{down}|{dist_bucket}|z{zone}".
--
--   analytics.ep_states -- the solved chain per era: drive expected points
--     (ep_drive; Goldner basis: absorption probabilities x scoring values),
--     the absorption probabilities themselves, bootstrap standard error
--     (cluster-resampled by game; Brill/Yurko/Wyner "intervals, not
--     verdicts"), and ep_net reserved NULL until P2's net next-score basis
--     lands. NULL, never 0: an unsolved or unpublished value must not read
--     as "zero points".
--
-- Era grain, not season: transition probabilities are estimated per rules/
-- scoring era (2004-2013, 2014-2020, 2021+; see the design doc's era
-- section). EPA computations must join a play to its own era's curve.
--
-- analytics.* is contract-internal (docs/SCHEMA_CONTRACT.md) -- downstream
-- consumers read marts, never these tables. No anon/authenticated grants.
--
-- Idempotent DELETE-per-era + INSERT by the compute script, same tier
-- convention as analytics.adjusted_epa_week_build (027) / house_elo (025).
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-028 and 041+. Idempotent (IF NOT EXISTS throughout).

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.drive_chain_transitions (
    era text NOT NULL,
    from_state text NOT NULL,
    to_state text NOT NULL,
    n bigint NOT NULL,
    p_raw double precision NOT NULL,
    p_shrunk double precision NOT NULL,
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (era, from_state, to_state)
);

COMMENT ON TABLE analytics.drive_chain_transitions IS
    'League drive-state transition matrix per era. Written by scripts/compute_drive_chain.py; design docs/plans/2026-08-08-drive-chain-ep-model-plan.md. p_shrunk = empirical-Bayes shrink toward the parent state (distance dimension dropped, then zone) so starved cells inherit their parent distribution.';

CREATE TABLE IF NOT EXISTS analytics.ep_states (
    era text NOT NULL,
    state text NOT NULL,
    n_obs bigint NOT NULL,
    ep_drive double precision NOT NULL,
    ep_net double precision,
    p_td double precision NOT NULL,
    p_fg double precision NOT NULL,
    p_punt double precision NOT NULL,
    p_turnover double precision NOT NULL,
    se_boot double precision,
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (era, state)
);

COMMENT ON TABLE analytics.ep_states IS
    'Solved drive chain per era: Goldner-basis drive EP and absorption probabilities per state, with game-cluster bootstrap SE. ep_net is NULL until P2 (net next-score basis; the CFBD-PPA-comparable one) -- NULL, never 0.';
COMMENT ON COLUMN analytics.ep_states.ep_drive IS
    'Absorption probs x values {TD 6.97, FG 3, SAFETY -2, TURNOVER_TD -6.97, else 0}. Drive-scoring basis: what this possession is worth, ignoring field-position handoff.';
COMMENT ON COLUMN analytics.ep_states.se_boot IS
    'Bootstrap SE of ep_drive, cluster-resampled by game_id. NULL when the compute ran with --no-bootstrap.';

CREATE INDEX IF NOT EXISTS idx_ep_states_era ON analytics.ep_states (era);
CREATE INDEX IF NOT EXISTS idx_dct_era_from
    ON analytics.drive_chain_transitions (era, from_state);
