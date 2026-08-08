-- House expected-points API view
-- Exposes analytics.ep_states (the drive-state Markov chain solved per era;
-- design docs/plans/2026-08-08-drive-chain-ep-model-plan.md, populated by
-- scripts/compute_drive_chain.py) as the contract surface for cfb-app and
-- the cfb_mcp agent. The state key is parsed into filterable columns so
-- PostgREST consumers can ask "1st-and-10 at the opponent 25 in the modern
-- era" without string surgery:
--   ?era=eq.2021%2B&down=eq.1&distance_bucket=eq.standard&field_zone=eq.3
-- (note the %2B -- the era value "2021+" contains a literal plus sign).
--
-- Consumer contract (docs/SCHEMA_CONTRACT.md + the 2026-08-08 handoff doc):
--   * ep_drive is the DRIVE basis (points this possession is worth); it is
--     not comparable to CFBD PPA until ep_net (P2) lands. ep_net is NULL on
--     every row today -- NULL means "not yet computed", never zero.
--   * Join a game/play to ITS OWN era's curve; eras differ by up to 0.22 EP
--     (~15 bootstrap SEs) and must not be mixed or averaged.
--   * down=4 rows are GO-FOR-IT-CONDITIONAL: a 4th-down state exists only
--     when the offense lined up to go (punts/FGs exit the chain from the
--     3rd-down play), so d4 can legitimately price above d3.
--   * se_boot is the game-cluster bootstrap SE of ep_drive -- surface
--     intervals, not point estimates.
-- Exposed via PostgREST as /api/expected_points

DROP VIEW IF EXISTS api.expected_points;

CREATE VIEW api.expected_points AS
SELECT
    era,
    state,
    substring(state FROM 'd(\d)')::int AS down,
    split_part(state, '|', 2) AS distance_bucket,
    substring(state FROM 'z(\d+)')::int AS field_zone,
    ((substring(state FROM 'z(\d+)')::int - 1) * 10 + 1) AS yards_to_goal_min,
    LEAST(substring(state FROM 'z(\d+)')::int * 10, 99) AS yards_to_goal_max,
    n_obs,
    ep_drive,
    ep_net,
    p_td,
    p_fg,
    p_punt,
    p_turnover,
    se_boot,
    computed_at
FROM analytics.ep_states;

COMMENT ON VIEW api.expected_points IS 'House expected points per drive state and era (Goldner-basis drive EP from the play-by-play Markov chain, with game-cluster bootstrap SEs). ep_net NULL until the net next-score basis lands (P2). down=4 rows are go-for-it-conditional. Backed by analytics.ep_states; see docs/handoffs/2026-08-08-expected-points-handoff.md.';

-- Grants are part of the definition (021 convention: no ALTER DEFAULT
-- PRIVILEGES in this database). The underlying analytics tables were created
-- AFTER grant_read_access_for_security_invoker's blanket GRANT SELECT ON ALL
-- TABLES ran, so the SECURITY INVOKER view needs these explicit table grants
-- or anon reads fail even though the schema USAGE grant exists.
GRANT SELECT ON analytics.ep_states TO anon, authenticated;
GRANT SELECT ON analytics.drive_chain_transitions TO anon, authenticated;
GRANT SELECT ON api.expected_points TO anon, authenticated;
