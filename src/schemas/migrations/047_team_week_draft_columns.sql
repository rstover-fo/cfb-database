-- features.team_week: draft production and draft departures
-- =============================================================================
-- Preseason outlook plan Phase 2 / section 6.0 Tier A, adjudicated only after
-- the draft backfill. Follows migrations 042 and 046.
--
-- Column contract authority: migration 028's header forbids adding columns to
-- features.team_week without first updating
-- docs/brainstorms/2026-07-21-team-week-feature-design.md. That doc has been
-- amended -- section 1f carries both rows, section 1i their NULL rule (which is
-- NOT the blanket never-zero rule, see below), section 2a their position in the
-- fitted_v1 vector (20 -> 22 features), and the column count goes 37 -> 39.
--
-- WHY THESE TWO, AND WHY ONLY NOW. Both were REJECTED on 2026-07-26 and the
-- rejections were void. `draft.draft_picks` held 2020-2026 only while
-- years.py configured 2000-2026, and `COALESCE(..., 0)` turned "this draft was
-- never ingested" into "this program produced zero NFL picks" on 54.2% of the
-- screening frame. Nothing errored; the columns simply measured the load state
-- of the warehouse. After backfilling 2000-2019 (27 drafts, 6,901 picks, no
-- gaps) the same screen on the same frame returns:
--
--     column              was (void)   now      vs prior SP+ only
--     draft_picks_3yr       +0.0068   +0.0834        +0.2342
--     draft_departures      -0.0728   -0.0925        +0.0301
--
-- Two recorded findings are REVERSED, not restored. Draft production is not a
-- recruiting proxy -- it survives the recruiting control. And draft_departures
-- is a real negative rather than a sign-flipped confound: losing draftable
-- players predicts a worse season with prior quality and recruiting held fixed.
--
-- The opposite signs are the point, and the plan predicted them before the data
-- existed: picks PRODUCED over S-1..S-3 measure a program that develops talent;
-- picks LOST in year S measure the best players leaving. Conflating them nets
-- to roughly zero, which is what a single combined draft column would do.
--
-- Attribution check that makes these trustworthy: every non-draft candidate in
-- the same run reproduces its prior value to four decimals. The backfill moved
-- the draft columns and nothing else.
--
-- WHAT IS NOT SHIPPED, and why. `talent_stock` (+0.0811) and `pipeline_index`
-- (+0.0846) also cleared the floor, and both are arithmetic combinations of
-- these two plus the control -- they add no construct and score between their
-- own components. `conversion` / `draft_yield` (+0.0709), the DEVELOPMENT term,
-- is real (q=0.0101) but lands 0.0091 under the pre-registered floor, about a
-- third of a standard error at n=1,439. It is rejected on the rule rather than
-- on the evidence, and it is close to what the second-order partial already
-- does to draft_picks_3yr. All three are recorded, not deleted.
--
-- NULL SEMANTICS -- and this is the one place the blanket "NULL, never 0" rule
-- of section 1i does NOT apply mechanically. These are COUNTS, and for a count
-- a zero can be a true measurement: a program that sent nobody to the NFL over
-- S-1..S-3 really did produce zero picks, and recording that as NULL would
-- throw away the signal. But a zero is only true if the drafts were LOADED.
-- The distinction is exactly the one that produced this migration, so it is
-- enforced structurally rather than assumed:
--
--     source years present, team absent  -> 0.0   (a real zero)
--     source years absent entirely       -> NULL  (not a measurement)
--
-- build_features.py implements this with an EXISTS over the draft years alone,
-- deliberately with no team predicate -- adding one would collapse the two
-- cases back together and reintroduce the defect. Over the 2015+ build range
-- every lookback year (2012+) is loaded, so the NULL branch should never fire
-- today; it exists so that a future gap yields missing data instead of a
-- fabricated zero, and scripts/screen_preseason_features.py --audit-imputation
-- carries the matching counters.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-028 and 041-046. Idempotent.

ALTER TABLE features.team_week
    ADD COLUMN IF NOT EXISTS draft_picks_3yr  NUMERIC(6, 1),
    ADD COLUMN IF NOT EXISTS draft_departures NUMERIC(6, 1);

COMMENT ON COLUMN features.team_week.draft_picks_3yr IS
    'Count of NFL draft picks this program produced across the three drafts before season S (draft years S-3..S-1), never season S''s own draft. Preseason-known: all three drafts concluded before week 1. Screened +0.0834 against season-S SP+ controlling for BOTH prior-season SP+ and recruiting_points_3yr, and +0.2342 controlling for prior SP+ alone -- so it is NOT merely a recruiting proxy, reversing the earlier recorded finding, which was measured on a column that was a fabricated zero on 54.2% of rows before the 2000-2019 backfill. 0.0 is a REAL value (this program produced no picks); NULL means the source drafts are absent from draft.draft_picks entirely and the count is not a measurement.';

COMMENT ON COLUMN features.team_week.draft_departures IS
    'Count of NFL draft picks this program LOST in the April draft of year S -- players gone before the season starts. Preseason-known: the draft precedes week 1. Screened -0.0925 controlling for prior SP+ and recruiting_points_3yr: losing draftable players predicts a worse season once prior quality is held fixed. Deliberately a SEPARATE column from draft_picks_3yr rather than netted against it, because the two carry opposite signs (+0.0834 vs -0.0925) and combining them cancels most of both. The earlier recorded reading of this column as a sign-flipped confound was measured on 45.1% fabricated zeros and is withdrawn. 0.0 is a REAL value; NULL means year S''s draft is absent from draft.draft_picks.';
