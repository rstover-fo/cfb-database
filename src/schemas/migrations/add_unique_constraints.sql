-- Migration: add_unique_constraints
-- Applied: 2026-02-06
--
-- Adds UNIQUE constraints to business keys on reference and core tables.
-- Pre-checks confirmed zero duplicates on these columns.
--
-- SKIPPED: ref.teams(school) — 35 school names have duplicate rows
-- with different API IDs (e.g., Albany x3, Blinn College x4, Butler C.C. x3).
-- These are legitimately different team records sharing the same school name.
-- Consider a UNIQUE constraint on ref.teams(id) instead.

ALTER TABLE ref.conferences
  ADD CONSTRAINT uq_conferences_id UNIQUE (id);

ALTER TABLE ref.venues
  ADD CONSTRAINT uq_venues_id UNIQUE (id);

ALTER TABLE core.games
  ADD CONSTRAINT uq_games_id UNIQUE (id);
