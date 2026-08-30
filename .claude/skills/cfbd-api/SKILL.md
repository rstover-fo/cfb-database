---
name: cfbd-api
description: CFBD (CollegeFootballData) API conventions, gotchas, and endpoint-selection guidance. Use when building or debugging anything that calls api.collegefootballdata.com — pipelines, scripts, notebooks — across cfb-database, cfb-app, and cfb-scout.
---

# CFBD API

Sources: the CFBD AI Builder Pack canonical context (2026) merged with
gotchas earned in the cfb-database warehouse. The complete 74-endpoint
inventory lives in `cfb-database/docs/cfbd-api-endpoints.md`; this skill
carries conventions and traps, not the endpoint list.

## Authentication and identity

- Bearer token via `CFBD_API_KEY` env var or private ignored config
  (`.dlt/secrets.toml` in cfb-database). Official clients accept the key as
  an access token and build the header themselves — do not add the `Bearer `
  prefix twice.
- Never print keys, Authorization headers, or full environment dumps in
  logs, prompts, issue reports, or screenshots.
- Current account: Tier 4, 125,000 calls/month (as of 2026-07; verify in
  `.dlt/config.toml` — do not hardcode tier quotas in new code, access
  policies change).

## Parameter and field conventions

- Raw HTTP query params are camelCase (`seasonType`); official Python
  clients expose snake_case (`season_type`). Use the spelling for your
  access method; never send an optional param as `null`.
- Raw JSON responses are camelCase; dlt normalizes to snake_case on load
  (`homeClassification` -> `home_classification`). **Verify loaded column
  names via `pg_attribute`, never from API docs** — dlt may rename.
- `team` params take the full school name ("Ohio State"); `conference`
  takes the abbreviation ("SEC", "B1G"). Conference membership is
  season-specific.
- `year` means season year. Postseason weeks restart at 1 (bowls are
  postseason week 1) — regular week 1 and postseason week 1 collide unless
  you key on `season_type` too. cfb-database orders across the boundary
  with `week_index = week + 100` for postseason.
- Recruiting class year is not the season year, roster year, or
  eligibility class. `recruiting.recruits` matches players on `athlete_id`
  (not `id`) and uses `ranking` (not `national_ranking`).
- EPA/PPA: PPA is CFBD's implementation of EPA. Confirm sign and
  perspective per endpoint — offensive PPA higher is better, defensive
  lower is better.

## Pagination and volume

- There is no native pagination. Do not invent `page`, `offset`, `limit`,
  or cursor params. Split work only with supported filters (year, week,
  team, conference, seasonType) and estimate the call count first.
- `/plays/stats` caps at ~2,000 records per request — complete coverage
  requires one request per `gameId` (~1,640 calls for a full season).
  Check for already-loaded games before fanning out.
- Prefer summary/aggregate endpoints over granular ones when they satisfy
  the output; do not fetch plays when a game or season summary suffices.
- Avoid uncontrolled loops over seasons x teams x weeks; never trigger
  live calls from imports, default tests, or UI renders.

## Failure classification and retries

Classify before retrying: local validation, 400 invalid request, 401 auth,
403 tier/authorization, 404 missing, 429 rate limit, transient network,
5xx. Two subtleties:

- An empty `200` is not an error and not proof a tier lacks access; a
  `403` is not proof the endpoint contract is wrong.
- Cloudflare burst-blocks (~10 min) surface as 429s that are NOT quota
  exhaustion. For one-off scripts, do not auto-retry 401/403/429. For
  long-lived pipelines, the cfb-database pattern is correct: honor
  `Retry-After` (capped), count consecutive 429s into a circuit breaker,
  and raise a distinct exhausted error rather than returning `[]` — an
  empty result must never masquerade as "no data".

## Version and data-shape traps

- v2 has breaking changes from v1; many fields are nullable — inspect a
  real or fixture response before writing transforms.
- Games can be marked completed with NULL scores — filter both scores
  non-null before computing margins.
- `ref.teams` has 35 legitimate duplicate school names — `DISTINCT ON
  (school)` before joining on school name or accept fanout.
- Data depth varies: games 1869+, play-by-play ~2004+, recruiting ~2000+,
  advanced metrics (SP+/PPA) ~2014+.

## Endpoint discovery

Endpoint names, params, and enums come from official docs/OpenAPI, the
official client, or an inspected response — never invent them. For a fresh
machine-readable inventory, the CFBD AI Builder Pack ships an
`openapi-context-generator` (run per its README) whose output can refresh
`docs/cfbd-api-endpoints.md` when the API changes. Default tests use
fixtures or mocks, never live calls; live requests are explicit, narrow,
and logged.
