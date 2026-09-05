---
name: cfbd-api
description: Use when implementing or debugging CFBD requests or interpreting CFBD response fields.
---

# CFBD API

Use repository code, fixtures, and official CFBD documentation to establish an
endpoint contract. The warehouse endpoint inventory is
`docs/cfbd-api-endpoints.md`; consult only the entries relevant to the task.
For passing or rushing charting, also read
[references/charting.md](references/charting.md).

## Request contract

- Authenticate with the configured access token. Official clients construct
  the authorization header; do not add a second `Bearer` prefix. Never print
  keys, authorization headers, or broad environment dumps.
- Raw HTTP parameters and responses use camelCase; client libraries may expose
  snake_case, and dlt normalizes loaded columns. Use the spelling required by
  the actual access layer and omit absent optional parameters rather than
  sending `null`.
- Confirm endpoint names, supported filters, enums, and fields from official
  docs/OpenAPI, the client, or an inspected fixture/response. Do not invent
  parameters.
- CFBD has no native pagination. Bound expensive fan-out with supported filters
  and estimate calls before execution. Prefer an aggregate endpoint when it
  meets the request.
- `/plays/stats` has historically capped responses near 2,000 records; use
  per-game requests for complete coverage and budget that fan-out explicitly.
- A request for `year` refers to a season. Postseason week numbering restarts at
  1; warehouse ordering uses `week_index = week` for regular season and
  `week + 100` for postseason. Keys that span both season types must retain
  `season_type` or an equivalent unambiguous game identifier.

## Identity and shape

- Prefer provider identifiers. When only names are available, use a reviewed
  crosswalk or an explicit, deterministic identity rule whose cardinality is
  checked for the requested season and grain. `ref.teams.school` is not unique;
  an unordered `DISTINCT ON (school)` silently selects an arbitrary row and is
  not identity resolution.
- Conference membership is season-specific. Team filters take the full school
  name while conference filters use the CFBD abbreviation.
- Verify warehouse column names from the catalog when a database is available;
  dlt can rename or create variant columns. Without database access, use current
  migrations, fixtures, and source code and state that catalog verification was
  not run.
- Preserve endpoint grain and nullable fields. Completed games can still have
  null scores, so require both scores before computing a result or margin.
- PPA is CFBD's expected-points implementation. Confirm perspective for each
  field: higher offensive PPA is better, while lower defensive PPA is better.

## Outcomes and retries

Classify validation errors, 400, 401, 403, 404, 429, transient network errors,
and 5xx responses distinctly. An empty successful response is expected no-data,
not evidence of missing authorization. An authorization or transport failure
must not become an empty result.

Use the repository's shared retry and rate-limit behavior for CFBD ingestion.
Do not retry authentication/authorization failures. Honor `Retry-After`, bound
429/transient retries through the shared client, and surface exhaustion explicitly.
One-off probes must not add an automatic retry loop around 401/403/429 responses. Live
requests are optional evidence and must be narrow and authorized; fixtures and
mocks are sufficient for drafting or fixing request behavior.
