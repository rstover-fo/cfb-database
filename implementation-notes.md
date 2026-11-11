# Implementation notes

## Catalog drift check (2026-11-10)

Plan as stated before building: compare production's catalog with the managed
build using a structured inventory; treat what dlt adds during loads as
expected; ship report-only; keep access details out of public logs.

### Deviations

Each came from one of two independent reviews or from a measurement, and each
took the more conservative option.

- **"Added by dlt" is much narrower than planned.** The first version treated
  any relation with a `_dlt_id` column as dlt's, which would have passed a
  hand-made view or backup copy of a raw table, and any later grant on a
  dlt-created table. It now requires a NOT NULL `_dlt_id` (or one of dlt's
  three state-table names) in a schema that already has dlt tables. A new
  column must be nullable, default-less and of a type dlt already uses. A new
  table must be a plain table or a list partition of a dlt table, hold only
  what dlt creates, and match a sibling's owner, grants and options. Measured
  on a managed build: every dlt table has a NOT NULL `_dlt_id`, no view
  carries one, and a table created in a dlt schema gets its siblings' access
  from default privileges, so routine loads raise nothing.
- **Access differences are counted, not redacted.** The first version hid the
  role and privilege but still published the object name and a fingerprint,
  and the fingerprint could be brute-forced from public inputs. Now nothing
  about an access difference is published except a count per schema and kind.
  The cost: seeing or accepting one needs a dispatch with
  `include_sensitive_details`, which publishes it for that run.
- **Production's deparsed SQL is withheld by default.** Function bodies and
  view definitions were printed in full. PR review extended this to index,
  constraint and trigger definitions and to columns with a default: Supabase
  database webhooks, for one, put request headers in trigger arguments.
- **View definitions are read separately, last.** Measured: `pg_get_viewdef`
  is the only statement that locks user relations (the views and every
  relation they read). It moved out of the snapshot into its own short
  transaction with a five-second lock timeout and a bounded retry.
- **A missing accepted file is an error**, as is one that does not state
  `enforce`, and an accepted entry whose label does not match the difference
  its fingerprint identifies fails the run.
- **Added from PR review:** event triggers (the ledger installs six as
  safeguards), column order (appending is not a reorder), and enum labels
  stored as a list rather than joined text.
- **Default privileges:** global ones and emptied ones are included; an inner
  join dropped them in the first version. Sequence ACL defaults used the
  foreign-server type letter and now use the sequence one.

### Known limits

- Indistinguishable from dlt's work, so not detected: a nullable default-less
  column of a known type added by hand to a dlt table; a hand-made table that
  imitates a dlt table in every checked respect; an index or constraint added
  to a table that exists only in production.
- Per-schema counts of access differences are published and can hint at how
  many roles reach a lone new relation.
- A definition file outside the managed manifest that differs from what
  production runs is invisible to the check; see the first run below.

## First production run (2026-11-11)

Run 42955589462, report-only: 19 differences in about 13,000 catalog facts,
none in `api` or `public`, and no grant or owner difference outside one
staging schema.

- **A prediction above was wrong.** These notes expected the first report to
  list definitions deployed outside the manifest. It listed none, because the
  baseline was dumped from production and already carries them.
- **Seven view definitions differ only by the pg_dump round trip.** A restored
  dump deparses `IN (...)` on a varchar column as
  `ARRAY['a'::character varying::text]` where the original deparses as
  `ARRAY['a'::character varying]::text[]`, and it names unaliased constant
  columns. Applying those two rewrites to the managed build's text reproduces
  production's published sha256 for all seven. Accepted.
- **Three dlt state tables in `warehouse_source_stage`**, created by the
  source-publication pipelines, with owner-only access. The classifier does
  not treat them as dlt's because the managed build has no dlt table in that
  schema to compare their access with. Accepted, with their nine access facts.
- **Deviation: the nine access entries were derived without a details
  dispatch**, which would have published them in a run log. Production's
  catalog was read privately and the tables were reproduced on a local managed
  build; the three relation fingerprints and dependent-fact digests matched
  the published report. The access entries are confirmed only when a
  production run reports no drift and no stale entry, so `enforce` stays
  `false` until then.

### Found while tracing the differences

- **Fixed: `src/schemas/marts/015_team_style_profile.sql` did not build what
  production serves.** Production runs the definition added as
  `marts/010_team_style_profile.sql` (b204249). 09ecf46 deleted that file as
  dead code and kept an older one that was never deployed; a release naming
  it would have changed the mart's numbers. The file holds b204249's text
  again. Applied to a managed build, it deparses to production's sha256 with
  the same columns and indexes. The refresh registry now declares what the
  view reads (`core.games`, `core.plays`) instead of `marts.play_epa`: a plan
  for a `core.games` change now includes the view, one for `marts.play_epa`
  alone no longer does, and the full plan's order is unchanged. Whether the
  mart should move to the `marts.play_epa` version (garbage time excluded,
  four more columns) is a product decision this does not make. Review of this
  change raised one consequence of the current definition to weigh in it:
  `total_plays` counts every play that carries a `ppa`, whatever its type,
  where `marts.play_epa` also drops kickoffs and clock events. `run_rate`,
  `pass_rate`, `plays_per_game` and `tempo_category` inherit that. This is
  what production serves today, and it has not been measured against data.
- Not fixed: `marts.team_returning_production` is in production and the
  baseline with three indexes, but has no source file, no refresh-registry
  entry, no dependent and no recorded read. `api.team_returning_production`
  reads `marts.returning_production`. Removing it needs a ledger migration
  and a production deploy.
- No action needed: `stats.passing_team_season` and
  `stats.passing_team_games` are about 460 columns wide since the provider
  added per-location passing metrics. The set follows the payload's shape and
  does not grow per load; Postgres allows 1,600.
