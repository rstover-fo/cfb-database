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
- **Production's function and view text is withheld by default.** It was
  printed in full; a production-only function body can hold a token.
- **View definitions are read separately, last.** Measured: `pg_get_viewdef`
  is the only statement that locks user relations (the views and every
  relation they read). It moved out of the snapshot into its own short
  transaction with a five-second lock timeout and a bounded retry.
- **A missing accepted file is an error**, and an accepted entry whose label
  does not match the difference its fingerprint identifies fails the run.
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
- The expected side does not include definitions applied outside the managed
  manifest, so the first production report will list those as drift. That list
  is the input for moving them into the ledger.
- Nothing here has run against production. The first run is report-only for
  that reason.
