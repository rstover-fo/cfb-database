# F09: plays index ownership and partition coverage

F09 addresses the historical table-swap sequence that leaves an index name on
`core.plays_old` while `CREATE INDEX IF NOT EXISTS` skips the new partitioned
`core.plays`. Index names alone cannot establish the indexed table, definition,
validity, or coverage of attached partitions.

## Production finding, 2026-09-08

Read-only inspection of the `cfbd-database` warehouse found **no current ownership
defect**: `core.plays_old` is absent, and all nine index families in the reviewed
2026-09-07 baseline belong to `core.plays`. Every parent is valid and ready and
has 23 valid, ready child indexes on 23 distinct, correct attached partitions.
No production DDL was applied. Partition provisioning for F08 remains separate.

The inspected baseline is
`src/schemas/baseline/20260907_pre_f05.sql`, not the union of index names in old
002/011/016 migration files. Restoring that historical union would add indexes
which are absent from the captured warehouse and have not been justified.

| Index | Expected btree keys / predicate | Child-index bytes | Scans at inspection |
|---|---|---:|---:|
| `plays_dlt_id_unique` | **UNIQUE** `(_dlt_id, season)` | 171,433,984 | 0 |
| `idx_plays_game_id` | `(game_id)` | 29,360,128 | 176,961 |
| `idx_plays_offense` | `(offense)` | 33,316,864 | 937 |
| `idx_plays_defense` | `(defense)` | 33,275,904 | 183 |
| `idx_plays_offense_season` | `(offense, season)` | 33,939,456 | 1,287 |
| `idx_plays_defense_season` | `(defense, season)` | 33,464,320 | 25 |
| `idx_plays_play_type` | `(play_type)` | 34,676,736 | 20 |
| `idx_plays_score_diff` | `(score_diff)` | 33,824,768 | 0 |
| `idx_plays_competitive` | `(game_id, period) WHERE abs(offense_score - defense_score) <= 28` | 27,942,912 | 0 |

Total child-index storage was 431,235,072 bytes (411.26 MiB). These are cumulative
scan counters with an unknown observation interval (`pg_stat_database.stats_reset`
returned NULL); they are not a controlled workload comparison. In particular,
zero scans do not remove the need for uniqueness enforcement.

## Executed read-only query evidence

`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` ran ordinary SELECTs on season 2026,
using observed game `401867684`, offense `UT Martin`, defense `Chicago State`,
play type `Timeout`, and score difference `14`. These narrow examples were not
forced to use an index and do not establish whole-warehouse latency or the
incremental value of every overlapping index.

| Query shape | Chosen child index | Rows read from index | Execution ms |
|---|---|---:|---:|
| Game lookup, `id, play_text` | `plays_y2026_game_id_idx` | 175 | 1.523 |
| Offense-season count / average PPA | `plays_y2026_offense_idx` | 170 | 2.160 |
| Defense-season count / average PPA | `plays_y2026_defense_idx` | 188 | 2.231 |
| Count timeout plays | `plays_y2026_play_type_idx` | 1,837 | 11.601 |
| Game + period <= 4 + competitive predicate | `plays_y2026_game_id_idx` | 175 (5 filtered) | 1.463 |
| Count score difference = 14 | `plays_y2026_score_diff_idx` | 1,263 | 4.212 |

The competitive query did not choose `idx_plays_competitive`. Its marginal value
and that of the overlapping team/season indexes remain tuning questions. No
production writes, index removals, or counter resets were authorized or performed;
per-index write overhead is unmeasured. This change protects the captured
structure and provides selected repair, not a claim that all nine indexes are
optimal. Any future retention/removal decision needs a representative workload
and write-cost evidence.

## Implementation boundaries

- Audit the nine reviewed definitions, their exact parent relation, and the full
  set of valid/ready child targets. A matching count is insufficient.
- Before fetching plays, require only the `plays_dlt_id_unique` family. Its
  uniqueness and `(_dlt_id, season)` coverage protect loaded identity; missing
  optional performance indexes are maintenance decisions.
- Expose repair only through explicit index selection and `--apply`. Validate
  every selected change before issuing DDL. Recognized old-heap collisions get
  deterministic retained names; old rows and tables remain intact.
- Run parent index creation in a bounded, atomic maintenance transaction. This
  blocks writes during index construction and is not a concurrent online build.
  Unexpected owners, definitions, rename collisions, or incomplete/invalid
  index trees require investigation rather than automatic destructive repair.
- Preserve immutable historical migrations and the captured baseline. No new
  index set is silently installed by routine ingestion or bootstrap.

PostgreSQL documents that `IF NOT EXISTS` does not verify equivalence, and that
concurrent builds are unsupported directly on partitioned parents. Online builds
would need a separate leaf-build/attach procedure. See the
[PostgreSQL 17 CREATE INDEX documentation](https://www.postgresql.org/docs/17/sql-createindex.html).

## Verification

The executed regression suite is `tests/test_play_indexes_sql.py`, using an
explicit disposable loopback PostgreSQL target through `F06_TEST_DB_URL`.
It reproduces the old table swap, checks repair and repeated no-op behavior,
and exercises invalid catalog state, rollback, inherited indexes on a future
partition, uniqueness, unchanged caller access, concurrent index renaming, and
builtin predicate identity under a shadowed search path. CI runs it alongside
the warehouse upgrade tests.

Final local checks passed: 532 focused unit/loader/source tests and all ten
executed PostgreSQL 17 scenarios (the final positive predicate case was rerun
after its test update). Ruff, formatting, whitespace checks, and agent setup
validation passed. Independent review found no remaining material findings.
Production evidence above came from read-only catalog/EXPLAIN queries through
the connected database tool. The new CLI was not run against production because
this environment has no local warehouse DSN. Live CFBD/dlt loading and production
write overhead remain unverified; no production changes were made.
