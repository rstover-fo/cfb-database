# dlt resource name vs. destination table name (core.roster / core.rosters)

**Symptom (2026-09-03):** `api.roster_lookup` had no 2026 rows the week of
Week 1, even though the `rosters` backfill ran green: 716 `/roster` calls,
all 200, dlt package LOADED, zero warnings.

**Cause:** dlt names the destination table after the resource unless
`table_name` is set. `rosters_resource` is named `rosters`, so it wrote
`core.rosters`. Every consumer -- `api.roster_lookup`, marts 011/017/020/
025/045/050, `public.player_search`, `scouting.player_mart`,
`check_presence.py` -- reads `core.roster` (singular), a dlt-shaped table
from an earlier load path that stopped at 2025. Both tables had the same 17
columns and a `*__recruit_ids` child, so nothing failed; the data simply
landed where nobody looked. Presence-check recon showed core.roster at
2025 / 315 teams and core.rosters at 2026 / 31,076 rows / 284 teams.

**Fix:** `table_name="roster"` on the resource
(`src/pipelines/sources/rosters.py`), plus a unit test pinning it. The
merge then lands in the existing `core.roster` / `core.roster__recruit_ids`
(dlt reconciles an existing table by column name; identical columns, no
ALTERs). `core.rosters` / `core.rosters__recruit_ids` are now orphans and
can be dropped in a later migration once a load has been verified against
`core.roster`.

**Lesson:** when a load reports success but a view stays stale, compare the
resource/table name dlt actually wrote (`Job for <table>.<hash>.insert_values`
in the load log) against the table the view reads before suspecting the
API. A row-count floor on the consumer view (`verify_load.py`) would have
caught this the day it happened; a green load log never will.
