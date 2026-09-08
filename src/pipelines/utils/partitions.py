"""Catalog-safe lifecycle for ``core.plays`` season partitions.

The helper validates the complete live partition set before it emits DDL.  This
is intentionally stricter than ``CREATE TABLE IF NOT EXISTS``: an orphaned
same-name table, a default partition, or a child whose name disagrees with its
bound must stop ingestion rather than silently route plays to the wrong table.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from psycopg2 import sql
from psycopg2.extensions import TRANSACTION_STATUS_IDLE

MIN_PLAY_SEASON = 2004
LOCK_TIMEOUT = "10s"
ADVISORY_LOCK_KEYS = (1_128_677_364, 80_008)

_PARTITION_NAME_RE = re.compile(r"plays_y([0-9]+)\Z")
_PARTITION_BOUND_RE = re.compile(r"FOR VALUES IN \(('?)([0-9]+)\1\)\Z")


class PartitionStateError(RuntimeError):
    """The live ``core.plays`` partition catalog is unsafe to maintain."""


@dataclass(frozen=True)
class PlayPartitionPlan:
    """Validated partition state and the bounded work requested by the caller."""

    required_years: tuple[int, ...]
    existing_years: tuple[int, ...]
    missing_years: tuple[int, ...]
    created_years: tuple[int, ...] = ()


def _required_years(years: Iterable[int], calendar_year: int) -> tuple[int, ...]:
    maximum = calendar_year + 1
    requested: set[int] = set()
    for year in years:
        # bool is an int subclass but accepting it as season 0/1 makes caller bugs obscure.
        if isinstance(year, bool) or not isinstance(year, int):
            raise ValueError(f"play partition year must be an integer, got {year!r}")
        if not MIN_PLAY_SEASON <= year <= maximum:
            raise ValueError(f"play partition year {year} is outside {MIN_PLAY_SEASON}..{maximum}")
        requested.add(year)
    requested.update((calendar_year, maximum))
    return tuple(sorted(requested))


def _fetch_parent(cur: object) -> int:
    cur.execute(
        """
        SELECT parent.oid, parent.relkind, partitioned.partstrat,
               partitioned.partnatts,
               ARRAY(
                   SELECT attribute.attname
                   FROM unnest(partitioned.partattrs::smallint[]) WITH ORDINALITY
                       AS partition_key(attnum, key_order)
                   LEFT JOIN pg_catalog.pg_attribute attribute
                     ON attribute.attrelid = parent.oid
                    AND attribute.attnum = partition_key.attnum
                   ORDER BY partition_key.key_order
               )
        FROM pg_catalog.pg_class parent
        JOIN pg_catalog.pg_namespace namespace
          ON namespace.oid = parent.relnamespace
        LEFT JOIN pg_catalog.pg_partitioned_table partitioned
          ON partitioned.partrelid = parent.oid
        WHERE namespace.nspname = %s AND parent.relname = %s
        """,
        ("core", "plays"),
    )
    rows = cur.fetchall()
    if len(rows) != 1:
        raise PartitionStateError("expected exactly one core.plays relation")
    oid, relkind, strategy, key_count, key_names = rows[0]
    if relkind != "p" or strategy != "l" or key_count != 1 or key_names != ["season"]:
        raise PartitionStateError(
            "core.plays must be partitioned by exactly LIST (season); "
            f"found relkind={relkind!r}, strategy={strategy!r}, keys={key_names!r}"
        )
    return int(oid)


def _fetch_children(cur: object, parent_oid: int) -> list[tuple[object, ...]]:
    cur.execute(
        """
        SELECT child.oid, child_namespace.nspname, child.relname,
               child.relkind, child.relispartition,
               pg_catalog.pg_get_expr(child.relpartbound, child.oid)
        FROM pg_catalog.pg_inherits inheritance
        JOIN pg_catalog.pg_class child ON child.oid = inheritance.inhrelid
        JOIN pg_catalog.pg_namespace child_namespace
          ON child_namespace.oid = child.relnamespace
        WHERE inheritance.inhparent = %s
        ORDER BY child_namespace.nspname, child.relname
        """,
        (parent_oid,),
    )
    return list(cur.fetchall())


def _fetch_reserved_relations(cur: object) -> list[tuple[object, ...]]:
    cur.execute(
        """
        SELECT relation.oid, relation.relname, relation.relkind
        FROM pg_catalog.pg_class relation
        JOIN pg_catalog.pg_namespace namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = %s AND relation.relname ~ %s
        ORDER BY relation.relname
        """,
        ("core", r"^plays_y[0-9]+$"),
    )
    return list(cur.fetchall())


def inspect_play_partitions(
    cur: object,
    years: Iterable[int] = (),
    *,
    calendar_year: int | None = None,
) -> PlayPartitionPlan:
    """Validate the full catalog and report missing requested/rolling partitions.

    Existing correctly named single-season partitions beyond the rolling horizon
    are retained and accepted.  The upper bound applies to newly requested years,
    preventing accidental unbounded future creation.
    """

    resolved_calendar_year = date.today().year if calendar_year is None else calendar_year
    if isinstance(resolved_calendar_year, bool) or not isinstance(resolved_calendar_year, int):
        raise ValueError("calendar_year must be an integer")
    if resolved_calendar_year < MIN_PLAY_SEASON:
        raise ValueError(f"calendar_year must be at least {MIN_PLAY_SEASON}")
    required = _required_years(years, resolved_calendar_year)

    parent_oid = _fetch_parent(cur)
    children = _fetch_children(cur, parent_oid)
    child_oids: set[int] = set()
    existing: set[int] = set()
    problems: list[str] = []

    for raw_oid, schema_name, relation_name, relkind, is_partition, bound in children:
        oid = int(raw_oid)
        child_oids.add(oid)
        name_match = _PARTITION_NAME_RE.fullmatch(str(relation_name))
        bound_match = _PARTITION_BOUND_RE.fullmatch(str(bound)) if bound is not None else None
        if schema_name != "core":
            problems.append(f"attached child {schema_name}.{relation_name} is outside core")
            continue
        if relkind != "r" or not is_partition:
            problems.append(
                f"core.{relation_name} is not an ordinary attached partition "
                f"(relkind={relkind!r}, relispartition={is_partition!r})"
            )
            continue
        if name_match is None:
            problems.append(f"unexpected attached child core.{relation_name}")
            continue
        if bound == "DEFAULT":
            problems.append(f"default partition core.{relation_name} is not allowed")
            continue
        if bound_match is None:
            problems.append(f"core.{relation_name} has unsupported bound {bound!r}")
            continue
        name_year = int(name_match.group(1))
        bound_year = int(bound_match.group(2))
        if relation_name != f"plays_y{name_year}":
            problems.append(f"core.{relation_name} is not a canonical partition name")
            continue
        if name_year < MIN_PLAY_SEASON:
            problems.append(
                f"core.{relation_name} precedes the play coverage floor {MIN_PLAY_SEASON}"
            )
            continue
        if name_year != bound_year:
            problems.append(
                f"core.{relation_name} has bound for {bound_year}, expected {name_year}"
            )
            continue
        if name_year in existing:
            problems.append(f"more than one core.plays child covers season {name_year}")
            continue
        existing.add(name_year)

    for raw_oid, relation_name, relkind in _fetch_reserved_relations(cur):
        if int(raw_oid) not in child_oids:
            problems.append(
                f"reserved relation core.{relation_name} exists but is not attached to "
                f"core.plays (relkind={relkind!r})"
            )

    if problems:
        raise PartitionStateError("invalid core.plays partition catalog: " + "; ".join(problems))

    missing = tuple(year for year in required if year not in existing)
    return PlayPartitionPlan(
        required_years=required,
        existing_years=tuple(sorted(existing)),
        missing_years=missing,
    )


def _ensure_idle_connection(conn: object) -> None:
    if getattr(conn, "autocommit", False):
        raise RuntimeError("partition maintenance requires autocommit=False")
    get_status = getattr(conn, "get_transaction_status", None)
    if get_status is not None and get_status() != TRANSACTION_STATUS_IDLE:
        raise RuntimeError("partition maintenance requires an idle connection")


def ensure_play_partitions(
    conn: object,
    years: Iterable[int] = (),
    *,
    create: bool = False,
    calendar_year: int | None = None,
) -> PlayPartitionPlan:
    """Inspect or atomically create the requested and rolling play partitions."""

    resolved_calendar_year = date.today().year if calendar_year is None else calendar_year
    # Validate caller input before opening a transaction or taking locks.
    required = _required_years(years, resolved_calendar_year)
    _ensure_idle_connection(conn)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN" if create else "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        if not create:
            plan = inspect_play_partitions(cur, required, calendar_year=resolved_calendar_year)
            conn.rollback()
            return plan

        cur.execute("SET LOCAL lock_timeout = %s", (LOCK_TIMEOUT,))
        cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", ADVISORY_LOCK_KEYS)
        # The advisory lock coordinates this helper; the table lock also excludes
        # direct external partition DDL between validation and creation.
        cur.execute("LOCK TABLE ONLY core.plays IN SHARE UPDATE EXCLUSIVE MODE")
        before = inspect_play_partitions(cur, required, calendar_year=resolved_calendar_year)
        for year in before.missing_years:
            cur.execute(
                sql.SQL("CREATE TABLE {}.{} PARTITION OF {}.{} FOR VALUES IN (%s)").format(
                    sql.Identifier("core"),
                    sql.Identifier(f"plays_y{year}"),
                    sql.Identifier("core"),
                    sql.Identifier("plays"),
                ),
                (year,),
            )
        after = inspect_play_partitions(cur, required, calendar_year=resolved_calendar_year)
        if after.missing_years:
            raise PartitionStateError(
                "partition creation completed without satisfying: "
                + ", ".join(str(year) for year in after.missing_years)
            )
        conn.commit()
        return PlayPartitionPlan(
            required_years=after.required_years,
            existing_years=after.existing_years,
            missing_years=(),
            created_years=before.missing_years,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
