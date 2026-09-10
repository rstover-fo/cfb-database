"""PostgreSQL 17 non-superuser role-creator compatibility checks."""

from __future__ import annotations

import uuid
from pathlib import Path

import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.extensions import make_dsn

from tests.test_warehouse_migrations_sql import query
from tests.test_warehouse_migrations_sql import warehouse_db as _warehouse_db  # noqa: F401

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_070 = ROOT / "src/schemas/migrations/070_generation_enforced_refresh.sql"
MIGRATION_071 = ROOT / "src/schemas/migrations/071_sdv_ratings_publication.sql"
MIGRATION_072 = ROOT / "src/schemas/migrations/072_sdv_source_batch_publication.sql"


def tagged_block(path: Path, tag: str) -> str:
    migration = path.read_text(encoding="utf-8")
    opening = f"DO ${tag}$"
    closing = f"${tag}$;"
    start = migration.index(opening)
    end = migration.index(closing, start) + len(closing)
    return migration[start:end]


ROLE_070 = tagged_block(MIGRATION_070, "role")
ROLE_071 = tagged_block(MIGRATION_071, "role")
DEPENDENCIES_072 = tagged_block(MIGRATION_072, "dependencies")


def for_refresher(role_name: str) -> str:
    return ROLE_070.replace("warehouse_refresher", role_name)


def for_publisher(block: str, role_name: str) -> str:
    return block.replace("warehouse_source_publisher", role_name)


def rejected(conn, statement: str) -> None:
    with pytest.raises(psycopg2.Error, match="bounded NOLOGIN role"):
        with conn.cursor() as cur:
            cur.execute(statement)
    conn.rollback()


def role_memberships(conn, role_name: str):
    return query(
        conn,
        """
        SELECT parent.rolname, member.rolname, grantor.rolname,
               membership.admin_option, membership.inherit_option,
               membership.set_option
        FROM pg_catalog.pg_auth_members membership
        JOIN pg_catalog.pg_roles parent ON parent.oid = membership.roleid
        JOIN pg_catalog.pg_roles member ON member.oid = membership.member
        JOIN pg_catalog.pg_roles grantor ON grantor.oid = membership.grantor
        WHERE parent.rolname = %s OR member.rolname = %s
        ORDER BY parent.rolname, member.rolname, grantor.rolname
        """,
        (role_name, role_name),
    )


def bootstrap_creator_edge(
    conn,
    role_name: str,
    creator_name: str,
    *,
    admin: bool = True,
    inherit: bool = False,
    set_option: bool = False,
):
    bootstrap_superuser = query(conn, "SELECT rolname FROM pg_catalog.pg_roles WHERE oid = 10")[0][
        0
    ]
    return [
        (
            role_name,
            creator_name,
            bootstrap_superuser,
            admin,
            inherit,
            set_option,
        )
    ]


def set_role_identity(conn, role_name: str):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role_name)))
        cur.execute("SELECT current_user, session_user")
        identity = cur.fetchone()
        cur.execute("RESET ROLE")
    conn.commit()
    return identity


@pytest.fixture
def pg17_creator_db(request):
    admin, target = request.getfixturevalue("_warehouse_db")
    version = query(admin, "SELECT current_setting('server_version_num')::integer")[0][0]
    if version < 170000 or version >= 180000:
        pytest.fail("creator-membership regression requires stock PostgreSQL 17")

    suffix = uuid.uuid4().hex[:12]
    names = {
        "creator": f"pg17_creator_{suffix}",
        "refresher": f"pg17_refresh_{suffix}",
        "publisher": f"pg17_publish_{suffix}",
        "runtime": f"pg17_runtime_{suffix}",
        "bystander": f"pg17_other_{suffix}",
        "parent": f"pg17_parent_{suffix}",
        "inherited": f"pg17_inherit_{suffix}",
        "noadmin": f"pg17_noadmin_{suffix}",
        "setexact": f"pg17_setexact_{suffix}",
    }
    creator_password = "creator-" + uuid.uuid4().hex
    runtime_password = "runtime-" + uuid.uuid4().hex
    database = query(admin, "SELECT current_database()")[0][0]

    query(
        admin,
        sql.SQL(
            "CREATE ROLE {} LOGIN INHERIT NOSUPERUSER NOCREATEDB "
            "CREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD {}"
        ).format(sql.Identifier(names["creator"]), sql.Literal(creator_password)),
    )
    query(
        admin,
        sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
            sql.Identifier(database), sql.Identifier(names["creator"])
        ),
    )
    creator = psycopg2.connect(make_dsn(target, user=names["creator"], password=creator_password))
    try:
        query(
            creator,
            "CREATE SCHEMA meta; "
            "CREATE SCHEMA warehouse_source_stage; "
            "CREATE TABLE meta.flat_file_loads(id bigint)",
        )
        yield admin, creator, target, names, runtime_password
    finally:
        creator.close()
        admin.rollback()
        query(
            admin,
            sql.SQL("DROP OWNED BY {} CASCADE").format(sql.Identifier(names["creator"])),
        )
        for role_name in (
            names["refresher"],
            names["publisher"],
            names["runtime"],
            names["bystander"],
            names["parent"],
            names["inherited"],
            names["noadmin"],
            names["setexact"],
            names["creator"],
        ):
            query(admin, sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role_name)))


def test_creator_admin_only_edges_pass_creation_revalidation_and_072_dependencies(
    pg17_creator_db,
):
    _, creator, _, names, _ = pg17_creator_db
    assert query(
        creator,
        "SELECT current_user=session_user,rolsuper,rolcreaterole,rolinherit "
        "FROM pg_catalog.pg_roles WHERE rolname=current_user",
    ) == [(True, False, True, True)]

    query(creator, for_refresher(names["refresher"]))
    assert role_memberships(creator, names["refresher"]) == bootstrap_creator_edge(
        creator, names["refresher"], names["creator"]
    )
    query(creator, for_refresher(names["refresher"]))

    with pytest.raises(psycopg2.Error, match="bounded NOLOGIN role"):
        with creator.cursor() as cur:
            cur.execute(for_publisher(DEPENDENCIES_072, names["publisher"]))
    creator.rollback()

    query(creator, for_publisher(ROLE_071, names["publisher"]))
    assert role_memberships(creator, names["publisher"]) == bootstrap_creator_edge(
        creator, names["publisher"], names["creator"]
    )
    query(creator, for_publisher(ROLE_071, names["publisher"]))
    query(creator, for_publisher(DEPENDENCIES_072, names["publisher"]))


def test_same_creator_can_enable_set_then_assume_role_and_guards_reject_activation(
    pg17_creator_db,
):
    _, creator, _, names, _ = pg17_creator_db
    query(creator, for_publisher(ROLE_071, names["publisher"]))
    query(
        creator,
        sql.SQL("GRANT {} TO {} WITH INHERIT FALSE, SET TRUE").format(
            sql.Identifier(names["publisher"]), sql.Identifier(names["creator"])
        ),
    )

    assert role_memberships(creator, names["publisher"]) == [
        (
            names["publisher"],
            names["creator"],
            names["creator"],
            False,
            False,
            True,
        ),
        *bootstrap_creator_edge(creator, names["publisher"], names["creator"]),
    ]
    assert set_role_identity(creator, names["publisher"]) == (
        names["publisher"],
        names["creator"],
    )
    rejected(creator, for_publisher(ROLE_071, names["publisher"]))
    rejected(creator, for_publisher(DEPENDENCIES_072, names["publisher"]))


def test_distinct_runtime_login_can_receive_set_only_membership_and_assume_role(
    pg17_creator_db,
):
    _, creator, target, names, runtime_password = pg17_creator_db
    query(creator, for_publisher(ROLE_071, names["publisher"]))
    query(
        creator,
        sql.SQL(
            "CREATE ROLE {} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE "
            "NOREPLICATION NOBYPASSRLS PASSWORD {}"
        ).format(sql.Identifier(names["runtime"]), sql.Literal(runtime_password)),
    )
    query(
        creator,
        sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT FALSE, SET TRUE").format(
            sql.Identifier(names["publisher"]), sql.Identifier(names["runtime"])
        ),
    )

    assert role_memberships(creator, names["publisher"]) == [
        *bootstrap_creator_edge(creator, names["publisher"], names["creator"]),
        (
            names["publisher"],
            names["runtime"],
            names["creator"],
            False,
            False,
            True,
        ),
    ]
    runtime = psycopg2.connect(make_dsn(target, user=names["runtime"], password=runtime_password))
    try:
        assert set_role_identity(runtime, names["publisher"]) == (
            names["publisher"],
            names["runtime"],
        )
    finally:
        runtime.close()

    rejected(creator, for_publisher(ROLE_071, names["publisher"]))
    rejected(creator, for_publisher(DEPENDENCIES_072, names["publisher"]))


def test_guards_reject_outbound_unrelated_and_invalid_creator_edges(pg17_creator_db):
    admin, creator, _, names, _ = pg17_creator_db

    query(creator, for_refresher(names["refresher"]))
    for options in (
        {"admin": False, "inherit": False, "set_option": False},
        {"admin": True, "inherit": True, "set_option": False},
        {"admin": True, "inherit": False, "set_option": True},
    ):
        query(
            admin,
            sql.SQL("GRANT {} TO {} WITH ADMIN {}, INHERIT {}, SET {}").format(
                sql.Identifier(names["refresher"]),
                sql.Identifier(names["creator"]),
                sql.SQL(str(options["admin"]).upper()),
                sql.SQL(str(options["inherit"]).upper()),
                sql.SQL(str(options["set_option"]).upper()),
            ),
        )
        assert role_memberships(creator, names["refresher"]) == bootstrap_creator_edge(
            creator, names["refresher"], names["creator"], **options
        )
        rejected(creator, for_refresher(names["refresher"]))
    query(
        admin,
        sql.SQL("GRANT {} TO {} WITH ADMIN TRUE, INHERIT FALSE, SET FALSE").format(
            sql.Identifier(names["refresher"]), sql.Identifier(names["creator"])
        ),
    )
    query(creator, for_refresher(names["refresher"]))

    query(
        creator,
        sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(sql.Identifier(names["parent"])),
    )
    query(
        creator,
        sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT FALSE, SET TRUE").format(
            sql.Identifier(names["parent"]), sql.Identifier(names["refresher"])
        ),
    )
    rejected(creator, for_refresher(names["refresher"]))

    query(creator, for_publisher(ROLE_071, names["publisher"]))
    query(
        creator,
        sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(sql.Identifier(names["bystander"])),
    )
    query(
        creator,
        sql.SQL("GRANT {} TO {} WITH ADMIN TRUE, INHERIT FALSE, SET FALSE").format(
            sql.Identifier(names["publisher"]), sql.Identifier(names["bystander"])
        ),
    )
    rejected(creator, for_publisher(ROLE_071, names["publisher"]))
    rejected(creator, for_publisher(DEPENDENCIES_072, names["publisher"]))

    inherited_role = names["inherited"]
    query(creator, for_publisher(ROLE_071, inherited_role))
    query(
        admin,
        sql.SQL("GRANT {} TO {} WITH ADMIN TRUE, INHERIT TRUE, SET FALSE").format(
            sql.Identifier(inherited_role), sql.Identifier(names["creator"])
        ),
    )
    assert role_memberships(creator, inherited_role) == bootstrap_creator_edge(
        creator, inherited_role, names["creator"], inherit=True
    )
    rejected(creator, for_publisher(ROLE_071, inherited_role))
    rejected(creator, for_publisher(DEPENDENCIES_072, inherited_role))

    no_admin_role = names["noadmin"]
    query(creator, for_publisher(ROLE_071, no_admin_role))
    query(
        admin,
        sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT FALSE, SET FALSE").format(
            sql.Identifier(no_admin_role), sql.Identifier(names["creator"])
        ),
    )
    assert role_memberships(creator, no_admin_role) == bootstrap_creator_edge(
        creator, no_admin_role, names["creator"], admin=False
    )
    rejected(creator, for_publisher(ROLE_071, no_admin_role))
    rejected(creator, for_publisher(DEPENDENCIES_072, no_admin_role))

    set_exact_role = names["setexact"]
    query(creator, for_publisher(ROLE_071, set_exact_role))
    query(
        admin,
        sql.SQL("GRANT {} TO {} WITH ADMIN TRUE, INHERIT FALSE, SET TRUE").format(
            sql.Identifier(set_exact_role), sql.Identifier(names["creator"])
        ),
    )
    assert role_memberships(creator, set_exact_role) == bootstrap_creator_edge(
        creator, set_exact_role, names["creator"], set_option=True
    )
    rejected(creator, for_publisher(ROLE_071, set_exact_role))
    rejected(creator, for_publisher(DEPENDENCIES_072, set_exact_role))
