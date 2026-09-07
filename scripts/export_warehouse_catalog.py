"""Export schema-only warehouse catalog for reviewed bootstrap baselines.

Uses read-only transactions, never exports table data, and leaves credentials
in environment variables. Run through the authorized Deploy Schema workflow.
"""

import json
import os
import subprocess
from pathlib import Path

import psycopg2
from psycopg2.extensions import parse_dsn

PROJECT_SCHEMAS = {
    "analytics",
    "api",
    "betting",
    "core",
    "draft",
    "features",
    "live",
    "marts",
    "meta",
    "metrics",
    "ncaa",
    "pff",
    "predictions",
    "public",
    "ratings",
    "raw",
    "recruiting",
    "ref",
    "scouting",
    "stats",
}
OUTPUT = Path("/tmp/warehouse-catalog")


def main():
    url = os.environ["SUPABASE_DB_URL"]
    with psycopg2.connect(url) as conn:
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
        _export_snapshot(conn, url)


def _export_snapshot(conn, url):
    with conn.cursor() as cur:
        cur.execute("SELECT nspname FROM pg_namespace ORDER BY nspname")
        all_schemas = [row[0] for row in cur.fetchall()]
        schemas = [
            n
            for n in all_schemas
            if n in PROJECT_SCHEMAS or n.removesuffix("_staging") in PROJECT_SCHEMAS
        ]
        if not schemas:
            raise RuntimeError("No project schemas found; refusing an unrestricted dump")
        cur.execute(
            "SELECT extname, extversion, nspname FROM pg_extension "
            "JOIN pg_namespace ON extnamespace=pg_namespace.oid ORDER BY extname"
        )
        extensions = cur.fetchall()
        cur.execute("SHOW server_version")
        version = cur.fetchone()[0]
        cur.execute(
            "SELECT nspname, relkind, count(*) FROM pg_class JOIN pg_namespace "
            "ON relnamespace=pg_namespace.oid WHERE nspname=ANY(%s) "
            "GROUP BY 1,2 ORDER BY 1,2",
            (schemas,),
        )
        counts = cur.fetchall()
        cur.execute(
            "SELECT nspname, pg_get_userbyid(relowner), count(*) "
            "FROM pg_class JOIN pg_namespace ON relnamespace=pg_namespace.oid "
            "WHERE nspname=ANY(%s) GROUP BY 1,2 ORDER BY 1,2",
            (schemas,),
        )
        owners = cur.fetchall()
        cur.execute(
            "SELECT nspname, relname, relkind, pg_get_userbyid(relowner) "
            "FROM pg_class JOIN pg_namespace ON relnamespace=pg_namespace.oid "
            "WHERE nspname=ANY(%s) ORDER BY 1,2",
            (schemas,),
        )
        relation_owners = cur.fetchall()
        cur.execute(
            "SELECT nspname, proname, pg_get_function_identity_arguments(pg_proc.oid), "
            "pg_get_userbyid(proowner), prosecdef FROM pg_proc "
            "JOIN pg_namespace ON pronamespace=pg_namespace.oid "
            "WHERE nspname=ANY(%s) ORDER BY 1,2,3",
            (schemas,),
        )
        function_owners = cur.fetchall()
        cur.execute(
            "SELECT parent.rolname, member.rolname, admin_option FROM pg_auth_members "
            "JOIN pg_roles parent ON parent.oid=roleid "
            "JOIN pg_roles member ON member.oid=pg_auth_members.member "
            "WHERE member.rolname IN ('anon','authenticated','service_role','analyst_ro') "
            "ORDER BY 1,2"
        )
        memberships = cur.fetchall()
        cur.execute("SELECT pg_export_snapshot()")
        snapshot = cur.fetchone()[0]
    env = os.environ.copy()
    mapping = {
        "host": "PGHOST",
        "port": "PGPORT",
        "dbname": "PGDATABASE",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "sslmode": "PGSSLMODE",
    }
    for key in mapping.values():
        env.pop(key, None)
    for key, value in parse_dsn(url).items():
        if key in mapping:
            env[mapping[key]] = value
    env["PGOPTIONS"] = "-c default_transaction_read_only=on -c statement_timeout=120000"
    args = ["docker", "run", "--rm"]
    for key in [*mapping.values(), "PGOPTIONS"]:
        if key in env:
            args += ["-e", key]
    args += ["postgres:17", "pg_dump", "--schema-only", "--no-owner", "--lock-wait-timeout=10s"]
    args += ["--snapshot", snapshot]
    for schema in schemas:
        args += ["--schema", schema]
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with (OUTPUT / "schema.sql").open("w") as stream:
        subprocess.run(args, env=env, stdout=stream, check=True)
    metadata = {
        "server_version": version,
        "schemas": schemas,
        "excluded_schemas": [n for n in all_schemas if n not in schemas],
        "extensions": extensions,
        "object_counts": counts,
        "owners": owners,
        "relation_owners": relation_owners,
        "function_owners": function_owners,
        "memberships": memberships,
    }
    (OUTPUT / "catalog.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(json.dumps(metadata))
    print("Schema-only catalog export complete; no table data exported")


if __name__ == "__main__":
    main()
