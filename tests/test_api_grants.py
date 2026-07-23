"""Static grant-coverage guard for the api view layer.

Prod has NO default privileges for the PostgREST roles (anon/authenticated)
-- see the incident note in src/schemas/api/005_leaderboard_teams.sql. Any
definition file that DROPs and recreates its view therefore strips read
access unless the file itself re-grants it. This pins the invariant that
EVERY api view definition carries its grant, so a re-apply (or a future
CREATE OR REPLACE -> DROP/CREATE refactor) can never silently take a view
away from cfb-app or the analyst RPC's consumers.
"""

import re
from pathlib import Path

import pytest

API_DIR = Path(__file__).resolve().parent.parent / "src" / "schemas" / "api"
DEFINITION_FILES = sorted(API_DIR.glob("[0-9]*.sql"))

VIEW_RE = re.compile(r"CREATE (?:OR REPLACE )?VIEW (api\.\w+)")


def _strip_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def test_definition_files_found():
    assert len(DEFINITION_FILES) >= 38, "api definition files went missing"


@pytest.mark.parametrize("path", DEFINITION_FILES, ids=lambda p: p.name)
def test_every_view_grants_postgrest_roles(path):
    sql = _strip_comments(path.read_text())
    views = list(dict.fromkeys(VIEW_RE.findall(sql)))
    assert views, f"{path.name}: no CREATE VIEW api.* statement found"
    for view in views:
        pattern = rf"GRANT SELECT ON {re.escape(view)}\s+TO anon, authenticated;"
        assert re.search(pattern, sql), (
            f"{path.name}: missing 'GRANT SELECT ON {view} TO anon, authenticated;'"
            " -- a DROP/CREATE apply would strip PostgREST read access"
        )
