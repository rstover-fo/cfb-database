"""Classification, acceptance, redaction and safety rules of the catalog drift check."""

import json

import pytest

from scripts import check_catalog_drift as drift
from scripts.check_catalog_drift import CHANGED, MISSING, UNEXPECTED, Item

FACTS = {"server_version": "17.6", "schemas": ["core"], "items": 0}
LOCAL_URL = "postgresql://postgres:local-secret@127.0.0.1:5432/expected"
PRODUCTION_URL = "postgresql://reader:prod-secret@db.example:5432/warehouse"


def inventory(*items: Item) -> drift.Inventory:
    return {(item.kind, item.key): item for item in items}


def fact(kind: str, key: str, value: str, relation: str | None = None) -> Item:
    """A catalog fact whose schema is read off its relation or its key."""
    schema = (relation or key.split()[-1]).split(".", 1)[0]
    return Item(kind, key, value, schema, relation)


def relation(name: str, *columns: str, shape: str = "table", grants=(("anon", "SELECT"),)):
    """A relation with its columns and the access facts every relation has."""
    return [
        fact("relation", name, shape, name),
        fact("owner", f"relation {name}", "postgres", name),
        fact("relation_options", name, "row_security=False; options=", name),
        *(fact("grant", f"relation {name} -> {role}", held, name) for role, held in grants),
        *(fact("column", f"{name}.{column}", "bigint", name) for column in columns),
    ]


def dlt_table(name: str, *columns: str, **options):
    """A table the way dlt declares it: keyed by a NOT NULL ``_dlt_id``."""
    return [
        *relation(name, *columns, **options),
        fact("column", f"{name}._dlt_id", "character varying; not null", name),
    ]


def report_for(expected, production, accepted=None, *, details=True):
    return drift.build_report(
        expected,
        production,
        accepted or {},
        expected_facts=FACTS,
        production_facts=FACTS,
        include_details=details,
    )


def keys(entries):
    return {(entry["kind"], entry["key"]) for entry in entries}


def test_identical_inventories_have_no_differences():
    catalog = inventory(*dlt_table("core.games", "id"))

    assert drift.diff_inventories(catalog, dict(catalog)) == []


def test_each_status_is_reported_with_both_values():
    expected = inventory(fact("index", "core.a", "old"), fact("index", "core.gone", "x"))
    production = inventory(fact("index", "core.a", "new"), fact("index", "core.extra", "y"))

    found = {item.key: item for item in drift.diff_inventories(expected, production)}

    assert (found["core.a"].status, found["core.a"].expected, found["core.a"].production) == (
        CHANGED,
        "old",
        "new",
    )
    assert found["core.gone"].status == MISSING
    assert found["core.extra"].status == UNEXPECTED


def test_an_extra_relation_is_one_line_plus_its_access_facts():
    production = inventory(*relation("core.rogue", "id", "name"))

    found = drift.diff_inventories({}, production)

    lines = {(item.kind, item.key): item for item in found}
    assert set(lines) == {
        ("relation", "core.rogue"),
        ("owner", "relation core.rogue"),
        ("relation_options", "core.rogue"),
        ("grant", "relation core.rogue -> anon"),
    }
    assert "2 dependent facts" in lines[("relation", "core.rogue")].production


def test_a_relation_missing_from_production_is_a_single_line():
    expected = inventory(*relation("core.dropped", "id"))

    (only,) = drift.diff_inventories(expected, {})

    assert (only.kind, only.key, only.status) == ("relation", "core.dropped", MISSING)
    assert "4 dependent facts" in only.expected


def test_a_new_column_inside_an_accepted_extra_relation_changes_its_fingerprint():
    before = drift.diff_inventories({}, inventory(*relation("core.rogue", "id")))
    after = drift.diff_inventories({}, inventory(*relation("core.rogue", "id", "leaked")))

    def line(found):
        return next(item for item in found if item.kind == "relation")

    assert line(before).fingerprint != line(after).fingerprint


def test_columns_and_tables_dlt_adds_are_not_drift():
    expected = inventory(*dlt_table("core.games", "id"))
    production = inventory(
        *dlt_table("core.games", "id", "attendance__v_double"),
        *dlt_table("core.games__notes", "value"),
        *relation("core._dlt_loads", "load_id"),
    )

    report = report_for(expected, production)

    assert report["summary"]["drift"] == 0
    assert {key for kind, key in keys(report["ingest_evolved"]) if kind == "relation"} == {
        "core.games__notes",
        "core._dlt_loads",
    }
    assert ("column", "core.games.attendance__v_double") in keys(report["ingest_evolved"])


@pytest.mark.parametrize(
    "lookalike",
    [
        # A view over a dlt table exposes _dlt_id without being dlt's.
        relation("api.games_raw", "id", shape="view")
        + [fact("column", "api.games_raw._dlt_id", "character varying", "api.games_raw")],
        # CREATE TABLE AS copies the column but not its NOT NULL.
        relation("core.games_bak", "id")
        + [fact("column", "core.games_bak._dlt_id", "character varying", "core.games_bak")],
        # A real dlt shape in a schema where the managed build has no dlt tables.
        dlt_table("meta.smuggled", "id"),
        # dlt's state tables are tables; a view borrowing a state-table name is not one.
        relation("core._dlt_loads", "id", shape="view"),
        dlt_table("core.remote_copy", "id", shape="foreign table"),
        # Only dlt's three state tables are trusted by name.
        relation("core._dlt_games_backup", "id"),
        dlt_table("core.games__fast", "id", shape="table; persistence=u"),
        dlt_table("core.games__parted", "id", shape="partitioned table; partition by LIST (id)"),
        dlt_table("core.plays_rest", "id", shape="table; partition of core.games; bound DEFAULT"),
        dlt_table(
            "core.side_y2027",
            "id",
            shape="table; partition of core.not_dlt; bound FOR VALUES IN ('2027')",
        ),
    ],
    ids=[
        "view",
        "copy",
        "schema-without-dlt",
        "state-table-named-view",
        "foreign-table",
        "dlt-prefixed-name",
        "unlogged",
        "new-partitioned-table",
        "default-partition",
        "partition-of-a-non-dlt-table",
    ],
)
def test_relations_that_only_resemble_dlt_tables_are_drift(lookalike):
    expected = inventory(*dlt_table("core.games", "id"))
    production = inventory(*dlt_table("core.games", "id"), *lookalike)

    report = report_for(expected, production)

    assert report["summary"]["ingest_evolved"] == 0
    assert lookalike[0].key in {key for kind, key in keys(report["drift"]) if kind == "relation"}


@pytest.mark.parametrize(
    "column",
    [
        "bigint; not null",
        "bigint; default 0",
        "bigint; generated=s",
        "bigint; identity=a",
        # A domain can carry a default and a check without showing either.
        "public.sneaky_domain",
    ],
)
def test_a_column_dlt_would_not_add_is_drift_even_on_a_dlt_table(column):
    expected = inventory(*dlt_table("core.games", "id"))
    production = inventory(
        *dlt_table("core.games", "id"), fact("column", "core.games.by_hand", column, "core.games")
    )

    report = report_for(expected, production)

    assert keys(report["drift"]) == {("column", "core.games.by_hand")}
    assert report["summary"]["ingest_evolved"] == 0


@pytest.mark.parametrize(
    "extra",
    [
        fact("grant", "relation core.games__notes -> anon", "INSERT, SELECT", "core.games__notes"),
        fact("grant", "relation core.games__notes -> intruder", "SELECT", "core.games__notes"),
        fact("owner", "relation core.games__notes", "intruder", "core.games__notes"),
        fact("policy", "core.games__notes.open", "PERMISSIVE ALL", "core.games__notes"),
        fact("trigger", "core.games__notes.audit", "CREATE TRIGGER audit", "core.games__notes"),
        fact("grant", "column core.games__notes.value -> anon", "UPDATE", "core.games__notes"),
    ],
    ids=["wider-grant", "new-grantee", "owner", "policy", "trigger", "column-grant"],
)
def test_a_new_dlt_table_reachable_unlike_its_siblings_is_drift(extra):
    expected = inventory(*dlt_table("core.games", "id"))
    new_table = {(item.kind, item.key): item for item in dlt_table("core.games__notes", "value")}
    new_table[(extra.kind, extra.key)] = extra
    production = inventory(*dlt_table("core.games", "id"), *new_table.values())

    report = report_for(expected, production)

    assert report["summary"]["ingest_evolved"] == 0
    assert ("relation", "core.games__notes") in keys(report["drift"])
    # Every access fact on that table is reported, not only the unusual one.
    assert ("owner", "relation core.games__notes") in keys(report["drift"])


def test_losing_or_altering_a_dlt_object_is_drift():
    expected = inventory(*dlt_table("core.games", "id", "notes"))
    altered = {(item.kind, item.key): item for item in dlt_table("core.games", "id")}
    altered[("column", "core.games.id")] = fact("column", "core.games.id", "text", "core.games")

    report = report_for(expected, altered)

    assert {entry["key"]: entry["status"] for entry in report["drift"]} == {
        "core.games.id": CHANGED,
        "core.games.notes": MISSING,
    }
    assert report["summary"]["ingest_evolved"] == 0


def access_drift():
    expected = inventory(*relation("core.games", "id"))
    changed = {(item.kind, item.key): item for item in relation("core.games", "id")}
    changed[("grant", "relation core.games -> anon")] = fact(
        "grant", "relation core.games -> anon", "INSERT, SELECT", "core.games"
    )
    production = inventory(
        *changed.values(),
        Item("owner", "function public.rpc(integer)", "intruder", "public"),
        Item(
            "default_acl", "r in all schemas created by postgres -> anon", "SELECT", "(all schemas)"
        ),
    )
    return expected, production


def test_access_differences_are_only_counted_by_default():
    expected, production = access_drift()

    hidden = report_for(expected, production, details=False)
    text = json.dumps(hidden) + drift.render_summary(hidden)

    assert hidden["summary"]["drift"] == 3
    assert hidden["drift"] == [
        {
            "kind": "default_acl",
            "schema": "(all schemas)",
            "status": UNEXPECTED,
            "count": 1,
            "redacted": True,
        },
        {"kind": "grant", "schema": "core", "status": CHANGED, "count": 1, "redacted": True},
        {"kind": "owner", "schema": "public", "status": UNEXPECTED, "count": 1, "redacted": True},
    ]
    # Nothing that names the object, the role, the privilege, or lets them be guessed.
    for secret in ("core.games", "anon", "intruder", "INSERT", "rpc", 'fingerprint": "'):
        assert secret not in text


def test_access_details_appear_only_when_asked_for():
    expected, production = access_drift()

    shown = report_for(expected, production, details=True)

    assert ("grant", "relation core.games -> anon") in keys(shown["drift"])
    assert "INSERT, SELECT" in json.dumps(shown)


def test_redacted_access_drift_cannot_be_proposed_for_acceptance():
    expected, production = access_drift()
    production |= inventory(fact("index", "core.extra", "x"))

    proposal = drift.propose_accepted(
        report_for(expected, production, details=False), {}, enforce=False
    )

    assert [item["key"] for item in proposal["accepted"]] == ["core.extra"]
    assert proposal["omitted_access_differences"] == 3


def test_accepted_differences_stop_counting_and_stale_ones_are_reported():
    production = inventory(fact("index", "core.extra", "definition"))
    (difference,) = drift.diff_inventories({}, production)
    accepted = {
        difference.fingerprint: entry(fingerprint=difference.fingerprint),
        "0123456789abcdef": entry(fingerprint="0123456789abcdef", key="core.fixed_since"),
    }

    report = report_for({}, production, accepted)

    assert {name: report["summary"][name] for name in ("drift", "accepted", "stale_accepted")} == {
        "drift": 0,
        "accepted": 1,
        "stale_accepted": 1,
    }
    assert report["stale_accepted"] == [accepted["0123456789abcdef"]]


def test_an_accepted_difference_that_changes_again_is_drift():
    first = drift.diff_inventories({}, inventory(fact("function", "public.f()", "body one")))[0]
    accepted = {first.fingerprint: entry(fingerprint=first.fingerprint, key="public.f()")}

    report = report_for({}, inventory(fact("function", "public.f()", "body two")), accepted)

    assert (report["summary"]["drift"], report["summary"]["stale_accepted"]) == (1, 1)


def test_summary_names_drift_and_caps_long_lists():
    production = inventory(*(fact("index", f"core.extra_{n:03d}", "x") for n in range(130)))

    text = drift.render_summary(report_for({}, production))

    assert "| Drift (not accepted) | 130 |" in text
    assert "`core.extra_000`" in text and "`core.extra_129`" not in text
    assert "30 more in the JSON report" in text


def write_accepted(tmp_path, document):
    path = tmp_path / "drift-accepted.json"
    path.write_text(json.dumps(document))
    return path


def entry(**overrides):
    base = {
        "fingerprint": "aaaaaaaaaaaaaaaa",
        "kind": "index",
        "key": "core.extra",
        "status": UNEXPECTED,
        "reason": "kept for the legacy consumer",
    }
    return base | overrides


def test_committed_accepted_file_is_valid():
    enforce, accepted = drift.load_accepted(drift.DEFAULT_ACCEPTED)

    assert isinstance(enforce, bool)
    assert all(item["reason"].strip() for item in accepted.values())


def test_a_missing_accepted_file_is_an_error_not_an_empty_list(tmp_path):
    with pytest.raises(drift.DriftCheckError, match="not readable JSON"):
        drift.load_accepted(tmp_path / "absent.json")


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ([], "'accepted' list"),
        ({"accepted": {}, "enforce": False}, "'accepted' list"),
        ({"accepted": [], "enforce": "yes"}, "must state 'enforce' as true or false"),
        # A dropped or misspelled key must not read as "do not enforce".
        ({"accepted": []}, "must state 'enforce' as true or false"),
        ({"accepted": [], "enforced": True}, "must state 'enforce' as true or false"),
        ({"accepted": [entry(reason="  ")], "enforce": False}, "nonempty strings"),
        ({"accepted": [entry(reason="TODO")], "enforce": False}, "placeholder reason"),
        ({"accepted": [entry(reason="  todo: explain")], "enforce": False}, "placeholder reason"),
        ({"accepted": [entry(), entry()], "enforce": False}, "repeats fingerprint"),
    ],
)
def test_malformed_accepted_files_are_rejected(tmp_path, document, message):
    with pytest.raises(drift.DriftCheckError, match=message):
        drift.load_accepted(write_accepted(tmp_path, document))


def test_proposal_keeps_reasons_and_marks_new_drift_for_review():
    production = inventory(fact("index", "core.kept", "a"), fact("index", "core.new", "b"))
    kept = next(d for d in drift.diff_inventories({}, production) if d.key == "core.kept")
    accepted = {kept.fingerprint: entry(fingerprint=kept.fingerprint, key="core.kept")}

    proposal = drift.propose_accepted(report_for({}, production, accepted), accepted, True)

    assert proposal["enforce"] is True
    assert {item["key"]: item["reason"] for item in proposal["accepted"]} == {
        "core.kept": "kept for the legacy consumer",
        "core.new": "TODO",
    }
    assert "omitted_access_differences" not in proposal


class Connection:
    def __init__(self, side):
        self.side = side
        self.closed = False

    def close(self):
        self.closed = True


@pytest.fixture
def canned(monkeypatch, tmp_path):
    """Run the CLI against two in-memory catalogs instead of two databases."""
    monkeypatch.setenv(drift.EXPECTED_ENV, LOCAL_URL)
    monkeypatch.setenv(drift.PRODUCTION_ENV, PRODUCTION_URL)
    catalogs = {"expected": {}, "production": {}}
    monkeypatch.setattr(drift, "connect_database", lambda url: Connection("expected"))
    monkeypatch.setattr(drift, "_connect_production", lambda url: Connection("production"))
    monkeypatch.setattr(
        drift, "capture_inventory", lambda conn: (catalogs[conn.side], FACTS | {"side": conn.side})
    )
    catalogs["accepted"] = write_accepted(tmp_path, {"enforce": False, "accepted": []})
    return catalogs


def test_report_only_run_exits_zero_and_writes_outputs(canned, tmp_path, capsys):
    canned["production"] = inventory(fact("index", "core.extra", "x"))
    report, proposal = tmp_path / "report.json", tmp_path / "proposed.json"
    summary = tmp_path / "summary.md"
    summary.write_text("earlier step\n")

    code = drift.main(
        [
            *("--accepted", str(canned["accepted"])),
            *("--report", str(report), "--summary", str(summary)),
            *("--propose-accepted", str(proposal)),
        ]
    )

    assert code == 0
    assert json.loads(report.read_text())["summary"]["drift"] == 1
    assert summary.read_text().startswith("earlier step\n### Warehouse catalog drift")
    assert json.loads(proposal.read_text())["accepted"][0]["reason"] == "TODO"
    assert "core.extra" in capsys.readouterr().out


@pytest.mark.parametrize("switch", ["flag", "file"])
def test_enforcement_fails_on_drift_and_on_stale_acceptances(canned, tmp_path, switch):
    flags = ["--enforce"] if switch == "flag" else []

    def run(accepted):
        path = write_accepted(tmp_path, {"enforce": switch == "file", "accepted": accepted})
        return drift.main(["--accepted", str(path), *flags])

    canned["production"] = inventory(fact("index", "core.extra", "x"))
    assert run([]) == 1

    canned["production"] = {}
    assert run([entry()]) == 1
    assert run([]) == 0


def test_redacted_access_drift_still_fails_an_enforced_run(canned):
    canned["expected"], canned["production"] = access_drift()

    assert drift.main(["--accepted", str(canned["accepted"]), "--enforce"]) == 1


def test_a_missing_accepted_file_stops_the_run(canned, tmp_path, capsys):
    assert drift.main(["--accepted", str(tmp_path / "renamed.json")]) == 2
    assert "not readable JSON" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("expected_url", "production_url", "message"),
    [
        (None, PRODUCTION_URL, "both required"),
        (LOCAL_URL, None, "both required"),
        (LOCAL_URL, LOCAL_URL, "identical"),
        (PRODUCTION_URL, LOCAL_URL, "disposable loopback database"),
    ],
)
def test_unsafe_configuration_is_refused_before_connecting(
    monkeypatch, capsys, expected_url, production_url, message
):
    for name, value in ((drift.EXPECTED_ENV, expected_url), (drift.PRODUCTION_ENV, production_url)):
        monkeypatch.delenv(name, raising=False)
        if value:
            monkeypatch.setenv(name, value)
    monkeypatch.setattr(drift, "connect_database", pytest.fail)
    monkeypatch.setattr(drift, "_connect_production", pytest.fail)

    assert drift.main([]) == 2
    assert message in capsys.readouterr().err


def test_connection_failure_exits_two_without_naming_the_database(canned, monkeypatch, capsys):
    def refuse(url):
        # The shape of a real libpq failure: no URL, but the host and its address.
        raise RuntimeError(
            'connection to server at "db.example" (203.0.113.7), port 5432 failed: '
            'FATAL:  password authentication failed for user "reader" password=prod-secret'
        )

    monkeypatch.setattr(drift, "_connect_production", refuse)

    assert drift.main(["--accepted", str(canned["accepted"]), "--enforce"]) == 2
    printed = capsys.readouterr().err
    assert "could not read the production catalog" in printed
    for secret in ("prod-secret", "db.example", "203.0.113.7", "reader"):
        assert secret not in printed


def test_capture_retries_only_when_a_lock_was_unavailable(monkeypatch):
    from psycopg2 import errors

    outcomes, opened, naps = [], [], []

    def capture(conn):
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def connect():
        opened.append(Connection("production"))
        return opened[-1]

    def attempt():
        return drift._capture_with_retry(connect, sleep=naps.append)

    monkeypatch.setattr(drift, "capture_inventory", capture)

    outcomes[:] = [errors.LockNotAvailable("busy"), errors.LockNotAvailable("busy"), ({}, FACTS)]
    assert attempt() == ({}, FACTS)
    assert len(opened) == 3 and naps == [drift.RETRY_SECONDS] * 2

    outcomes[:] = [errors.LockNotAvailable("busy")] * drift.CAPTURE_ATTEMPTS
    naps.clear()
    with pytest.raises(errors.LockNotAvailable):
        attempt()
    # It gives up straight after the last attempt instead of sleeping once more.
    assert naps == [drift.RETRY_SECONDS] * (drift.CAPTURE_ATTEMPTS - 1)

    outcomes[:] = [errors.UndefinedTable("gone"), ({}, FACTS)]
    with pytest.raises(errors.UndefinedTable):
        attempt()
    assert len(outcomes) == 1
    assert all(conn.closed for conn in opened)


@pytest.mark.parametrize(
    "column",
    [
        "bigint; default 0",
        "bigint; generated=s",
        "public.sneaky_domain",
        "text; not null; default 1",
    ],
)
def test_a_new_dlt_table_with_a_column_dlt_would_not_create_is_drift(column):
    expected = inventory(*dlt_table("core.games", "id"))
    crafted = fact("column", "core.games__x.v", column, "core.games__x")
    production = inventory(*dlt_table("core.games", "id"), *dlt_table("core.games__x"), crafted)

    report = report_for(expected, production)

    assert report["summary"]["ingest_evolved"] == 0
    assert ("relation", "core.games__x") in keys(report["drift"])


def test_a_list_partition_of_a_dlt_table_inherits_its_parents_columns():
    generated = "integer; default (a - b); generated=s"
    parent = [
        *dlt_table("core.plays", "id", shape="partitioned table; partition by LIST (season)"),
        fact("column", "core.plays.score_diff", generated, "core.plays"),
    ]
    shape = "table; partition of core.plays; bound FOR VALUES IN ('2027')"
    child = [
        *dlt_table("core.plays_y2027", "id", shape=shape),
        fact("column", "core.plays_y2027.score_diff", generated, "core.plays_y2027"),
    ]

    report = report_for(inventory(*parent), inventory(*parent, *child))

    assert report["summary"]["drift"] == 0
    assert ("relation", "core.plays_y2027") in keys(report["ingest_evolved"])

    # The same partition with a column its parent does not have is not dlt's.
    odd = fact("column", "core.plays_y2027.extra", "bigint; default 0", "core.plays_y2027")
    report = report_for(inventory(*parent), inventory(*parent, *child, odd))
    assert ("relation", "core.plays_y2027") in keys(report["drift"])


def test_a_table_made_to_look_like_dlt_only_in_production_gets_no_free_columns():
    # core is a dlt schema on both sides; only core.manual was dressed up.
    games = dlt_table("core.games", "id")
    expected = inventory(*games, *relation("core.manual", "id"))
    production = inventory(*games, *dlt_table("core.manual", "id", "extra"))

    report = report_for(expected, production)

    assert report["summary"]["ingest_evolved"] == 0
    assert keys(report["drift"]) == {
        ("column", "core.manual._dlt_id"),
        ("column", "core.manual.extra"),
    }


def test_the_access_of_a_table_dlt_added_is_withheld_too():
    expected = inventory(*dlt_table("core.games", "id"))
    production = inventory(*dlt_table("core.games", "id"), *dlt_table("core.games__notes", "v"))

    hidden = report_for(expected, production, details=False)

    assert hidden["summary"]["drift"] == 0
    assert [entry for entry in hidden["ingest_evolved"] if "key" in entry] == [
        {
            "kind": "relation",
            "key": "core.games__notes",
            "status": UNEXPECTED,
            "fingerprint": hidden["ingest_evolved"][0]["fingerprint"],
            "expected": None,
            "production": hidden["ingest_evolved"][0]["production"],
        }
    ]
    assert "anon" not in json.dumps(hidden)


def test_production_function_and_view_text_is_withheld_unless_asked_for():
    secret = "SELECT net.http_post(headers := 'Bearer live-token')"
    expected = inventory(fact("function", "public.notify()", "SELECT 1"))
    production = inventory(
        fact("function", "public.notify()", secret),
        fact("definition", "api.extra", secret, "api.extra"),
        fact("relation", "api.extra", "view", "api.extra"),
    )

    hidden = report_for(expected, production, details=False)
    shown = report_for(expected, production, details=True)

    assert "live-token" not in json.dumps(hidden) + drift.render_summary(hidden)
    changed = next(entry for entry in hidden["drift"] if entry["kind"] == "function")
    assert changed["expected"] == "SELECT 1"
    assert changed["production"].startswith(f"[withheld: {len(secret)} characters, sha256 ")
    assert "live-token" in json.dumps(shown)


def test_an_accepted_entry_must_describe_the_difference_it_matches():
    production = inventory(fact("function", "public.definer()", "SECURITY DEFINER body"))
    (difference,) = drift.diff_inventories({}, production)
    disguised = {difference.fingerprint: entry(fingerprint=difference.fingerprint)}

    with pytest.raises(drift.DriftCheckError, match="not what its fingerprint identifies"):
        report_for({}, production, disguised)


@pytest.mark.parametrize(
    ("kind", "key", "relation_name"),
    [
        ("trigger", "core.games.notify", "core.games"),
        ("index", "core.games_partial_idx", "core.games"),
        ("constraint", "core.games.token_check", "core.games"),
        ("definition", "api.games", "api.games"),
        ("function", "public.notify()", None),
    ],
)
def test_production_sql_of_every_deparsed_kind_is_withheld_by_default(kind, key, relation_name):
    secret = "EXECUTE FUNCTION net.http_request('Bearer live-token')"
    expected = inventory(fact(kind, key, "the reviewed definition", relation_name))
    production = inventory(fact(kind, key, secret, relation_name))

    hidden = report_for(expected, production, details=False)
    (changed,) = hidden["drift"]

    assert "live-token" not in json.dumps(hidden) + drift.render_summary(hidden)
    assert changed["expected"] == "the reviewed definition"
    assert changed["production"].startswith(f"[withheld: {len(secret)} characters, sha256 ")
    assert "live-token" in json.dumps(report_for(expected, production, details=True))


def test_a_column_default_is_withheld_but_a_bare_column_is_shown():
    expected = inventory(*relation("meta.jobs", "id"))
    production = inventory(
        *relation("meta.jobs", "id"),
        fact("column", "meta.jobs.token", "text; default 'live-token'::text", "meta.jobs"),
        fact("column", "meta.jobs.attempts", "integer; not null", "meta.jobs"),
    )

    hidden = report_for(expected, production, details=False)
    values = {entry["key"]: entry["production"] for entry in hidden["drift"]}

    assert "live-token" not in json.dumps(hidden)
    assert values["meta.jobs.token"].startswith("[withheld: ")
    assert values["meta.jobs.attempts"] == "integer; not null"


def order(name: str, *columns: str) -> Item:
    return fact("column_order", name, json.dumps(columns), name)


def test_reordered_columns_are_drift_but_appended_ones_are_not_a_reorder():
    expected = inventory(*relation("meta.jobs", "id", "name"), order("meta.jobs", "id", "name"))
    appended = inventory(
        *relation("meta.jobs", "id", "name", "extra"), order("meta.jobs", "id", "name", "extra")
    )
    swapped = inventory(*relation("meta.jobs", "id", "name"), order("meta.jobs", "name", "id"))
    swapped_and_trimmed = inventory(
        *relation("meta.jobs", "id", "name", "extra"), order("meta.jobs", "extra", "name", "id")
    )

    assert keys(report_for(expected, appended)["drift"]) == {("column", "meta.jobs.extra")}
    assert keys(report_for(expected, swapped)["drift"]) == {("column_order", "meta.jobs")}
    assert ("column_order", "meta.jobs") in keys(report_for(expected, swapped_and_trimmed)["drift"])


def test_a_dlt_column_does_not_make_its_table_look_reordered():
    expected = inventory(*dlt_table("core.games", "id"), order("core.games", "id", "_dlt_id"))
    production = inventory(
        *dlt_table("core.games", "id", "new_field"),
        order("core.games", "id", "_dlt_id", "new_field"),
        *dlt_table("core.games__notes", "v"),
        order("core.games__notes", "v", "_dlt_id"),
    )

    report = report_for(expected, production)

    assert report["summary"]["drift"] == 0
    assert ("relation", "core.games__notes") in keys(report["ingest_evolved"])


def test_a_disabled_or_missing_event_trigger_is_drift():
    def trigger(name, enabled="O"):
        value = f"on ddl_command_end when tag in () execute meta.invalidate() [enabled={enabled}]"
        return Item("event_trigger", name, value, "meta")

    expected = inventory(trigger("invalidate_ddl"), trigger("invalidate_drop"))
    production = inventory(trigger("invalidate_ddl", enabled="D"))

    report = report_for(expected, production)

    assert {entry["key"]: entry["status"] for entry in report["drift"]} == {
        "invalidate_ddl": CHANGED,
        "invalidate_drop": MISSING,
    }
