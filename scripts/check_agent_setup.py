#!/usr/bin/env python3
"""Check portable instruction wiring without a harness, network, or credentials."""

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path

CORE_ROLES = {"explorer", "implementer", "reviewer", "modeling-scientist"}
READ_ONLY = {"explorer", "reviewer", "schema-architect"}
PROJECT_REF = re.compile(r"(?:\.agents|\.claude|\.codex|docs|scripts)/[\w./-]+\.(?:md|py|sh|toml)")
ROLE_REF = re.compile(r"\.agents/roles/([\w-]+)\.md")


def frontmatter(path: Path) -> tuple[dict[str, str], str]:
    """Read top-level string fields used by our adapters; not a general YAML parser."""
    text = path.read_text()
    parts = text.split("---", 2)
    if len(parts) != 3 or parts[0].strip():
        raise ValueError(f"{path}: missing frontmatter")
    fields = {}
    for line in parts[1].splitlines():
        if not line or line[0].isspace() or ":" not in line:
            continue
        key, value = line.split(":", 1)
        value = value.strip()
        if value.startswith('"'):
            value = json.loads(value)
        fields[key] = value
    for key in ("name", "description"):
        if not fields.get(key):
            raise ValueError(f"{path}: missing {key}")
    return fields, parts[2]


def check(root: Path) -> list[str]:
    errors = []

    def require(condition: bool, message: str) -> None:
        if not condition:
            errors.append(message)

    def references(path: Path, text: str) -> None:
        for target in set(PROJECT_REF.findall(text)):
            require((root / target).is_file(), f"{path}: missing reference {target}")

    agreement = root / "AGENTS.md"
    claude = root / "CLAUDE.md"
    require("@AGENTS.md" in claude.read_text().splitlines(), "CLAUDE.md must import AGENTS.md")
    references(agreement, agreement.read_text())
    references(claude, claude.read_text())

    skills = root / ".agents/skills"
    names = set()
    for folder in sorted(skills.iterdir()):
        if not folder.is_dir():
            continue
        entry = folder / "SKILL.md"
        fields, body = frontmatter(entry)
        require(fields["name"] == folder.name, f"{entry}: name differs from directory")
        require(fields["name"] not in names, f"{entry}: duplicate skill name")
        names.add(fields["name"])
        link = root / ".claude/skills" / folder.name
        require(link.is_symlink(), f"{link}: expected a link to the canonical skill")
        if link.is_symlink():
            require(not link.readlink().is_absolute(), f"{link}: absolute link is not portable")
            require(link.resolve() == folder.resolve(), f"{link}: wrong or broken skill target")
        references(entry, body)
        for target in re.findall(r"\]\((references/[^)#]+)(?:#[^)]*)?\)", body):
            require((folder / target).is_file(), f"{entry}: missing skill reference {target}")
    require(
        {"cfbd-api", "dlt-pipelines", "schema-migrations", "supabase-postgres-best-practices"}
        <= names,
        "Missing a required project skill",
    )

    shared = {p.stem for p in (root / ".agents/roles").glob("*.md")}
    require(CORE_ROLES <= shared, "Missing a shared core role")
    for path in (root / ".agents/roles").glob("*.md"):
        references(path, path.read_text())

    config = tomllib.loads((root / ".codex/config.toml").read_text())
    require("model" not in config, "Project config must preserve the user's lead model")
    agents = config.get("agents", {})
    require(agents.get("enabled") is True, "Codex subagents must be enabled")
    require(agents.get("max_concurrent_threads_per_session") == 3, "Codex worker cap must be 3")
    native_roles = {"codex": set(), "claude": set()}
    for path in (root / ".codex/agents").glob("*.toml"):
        data = tomllib.loads(path.read_text())
        for key in (
            "name",
            "description",
            "model",
            "model_reasoning_effort",
            "developer_instructions",
        ):
            require(bool(data.get(key)), f"{path}: missing {key}")
        require(data.get("name") == path.stem, f"{path}: name differs from filename")
        body = data.get("developer_instructions", "")
        targets = ROLE_REF.findall(body)
        require(len(targets) == 1, f"{path}: reference exactly one shared role")
        if targets:
            native_roles["codex"].add(targets[0])
            require(targets[0] == path.stem.replace("_", "-"), f"{path}: wrong role contract")
        if path.stem in READ_ONLY:
            require(data.get("sandbox_mode") == "read-only", f"{path}: must remain read-only")
        references(path, body)
    for path in (root / ".claude/agents").glob("*.md"):
        fields, body = frontmatter(path)
        require(fields["name"] == path.stem, f"{path}: name differs from filename")
        require(bool(fields.get("model")), f"{path}: missing explicit worker model")
        targets = ROLE_REF.findall(body)
        expected = {"pipeline-engineer": "implementer", "schema-architect": "reviewer"}.get(
            path.stem, path.stem
        )
        require(targets == [expected], f"{path}: wrong shared role contract")
        native_roles["claude"].update(targets)
        if path.stem in READ_ONLY:
            tools = {item.strip() for item in fields.get("tools", "").split(",")}
            require(bool(tools) and tools <= {"Read", "Grep", "Glob"}, f"{path}: read-only tools")
        references(path, body)
    for harness, roles in native_roles.items():
        require(CORE_ROLES <= roles, f"{harness}: missing a core role adapter")

    settings = json.loads((root / ".claude/settings.json").read_text())
    hooks = settings.get("hooks", {}).get("SessionStart", [])
    expected_command = 'bash "$CLAUDE_PROJECT_DIR/scripts/claude_cloud_setup.sh"'
    require(
        any(
            entry.get("matcher") == "startup|resume"
            and any(h.get("command") == expected_command for h in entry.get("hooks", []))
            for entry in hooks
        ),
        "Claude cloud startup/resume hook must use the shared setup wrapper",
    )
    for target in (
        "scripts/setup_dev.sh",
        "scripts/claude_cloud_setup.sh",
        "docs/modeling-contract.md",
    ):
        require((root / target).is_file(), f"Missing {target}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    try:
        errors = check(args.root.resolve())
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors = [str(exc)]
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("Agent setup wiring passed: shared instructions, skills, roles, and cloud hook.")
    print("Host model availability, cloud settings, and live integrations require separate checks.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
