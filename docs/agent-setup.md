# Agent setup across harnesses

The repository supplies the same working agreement, skills, domain contracts,
and Python development environment to Codex and Claude Code. Model availability,
sandbox enforcement, account credentials, and cloud settings belong to the host;
they are not made identical by committing configuration files.

## Instruction sources

| Layer | Canonical source | How it loads |
|---|---|---|
| Working agreement | `AGENTS.md` | Codex discovers it; Claude imports it from `CLAUDE.md` |
| Domain skills | `.agents/skills/*/SKILL.md` | Codex scans this directory; `.claude/skills/*` are relative symlinks to it |
| Worker behavior | `.agents/roles/*.md` | Both native adapters tell workers to read the matching contract |
| Models and capabilities | `.codex/agents/*.toml`, `.claude/agents/*.md` | Harness-specific settings; the lead model stays user-selected |
| Statistical methodology | `docs/modeling-contract.md` | Read for governed feature/model work |
| Setup and checks | `scripts/setup_dev.sh`, `scripts/check_agent_setup.py` | Same commands on macOS/Linux, in CI, and cloud checkouts |

Keep task-specific knowledge in skills/references. Do not add incident narratives,
live row counts, or model versions to the always-loaded working agreement.
Relative links keep worktrees and fresh clones independent of a user's home
directory. Historical `.claude/skills/` paths remain usable through the links.

The four shared roles are explorer, implementer, reviewer, and modeling-scientist.
Claude's pipeline-engineer and schema-architect are compatibility specializations
of implementer and reviewer. Codex names modeling-scientist `modeling_scientist`.
Exact models live in each adapter; they are provider-specific choices for the
same responsibilities, not claims that models are interchangeable.

Codex limits workers in `.codex/config.toml`; AGENTS.md supplies the same
three-worker ceiling for other harnesses. Claude read-only roles have only
Read/Grep/Glob tools; provide a diff in their handoff if git inspection is needed.
Codex uses a read-only sandbox for those roles. A generic spawning API may not
enforce these native restrictions: pass the role instructions explicitly, honor
the available permissions, and keep mutations with an authorized implementer.
If native subagents or a requested model are unavailable, the lead follows the
same contract sequentially and reports the reduced capability.

## Local setup

From the repository root, with Python 3.11+ available:

```bash
bash scripts/setup_dev.sh
.venv/bin/python scripts/check_agent_setup.py
PATH="$PWD/.venv/bin:$PATH" .venv/bin/python -m pytest -q --tb=short
.venv/bin/python -m pytest mcp/tests -q --tb=short
```

The setup script installs both editable packages and their development extras.
It can run again after dependency changes. It does not install a harness, copy
credentials, change global instructions, configure git hooks, or load data.
`CFB_VENV_DIR` can select another environment for validation; normal task commands
use `.venv`. Dependencies currently use the ranges in the two pyproject files;
this aligns installation inputs, not a lockfile guaranteeing identical resolves.

Repository config does not override organization policy or an explicit session
selection. Codex needs a trusted checkout to use project configuration. This
setup was introduced with Codex 0.153.3; older 0.133.0 rejected its agents settings.
Use a client that accepts the current config rather than enabling trust globally.
After instruction changes start a fresh session to verify discovery. For Claude,
`/context` shows memory files and `claude plugin validate .claude/agents` checks
agent definitions in current clients; for Codex, ask which project instructions
and skills are available. Verify actual model
selection from the runtime, since user/admin overrides can change it.

## Cloud setup

Cloud sessions need the branch containing these files available to their checkout.
Uncommitted local changes and user-level skills/settings do not establish cloud
parity. Launch from the repository root, or include it in the harness's project
instruction discovery path.

For **Codex cloud**, configure the environment's setup script as:

```bash
bash scripts/setup_dev.sh
.venv/bin/python scripts/check_agent_setup.py
```

Use the same commands for its maintenance script so cached environments pick up
dependency changes. Select Python 3.11 or newer in the environment. Setup needs
package-registry access; ordinary code/tests do not need warehouse credentials.
Setup-shell exports do not persist into the agent phase, which is why commands
use `.venv/bin` explicitly. Codex cloud secrets are available to setup scripts
only; do not copy them into files to extend their lifetime.

For **Claude Code cloud**, the committed `.claude/settings.json` SessionStart
hook invokes `scripts/claude_cloud_setup.sh` on startup/resume. It installs project
dependencies only when `CLAUDE_CODE_REMOTE=true`. Local starts skip installation.
Allow package registries in the cloud environment; this hook does not change the
environment's network policy. An environment setup script can also run the shared
setup command; the hook ensures a later checkout still installs its dependencies.

These files prepare the repository; account environment settings and hosted
execution must be checked separately. A portable checkout test is useful evidence
for paths/dependencies, but is not a hosted Codex or Claude execution test.

## Verification and live access

The main test suite's `db_conn` fixture skips by default even if credentials exist.
For an explicitly authorized database target, add `--live-db`; its connection
uses `SUPABASE_DB_URL` or `.dlt/secrets.toml` and is **autocommit**, not a disposable
fixture database. Missing credentials still skip. Report those skips. The MCP
tests use mocked HTTP; main unit/source tests use fixtures and mocks. Keep new
live integrations opt-in as well.

GitHub CI explicitly opts its existing integration job into `--live-db` with the
repository's configured secret. Local pre-push checks stay offline. A missing
credential or connector does not prevent implementation, static SQL review, or
offline tests; it limits what can be claimed about deployed behavior.

The existing `.mcp.json` config is Claude's optional Supabase connection and uses
an externally supplied `SUPABASE_ACCESS_TOKEN`. Desktop plugins, MCP logins, and
cloud permissions are host-specific. Do not commit tokens or assume a local
connector exists in another harness. Database/API operations still require an
authorized target and the appropriate credentials/network access.

## Maintaining parity

Run `.venv/bin/python scripts/check_agent_setup.py` after changing instructions. CI checks
relative skill links, matching skill identities, shared role references, required
adapter metadata, the Claude import, and the cloud hook. Keep this check structural;
validate behavioral changes with realistic tasks and independent review rather
than tests that assert exact prompt wording.

Useful smoke tasks are a documentation typo (no domain workflow), an API retry
bug (CFBD/pipeline guidance), an exposed-view change (schema/caller contract), and
a new feature screen (modeling pre-registration). Inspect chosen skills and scope.

## Upstream references

- [Codex skills and symlink discovery](https://learn.chatgpt.com/docs/build-skills)
- [Codex project instructions](https://learn.chatgpt.com/docs/agent-configuration/agents-md)
- [Codex cloud setup and maintenance](https://learn.chatgpt.com/docs/environments/cloud-environment)
- [Claude importing AGENTS.md](https://code.claude.com/docs/en/memory#agentsmd)
- [Claude custom subagents](https://code.claude.com/docs/en/sub-agents)
- [Claude cloud project hooks](https://code.claude.com/docs/en/cloud-environments#install-dependencies-with-a-sessionstart-hook)
