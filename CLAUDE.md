@AGENTS.md

# Claude Code adapter

Use the shared agreement above in local and cloud sessions. Project agents in
`.claude/agents/` provide Claude model choices and link to `.agents/roles/`.
The skill directories here link to the canonical `.agents/skills/` copies.

Keep the user's lead model. Use the task's relevant roles; pipeline-engineer
and schema-architect remain compatibility names for scoped implementation and
SQL review. Shared methodology lives in `docs/modeling-contract.md`.

For environment setup or missing capabilities, read `docs/agent-setup.md`.
