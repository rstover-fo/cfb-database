# cfb-mcp

A read-only [MCP](https://modelcontextprotocol.io) server over the `cfb-database` Supabase
warehouse. Local **stdio** transport, built with the official Python `mcp` SDK (FastMCP
style). It exposes eight LLM-facing tools covering teams, games, matchups, rankings,
leaderboards, situational splits, player search, and data freshness.

## What this is (and isn't)

- **Data path: PostgREST over HTTPS only.** Every tool call is either a
  `GET {SUPABASE_URL}/rest/v1/<view>` (for `api.*` views, `Accept-Profile: api`) or a
  `POST {SUPABASE_URL}/rest/v1/rpc/<fn>` (for `public.*` RPCs, `Content-Profile: public`).
  There is **no direct Postgres connection** anywhere in this server.
- **No dynamic-SQL tool.** See [SQL tool deferral](#sql-tool-deferral-p35) below.
- **Contract Rule 4.** Per `docs/SCHEMA_CONTRACT.md`, downstream consumers must never touch
  raw tables directly -- only `api.*` views and the documented `public` RPCs. This server
  enforces that by construction: `postgrest.py` only ever builds URLs under
  `/rest/v1/<view>` or `/rest/v1/rpc/<fn>`, and every view/RPC name used by a tool is one
  from the Schema Contract's public surface. If a future contract change renames or removes
  one of those objects, the corresponding tool will start returning a `404` error from
  PostgREST -- see [RLS / permissions posture](#rls--permissions-posture) for why that fails
  safe rather than falling back to a broader query.
- **Hard row cap.** Every read is capped at 100 rows server-side (`postgrest.DEFAULT_ROW_CAP`).
  Tools that accept a `limit` argument can request fewer rows, never more.

## Install

```bash
cd mcp
pip install -e ".[dev]"
```

This installs the `cfb_mcp` package and its dev dependencies (`pytest`, `pytest-asyncio`,
`respx`, `ruff`) from `mcp/pyproject.toml`. Verify with:

```bash
python -c "import cfb_mcp"
```

## Environment variables

Copy `mcp/.env.example` to `.env` (or otherwise export the variables) and fill in:

| Variable | Description |
|----------|-------------|
| `SUPABASE_URL` | Your Supabase project URL, e.g. `https://xxxx.supabase.co` |
| `SUPABASE_ANON_KEY` | The **anon** (public) API key, from Supabase Dashboard > Project Settings > API. **Not** the `service_role` key. |

The server reads these from the process environment at request time (no `.env` autoloading
in-process) -- for local testing you can `export` them, and for MCP-client integrations
(below) you supply them via the client's server config `env` block.

## RLS / permissions posture

`cfb-database` does **not** use Row Level Security policies for read access. Instead, read-only
is enforced at the grant level (see
[`docs/solutions/database-issues/security-invoker-schema-grants.md`](../docs/solutions/database-issues/security-invoker-schema-grants.md)):
the `anon` role has `USAGE` + `SELECT` granted on every schema the `api.*` views read from,
and `INSERT`/`UPDATE`/`DELETE`/`TRUNCATE` are `REVOKE`d from `anon`/`authenticated` everywhere.
`api.*` views are `SECURITY INVOKER`, so they execute as `anon` and are bound by those grants.

This means: **the anon key is the intended trust model for this server.** It cannot write
regardless of which query it sends (writes fail at the database grant level, not just
because this server declines to expose write tools), and it can read exactly the public
surface documented in `SCHEMA_CONTRACT.md`. There's no separate secrets tier to protect here
-- treat `SUPABASE_ANON_KEY` as a low-sensitivity, read-only credential, but still don't
commit it (`.env` is gitignored; only `.env.example` is checked in).

## SQL tool deferral (P3.5)

An earlier design considered a general "run arbitrary read-only SQL" tool for maximum
flexibility. It was **rejected for v1**: the `anon` role (see above) has `SELECT` on every
schema in the warehouse, including internal ones (`core`, `stats`, `ratings`, `betting`,
`recruiting`, `metrics`, dlt pipeline metadata, etc. -- see the "Internal" section of
`SCHEMA_CONTRACT.md`). A raw-SQL tool using the anon key cannot be confined to the `api.*` /
public-RPC surface the way PostgREST's REST endpoints naturally are, so it would silently
violate Contract Rule 4 and expose internal, unstable objects to the model. Building that
safely needs either a dedicated read-only Postgres role scoped to `api`/allowed `public`
objects only, or a query-rewriting/allowlisting layer -- both out of scope here. Deferred to
a future phase (tracked as P3.5); this server instead ships eight purpose-built tools that
each hit one known-safe view or RPC.

## Tool catalog

| Tool | Arguments | Backing object(s) |
|------|-----------|--------------------|
| `query_team` | `team` | `api.team_detail`, `api.team_history` |
| `query_games` | `season?`, `week?`, `team?`, `min_excitement?`, `limit?` | `api.game_detail` |
| `query_matchup` | `team_a`, `team_b` | `api.matchup` |
| `get_rankings` | `season`, `week?`, `poll?`, `season_type?` (`regular`\|`postseason`), `limit?` | `api.poll_rankings` |
| `get_leaderboard` | `season`, `metric` (`wins`\|`ppg`\|`scoring_defense`\|`epa`\|`sp_rating`\|`wepa`), `limit?` | `api.leaderboard_teams`, `api.team_wepa_season` (for `wepa`) |
| `situational_splits` | `team`, `season`, `split_type` (`home_away`\|`conference`\|`red_zone`\|`down_distance`\|`field_position`) | `get_home_away_splits`, `get_conference_splits`, `get_red_zone_splits`, `get_down_distance_splits`, `get_field_position_splits` (all `public` RPCs) |
| `search_players` | `query`, `team?`, `season?`, `limit?` | `get_player_search` then `get_player_detail` for the top hit (`public` RPCs) |
| `get_data_freshness` | *(none)* | `get_data_freshness` (`public` RPC) |

Every tool response is JSON with a `_source` field (or one per sub-object) naming the exact
view/RPC used, so downstream consumers of the tool output can qualify claims with their
provenance.

## Claude Desktop configuration

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cfb": {
      "command": "python",
      "args": ["-m", "cfb_mcp"],
      "env": {
        "SUPABASE_URL": "https://[PROJECT-REF].supabase.co",
        "SUPABASE_ANON_KEY": "[YOUR-ANON-KEY]"
      }
    }
  }
}
```

Equivalent with the `claude` CLI:

```bash
claude mcp add cfb \
  --env SUPABASE_URL=https://[PROJECT-REF].supabase.co \
  --env SUPABASE_ANON_KEY=[YOUR-ANON-KEY] \
  -- python -m cfb_mcp
```

(If you installed with `pip install -e ".[dev]"` inside a virtualenv, point `command` at
that venv's `python`, or use the `cfb-mcp` console script installed alongside it.)

## Smoke-test prompts

Once connected, try these against your Claude client to sanity-check the server end-to-end:

1. **"How is Oklahoma doing this season, and how have they trended over the last five
   years?"** -- exercises `query_team`.
2. **"Who was ranked in the AP Top 25 in week 10 of the 2024 season, and how does that
   compare to the final CFP rankings that year?"** -- exercises `get_rankings` with both
   `season_type='regular'` and `season_type='postseason'`.
3. **"Find the player Caleb Williams, and tell me about Oklahoma's red zone efficiency in
   2022."** -- exercises `search_players` and `situational_splits` in the same conversation.

## Development

```bash
cd mcp
pip install -e ".[dev]"
ruff check .
ruff format --check .
pytest tests -q
```

Tests use [`respx`](https://lundberg.pro/respx/) to mock every PostgREST call -- there is no
live network access in the test suite (and the sandbox's outbound proxy blocks
`*.supabase.co` regardless).
