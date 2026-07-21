"""cfb_mcp: read-only MCP server for the cfb-database Supabase warehouse.

This package talks to the warehouse exclusively over PostgREST (HTTPS) --
never direct Postgres -- and only touches objects listed as public in
docs/SCHEMA_CONTRACT.md of the cfb-database repo (api.* views and a fixed
set of public RPCs). See mcp/README.md for setup and the tool catalog.
"""

__version__ = "0.1.0"
