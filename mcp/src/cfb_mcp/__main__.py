"""Entry point for the cfb-mcp stdio server.

Run with `python -m cfb_mcp` or the `cfb-mcp` console script installed by
`pip install -e mcp[dev]`. Requires SUPABASE_URL and SUPABASE_ANON_KEY to be
set in the environment (see mcp/.env.example).
"""

from cfb_mcp.server import mcp


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
