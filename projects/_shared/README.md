# _shared

Small generic helpers reused across more than one project in this
curriculum, factored out instead of copy-pasted:

- `tool_loop.py` -- `run_tool_loop()`, the Claude tool-use loop mechanics
  (send tools, execute a requested call, send the result back, repeat).
  Used by `ez_agent_tool_loop` (local Python function tools) and
  `ez_agent_meets_mcp` (real MCP server tools) -- same loop, different
  `dispatch` callback.
- `connections.py` -- `get_snowflake_connection()` / `get_s3_client()`,
  the same env-var wiring already used in `mcp_servers/snowflake_server.py`
  and `test_connections.py`. Used by `ez_snowflake_s3_etl` now, and by
  `top_dynamic_etl_pipeline` later.

Not a package meant to be `pip install`-ed -- consuming scripts add the
repo root to `sys.path` at import time (see any consuming project's
`README.md` for why) and import it as `projects._shared.<module>`.
