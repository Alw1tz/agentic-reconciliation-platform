# ez_agent_meets_mcp

Tier 1, project 6 -- the last one in this tier. An agent whose tools are
*discovered from an MCP server*, not hardcoded like `ez_agent_tool_loop`'s
`LOCAL_TOOLS` dict.

## What it does

Reuses `projects/ez_mcp_basics/server.py` directly (the toy notes server)
instead of building a new one. The flow:

1. Connect to the MCP server, same as `ez_mcp_basics/client.py`.
2. `list_tools()` -- discover `add_note`/`list_notes`/`clear_notes` at
   runtime, with no tool names hardcoded anywhere in this file.
3. Convert each MCP tool's schema into the shape the Anthropic API
   expects (`mcp_tool_to_anthropic_schema` -- turns out to just be a
   rename of fields, not a redesign).
4. Hand those to the same `run_tool_loop()` from `ez_agent_tool_loop`,
   with a `dispatch` that calls `session.call_tool()` on the real MCP
   session instead of looking up a local Python function.

Given the instruction "add a note, add another note, then list them back,"
Claude has to call three tools in sequence against the live MCP server.

This is what `agents/base/agent.py`'s `call_mcp_tool()` is meant to enable
in the real pipeline -- `agents/minimal/core_agent.py` currently *fakes*
this with hardcoded simulated responses; this project does the real
version.

## Run it

```bash
uv run python3 projects/ez_agent_meets_mcp/agent.py
```

Launches `ez_mcp_basics/server.py` itself as a subprocess -- no separate
setup needed. Uses `ANTHROPIC_API_KEY` from `.env`.

## What to look for

- "Discovered 3 tool(s) ..." printed before any tool calls happen --
  confirms the tool list came from the server, not from this file.
- Three `[tool call]` / `[tool result]` pairs: two `add_note` calls, then
  one `list_notes` call whose result includes both notes just added --
  proof state is round-tripping through the real MCP server, not being
  faked locally.
- Compare `dispatch()` here to `ez_agent_tool_loop`'s: same signature,
  same shared loop calling it, but this one calls `session.call_tool()`
  instead of a dict lookup -- that's the entire difference between "agent
  with hardcoded tools" and "agent with MCP-discovered tools."
