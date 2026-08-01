# ez_mcp_basics

The first project in the curriculum (see `../README.md`). Raw MCP protocol,
no LLM anywhere in the picture.

## What MCP is

The Model Context Protocol is a client/server protocol for exposing "tools"
(functions with a name, a description, and a JSON Schema for their
arguments) to whatever is on the other end of the connection. In this repo's
real pipeline, that "whatever" is a Claude agent. Here, it's just a plain
Python script printing what it sees -- so the protocol mechanics aren't
tangled up with agent reasoning.

## Files

- `server.py` -- an MCP server exposing three tools (`add_note`,
  `list_notes`, `clear_notes`) over an in-memory list. Same shape as
  `mcp_servers/snowflake_server.py` and `mcp_servers/slack_server.py` in the
  repo root, minus the real external system.
- `client.py` -- connects to `server.py` over stdio, discovers its tools,
  and calls each one, printing the schema and the request/response at every
  step.

## Run it

```bash
uv run python3 projects/ez_mcp_basics/client.py
```

`client.py` launches `server.py` itself as a subprocess -- you don't need to
start the server separately.

## What to look for in the output

1. **The handshake** -- `session.initialize()` happens before anything else.
2. **Discovery, not hardcoding** -- `list_tools()` returns each tool's name,
   description, and input schema, generated automatically from `server.py`'s
   type hints and docstrings. The client never imports `server.py`; it only
   knows what the protocol tells it.
3. **The call/response shape** -- `call_tool()` takes a tool name and a dict
   of arguments, and returns content blocks. Every tool call in this repo
   (including real Snowflake queries and real Slack messages) returns data
   in this same shape.

## Where this goes next

`agents/base/agent.py`'s `call_mcp_tool()` wraps this exact
`StdioServerParameters` / `ClientSession` pattern for use inside an agent.
Project 3 (`ez_agent_meets_mcp`) does that for real: an LLM sees the tool
list this script just printed, and decides which tool to call instead of a
human reading the script.
