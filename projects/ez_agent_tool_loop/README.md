# ez_agent_tool_loop

Tier 1, project 5. The raw Claude tool-use loop via the `anthropic` SDK
directly -- no MCP, no LangGraph. This is the primitive every agent
framework, LangGraph included, is built on top of.

## What it does

Two plain Python functions (`add`, `multiply`) are described to Claude as
JSON Schema tools. Given "What is (12 + 30) multiplied by 3?" with an
instruction to never compute answers itself, Claude has to:

1. Request `add(12, 30)` -- the loop executes it locally, returns `42`.
2. Request `multiply(42, 3)` -- executes it, returns `126`.
3. Reply with the final answer, no more tool requests -- the loop exits.

The loop itself (`run_tool_loop`) lives in `projects/_shared/tool_loop.py`,
shared with `ez_agent_meets_mcp` -- see that project's README for why:
the loop mechanics are identical whether a tool is a local function or an
MCP server call, so only the `dispatch` function (how a call actually
gets executed) differs between the two projects.

## Run it

```bash
uv run python3 projects/ez_agent_tool_loop/agent.py
```

Uses `ANTHROPIC_API_KEY` from `.env` -- minimal token cost (a couple of
short turns).

## What to look for

- `[tool call] add({'a': 12, 'b': 30})` / `[tool result] 42`, then the
  same for `multiply` -- printed by the shared loop as each tool request
  is dispatched and executed.
- The loop runs **more than once**: Claude can't get to the final answer
  in a single turn here, because it needs `add`'s result before it can
  call `multiply`. That's the loop actually looping, not just a single
  request/response.
- Final line is Claude's answer (126) stated in text, with `stop_reason`
  no longer `tool_use` -- that's what ends the loop.
