"""Generic Claude tool-use loop, shared by ez_agent_tool_loop and
ez_agent_meets_mcp.

The loop mechanics -- send tools, inspect the response for tool_use
blocks, execute each one, send tool_result blocks back, repeat until
Claude stops asking -- are identical whether a tool is a local Python
function or a real MCP server call. Only *how a call is dispatched*
differs between those two projects, so that's the one thing callers plug
in via the `dispatch` argument.
"""
from typing import Any, Awaitable, Callable

import anthropic

Dispatch = Callable[[str, dict[str, Any]], Awaitable[str]]


async def run_tool_loop(
    client: anthropic.Anthropic,
    model: str,
    tools: list[dict],
    dispatch: Dispatch,
    user_message: str,
    system: str | None = None,
    max_turns: int = 8,
) -> str:
    """Run Claude's tool-use loop to completion and return its final text reply.

    `dispatch(tool_name, tool_input)` is awaited for every tool Claude
    requests and must return the tool's result as a string.
    """
    messages: list[dict] = [{"role": "user", "content": user_message}]

    for _ in range(max_turns):
        response = client.messages.create(
            model=model,
            max_tokens=1024,
            system=system,
            tools=tools,
            messages=messages,
        )

        if response.stop_reason != "tool_use":
            # No more tool calls requested -- this is the loop's normal
            # exit. Concatenate any text blocks and hand back the answer.
            return "".join(block.text for block in response.content if block.type == "text")

        # Claude's response (including its tool_use requests) has to be
        # echoed back as an assistant turn before the tool_result turn --
        # the API rejects a tool_result with no matching prior tool_use.
        messages.append({"role": "assistant", "content": response.content})

        # A single turn can request multiple tool calls at once; each
        # needs its own tool_result block, all sent back together in one
        # user turn.
        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            print(f"  [tool call] {block.name}({block.input})")
            result_text = await dispatch(block.name, block.input)
            print(f"  [tool result] {result_text}")
            tool_results.append(
                {"type": "tool_result", "tool_use_id": block.id, "content": result_text}
            )

        messages.append({"role": "user", "content": tool_results})

    raise RuntimeError(f"Tool loop did not finish within {max_turns} turns")
