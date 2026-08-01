"""Raw Claude tool-use loop -- tier 1, project 5 of the learning
curriculum (README.md in this folder). No MCP, no LangGraph -- the
mechanism every agent framework (LangGraph included) is built on top of:
describe tools as JSON Schema, let Claude request one, execute it locally,
send the result back, repeat until Claude stops asking.

The loop itself lives in projects/_shared/tool_loop.py, shared with
ez_agent_meets_mcp -- this file only supplies what's specific to "local
Python functions as tools": the schemas, the functions, and how a call
gets dispatched to one of them.
"""
import asyncio
import os
import sys
from pathlib import Path

import anthropic
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from projects._shared.tool_loop import run_tool_loop

load_dotenv()

# Anthropic's tools=[...] format: JSON Schema describing each tool's
# arguments. Claude never executes these itself -- every "tool use" is
# just Claude asking, by name, for something to run this schema's
# arguments through.
TOOLS = [
    {
        "name": "add",
        "description": "Add two numbers and return the sum.",
        "input_schema": {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
        },
    },
    {
        "name": "multiply",
        "description": "Multiply two numbers and return the product.",
        "input_schema": {
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
        },
    },
]

LOCAL_TOOLS = {
    "add": lambda a, b: a + b,
    "multiply": lambda a, b: a * b,
}


async def dispatch(name: str, tool_input: dict) -> str:
    """The one thing specific to this project: a tool call means looking
    up and calling a local Python function. Compare to
    ez_agent_meets_mcp's dispatch, which routes to a real MCP server
    instead -- run_tool_loop() doesn't know or care which."""
    result = LOCAL_TOOLS[name](**tool_input)
    return str(result)


async def main() -> None:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    reply = await run_tool_loop(
        client=client,
        model="claude-haiku-4-5-20251001",
        tools=TOOLS,
        dispatch=dispatch,
        system="You have add and multiply tools. Use them for every calculation -- never compute an answer yourself.",
        user_message="What is (12 + 30) multiplied by 3?",
    )

    print(f"\nFinal answer: {reply}")


if __name__ == "__main__":
    asyncio.run(main())
