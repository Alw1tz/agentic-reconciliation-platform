"""An agent whose tools are discovered from an MCP server at runtime --
tier 1, project 6 of the learning curriculum (README.md in this folder).

Wires together ez_mcp_basics (MCP discovery + calling, reusing its
server.py directly) and ez_agent_tool_loop's shared run_tool_loop(). This
is what agents/base/agent.py's call_mcp_tool() is meant to enable for the
real pipeline -- done here for real, instead of the hardcoded simulated
responses in agents/minimal/core_agent.py.
"""
import asyncio
import os
import sys
from pathlib import Path

import anthropic
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import Tool

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from projects._shared.tool_loop import run_tool_loop

load_dotenv()

SERVER_SCRIPT = str(Path(__file__).resolve().parents[1] / "ez_mcp_basics" / "server.py")


def mcp_tool_to_anthropic_schema(tool: Tool) -> dict:
    """MCP and Anthropic both describe a tool as {name, description, JSON
    Schema for its arguments} -- converting between them is a rename, not
    a redesign."""
    return {
        "name": tool.name,
        "description": tool.description or "",
        "input_schema": tool.input_schema,
    }


async def main() -> None:
    anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    params = StdioServerParameters(command=sys.executable, args=[SERVER_SCRIPT])
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Discovery, not hardcoding: nothing here names add_note,
            # list_notes, or clear_notes -- whatever server.py exposes is
            # what gets converted into tools Claude can request.
            discovered = await session.list_tools()
            tools = [mcp_tool_to_anthropic_schema(t) for t in discovered.tools]
            print(f"Discovered {len(tools)} tool(s) from the MCP server: {[t['name'] for t in tools]}\n")

            async def dispatch(name: str, tool_input: dict) -> str:
                """The one thing specific to this project: a tool call
                means a real MCP call_tool(), not a local function lookup
                like ez_agent_tool_loop's dispatch."""
                result = await session.call_tool(name, tool_input)
                return "\n".join(block.text for block in result.content if hasattr(block, "text"))

            reply = await run_tool_loop(
                client=anthropic_client,
                model="claude-haiku-4-5-20251001",
                tools=tools,
                dispatch=dispatch,
                system="You manage a notes list using only the tools available to you.",
                user_message=(
                    "Add a note that says 'buy milk', then add a note that says "
                    "'call the bank', then list all notes back to me."
                ),
            )

            print(f"\nFinal answer: {reply}")


if __name__ == "__main__":
    asyncio.run(main())
