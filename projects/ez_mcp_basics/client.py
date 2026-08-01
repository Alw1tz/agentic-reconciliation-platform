"""A raw MCP client -- no agent, no LLM, just the protocol.

mcp_servers/wire_test.py in the repo root proves a server is reachable
(initialize + list_tools). This script goes one step further and actually
calls tools, so you can see the full request/response shape: what a tool's
schema looks like after discovery, what arguments look like going out, and
what content blocks look like coming back.

This is also the exact mechanism agents/base/agent.py's call_mcp_tool()
wraps for the real pipeline -- same StdioServerParameters / ClientSession
pattern, just spawning server.py in this folder instead of a real Snowflake
or Slack server. Once this feels mechanical, ez_agent_meets_mcp (next in the
curriculum) swaps the print statements below for an LLM deciding which tool
to call.
"""
import asyncio
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

SERVER_SCRIPT = os.path.join(os.path.dirname(__file__), "server.py")


async def main() -> None:
    # StdioServerParameters describes how to *launch* the server -- it
    # doesn't connect to anything yet. The client owns the server's
    # lifecycle: it starts the subprocess, and closing this `async with`
    # block terminates it.
    params = StdioServerParameters(command=sys.executable, args=[SERVER_SCRIPT])

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            # initialize() is the MCP handshake: client and server exchange
            # protocol versions and capabilities before anything else is
            # allowed. Skipping it is not optional -- list_tools/call_tool
            # will fail on an uninitialized session.
            init = await session.initialize()
            print(f"Connected to server: {init.server_info.name}\n")

            # list_tools() is how a client discovers what a server can do --
            # nothing is hardcoded on the client side. Each tool comes back
            # with a JSON Schema for its inputs, generated from server.py's
            # type hints and docstring by the @mcp.tool() decorator.
            tools = await session.list_tools()
            print("Discovered tools:")
            for tool in tools.tools:
                print(f"  - {tool.name}: {tool.description}")
                print(f"    input schema: {tool.input_schema}")
            print()

            # call_tool() takes a tool name plus a dict of arguments matching
            # that schema, and returns a list of content blocks (text, here)
            # -- the same shape whether the tool wraps an in-memory list
            # (this server) or a real Snowflake query (snowflake_server.py).
            async def call(name: str, arguments: dict) -> str:
                result = await session.call_tool(name, arguments)
                text = "\n".join(block.text for block in result.content if hasattr(block, "text"))
                print(f"call_tool({name!r}, {arguments}) -> {text}")
                return text

            print("Calling tools:")
            await call("add_note", {"text": "learn MCP basics"})
            await call("add_note", {"text": "then chain two agents"})
            await call("list_notes", {})
            await call("clear_notes", {})
            await call("list_notes", {})


if __name__ == "__main__":
    asyncio.run(main())
