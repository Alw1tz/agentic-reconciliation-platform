"""One-off smoke test: confirm both MCP servers respond to initialize +
tools/list over stdio. Doesn't require real Snowflake/Slack credentials --
only actual tool calls do."""
import asyncio
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def check(server_script: str) -> None:
    params = StdioServerParameters(command=sys.executable, args=[server_script])
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            tools = await session.list_tools()
            print(f"{server_script}: server={init.server_info.name}, tools={[t.name for t in tools.tools]}")


async def main() -> None:
    await check("mcp_servers/snowflake_server.py")
    await check("mcp_servers/slack_server.py")


if __name__ == "__main__":
    asyncio.run(main())
