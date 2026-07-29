"""Real MCP server exposing Slack as the human-approval channel.

This is the piece the interviewer specifically asked about: an agent
posts its finding + drafted fix to Slack, and a human approves by
reacting rather than the agent auto-applying anything. Two tools:
post a message, and read back the reactions on it.
"""
import os

from dotenv import load_dotenv
from mcp.server.mcpserver import MCPServer
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

load_dotenv()

mcp = MCPServer("slack")

_client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])


@mcp.tool()
def post_message(text: str) -> dict:
    """Post a message to the configured Slack channel.

    Returns the message timestamp ('ts'), which is the handle needed to
    later check for an approval reaction on this specific message.
    """
    channel = os.environ["SLACK_CHANNEL_ID"]
    response = _client.chat_postMessage(channel=channel, text=text)
    return {"channel": response["channel"], "ts": response["ts"]}


@mcp.tool()
def get_reactions(ts: str) -> list[str]:
    """List the emoji reaction names on a previously posted message.

    A '+1' or 'white_check_mark' reaction is treated as human approval
    by the agent pipeline that calls this tool.
    """
    channel = os.environ["SLACK_CHANNEL_ID"]
    try:
        response = _client.reactions_get(channel=channel, timestamp=ts)
    except SlackApiError as e:
        if e.response.get("error") == "no_reaction":
            return []
        raise
    reactions = response.get("message", {}).get("reactions", [])
    return [r["name"] for r in reactions]


if __name__ == "__main__":
    mcp.run(transport="stdio")
