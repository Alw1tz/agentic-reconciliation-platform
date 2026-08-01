"""A toy MCP server with no LLM anywhere near it.

Every other server in this repo (mcp_servers/snowflake_server.py,
mcp_servers/slack_server.py) wires MCP to a real external system. That's
useful, but it also means the MCP protocol itself -- what a "tool" actually
is on the wire, how a client discovers them, how arguments/results are
shaped -- is easy to miss under all the Snowflake/Slack plumbing.

This server has three tools over an in-memory list. There's nothing to
configure and nothing external to fail, so client.py in this same folder can
demonstrate the raw protocol mechanics in isolation.
"""
from mcp.server.mcpserver import MCPServer

mcp = MCPServer("notes")

# Module-level state, not a database -- the point here is the protocol, not
# persistence. Every tool call from every connected client shares this list
# for the life of the server process.
_notes: list[str] = []


@mcp.tool()
def add_note(text: str) -> str:
    """Append a note and return a confirmation with its new index."""
    _notes.append(text)
    return f"Added note #{len(_notes) - 1}: {text!r}"


@mcp.tool()
def list_notes() -> list[str]:
    """Return every note currently stored."""
    return list(_notes)


@mcp.tool()
def clear_notes() -> str:
    """Delete all stored notes and report how many were removed."""
    count = len(_notes)
    _notes.clear()
    return f"Cleared {count} note(s)"


if __name__ == "__main__":
    # stdio transport: the client launches this file as a subprocess and
    # talks to it over stdin/stdout. That's why client.py doesn't "import"
    # anything from this module -- it spawns this script and speaks MCP to
    # it as a black box, exactly like a real MCP client would with a server
    # it didn't write. (MCP also supports HTTP/SSE transports for servers
    # that live outside the client's process tree; stdio is the simplest
    # case and what every server in this repo uses.)
    mcp.run(transport="stdio")
