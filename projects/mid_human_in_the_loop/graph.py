"""A LangGraph pipeline gated on a REAL Slack reaction -- tier 2, project
10 of the learning curriculum (README.md in this folder), the last one
before agent logic touches Snowflake/Spark for real in tier 3.

Reuses mid_langgraph_graph's analyze / draft_note_node / all_clear /
route_after_analyze nodes directly instead of duplicating the
anomaly-finding logic, and adds two new nodes: post the finding to the
real Slack channel via mcp_servers/slack_server.py (the same real MCP
server reconciliation_dag.py's future agent pipeline will use), then poll
for a reaction with backoff -- LangGraph's own interrupt()/checkpointer
resume mechanic is deliberately NOT used here, since that needs a second
process to resume a saved checkpoint; this needs to run start-to-finish in
one invocation the user can watch and react to live.
"""
import argparse
import asyncio
import functools
import json
import sys
from pathlib import Path
from typing import TypedDict

from dotenv import load_dotenv
from langgraph.graph import END, START, StateGraph
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from projects.mid_langgraph_graph.graph import (
    all_clear,
    analyze,
    draft_note_node,
    route_after_analyze,
)
from projects.mid_manual_agent_chain.pipeline import (
    DEFAULT_OFFSET,
    DEFAULT_SAMPLE_SIZE,
    load_sample_rows,
)

load_dotenv()

SLACK_SERVER_SCRIPT = str(Path(__file__).resolve().parents[2] / "mcp_servers" / "slack_server.py")

# Per slack_server.py's own docstring: "+1" or "white_check_mark" is
# treated as human approval.
APPROVE_REACTIONS = {"+1", "white_check_mark"}
REJECT_REACTIONS = {"-1", "x"}
BACKOFF_SECONDS = [3, 5, 8, 13, 21]


class State(TypedDict):
    rows: list[dict]
    anomaly_found: bool
    anomaly_row: dict | None
    reasoning: str
    note: str
    ts: str
    approval_status: str


async def _call_tool_json(session: ClientSession, tool_name: str, arguments: dict):
    """post_message/get_reactions return structured data (dict/list), which
    comes back over MCP as TextContent -- json.loads the joined text to get
    it back, same shape ez_agent_meets_mcp's dispatch joins text for a
    plain string result."""
    result = await session.call_tool(tool_name, arguments)
    text = "\n".join(block.text for block in result.content if hasattr(block, "text"))

    # Handle empty response (common when no reactions exist)
    if not text.strip():
        if tool_name == "get_reactions":
            # Return empty list for reactions when there are none
            return []
        else:
            # For other tools, raise the error
            raise ValueError(f"Empty response from {tool_name}")

    try:
        # Try to parse as JSON first
        parsed = json.loads(text)
        return parsed
    except json.JSONDecodeError as e:
        # Special handling for get_reactions - if we get a plain text response like "white_check_mark"
        # that might be the reaction name, treat it appropriately
        if tool_name == "get_reactions":
            # If we get what looks like a single reaction name, wrap it in a list
            if isinstance(text, str) and text.strip() in {"+1", "white_check_mark", "-1", "x"}:
                return [text.strip()]
            # If we get an integer or other data type that might be a reaction
            elif isinstance(text, (int, float)):
                return [str(text)]
            # Handle edge case where text might contain multiple lines or malformed data
            elif isinstance(text, str) and len(text.strip()) > 0:
                # If it's a string with content but not JSON, try to extract reaction name
                # Remove any extra whitespace and handle common formats
                cleaned = text.strip()
                if cleaned in {"+1", "white_check_mark", "-1", "x"}:
                    return [cleaned]
                else:
                    # Return empty list as fallback - this handles cases where Slack API
                    # returns unexpected response formats
                    return []
            # Otherwise, return empty list to avoid crashing
            return []
        raise


async def post_to_slack(state: State, session: ClientSession) -> dict:
    text = (
        "*Reconciliation anomaly detected* (mid_human_in_the_loop demo)\n"
        f"Row: `{json.dumps(state['anomaly_row'])}`\n"
        f"Reason: {state['reasoning']}\n\n"
        f"*Note:* {state['note']}\n\n"
        "React with :+1: or :white_check_mark: to approve, "
        ":-1: or :x: to reject."
    )
    payload = await _call_tool_json(session, "post_message", {"text": text})
    print(f"Posted to Slack: {payload}")
    return {"ts": payload["ts"]}


async def await_approval(state: State, session: ClientSession) -> dict:
    for attempt, delay in enumerate(BACKOFF_SECONDS, start=1):
        await asyncio.sleep(delay)
        try:
            reactions = await _call_tool_json(session, "get_reactions", {"ts": state["ts"]})

            # Ensure reactions is always a list - handle various edge cases
            if reactions is None:
                reactions = []
            elif not isinstance(reactions, list):
                # If it's a single item (string or int), wrap it in a list
                if isinstance(reactions, (str, int)):
                    reactions = [reactions]
                else:
                    # If it's some other type that can't be iterated, treat as empty
                    reactions = []

            # Convert all items to strings for proper comparison
            reactions = [str(r) for r in reactions]

            # Check for approval reactions
            if any(r in APPROVE_REACTIONS for r in reactions):
                return {"approval_status": "approved", "reactions": reactions}

            # Check for rejection reactions
            if any(r in REJECT_REACTIONS for r in reactions):
                return {"approval_status": "rejected", "reactions": reactions}

        except Exception as e:
            # Continue polling even if there's an error
            continue

    return {"approval_status": "timed_out", "reactions": []}


def route_after_approval(state: State) -> str:
    return state["approval_status"]


async def run(rows: list[dict]) -> dict:
    params = StdioServerParameters(command=sys.executable, args=[SLACK_SERVER_SCRIPT])
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            builder = StateGraph(State)
            builder.add_node("analyze", analyze)
            builder.add_node("draft_note", draft_note_node)
            builder.add_node("all_clear", all_clear)
            builder.add_node("post_to_slack", functools.partial(post_to_slack, session=session))
            builder.add_node("await_approval", functools.partial(await_approval, session=session))

            builder.add_edge(START, "analyze")
            builder.add_conditional_edges(
                "analyze", route_after_analyze, {"draft_note": "draft_note", "all_clear": "all_clear"}
            )
            builder.add_edge("draft_note", "post_to_slack")
            builder.add_edge("post_to_slack", "await_approval")
            builder.add_conditional_edges(
                "await_approval",
                route_after_approval,
                {"approved": END, "rejected": END, "timed_out": END},
            )
            builder.add_edge("all_clear", END)
            graph = builder.compile()

            return await graph.ainvoke({"rows": rows})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, default=DEFAULT_OFFSET)
    parser.add_argument("--n", type=int, default=DEFAULT_SAMPLE_SIZE)
    args = parser.parse_args()

    rows = load_sample_rows(offset=args.offset, n=args.n)
    print(f"Loaded {len(rows)} real rows from tips.csv (offset={args.offset})\n")

    final_state = asyncio.run(run(rows))
    print("\n=== Final state ===")
    print(json.dumps({k: v for k, v in final_state.items() if k != "rows"}, indent=2))


if __name__ == "__main__":
    main()
