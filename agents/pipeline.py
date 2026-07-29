"""LangGraph pipeline: scout -> remediate -> post_to_slack -> await_approval.

The paid, real-infra version of Saturday's scout_agent_graph.py — same
node shape, but scout/remediate now call a real MCP server over Snowflake
instead of DuckDB, the model is Claude Sonnet 5 (paid API) instead of
local qwen3, and there's a genuinely new node: post_to_slack, which is
the human-approval interface the JD's own question about MCP+Slack was
pointing at. The pipeline drafts and posts; it never applies anything to
Snowflake itself -- see this repo's CLAUDE.md for why.
"""
import asyncio
import json
import os
import sys
import time
from typing import TypedDict

import anthropic
from dotenv import load_dotenv
from langgraph.graph import END, StateGraph
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

load_dotenv()

MCP_SERVERS_DIR = os.path.join(os.path.dirname(__file__), "..", "mcp_servers")
client = anthropic.Anthropic()
MODEL = "claude-sonnet-5"


async def _call_mcp_tool(server_script: str, tool_name: str, arguments: dict) -> str:
    # sys.executable, not "uv run" -- this module runs both locally (inside
    # the uv-managed venv, where sys.executable already resolves correctly)
    # and inside the Airflow container (a plain requirements.txt install,
    # where "uv" doesn't exist at all).
    params = StdioServerParameters(command=sys.executable, args=[server_script])
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, arguments)
            return "\n".join(block.text for block in result.content if hasattr(block, "text"))


def call_mcp_tool(server_name: str, tool_name: str, arguments: dict) -> str:
    server_script = os.path.join(MCP_SERVERS_DIR, f"{server_name}_server.py")
    return asyncio.run(_call_mcp_tool(server_script, tool_name, arguments))


class PipelineState(TypedDict):
    diff_report: dict
    anomaly_summary: str
    draft_sql: str
    draft_test: str
    slack_ts: str
    approved: bool


def scout_node(state: PipelineState) -> dict:
    mismatches = state["diff_report"].get("amounts_match", {}).get("detail", [])
    if not mismatches:
        return {"anomaly_summary": "No amount mismatches found in this run."}

    top = mismatches[0]
    confirm = call_mcp_tool(
        "snowflake",
        "run_sql_query",
        {
            "sql": (
                "SELECT order_id, amount_usd FROM revenue_daily "
                f"WHERE order_id = {top['order_id']}"
            )
        },
    )
    prompt = (
        "You are a data pipeline scout agent. A Spark reconciliation job flagged "
        f"this mismatch: {top}. You independently confirmed it via Snowflake: "
        f"{confirm}. In 2-3 sentences, summarize the anomaly for a data engineer."
    )
    resp = client.messages.create(
        model=MODEL, max_tokens=1024, messages=[{"role": "user", "content": prompt}]
    )
    summary = next(b.text for b in resp.content if b.type == "text")
    print(f"[scout] {summary}\n")
    return {"anomaly_summary": summary}


def remediate_node(state: PipelineState) -> dict:
    mismatches = state["diff_report"].get("amounts_match", {}).get("detail", [])
    top = mismatches[0] if mismatches else {}
    prompt = (
        "You are a remediation agent. Anomaly: "
        f"{state['anomaly_summary']}\nRaw mismatch data: {top}\n"
        "The table is revenue_daily(order_id INT, amount_usd DECIMAL). "
        "Write:\n1) A single UPDATE statement scoped to exactly this order_id, "
        "correcting amount_usd to the source value.\n"
        "2) One SQL assertion query that would FAIL (return rows) if this bug "
        "recurs for this order_id.\n"
        "Return ONLY the two SQL statements, labeled 'FIX:' and 'TEST:'. No prose."
    )
    resp = client.messages.create(
        model=MODEL, max_tokens=1024, messages=[{"role": "user", "content": prompt}]
    )
    draft = next(b.text for b in resp.content if b.type == "text")
    print(f"[remediate]\n{draft}\n")
    fix = draft.split("TEST:")[0].replace("FIX:", "").strip()
    test = draft.split("TEST:")[1].strip() if "TEST:" in draft else ""
    return {"draft_sql": fix, "draft_test": test}


def post_to_slack_node(state: PipelineState) -> dict:
    message = (
        ":rotating_light: *Reconciliation anomaly detected*\n\n"
        f"{state['anomaly_summary']}\n\n"
        f"*Proposed fix:*\n```{state['draft_sql']}```\n"
        f"*Regression test:*\n```{state['draft_test']}```\n\n"
        "React with :white_check_mark: to approve deployment. This agent will "
        "*not* apply the fix itself."
    )
    result = call_mcp_tool("slack", "post_message", {"text": message})
    ts = json.loads(result)["ts"]
    print(f"[post_to_slack] posted, ts={ts}\n")
    return {"slack_ts": ts}


def await_approval_node(state: PipelineState) -> dict:
    for attempt in range(6):
        reactions = json.loads(
            call_mcp_tool("slack", "get_reactions", {"ts": state["slack_ts"]})
        )
        if "white_check_mark" in reactions or "+1" in reactions:
            print("[await_approval] approved via Slack reaction\n")
            return {"approved": True}
        print(f"[await_approval] no approval yet (check {attempt + 1}/6), waiting...\n")
        time.sleep(5)
    print("[await_approval] still pending after polling window -- check Slack manually\n")
    return {"approved": False}


graph = StateGraph(PipelineState)
graph.add_node("scout", scout_node)
graph.add_node("remediate", remediate_node)
graph.add_node("post_to_slack", post_to_slack_node)
graph.add_node("await_approval", await_approval_node)

graph.set_entry_point("scout")
graph.add_edge("scout", "remediate")
graph.add_edge("remediate", "post_to_slack")
graph.add_edge("post_to_slack", "await_approval")
graph.add_edge("await_approval", END)

app = graph.compile()


def run_pipeline(diff_report: dict) -> dict:
    return app.invoke({"diff_report": diff_report})


if __name__ == "__main__":
    with open(os.environ.get("DIFF_REPORT_PATH", "diff_report_sample.json")) as f:
        report = json.load(f)
    final_state = run_pipeline(report)
    print("=== FINAL STATE ===")
    print(f"approved: {final_state['approved']}")
