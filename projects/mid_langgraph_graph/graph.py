"""mid_manual_agent_chain's exact pipeline, rebuilt as an explicit
LangGraph StateGraph -- tier 2, project 9 of the learning curriculum
(README.md in this folder). Same two Claude calls, reused directly from
mid_manual_agent_chain/pipeline.py (analyze_for_anomaly, draft_note) --
the point of this project is what LangGraph adds around them: typed
shared state and a declarative conditional edge, not a rewrite of the
prompts.

Compare route_after_analyze()'s conditional edge here to project 8's
`if not analysis.get("anomaly_found"): return` -- same branch, now a
first-class part of the graph instead of an early return buried in main().
"""
import argparse
import json
import os
import sys
from pathlib import Path
from typing import TypedDict

import anthropic
from dotenv import load_dotenv
from langgraph.graph import END, START, StateGraph

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from projects.mid_manual_agent_chain.pipeline import (
    DEFAULT_OFFSET,
    DEFAULT_SAMPLE_SIZE,
    analyze_for_anomaly,
    draft_note,
    load_sample_rows,
)

load_dotenv()


class State(TypedDict):
    rows: list[dict]
    anomaly_found: bool
    anomaly_row: dict | None
    reasoning: str
    note: str


def analyze(state: State) -> dict:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    analysis = analyze_for_anomaly(client, state["rows"])
    return {
        "anomaly_found": analysis.get("anomaly_found", False),
        "anomaly_row": analysis.get("row"),
        "reasoning": analysis.get("reason", ""),
    }


def draft_note_node(state: State) -> dict:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    note = draft_note(
        client,
        {"row": state["anomaly_row"], "reason": state["reasoning"]},
    )
    return {"note": note}


def all_clear(state: State) -> dict:
    return {"note": "No anomaly found in this batch -- nothing to draft."}


def route_after_analyze(state: State) -> str:
    """The conditional edge: project 8 expressed this same branch as an
    early `return` in main(); here it's a declared part of the graph, so
    the graph itself -- not prose in a function body -- shows both
    possible paths."""
    return "draft_note" if state["anomaly_found"] else "all_clear"


builder = StateGraph(State)
builder.add_node("analyze", analyze)
builder.add_node("draft_note", draft_note_node)
builder.add_node("all_clear", all_clear)
builder.add_edge(START, "analyze")
builder.add_conditional_edges(
    "analyze",
    route_after_analyze,
    {"draft_note": "draft_note", "all_clear": "all_clear"},
)
builder.add_edge("draft_note", END)
builder.add_edge("all_clear", END)
graph = builder.compile()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, default=DEFAULT_OFFSET)
    parser.add_argument("--n", type=int, default=DEFAULT_SAMPLE_SIZE)
    args = parser.parse_args()

    rows = load_sample_rows(offset=args.offset, n=args.n)
    print(f"Loaded {len(rows)} real rows from tips.csv (offset={args.offset})\n")

    final_state = graph.invoke({"rows": rows})
    print(json.dumps(
        {k: v for k, v in final_state.items() if k != "rows"},
        indent=2,
    ))


if __name__ == "__main__":
    main()
