"""Two Claude agents chained by hand -- tier 2, project 8 of the learning
curriculum (README.md in this folder). No orchestration framework: agent
A's output is passed as a plain Python argument into agent B. The awkward
parts of that hand-off (parsing free text back into something the next
call can use, no shared typed state) are exactly what motivates rebuilding
this same pipeline as an explicit LangGraph StateGraph in mid_langgraph_graph.

Runs over a real dataset -- seaborn's tips.csv (real restaurant checks) --
instead of synthetic rows, sliced to a small batch to keep this a "local
toy data" exercise, not a full Spark-scale job (that's project 7's job).
"""
import argparse
import csv
import json
import os
import re
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv()

MODEL = "claude-haiku-4-5-20251001"
DATA_FILE = Path(__file__).resolve().parents[1] / "_shared" / "data" / "tips.csv"

# Rows 160-179 of tips.csv contain two genuine outliers (a 71% tip and a
# 42% tip) -- a good default batch for seeing the "anomaly found" path.
# Pass --offset 21 to see a clean Saturday-dinner batch instead (verified
# to hold no row outside a comfortable 11.8%-19.9% tip range).
DEFAULT_OFFSET = 160
DEFAULT_SAMPLE_SIZE = 20


def load_sample_rows(offset: int = DEFAULT_OFFSET, n: int = DEFAULT_SAMPLE_SIZE) -> list[dict]:
    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows[offset : offset + n]


def _extract_json(text: str) -> dict:
    """Claude is asked for a fenced JSON block; pull it out regardless of
    whatever surrounding prose it adds."""
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"No JSON object found in response: {text!r}")
    return json.loads(match.group(0))


def analyze_for_anomaly(client: anthropic.Anthropic, rows: list[dict]) -> dict:
    """Agent A ("analyst"): given a batch of real checks, find the one row
    that looks most like a data-entry anomaly (tip $0, extreme tip/bill
    ratio, etc.) -- or explicitly report none found. Nothing here decides
    in advance which row (if any) is anomalous; that's the model's job."""
    prompt = (
        "You are reviewing a batch of restaurant checks for data-entry "
        "anomalies -- things like a suspiciously low or high tip relative "
        "to the bill. Here is the batch as JSON rows:\n\n"
        f"{json.dumps(rows, indent=2)}\n\n"
        "Only flag a row if it looks like a likely data-entry error -- "
        "tip percentage below 5% or above 35%, or a $0 tip -- not merely "
        "the relatively lowest or highest in this particular batch. Normal "
        "variance within the typical 10-25% tipping range is NOT an anomaly, "
        "even if it's the most extreme value present.\n\n"
        "Respond with ONLY a fenced JSON object of this exact shape:\n"
        '{"anomaly_found": true or false, "row": <the single row object, '
        "or null if none found>, \"reason\": \"one or two sentences of "
        "explanation\"}\n"
        "If nothing looks anomalous, set anomaly_found to false and row to null."
    )
    response = client.messages.create(
        model=MODEL,
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}],
    )
    text = "".join(block.text for block in response.content if hasattr(block, "text"))
    return _extract_json(text)


def draft_note(client: anthropic.Anthropic, analysis: dict) -> str:
    """Agent B ("drafter"): takes agent A's finished analysis -- a plain
    dict, passed by hand, no shared framework state -- and drafts a short
    human-readable note about it."""
    prompt = (
        "A data-entry anomaly was flagged in a restaurant-check reconciliation "
        "review. Here is the finding:\n\n"
        f"{json.dumps(analysis, indent=2)}\n\n"
        "Draft a short (2-3 sentence) note a human reviewer could read to "
        "quickly understand what was flagged and why. No preamble, just the note."
    )
    response = client.messages.create(
        model=MODEL,
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}],
    )
    return "".join(block.text for block in response.content if hasattr(block, "text")).strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, default=DEFAULT_OFFSET)
    parser.add_argument("--n", type=int, default=DEFAULT_SAMPLE_SIZE)
    args = parser.parse_args()

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    rows = load_sample_rows(offset=args.offset, n=args.n)
    print(f"Loaded {len(rows)} real rows from tips.csv (offset={args.offset})\n")

    print("=== Stage 1: analyze_for_anomaly (agent A) ===")
    analysis = analyze_for_anomaly(client, rows)
    print(json.dumps(analysis, indent=2))

    if not analysis.get("anomaly_found"):
        print("\nNo anomaly found -- stopping here (see mid_langgraph_graph for "
              "the same branch expressed as a conditional edge).")
        return

    print("\n=== Stage 2: draft_note (agent B) ===")
    note = draft_note(client, analysis)
    print(note)


if __name__ == "__main__":
    main()
