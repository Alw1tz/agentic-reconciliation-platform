# mid_langgraph_graph

Tier 2, project 9. `mid_manual_agent_chain`'s exact pipeline, rebuilt as an
explicit LangGraph `StateGraph` -- same two Claude calls (imported
directly from `mid_manual_agent_chain/pipeline.py`), so this project is
purely about what LangGraph adds, not a rewrite of the prompts.

## What it does

- A typed `State` (`rows`, `anomaly_found`, `anomaly_row`, `reasoning`,
  `note`) replaces project 8's loose local variables.
- `analyze` and `draft_note_node` wrap the same `analyze_for_anomaly()` /
  `draft_note()` calls project 8 uses -- reused directly, not duplicated.
- `route_after_analyze` is a **conditional edge**: `add_conditional_edges`
  sends the graph to `draft_note` if an anomaly was found, or to
  `all_clear` if not. Compare this to project 8's `if not
  analysis.get("anomaly_found"): return` -- same branch, but here it's a
  declared part of the graph's structure instead of an early return buried
  in `main()`.

Same default batch (`--offset 160`, containing two genuine outliers) and
the same `--offset 21` escape hatch to exercise the `all_clear` branch.

## Run it

```bash
uv run python3 projects/mid_langgraph_graph/graph.py
uv run python3 projects/mid_langgraph_graph/graph.py --offset 21   # exercises the all_clear branch
```

Uses `ANTHROPIC_API_KEY` from `.env`.

## What to look for

- Final printed state includes `anomaly_found`, `anomaly_row`,
  `reasoning`, and `note` -- same information project 8 prints across two
  stages, now the output of one `graph.invoke()`.
- Running with `--offset 21` should show `anomaly_found: false` and a note
  saying nothing was found -- confirms the `all_clear` branch actually
  fired instead of `draft_note`.
