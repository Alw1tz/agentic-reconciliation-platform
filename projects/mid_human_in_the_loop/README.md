# mid_human_in_the_loop

Tier 2, project 10 -- the last one before agent logic touches
Snowflake/Spark for real in tier 3. A LangGraph pipeline that pauses and
polls a **real** Slack reaction via `mcp_servers/slack_server.py` before
branching approved/rejected/timed-out.

## What it does

Reuses `mid_langgraph_graph`'s `analyze`, `draft_note_node`, `all_clear`,
and `route_after_analyze` directly -- the anomaly-finding logic isn't
duplicated here, only two new nodes are added:

- **`post_to_slack`** -- opens `mcp_servers/slack_server.py` as a real MCP
  stdio subprocess (same pattern `ez_agent_meets_mcp/agent.py` uses) and
  calls its `post_message` tool with the flagged row + drafted note.
- **`await_approval`** -- polls the same server's `get_reactions` tool with
  backoff (`3s, 5s, 8s, 13s, 21s`), watching for `:+1:`/`:white_check_mark:`
  (approve, per `slack_server.py`'s own docstring) or `:-1:`/`:x:` (reject).
  Times out after the backoff schedule with no reaction.

This deliberately uses polling-with-backoff, not LangGraph's own
`interrupt()`/checkpointer-resume primitive -- that mechanism needs a
second process to resume a saved checkpoint later, whereas this needs to
run start-to-finish in one invocation you can watch and react to live.

Same real dataset and default batch as projects 8 and 9 (`--offset 160`,
containing genuine tip-percentage outliers).

## Run it

```bash
uv run python3 projects/mid_human_in_the_loop/graph.py
```

Uses `ANTHROPIC_API_KEY` and the real `SLACK_BOT_TOKEN`/`SLACK_CHANNEL_ID`
from `.env` -- posts a real message to your configured channel.

## What to look for

- A real message appears in your Slack channel describing the flagged row
  and drafted note, with instructions on how to react.
- React with :+1: (or :white_check_mark:) within the backoff window (up to
  ~50s total across five attempts) and watch the script print
  `"approval_status": "approved"` in the final state. React with :-1:/:x:
  for `"rejected"`, or don't react at all to see `"timed_out"`.
- `--offset 21` exercises the `all_clear` branch instead -- no Slack message
  gets posted at all, since the graph never reaches `post_to_slack`.
