# mid_manual_agent_chain

Tier 2, project 8. Two Claude agents chained by hand -- agent A's output
becomes agent B's input via a plain Python argument, no framework. Runs
over a real slice of seaborn's `tips.csv` (real restaurant checks) instead
of synthetic data.

## What it does

1. **`analyze_for_anomaly`** ("analyst" agent) -- given a batch of 20 real
   checks, asked to find the single row that looks like a data-entry
   anomaly (a suspiciously low or high tip relative to the bill) and
   explain why, or explicitly report none found. Nothing is hardcoded as
   "the" anomaly -- the model does the finding, returning a parseable
   fenced JSON object.
2. **`draft_note`** ("drafter" agent) -- takes agent A's finished
   dict and drafts a short human-readable note about it.

The default batch (`--offset 160`) contains two genuine outliers in the
real data: a $5.15 tip on a $7.25 bill (71%) and a $4.00 tip on a $9.60
bill (42%). Pass `--offset 21` for a clean Saturday-dinner batch (verified
to hold no row outside 11.8%-19.9%) to see the "no anomaly found" path instead.

This is the state hand-off project 9 (`mid_langgraph_graph`) rebuilds as
typed shared state with a declarative conditional edge -- worth noticing
here how the hand-off between stages is just "pass a dict to the next
function call," and how that gets awkward as soon as you want branching
logic around it.

## Run it

```bash
uv run python3 projects/mid_manual_agent_chain/pipeline.py
uv run python3 projects/mid_manual_agent_chain/pipeline.py --offset 21   # clean batch, no anomaly
```

Uses `ANTHROPIC_API_KEY` from `.env`. Two cheap Haiku calls per run.

## What to look for

- Stage 1's printed JSON: `anomaly_found`, the flagged `row`, and a
  `reason` -- confirms the model is reasoning over the real batch, not
  returning a fixed answer.
- With the default offset, stage 2 should run and print a short note about
  the outlier tip. With `--offset 21`, the script should stop after stage 1
  with no anomaly found.
