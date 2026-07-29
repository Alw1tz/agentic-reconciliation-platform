# agentic-reconciliation-platform

A full-stack, real-infra rebuild of the JD's "08:00 Intelligence
Synthesis" scenario for a Lead Agentic Data Systems Engineer interview:
Airflow orchestrates a nightly reconciliation, Spark computes day-over-day
diffs against real Snowflake tables, and a LangGraph agent pipeline
(scout → remediate) posts its finding and drafted fix to Slack via a
Slack MCP server for human approval — mirroring the candidate's real
production reconciliation DAG at Rappi, now with the agentic layer it was
missing.

Sibling to `../spark-aws-pipeline/` (its docker-compose service
definitions were the starting point here) and
`../principal-data-ai-engineer-handbook/labs/ai/agentic-demo/` (the free,
local-model prototype this project is the paid, real-infra version of).

## Stack

- Airflow 3.2.2 (SequentialExecutor + SQLite) — UI: http://localhost:8082
- Spark 4.0.3 standalone cluster — Master UI: http://localhost:9090
- Real Snowflake (own dedicated database, see `.env`)
- Real S3 bucket: `carlos-agentic-recon-platform`
- Real Slack workspace via a Slack app (bot token in `.env`)
- Claude Sonnet 5 via the `anthropic` SDK (real API spend — a few cents
  per pipeline run)

## Governance

The pipeline drafts a fix and posts it to Slack for a human ✅ reaction.
It does **not** auto-apply anything to Snowflake — that's a deliberate
scope boundary, not a missing feature; see the red-team finding in the
sibling `agentic-demo` project for why.

## Commands

```bash
docker compose up airflow-init   # first time only
docker compose up -d
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /opt/spark/apps/diff_source_target.py
```
