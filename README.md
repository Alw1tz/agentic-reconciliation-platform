# Agentic Reconciliation Platform

A full-stack, real-infrastructure sandbox: **Airflow orchestrates a
nightly reconciliation, a distributed Spark job diffs source vs. target
Snowflake data, and a LangGraph agent pipeline drafts a fix and posts it
to Slack for human approval via a Slack MCP server** — instead of
auto-applying anything itself.

## Why this project

Most "AI agent" demos skip the boring, load-bearing half of the problem:
how does an agent actually get secure access to real systems (Snowflake,
Slack), and how does a human stay in the loop before anything gets
written back? This project answers both with real infrastructure, not
mocks — a real Airflow + Spark cluster, a real Snowflake warehouse, a
real Slack workspace, and two real [MCP](https://modelcontextprotocol.io/)
servers gating that access.

## Architecture

```
Airflow DAG (reconciliation_dag.py)
  seed_and_export ──▶ spark_diff ──▶ run_agent_pipeline
  (seeds + exports      (standalone      (LangGraph pipeline, below)
   Snowflake data         Spark cluster,
   to S3)                 diffs via s3a)

LangGraph pipeline (agents/pipeline.py)
  scout ──▶ remediate ──▶ post_to_slack ──▶ await_approval
  (confirms      (drafts a       (posts the       (polls for a
   the anomaly    scoped fix +    finding + fix     ✅ reaction —
   via the        regression      via the Slack     the pipeline
   Snowflake       test SQL,       MCP server)       never applies
   MCP server)     via Claude)                       the fix itself)
```

Two MCP servers sit at the center: `mcp_servers/snowflake_server.py`
exposes exactly two read-only tools against Snowflake (SQL validated by
parsing with `sqlglot`, not a naive string-prefix check — a stacked
`SELECT 1; DROP TABLE ...;` statement is rejected outright), and
`mcp_servers/slack_server.py` exposes posting a message and reading back
reactions. Both are plain [MCP](https://modelcontextprotocol.io/) Python
SDK servers, runnable standalone or from an agent.

## Governance, by design

The pipeline **drafts** a fix and **posts** it — it never executes
anything against Snowflake on its own. A human approves via a Slack
reaction. That boundary is deliberate: the security/trust boundary lives
in the tool implementation (what `run_sql_query` will and won't execute),
not in whether the model "behaves."

## Minimal Architecture Demo

This repository also contains a minimal, production-ready demonstration showing:
- Reusable base agent classes for modularity
- Modular pipeline architecture with clear separation of concerns  
- MCP-style operations that can be extended
- Human approval workflows for security
- Scalable design pattern for multiple use cases

## Stack

- **Airflow 3.2.2** (SequentialExecutor + SQLite) — single-node, enough
  for a sandbox
- **Apache Spark 4.0.3** standalone cluster (1 master, 2 workers)
- **Snowflake** — the warehouse being reconciled
- **AWS S3** — snapshot storage + the Spark diff report
- **Slack** — the human-approval interface, via its own MCP server
- **Claude Sonnet 5** (`anthropic` SDK) — the scout/remediate reasoning

## Running it

```bash
cp .env.example .env   # fill in Snowflake, Slack, and Anthropic credentials
docker compose up airflow-init
docker compose up -d
```

- Airflow UI → http://localhost:8082 (airflow / airflow)
- Spark Master UI → http://localhost:9090
- Spark Worker UIs → http://localhost:9091, http://localhost:9092

Trigger the pipeline from the Airflow UI (`reconciliation_dag`), or:

```bash
docker compose exec airflow-apiserver airflow dags trigger reconciliation_dag
```

Watch the configured Slack channel for the posted anomaly + drafted fix.

## Project layout

```
airflow/dags/reconciliation_dag.py   # the DAG
spark/apps/diff_source_target.py     # the distributed diff job
mcp_servers/                         # the two MCP servers  
agents/pipeline.py                   # the LangGraph pipeline
agents/minimal_pipeline.py           # minimal demonstration pipeline
demo_minimal.py                      # quick demo script
```
