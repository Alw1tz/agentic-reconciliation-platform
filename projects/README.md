# Learning projects: Airflow, Spark, Snowflake/S3, MCP, agents, LangGraph

A progressive curriculum covering this repo's whole stack, one piece at a
time before combining them. Full detail (goals, what each teaches,
dependencies, real infra) is in `../CURRICULUM.md`; this file just tracks
status.

Note: DAGs and Spark jobs that need to run against the real Airflow
scheduler / Spark cluster live under `../airflow/dags/` and `../spark/apps/`
(the only two directories docker-compose mounts into those containers), not
here -- their `projects/<name>/` folder still holds the README and
explanation. Everything else runs fully standalone via
`uv run python3 projects/<name>/...`.

No new infra required beyond what's already configured in `.env` --
Snowflake, Slack, S3, and the Anthropic API are the same real credentials
the main pipeline uses.

## Tier 1: foundations (one piece of infra, isolated)

| # | Folder | Teaches | Status |
|---|---|---|---|
| 1 | `ez_airflow_hello` | Author an Airflow DAG from scratch -- tasks, dependencies, XComs, retries | done |
| 2 | `ez_spark_hello` | Author a standalone Spark job from scratch, submitted to the real cluster | done |
| 3 | `ez_snowflake_s3_etl` | Minimal Extract (S3) + Load (Snowflake), no orchestration | done |
| 4 | `ez_mcp_basics` | Raw MCP protocol: a toy server + a raw client, no LLM involved | done |
| 5 | `ez_agent_tool_loop` | Raw Claude tool-use loop via the `anthropic` SDK, no MCP/LangGraph | done |
| 6 | `ez_agent_meets_mcp` | An agent whose tools are discovered dynamically from an MCP server | done |

Tier 1 complete. `projects/_shared/` holds two helpers factored out once
repetition showed up across these six: `tool_loop.py` (the Claude
tool-use loop, shared verbatim by 5 and 6) and `connections.py`
(Snowflake/S3 connection setup, used by 3 and reused by tier 3 later).

## Tier 2: combining two pieces

| # | Folder | Teaches | Status |
|---|---|---|---|
| 7 | `mid_airflow_spark_pipeline` | An Airflow DAG orchestrating a real Spark job via `SparkSubmitOperator` | built |
| 8 | `mid_manual_agent_chain` | Two agents chained by hand, no framework | built |
| 9 | `mid_langgraph_graph` | Same pipeline rebuilt as an explicit LangGraph `StateGraph` | built |
| 10 | `mid_human_in_the_loop` | LangGraph interrupt/resume gated on a real Slack reaction | built |

## Tier 3: advanced capstones

| # | Folder | Teaches | Status |
|---|---|---|---|
| 11 | `top_dynamic_etl_pipeline` | Registry-driven dynamic DAG generation + Snowflake-native RAW/BRONZE parsing + dbt SILVER/GOLD models | planned |
| 12 | `top_recon_pipeline_live` | Implements the missing `agents/pipeline.py` for real | planned |
| 13 | `top_slack_requirements_bot` | Slack bot gathers requirements via chat, triggers a multi-agent pipeline, reports back | planned |

See each project's own README for what it demonstrates and why.
