# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a full-stack, real-infra rebuild of a JD "08:00 Intelligence Synthesis" scenario for a Lead Agentic Data Systems Engineer interview. The system:

- Airflow orchestrates a nightly reconciliation
- Spark computes day-over-day diffs against real Snowflake tables  
- A LangGraph agent pipeline (scout → remediate) posts findings and drafted fixes to Slack via a Slack MCP server for human approval
- Mirrors the candidate's actual production reconciliation DAG at Rappi, with the missing agentic layer

## Architecture Overview

The system follows a three-tier architecture pattern:

1. **Data Layer**: Snowflake warehouse containing source/target data
2. **Compute Layer**: Spark cluster for distributed diff processing  
3. **Agent Layer**: LangGraph pipeline with MCP servers for secure access to systems

Key components:
- Airflow DAG (`airflow/dags/reconciliation_dag.py`) orchestrates the full workflow
- Spark job (`spark/apps/diff_source_target.py`) performs distributed diffs 
- Agent pipeline (`agents/minimal_pipeline.py`) with modular agent design
- MCP servers (`mcp_servers/`) for secure system access:
  - `snowflake_server.py` - read-only Snowflake access with SQL validation
  - `slack_server.py` - Slack posting and reaction reading

## Key Design Patterns

The codebase follows several important patterns:

1. **Modular Agent Architecture**: Reusable base agent classes (`agents/base/agent.py`) with specific implementations in `agents/minimal/core_agent.py`
2. **MCP Protocol Integration**: All external system access goes through MCP servers for security and controlled access
3. **Human-in-the-Loop Design**: No automatic writes to Snowflake - all changes must be approved via Slack reactions
4. **Separation of Concerns**: Clear boundaries between data processing, agent logic, and system interaction

## Development Commands

```bash
# Setup and initialization
docker compose up airflow-init   # first time only
docker compose up -d             # start all services

# Running the pipeline  
docker compose exec airflow-apiserver airflow dags trigger reconciliation_dag

# Testing/development
python3 agents/minimal_pipeline.py  # run minimal demo
./run_demo.sh                       # run full demo script

# Monitoring
# Airflow UI: http://localhost:8082 (airflow / airflow)
# Spark Master UI: http://localhost:9090  
# Spark Worker UIs: http://localhost:9091, http://localhost:9092
```

## File Structure

Key directories and files:
- `airflow/dags/reconciliation_dag.py` - Main orchestration DAG
- `spark/apps/diff_source_target.py` - Spark diff job  
- `agents/` - Agent implementations with base classes
- `mcp_servers/` - Secure MCP servers for Snowflake and Slack access
- `run_demo.sh` - Quick demo script

## Known Gap

`airflow/dags/reconciliation_dag.py` imports `agents/pipeline.py` (a
`run_pipeline` function), which does not exist yet. Only
`agents/minimal_pipeline.py` and `agents/minimal/simple_pipeline.py` exist,
and neither is what the DAG actually calls. This gets fixed for real by
`projects/top_recon_pipeline_live` (see Learning Projects below) rather than
patched ad hoc.

## Coding Standards

- No emojis in code comments, documentation, or output — professional,
  technical language only.
- New standalone project folders under `projects/` are prefixed by
  difficulty: `ez_` (basic, single concept in isolation), `mid_` (moderate,
  chaining/orchestration), `top_` (expert, multi-agent + real infra).

## Learning Projects

`projects/` is a progressive curriculum (ez → mid → top) covering this
repo's whole stack — Airflow, Spark, Snowflake/S3, MCP, agents, LangGraph,
and (capstone-only) dbt — one piece in isolation before combining them,
ending in a registry-driven dynamic ELT pipeline and a conversational Slack
capstone. Most projects are
self-contained and run via `uv run python3 projects/<name>/...`; a few
(marked below) place their real DAG/Spark file under `airflow/dags/` or
`spark/apps/` instead, since those are the only two directories
docker-compose mounts into those containers — see `CURRICULUM.md` for full
detail on each project and `projects/README.md` for live status.

| # | Folder | Teaches |
|---|---|---|
| 1 | `ez_airflow_hello` | Author an Airflow DAG from scratch (`airflow/dags/`) |
| 2 | `ez_spark_hello` | Author a standalone Spark job from scratch (`spark/apps/`) |
| 3 | `ez_snowflake_s3_etl` | Minimal Extract (S3) + Load (Snowflake), no orchestration |
| 4 | `ez_mcp_basics` | Raw MCP protocol (toy server + client), no LLM involved |
| 5 | `ez_agent_tool_loop` | Raw Claude tool-use loop via the `anthropic` SDK, no MCP/LangGraph |
| 6 | `ez_agent_meets_mcp` | An agent whose tools are discovered dynamically from an MCP server |
| 7 | `mid_airflow_spark_pipeline` | An Airflow DAG orchestrating a real Spark job (`airflow/dags/` + `spark/apps/`) |
| 8 | `mid_manual_agent_chain` | Two agents chained by hand, no framework |
| 9 | `mid_langgraph_graph` | Same pipeline as an explicit LangGraph `StateGraph` |
| 10 | `mid_human_in_the_loop` | LangGraph interrupt/resume gated on a real Slack reaction |
| 11 | `top_dynamic_etl_pipeline` | Registry-driven dynamic DAGs + Snowflake-native RAW/BRONZE parsing + dbt SILVER/GOLD |
| 12 | `top_recon_pipeline_live` | Implements the missing `agents/pipeline.py` for real |
| 13 | `top_slack_requirements_bot` | Slack bot gathers requirements via chat, triggers a multi-agent pipeline, reports back |
