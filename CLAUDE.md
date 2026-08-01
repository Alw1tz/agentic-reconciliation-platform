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
