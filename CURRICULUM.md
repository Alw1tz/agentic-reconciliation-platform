# Full-stack teaching curriculum

Full proposal for the projects under `projects/` (see `projects/README.md`
for the live status table, and `CLAUDE.md` for the short version). This
covers the whole stack this repo runs -- Airflow, Spark, Snowflake, S3, MCP,
agents, LangGraph, and (as of the capstone tier) dbt -- starting with each
piece in isolation and ending in two advanced capstones: a registry-driven
dynamic ELT pipeline, and the agentic layer on top of it. Each project is
built one at a time, with comments and a README explaining *why* it's built
the way it is, not just what it does.

The dynamic ELT capstone (project 11) generalizes a real production
pattern -- a fixed-width-file ingestion framework -- around a fictitious
interface/company rather than reproducing real employer schema/data here.
dbt is scoped to that project's own `dbt/` folder rather than a repo-wide
dependency, since `~/Documents/dev/CLAUDE.md`'s sandbox-boundary rule
otherwise keeps dbt work in the sibling `dbt-snowflake-analytics` repo --
this is a deliberate, scoped exception for this one project, not a general
policy change.

All real credentials (Snowflake, Slack, S3, Anthropic) are already
configured in `.env` and verified working via `test_connections.py`.

**One infra constraint that shapes where files go**: `docker-compose.yaml`
only mounts `./airflow/dags` into the Airflow containers and `./spark/apps`
into the Spark containers. A DAG or Spark job that needs to run against the
*real* scheduler/cluster has to physically live there -- not under
`projects/`. Each such project's `projects/<name>/` folder holds the README
and explanation; the file Airflow or Spark actually executes lives in its
real location. This is the same split `reconciliation_dag.py` /
`diff_source_target.py` already use.

Reusable utilities worth knowing about before reinventing them:
- `airflow/dags/utils/settings.py` -- `get_default_args()`, `S3_BUCKET`, `SPARK_CONN_ID`
- `airflow/dags/utils/commons.py` -- `upload_csv_to_s3`, `download_from_s3`, `list_s3_objects`
- `mcp_servers/snowflake_server.py` / `slack_server.py` -- real MCP servers, reused directly in later tiers

---

## Tier 1: foundations -- one piece of infra, isolated, from scratch

### 1. `ez_airflow_hello`

**Goal**: author a brand-new Airflow DAG from scratch -- not the existing
reconciliation one. A few `@task`-decorated Python functions with explicit
dependencies and a value passed between them via XCom, plus retries.

**What you learn**: DAG anatomy -- tasks, dependencies, scheduling, XComs,
retries -- and the scheduler's mental model, before any agent logic gets
layered on top of it. This is the orchestration primitive everything else
in the "top" tier sits on.

**Dependencies**: none new -- Airflow's already running.

**Real infra needed**: the Airflow containers (already up).

**Files**: DAG at `airflow/dags/ez_hello_dag.py` (real location, so it shows
up in the Airflow UI and can actually be triggered/scheduled);
`projects/ez_airflow_hello/README.md` explains the DAG and how to watch it
run.

---

### 2. `ez_spark_hello`

**Goal**: author a standalone Spark job from scratch -- not the diff job --
that reads a small CSV and does a `groupBy`/aggregation with the DataFrame
API, submitted to the real 3-node standalone cluster via `spark-submit`.

**What you learn**: the distributed DataFrame model (lazy transformations,
an action that triggers a real distributed job, partitions across the 2
workers) and what "submitting a job" to a cluster actually means, as
opposed to a local pandas script.

**Dependencies**: `pyspark` (already in `pyproject.toml`) for local
testing; the container has its own Spark install for the real submit.

**Real infra needed**: the Spark cluster (master + 2 workers, already up).

**Files**: job at `spark/apps/ez_hello_job.py` (real location -- mounted
into all three Spark containers); `projects/ez_spark_hello/README.md` with
the exact `docker compose exec spark-master spark-submit ...` command.

---

### 3. `ez_snowflake_s3_etl`

**Goal**: a minimal Extract + Load, no Spark/Airflow involved -- pull a file
from S3 with `boto3`, load it into a new Snowflake table with
`snowflake-connector-python`.

**What you learn**: S3 client basics and Snowflake write patterns
(`CREATE TABLE`, `INSERT`/`COPY INTO`) in isolation, before Airflow
orchestrates this exact kind of step in tier 3.

**Dependencies**: `boto3`, `snowflake-connector-python` (both already in
`pyproject.toml`). No new deps.

**Real infra needed**: real S3 bucket + real Snowflake (already
configured).

**Files**: fully under `projects/ez_snowflake_s3_etl/` -- no shared infra
to mount, it's just a local script hitting real S3/Snowflake.

**Real gotcha hit building this**: `.env`'s `SNOWFLAKE_ROLE=SYSADMIN` had
no `CREATE TABLE` grant on `AGENTIC_PLATFORM.PROJECTS` -- that
database/schema were created under `ACCOUNTADMIN` (per `notes.txt`'s
original setup), so `SYSADMIN` only had implicit read access, not DDL.
Fixed with a one-time `GRANT CREATE TABLE ON SCHEMA
AGENTIC_PLATFORM.PROJECTS TO ROLE SYSADMIN;` run as `ACCOUNTADMIN`. Same
gap would have blocked `reconciliation_dag.py`'s `seed_and_export` task
the first time it actually ran, since it does the same
`CREATE OR REPLACE TABLE` in the same schema -- now fixed for both.

---

### 4. `ez_mcp_basics` -- DONE

**Goal**: understand the MCP protocol itself, with no LLM anywhere near it.

**What you learn**: what a "tool" is on the wire, the `initialize()`
handshake, `list_tools()` discovery, `call_tool()` request/response shape,
stdio transport.

**Dependencies**: `mcp`. No new deps.

**Real infra needed**: none -- fully local, in-memory toy server.

**Files**: `server.py`, `client.py`, `README.md` -- all in
`projects/ez_mcp_basics/`.

---

### 5. `ez_agent_tool_loop`

**Goal**: understand the Claude tool-use loop as a primitive, before any
framework (MCP, LangGraph) wraps it.

**What you learn**: how `tools=[...]` schemas are passed to the Anthropic
API, how a `tool_use` content block comes back, how you execute it locally
and send a `tool_result` back, and why the loop repeats until Claude stops
requesting tools. Every agent framework, including LangGraph, is built on
top of this.

**Dependencies**: `anthropic`, `python-dotenv`. No new deps.

**Real infra needed**: `ANTHROPIC_API_KEY` only (minimal token cost).

**Files**: `projects/ez_agent_tool_loop/agent.py`, `README.md`.

---

### 6. `ez_agent_meets_mcp`

**Goal**: wire projects 4 and 5 together -- an agent whose tools are
*discovered from an MCP server* instead of hardcoded Python functions.

**What you learn**: converting an MCP tool's JSON Schema into the shape the
Anthropic API expects, and routing a `tool_use` block to `call_tool()` on
an MCP session instead of a local function. `agents/minimal/core_agent.py`
in the repo root currently *fakes* this with hardcoded simulated responses;
this project does it for real.

**Dependencies**: `anthropic`, `mcp`, `python-dotenv`. No new deps.

**Real infra needed**: `ANTHROPIC_API_KEY`. Reuses project 4's toy server.

**Files**: `projects/ez_agent_meets_mcp/agent.py`, `README.md`.

---

## Tier 2: combining two pieces

### 7. `mid_airflow_spark_pipeline`

**Goal**: an Airflow DAG that orchestrates a real Spark job end-to-end via
`SparkSubmitOperator` -- from scratch, separate from
`reconciliation_dag.py`, so the orchestration pattern is understood on its
own before it's buried under agent logic later.

**What you learn**: `SparkSubmitOperator` configuration (connection ID,
application args, packages/conf), and how Airflow tracks a Spark job's
success/failure as a single task.

**Dependencies**: none new.

**Real infra needed**: Airflow + Spark cluster.

**Files**: DAG at `airflow/dags/mid_airflow_spark_dag.py`, reusing project
2's job or a new one in `spark/apps/`; `projects/mid_airflow_spark_pipeline/README.md`.

---

### 8. `mid_manual_agent_chain`

**Goal**: chain two agents by hand -- agent A's output becomes agent B's
input -- with no orchestration framework, over local toy data.

**What you learn**: what "state handoff" between agents actually requires,
and where it gets awkward as the chain grows -- the itch that motivates
project 9.

**Dependencies**: `anthropic`, `python-dotenv`. No new deps.

**Real infra needed**: `ANTHROPIC_API_KEY` only.

**Files**: `projects/mid_manual_agent_chain/pipeline.py`, `README.md`.

---

### 9. `mid_langgraph_graph`

**Goal**: rebuild project 8's exact pipeline as an explicit LangGraph
`StateGraph` -- typed state, nodes, a conditional edge -- for a direct
before/after comparison.

**What you learn**: what LangGraph adds over the manual chain -- typed
shared state, declarative edges, conditional routing.

**Dependencies**: `langgraph`, `langchain-core`, `anthropic`. No new deps.

**Real infra needed**: `ANTHROPIC_API_KEY` only.

**Files**: `projects/mid_langgraph_graph/graph.py`, `README.md`.

---

### 10. `mid_human_in_the_loop`

**Goal**: a LangGraph pipeline that pauses and polls a **real** Slack
reaction (via `mcp_servers/slack_server.py`) before branching approve/reject
-- isolated from Snowflake/Spark so it's fast to iterate on.

**What you learn**: LangGraph's interrupt/resume mechanics, calling a real
MCP server's tools from inside a graph node, polling-with-backoff for a
human action.

**Dependencies**: `langgraph`, `mcp`, `slack-sdk`, `anthropic`. No new deps.

**Real infra needed**: real Slack bot token + channel (already configured).
Posts real messages.

**Files**: `projects/mid_human_in_the_loop/graph.py`, `README.md`.

---

## Tier 3: advanced capstones

### 11. `top_dynamic_etl_pipeline`

**Goal**: a registry-driven, multi-interface ELT framework -- explicitly
modeled on a real production pattern (a fixed-width-file ingestion
framework at Rappi), generalized around a fictitious interface/company so
no real employer schema or data ends up in this repo. No Spark, no agents
-- pure data-engineering rigor first.

The shape: one `@dataclass` config per "interface" in a central registry,
one `build_dag()` factory function, and a loop that generates one Airflow
DAG per registry entry (`globals()[dag_id] = build_dag(...)`) -- N DAGs
from one factory instead of N copy-pasted files. Each generated DAG:
`S3KeySensor(mode='reschedule')` waits for the file -> loads it line-by-line
into a Snowflake **RAW** table (`FIELD_DELIMITER = 'NONE'`, whole line as
one column) -> parses fixed-width fields into a typed **BRONZE** table via
*generated* SQL (`SUBSTRING(LINE_CONTENT, pos, len)` per field, one
Airflow dynamic-mapped task per record type via `.expand()`) -> a count
validation (`sum(bronze rows) == raw line count`, with an allowlist for
interfaces that intentionally discard rows by design, same as
`SOFT_COUNT_INTERFACES`) gates a dbt run that builds **SILVER/GOLD** models
on top of BRONZE.

**Why no Spark here**: fixed-width parsing is exactly what SQL
`SUBSTRING`/`CAST` was built for, and Snowflake's own compute does the
"distributed" part -- adding a Spark job here would be a second compute
layer with no upside. Spark's place in this curriculum is projects 2 and 7,
where the transform genuinely doesn't map to SQL.

**Why dbt fits here and not at the RAW/BRONZE step**: RAW->BRONZE has to
stay dynamically generated SQL because the field layout differs per
interface (that's config, not a stable dbt model). BRONZE->SILVER/GOLD is
business logic -- joins, aggregations, dimensional modeling -- which is
exactly dbt's job, with `ref()`-based lineage and tests instead of more
generated SQL strings.

**What you learn**: dynamic DAG generation from a config registry (a real
senior-Airflow pattern, not a tutorial one); why RAW/BRONZE ingestion and
SILVER/GOLD transformation are different enough concerns to deserve
different tools; `mode='reschedule'` vs `mode='poke'` on sensors and why it
matters at scale (poke holds a worker slot hostage for the entire wait;
reschedule frees it between polls).

**Dependencies**: `boto3`, `snowflake-connector-python`, plus **dbt**
(`dbt-core` + `dbt-snowflake`) as a new dependency -- added when we get
here, scoped to this project's dbt models directory.

**Real infra needed**: Airflow + real S3 + real Snowflake. No Spark
cluster needed for this one.

**Files**: DAG factory + registry at `airflow/dags/top_dynamic_etl/`
(config registry, SQL generator, dynamic DAG builder -- same shape as the
production pattern, fictitious interface); dbt project under
`projects/top_dynamic_etl_pipeline/dbt/` for the SILVER/GOLD models;
`projects/top_dynamic_etl_pipeline/README.md` documents the RAW/BRONZE vs
dbt boundary and why each piece lives where it does.

**Builds on**: 1, 3 (Airflow authoring, S3->Snowflake basics), generalizing
the real `minsait_hybrid` pattern.

---

### 12. `top_recon_pipeline_live`

**Goal**: implement the still-missing `agents/pipeline.py`. Currently
`airflow/dags/reconciliation_dag.py` imports `from pipeline import
run_pipeline`, and that module does not exist. This builds it: LangGraph +
both real MCP servers, so the DAG finally runs end-to-end -- scout (confirm
anomaly via real SQL) -> remediate (draft a real SQL fix via Claude) -> post
to Slack -> await approval, against the real Snowflake data the DAG seeds.

**Dependencies**: `langgraph`, `mcp`, `snowflake-connector-python`,
`slack-sdk`, `sqlglot`, `anthropic`. No new deps.

**Real infra needed**: real Snowflake + real Slack + Anthropic API.

**Files**: `agents/pipeline.py` (real location -- this is the fix, not a
standalone exercise); `projects/top_recon_pipeline_live/README.md`
explains the design and how to trigger `reconciliation_dag` afterward.

**Builds on**: 6, 9, 10, and the real MCP servers directly.

**Design principle worth carrying from project 11**: the rule-based count
check there (`sum(rows) == total_lines`, with an allowlist for known
exceptions) stays a deterministic SQL check -- cheap and instant. The
agent's job is the tier above that: engage only when a mismatch doesn't
match any known allowlisted reason, i.e. when it needs judgment a fixed
rule can't cheaply express. Rules first; the agent is for what's left over,
not a replacement for `COUNT(*)`.

---

### 13. `top_slack_requirements_bot`

**Goal**: the final capstone. A Slack bot (Socket Mode) holds a multi-turn
conversation in a channel to gather requirements, asking clarifying
questions back. On a trigger (slash command or reaction), a supervisor
agent spawns sub-agents (planner -> executor -> reporter) over MCP tools and
reports results back into the same thread.

**What you learn**: event-driven agent triggering (not just
request/response), multi-turn conversational state across Slack messages,
and the supervisor/sub-agent orchestration pattern.

**Dependencies**: everything above, plus **one new dependency added when we
get here**: Slack Socket Mode needs `slack-sdk[socket_mode]` (pulls in
`websocket-client`) or `slack_bolt`. Not added yet.

**Real infra needed**: real Slack (Socket Mode needs an app-level token
with `connections:write`, in addition to the bot token already configured)
+ Anthropic API + whichever MCP servers the sub-agents need.

**Files**: `projects/top_slack_requirements_bot/bot.py` (Socket Mode
listener), `supervisor.py` (planner/executor/reporter agents), `README.md`.

**Builds on**: 11, 12, and every prior tier.

---

## Summary table

| # | Folder | Tier | Real location | New deps | Status |
|---|---|---|---|---|---|
| 1 | `ez_airflow_hello` | foundations | `airflow/dags/` | none | **done** |
| 2 | `ez_spark_hello` | foundations | `spark/apps/` | none | **done** |
| 3 | `ez_snowflake_s3_etl` | foundations | `projects/` | none | **done** |
| 4 | `ez_mcp_basics` | foundations | `projects/` | none | **done** |
| 5 | `ez_agent_tool_loop` | foundations | `projects/` | none | **done** |
| 6 | `ez_agent_meets_mcp` | foundations | `projects/` | none | **done** |
| 7 | `mid_airflow_spark_pipeline` | combining | `airflow/dags/` + `spark/apps/` | none | built |
| 8 | `mid_manual_agent_chain` | combining | `projects/` | none | built |
| 9 | `mid_langgraph_graph` | combining | `projects/` | none | built |
| 10 | `mid_human_in_the_loop` | combining | `projects/` | none | built |
| 11 | `top_dynamic_etl_pipeline` | capstone | `airflow/dags/` + `projects/` (dbt) | `dbt-core`, `dbt-snowflake` | planned |
| 12 | `top_recon_pipeline_live` | capstone | `agents/` | none | planned |
| 13 | `top_slack_requirements_bot` | capstone | `projects/` | Slack Socket Mode | planned |
