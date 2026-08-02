---
name: verify-projects
description: Runs one or all of this repo's learning-curriculum projects for real (Airflow trigger, spark-submit, or uv run, per each project's own README) and checks actual output against that README's documented expectations. Reports PASS/FAIL/SKIPPED per project. Use when asked to verify, test, smoke-test, or check that the ez_/mid_/top_ projects, DAGs, or Spark jobs still work.
argument-hint: "[project-name | ez|mid|top | all]"
disable-model-invocation: true
disallowed-tools: Write Edit NotebookEdit
---

# Verify projects

This repo's `projects/` directory is a progressive curriculum (`ez_` ->
`mid_` -> `top_`, see `CURRICULUM.md` and `projects/README.md`). Every
project's own `README.md` already documents, in a "Run it" section, the
exact command that runs it for real, and in a "What to look for" section,
what a correct run actually produces. This skill does not hardcode
per-project knowledge -- it reads those two sections and drives the run,
so it keeps working as new projects get added without needing to be
edited itself.

This runs real things: real Docker/Airflow triggers, real `spark-submit`
jobs, real Anthropic API calls, and for `mid_human_in_the_loop`, a real
Slack post. That's why `disable-model-invocation: true` is set on this
skill -- it must be explicitly requested (`/verify-projects ...`), never
auto-triggered. Be transparent in the final report about any real side
effect (API spend, a Slack message posted) the same way you would in
normal conversation.

`disallowed-tools` excludes `Write`/`Edit`/`NotebookEdit` deliberately --
this skill verifies, it does not fix. If a run fails, report it in detail;
do not edit code to make it pass. That's a separate, explicit follow-up
the user asks for afterward if they want it.

## Step 1: resolve the target project list

Read `projects/README.md`'s status table(s) to get the authoritative list
of projects and their current status (`done`/`built`/`planned`).

- A specific folder name (e.g. `mid_airflow_spark_pipeline`) -> just that
  one project.
- A tier prefix (`ez`, `mid`, `top`) -> every project under that tier.
- `all` or no argument -> every project across all tiers.

Any project whose status is still `planned` (not yet built) gets an
immediate `SKIPPED (not yet built)` result -- do not attempt to run it,
and do not treat this as a failure.

## Step 2: read each project's README

For each project in scope, read `projects/<name>/README.md` in full and
locate its "Run it" and "What to look for" sections (or equivalently named
sections if a README's headers vary slightly). If a README has neither
section, report `SKIPPED (README has no Run it / What to look for
section)` rather than guessing at a run command.

## Step 3: classify the run command and execute it for real

The "Run it" code block's shape tells you which pattern applies. This
repo currently has three shapes; a new project could introduce a fourth,
in which case adapt the same spirit (run it for real, capture output,
check exit status) rather than skipping it.

**A. Standalone script** -- `uv run python3 projects/<name>/...`
Run it directly. Capture stdout and the exit code. If the README documents
variant flags (e.g. `mid_*` projects' `--offset N` to exercise a different
code path), run each documented variant, not just the default -- the
point is confirming both branches actually work, not just the happy path.

**B. Airflow-DAG-backed** -- `docker compose exec airflow-apiserver airflow
dags trigger <dag_id>`
Trigger it, then poll task states the same way used throughout this
session's manual debugging:
```
docker compose exec -T airflow-apiserver airflow tasks states-for-dag-run <dag_id> <run_id>
```
Poll every ~8s until every task reaches a terminal state (`success` or
`failed`) or a reasonable timeout (a few minutes) elapses. All `success` =
PASS. Any `failed` = FAIL -- pull that task's log
(`docker compose exec -T airflow-apiserver bash -c "cat
/opt/airflow/logs/dag_id=<dag>/run_id=<run>/task_id=<task>/attempt=*.log"`,
same as used earlier this session) and include the actual error in the
report, not just "it failed."

**C. Standalone Spark job (no Airflow)** -- `docker compose exec
spark-master spark-submit ...`
Run it directly, check exit code. Note that "What to look for" bullets
referencing the Spark Master UI (port 9090) are not automatable from the
CLI -- call that out explicitly rather than skipping silently (see Step 4).

## Step 4: check "What to look for" against actual output

Go bullet by bullet through the README's "What to look for" section:

- If a bullet describes something CLI-observable (a specific printed
  string, a JSON key/value, a log line, an exit code) -- check the actual
  captured output against it and note whether it matched.
- If a bullet describes something only visible in a UI (Airflow Graph
  view, Spark Master UI, an XCom tab, a Slack reaction) -- explicitly mark
  it `not automatable -- verify visually`. Never claim a UI-only check
  passed just because the underlying run succeeded.

## Step 5: report

End with a summary table:

| Project | Tier | Result | Notes |
|---|---|---|---|

Then, for any `FAIL`, include full detail underneath: the actual command
run, the actual error/log excerpt, and which specific "What to look for"
expectation broke. For any run with a real side effect (Anthropic spend,
a Slack post), say so plainly in that project's row or notes -- don't bury
it.
