# ez_airflow_hello

Tier 1, project 1. A brand-new Airflow DAG, not the reconciliation one --
DAG anatomy in isolation, before anything agentic sits on top of it.

## The real file

The DAG itself lives at `airflow/dags/ez_hello_dag.py`, not in this folder
-- `docker-compose.yaml` only mounts `./airflow/dags` into the Airflow
containers, so a DAG has to live there to actually show up in the
scheduler/UI. This folder just holds the explanation.

## What it does

Three tasks, wired by data instead of by `>>`:

```python
load(transform(extract()))
```

- `extract()` returns a list of numbers.
- `transform(numbers)` receives it -- Airflow automatically pulled
  `extract()`'s return value as an XCom and passed it in, because calling
  `transform(extract())` *is* the dependency declaration under the
  TaskFlow API (`@task`). No `xcom_push`/`xcom_pull` written by hand.
- `load(summary)` receives `transform()`'s result the same way, and just
  prints it (no real system on purpose -- see `ez_snowflake_s3_etl` for a
  Load step against real infra).

`transform` also overrides `retries=2` (the DAG-wide default from
`get_default_args()` is 1), showing retries are a per-task setting for
when one step is flakier than the rest.

## Run it

The DAG file is picked up automatically by the DAG processor (already
running via `docker compose up -d`). Trigger it:

```bash
docker compose exec airflow-apiserver airflow dags trigger ez_hello_dag
```

Or trigger it from the Airflow UI (http://localhost:8082, airflow/airflow)
-- search for `ez_hello_dag`.

## What to look for

- The **Graph** view in the UI shows `extract -> transform -> load` as a
  straight line, inferred entirely from the data dependencies above.
- Click into `transform`'s task instance -> **XCom** tab: you'll see the
  dict `extract()` returned, listed as `return_value`, which is what
  `transform` received as its argument.
- `load`'s logs print the final summary dict -- confirms the whole chain
  passed data correctly end to end.
