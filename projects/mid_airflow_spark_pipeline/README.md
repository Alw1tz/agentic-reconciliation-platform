# mid_airflow_spark_pipeline

Tier 2, project 7. An Airflow DAG orchestrating a real Spark job via
`SparkSubmitOperator` -- built from scratch, separate from
`reconciliation_dag.py`, so the orchestration pattern itself (stage data,
hand off to Spark over `s3a`, read the result back) is understood on its
own before agent logic gets layered on top of it in tier 3.

Uses a real dataset -- seaborn's `tips.csv` (244 real restaurant checks:
total bill, tip, day, time, party size, BSD-3 licensed) -- instead of
hardcoded rows, unlike `ez_hello_dag.py`'s synthetic numbers or
`reconciliation_dag.py`'s scripted mismatch.

## What it does

Real files, per the docker-compose mount constraint (`./airflow/dags` and
`./spark/apps` are the only two directories mounted into their respective
containers -- see `CURRICULUM.md`):

- `airflow/dags/mid_airflow_spark_dag.py` -- the DAG
- `spark/apps/mid_airflow_spark_job.py` -- the Spark job
- `airflow/dags/data/tips.csv` -- the real dataset, local to the Airflow
  container's mount

Flow:

1. **`seed_and_export`** -- reads the local `tips.csv`, uploads it to S3 via
   `upload_csv_to_s3()` (`airflow/dags/utils/commons.py`) -- same shape as
   `reconciliation_dag.py`'s `seed_and_export`, real data instead of a
   hardcoded list.
2. **`spark_aggregate`** (`SparkSubmitOperator`) -- same `hadoop-aws`
   package and `s3a` conf block as `reconciliation_dag.py`'s `spark_diff`
   task. The Spark job reads the CSV over `s3a://`, groups by `day` and
   `time`, and aggregates check count, total bill, average tip, and
   average tip percentage -- writing a single JSON report back to S3
   (`coalesce(1).saveAsTextFile`, same pattern as `diff_source_target.py`).
3. **`read_summary`** -- downloads and prints that JSON report, so the
   aggregation is visible in the task's own logs, not just Spark's UI.

## Run it

```bash
docker compose exec airflow-apiserver airflow dags trigger mid_airflow_spark_dag
```

Or trigger `mid_airflow_spark_dag` from the Airflow UI (`localhost:8082`).

## What to look for

- `seed_and_export` logs: "Uploaded 244 rows to s3://...".
- Spark Master UI (`localhost:9090`) shows the `mid_airflow_spark_job`
  application running across the workers while `spark_aggregate` executes.
- `read_summary` task logs print the full JSON: a `row_count` of 244 and a
  `by_day_and_time` breakdown -- confirms the s3a round trip worked end to
  end (Airflow to S3 to Spark to S3 to Airflow).
