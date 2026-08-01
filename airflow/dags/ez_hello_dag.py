"""A minimal Airflow DAG built from scratch -- tier 1, project 1 of the
learning curriculum (projects/ez_airflow_hello/README.md).

Deliberately doesn't touch Snowflake/Spark/S3, unlike reconciliation_dag.py
-- the point here is DAG anatomy itself (tasks, dependencies, XComs,
retries), not what gets orchestrated.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.decorators import task

from utils.settings import get_default_args

with DAG(
    dag_id="ez_hello_dag",
    description="[Learning] Tier 1 -- DAG anatomy: tasks, XComs, retries.",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    default_args=get_default_args("learning"),
    catchup=False,
    tags=["learning", "ez", "airflow"],
) as dag:

    @task
    def extract() -> list[int]:
        """Stand-in for "pull records from somewhere". Under the TaskFlow
        API (@task), a task's return value is pushed as an XCom
        automatically -- no explicit xcom_push() call needed."""
        return [10, 25, 40, 15, 30]

    @task(retries=2, retry_delay=pendulum.duration(seconds=10))
    def transform(numbers: list[int]) -> dict:
        """`numbers` arrives here because calling transform(extract())
        below makes Airflow pull extract()'s XCom automatically -- that
        call *is* the dependency declaration, same effect as
        `extract_task >> transform_task` but with data flowing too.

        retries=2 here (vs. the DAG-wide default of 1 from
        get_default_args) shows retries are a per-task override, useful
        when one step is flakier than the rest of the DAG.
        """
        return {
            "count": len(numbers),
            "total": sum(numbers),
            "avg": round(sum(numbers) / len(numbers), 2),
        }

    @task
    def load(summary: dict) -> None:
        """Stand-in for "write results somewhere real". No real infra on
        purpose in this project -- see ez_snowflake_s3_etl for an actual
        Load step against S3 and Snowflake."""
        print(f"Loaded summary: {summary}")

    load(transform(extract()))
