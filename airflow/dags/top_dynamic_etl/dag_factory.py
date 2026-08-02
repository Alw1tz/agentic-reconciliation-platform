"""Builds one Airflow DAG per REGISTRY entry -- top_dynamic_etl_pipeline,
tier 3 capstone (projects/top_dynamic_etl_pipeline/README.md).

Per generated DAG: sense a one-time-sync completion marker in S3 -> load
the whole line as one column into a Snowflake RAW table via a single
COPY INTO (Snowflake fans out over however many files match internally --
no per-file Airflow task, since this repo's SequentialExecutor runs one
task at a time repo-wide and a per-file design would mean tens of
thousands of serialized task instances for a real interface like
noaa_isd's 13,345 files) -> one dynamically-mapped BRONZE parse task per
record type (`.expand()`) -> a count-validation gate -> a dbt run building
SILVER/GOLD on top of BRONZE.

Snowflake access is a plain `snowflake.connector.connect()` call inline in
each task, matching reconciliation_dag.py's and mid_airflow_spark_dag.py's
existing precedent (not projects/_shared/connections.py's
get_snowflake_connection() -- projects/ isn't mounted into the Airflow
container, so that module isn't actually importable here despite its own
docstring's claim). S3 access is plain boto3 with env-var credentials via
a `@task.sensor`, matching utils/commons.py's explicit documented
philosophy ("keeps this repo's setup to a single .env") rather than
apache-airflow-providers-amazon's S3KeySensor + an aws_default Connection
object, which this repo deliberately doesn't use anywhere else.
"""
from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.sdk import task
from airflow.sdk.bases.sensor import PokeReturnValue

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from registry import REGISTRY, SOFT_COUNT_INTERFACES, InterfaceConfig
from sql_generator import bronze_table_name, build_bronze_create_table_sql, build_bronze_insert_sql

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.settings import S3_BUCKET, get_default_args

SYNC_COMPLETE_MARKER = "_SYNC_COMPLETE"


def _snowflake_connect():
    """Plain snowflake.connector.connect() from the same seven env vars
    every other Snowflake caller in this repo uses -- matches
    reconciliation_dag.py's inline pattern exactly, not a shared import
    (projects/_shared/connections.py isn't mounted into this container)."""
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def _s3_client():
    import boto3

    return boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))


def build_dag(cfg: InterfaceConfig) -> DAG:
    dag_id = f"top_dynamic_etl_{cfg.interface_id}"

    with DAG(
        dag_id=dag_id,
        description=f"[Capstone] Registry-driven dynamic ETL -- {cfg.description}",
        schedule=None,
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        default_args=get_default_args("top_dynamic_etl"),
        catchup=False,
        tags=["capstone", "dynamic-etl", cfg.interface_id],
    ) as dag:

        @task.sensor(mode="reschedule", timeout=60 * 60, poke_interval=30)
        def wait_for_sync() -> PokeReturnValue:
            """Senses a single completion marker the one-time bulk-sync
            script writes after uploading, not individual filenames --
            the real files (potentially thousands) are loaded in one
            Snowflake-side COPY INTO next, not sensed/loaded one at a
            time."""
            marker_key = f"{cfg.s3_prefix}/{SYNC_COMPLETE_MARKER}"
            client = _s3_client()
            try:
                client.head_object(Bucket=S3_BUCKET, Key=marker_key)
                found = True
            except client.exceptions.ClientError:
                found = False
            return PokeReturnValue(is_done=found)

        @task
        def load_raw() -> None:
            """One COPY INTO, Snowflake-side fan-out over however many
            files match cfg.file_pattern under cfg.s3_prefix -- not a
            per-file Airflow task.

            Goes through a temporary external stage rather than a bare
            's3://...' URL in the COPY INTO's FROM clause -- METADATA$FILENAME
            (needed to persist source_file_name -- see below) is only
            resolvable when selecting from a stage, verified directly
            against Snowflake before wiring this in (a bare external URL
            raised "invalid URL prefix" the moment a SELECT wrapped it)."""
            stage_name = f"{cfg.raw_table}_STAGE"
            con = _snowflake_connect()
            try:
                cur = con.cursor()
                # source_file_name is captured here, at COPY INTO time, via
                # METADATA$FILENAME -- that pseudocolumn only exists when
                # selecting directly from a stage. Once loaded into a plain
                # table it's gone, so BRONZE (which SELECTs from this table,
                # not from the stage) needs it persisted as a real column.
                cur.execute(
                    f"CREATE OR REPLACE TABLE {cfg.raw_table} "
                    f"(line_content STRING, source_file_name STRING)"
                )
                compression = cfg.compression or "NONE"
                access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
                secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
                cur.execute(
                    f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}\n"
                    f"URL = 's3://{S3_BUCKET}/{cfg.s3_prefix}/'\n"
                    f"CREDENTIALS = (AWS_KEY_ID='{access_key}' AWS_SECRET_KEY='{secret_key}')\n"
                    f"FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = NONE "
                    f"COMPRESSION = {compression} ENCODING = 'UTF8')"
                )
                cur.execute(
                    f"COPY INTO {cfg.raw_table} (line_content, source_file_name)\n"
                    f"FROM (SELECT $1, METADATA$FILENAME FROM @{stage_name})\n"
                    f"PATTERN = '{cfg.file_pattern}'\n"
                    f"ON_ERROR = 'CONTINUE'"
                )
                con.commit()
                cur.execute(f"SELECT COUNT(*) FROM {cfg.raw_table}")
                print(f"{cfg.raw_table}: loaded {cur.fetchone()[0]} lines")
            finally:
                con.close()

        @task
        def parse_bronze(record_type_name: str) -> str:
            """Mapped over record-type NAMES (plain strings), not
            RecordType dataclass instances -- .expand() passes its inputs
            through XCom, and there's no guarantee a custom frozen
            dataclass round-trips through Airflow's default XCom
            serialization. The actual RecordType is looked up from the
            module-level REGISTRY inside the task body instead, which
            needs nothing serialized through XCom but a string."""
            record_type = next(rt for rt in cfg.record_types if rt.name == record_type_name)
            table = bronze_table_name(cfg, record_type)
            con = _snowflake_connect()
            try:
                cur = con.cursor()
                cur.execute(build_bronze_create_table_sql(record_type, table))
                cur.execute(build_bronze_insert_sql(record_type, cfg.raw_table, table))
                con.commit()
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"{table}: parsed {count} rows")
            finally:
                con.close()
            return table

        @task
        def validate_counts(bronze_tables: list[str]) -> None:
            con = _snowflake_connect()
            try:
                cur = con.cursor()
                cur.execute(f"SELECT COUNT(*) FROM {cfg.raw_table}")
                raw_count = cur.fetchone()[0]

                bronze_count = 0
                for table in bronze_tables:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    bronze_count += cur.fetchone()[0]
            finally:
                con.close()

            diff_pct = abs(raw_count - bronze_count) / raw_count if raw_count else 0.0
            tolerance = cfg.count_tolerance_pct / 100 if cfg.interface_id in SOFT_COUNT_INTERFACES else 0.0
            message = (
                f"{cfg.interface_id}: RAW={raw_count} BRONZE={bronze_count} "
                f"(diff={diff_pct:.4%}, tolerance={tolerance:.4%})"
            )
            if diff_pct > tolerance:
                raise ValueError(message)
            print(message + " -- within tolerance")

        @task
        def run_dbt() -> None:
            from dbt.cli.main import dbtRunner

            dbt_project_dir = "/opt/airflow/dbt"
            result = dbtRunner().invoke(
                [
                    "run",
                    "--project-dir", dbt_project_dir,
                    "--profiles-dir", dbt_project_dir,
                    "--select", f"tag:{cfg.interface_id}",
                ]
            )
            if not result.success:
                raise RuntimeError(f"dbt run failed for {cfg.interface_id}: {result.exception}")

        bronze_tables = parse_bronze.expand(record_type_name=[rt.name for rt in cfg.record_types])
        wait_for_sync() >> load_raw() >> bronze_tables >> validate_counts(bronze_tables) >> run_dbt()

    return dag


for _cfg in REGISTRY.values():
    globals()[f"top_dynamic_etl_{_cfg.interface_id}"] = build_dag(_cfg)
