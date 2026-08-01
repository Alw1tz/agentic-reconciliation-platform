"""Shared connection helpers for the learning projects.

mcp_servers/snowflake_server.py, airflow/dags/reconciliation_dag.py, and
test_connections.py each already open a Snowflake connection from the same
seven env vars. Projects under projects/ that need one use this instead of
repeating it again -- and this is the version later projects (like
top_dynamic_etl_pipeline) reuse too.
"""
import os

import boto3
import snowflake.connector


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def get_s3_client():
    return boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
