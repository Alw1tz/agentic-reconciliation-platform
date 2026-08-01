# ez_snowflake_s3_etl

Tier 1, project 3. A minimal Extract + Load, no Airflow/Spark involved --
S3 and Snowflake talking directly, in isolation, before Airflow
orchestrates this same kind of step for real in `top_dynamic_etl_pipeline`.

## What it does

- **Extract**: writes three sample order rows to S3 as CSV, then reads
  them back -- the round trip matters because it's standing in for "a
  file already landed in S3 from an external system," the same role S3
  plays in `reconciliation_dag.py` and the real Minsait-style pipeline
  this curriculum's capstone is modeled on.
- **Load**: `CREATE OR REPLACE TABLE EZ_HELLO_ETL`, then `INSERT` the rows,
  then a `SELECT COUNT(*)` to confirm the load. `CREATE OR REPLACE`
  (rather than `IF NOT EXISTS`) makes re-running the script idempotent --
  no duplicate rows piling up across runs.

Uses `get_s3_client()` / `get_snowflake_connection()` from
`projects/_shared/connections.py` instead of repeating the connection
boilerplate that's already duplicated across `mcp_servers/snowflake_server.py`
and `test_connections.py`.

## Run it

```bash
uv run python3 projects/ez_snowflake_s3_etl/etl.py
```

Hits your real S3 bucket and Snowflake database/schema from `.env`.

## What to look for

- Console output shows the S3 round trip (write, then read back) and the
  Snowflake load, ending with a row count confirming 3.
- Check the Snowflake UI: `EZ_HELLO_ETL` in your configured
  database/schema should show the 3 sample orders. Optional cleanup:
  `DROP TABLE EZ_HELLO_ETL;`.
