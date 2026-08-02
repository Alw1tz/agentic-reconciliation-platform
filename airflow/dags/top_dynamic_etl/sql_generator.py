"""Turns a RecordType's field list into the RAW -> BRONZE parsing SQL --
top_dynamic_etl_pipeline, tier 3 capstone
(projects/top_dynamic_etl_pipeline/README.md).

Pure functions, no Snowflake/Airflow calls -- unit-testable by asserting
exact string shape without touching real infra. sqlglot (already a
dependency, used the same way in mcp_servers/snowflake_server.py --
parse-and-validate, not an AST builder) is used only as a cheap "exactly
one INSERT statement came out" assertion on the generated string, not to
construct the SQL.
"""
import sys
from pathlib import Path

import sqlglot
import sqlglot.errors

# Self-sufficient rather than relying on dag_factory.py having already put
# this directory on sys.path -- Airflow's DAG file processor also parses
# this file independently (as its own standalone scan, not only via
# dag_factory.py's import), and /opt/airflow/dags isn't automatically on
# sys.path in this Airflow 3 setup (verified directly: it isn't, unlike
# older Airflow versions).
sys.path.insert(0, str(Path(__file__).resolve().parent))

from registry import FieldType, FixedWidthField, InterfaceConfig, RecordType


def bronze_table_name(cfg: InterfaceConfig, record_type: RecordType) -> str:
    return f"{cfg.bronze_table_prefix}_{record_type.name.upper()}"


_SNOWFLAKE_COLUMN_TYPE = {
    FieldType.STRING: "STRING",
    FieldType.INTEGER: "NUMBER(38,0)",
    FieldType.DECIMAL: "NUMBER(38,10)",
    FieldType.DATE: "DATE",
}


def build_bronze_create_table_sql(record_type: RecordType, bronze_table: str) -> str:
    """DDL for one BRONZE table, columns/types derived directly from the
    field list -- the INSERT this table receives (build_bronze_insert_sql)
    has an explicit column list per field, so the table has to have a
    real, matching column for each one rather than inheriting RAW's single
    line_content column."""
    columns = [f"  {f.name} {_SNOWFLAKE_COLUMN_TYPE[f.field_type]}" for f in record_type.fields]
    columns.append("  source_file_name STRING")
    columns.append("  load_ts TIMESTAMP_NTZ")
    columns_sql = ",\n".join(columns)
    return f"CREATE OR REPLACE TABLE {bronze_table} (\n{columns_sql}\n)"


def _substring_expr(field: FixedWidthField) -> str:
    return f"SUBSTRING(line_content, {field.start_pos}, {field.length})"


def _trimmed_expr(field: FixedWidthField) -> str:
    return f"TRIM({_substring_expr(field)})"


def _sentinel_predicate(field: FixedWidthField) -> str | None:
    if not field.missing_sentinels:
        return None
    quoted = ", ".join(f"'{s}'" for s in field.missing_sentinels)
    return f"{_trimmed_expr(field)} IN ({quoted})"


def _cast_expr(field: FixedWidthField) -> str:
    """The "real value" expression, before sentinel-to-NULL handling. Uses
    TRY_CAST, never CAST -- a hard CAST failure on one bad byte deep in a
    multi-million-row INSERT...SELECT would fail the entire statement,
    which defeats the point of the count-validation gate downstream (it
    needs individual bad rows to survive as NULLs, not take the whole load
    down)."""
    trimmed = _trimmed_expr(field)
    if field.field_type is FieldType.STRING:
        return trimmed
    if field.field_type is FieldType.INTEGER:
        return f"TRY_CAST({trimmed} AS NUMBER(38,0))"
    if field.field_type is FieldType.DECIMAL:
        return f"TRY_CAST({trimmed} AS NUMBER(38,10)) / {field.scaling_factor}"
    if field.field_type is FieldType.DATE:
        return f"TRY_TO_DATE({trimmed}, 'YYYYMMDD')"
    raise ValueError(f"Unhandled field_type: {field.field_type}")  # pragma: no cover


def _field_expr(field: FixedWidthField) -> str:
    """Full expression for one output column: sentinel check (against the
    raw trimmed substring, BEFORE casting -- TRY_CAST on a sentinel like
    "+99999" would otherwise silently produce a real bogus number instead
    of NULL) wrapping the cast expression."""
    sentinel_predicate = _sentinel_predicate(field)
    cast = _cast_expr(field)
    if sentinel_predicate is None:
        return f"{cast} AS {field.name}"
    return f"CASE WHEN {sentinel_predicate} THEN NULL ELSE {cast} END AS {field.name}"


def build_bronze_insert_sql(record_type: RecordType, raw_table: str, bronze_table: str) -> str:
    """RAW -> BRONZE parsing SQL for one record type: one INSERT INTO ...
    SELECT statement, one SUBSTRING-derived expression per configured
    field, gated by a minimum-line-length safety net (against truncated/
    malformed lines silently producing all-NULL rows via out-of-range
    SUBSTRING) and, if present, the record type's discriminator."""
    field_exprs = [_field_expr(f) for f in record_type.fields]
    select_cols = ",\n  ".join(field_exprs)

    where_clauses = [f"LENGTH(line_content) >= {record_type.min_line_length}"]
    if record_type.discriminator is not None:
        disc_pos, disc_len, disc_value = record_type.discriminator
        where_clauses.append(f"SUBSTRING(line_content, {disc_pos}, {disc_len}) = '{disc_value}'")
    where_sql = " AND ".join(where_clauses)

    sql = (
        f"INSERT INTO {bronze_table}\n"
        f"SELECT\n"
        f"  {select_cols},\n"
        # Plain column reference, not METADATA$FILENAME -- that
        # pseudocolumn only resolves when selecting from a stage; RAW
        # already persisted it as a real column at COPY INTO time (see
        # dag_factory.py's load_raw task).
        f"  source_file_name,\n"
        f"  CURRENT_TIMESTAMP() AS load_ts\n"
        f"FROM {raw_table}\n"
        f"WHERE {where_sql}"
    )
    _validate_single_insert(sql)
    return sql


def _validate_single_insert(sql: str) -> None:
    """Cheap sanity check on the generated string, not the primary safety
    net -- build_bronze_insert_sql's real testability lever is asserting
    exact position/length math and NULL-branch shape directly against
    FixedWidthField lists. This just catches "the f-string produced
    something that isn't valid SQL at all" before it ever reaches
    Snowflake."""
    try:
        statements = [s for s in sqlglot.parse(sql, read="snowflake") if s is not None]
    except sqlglot.errors.ParseError as e:
        raise ValueError(f"Generated SQL failed to parse: {e}") from e
    if len(statements) != 1:
        raise ValueError(f"Expected exactly one statement, generated {len(statements)}.")
    if statements[0].key.lower() != "insert":
        raise ValueError("Generated SQL is not an INSERT statement.")
