"""Unit checks for sql_generator.py -- no Snowflake/Airflow needed, just
asserts the generated SQL's exact position/length math and NULL-handling
shape against known FixedWidthField lists.

Run with: uv run python3 airflow/dags/top_dynamic_etl/test_sql_generator.py
"""
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from registry import DEMO_MULTI_RECORD, NOAA_ISD, FieldType, FixedWidthField, RecordType
from sql_generator import bronze_table_name, build_bronze_insert_sql


def check(label: str, condition: bool) -> bool:
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}")
    return condition


def test_substring_position_math() -> bool:
    field = FixedWidthField("latitude", 29, 6, FieldType.DECIMAL, scaling_factor=Decimal("1000"))
    record_type = RecordType(name="observation", fields=(field,), min_line_length=105)
    sql = build_bronze_insert_sql(record_type, "RAW_T", "BRONZE_T")
    return check(
        "SUBSTRING position/length come from field config exactly (29, 6)",
        "SUBSTRING(line_content, 29, 6)" in sql,
    )


def test_scaling_factor_applied() -> bool:
    field = FixedWidthField("latitude", 29, 6, FieldType.DECIMAL, scaling_factor=Decimal("1000"))
    record_type = RecordType(name="observation", fields=(field,), min_line_length=105)
    sql = build_bronze_insert_sql(record_type, "RAW_T", "BRONZE_T")
    return check("Scaling factor divisor appears in generated SQL", "/ 1000" in sql)


def test_sentinel_checked_pre_cast() -> bool:
    field = FixedWidthField(
        "elevation_m", 47, 5, FieldType.DECIMAL,
        scaling_factor=Decimal("1"), missing_sentinels=("+9999",),
    )
    record_type = RecordType(name="observation", fields=(field,), min_line_length=105)
    sql = build_bronze_insert_sql(record_type, "RAW_T", "BRONZE_T")
    ok = True
    ok &= check("Sentinel literal appears quoted in the WHEN clause", "'+9999'" in sql)
    # The sentinel check must precede the TRY_CAST branch textually (CASE
    # WHEN ... THEN NULL ELSE TRY_CAST(...) END) -- guards against a sentinel
    # like "+9999" being silently cast into a real (bogus) number.
    when_idx = sql.index("CASE WHEN")
    try_cast_idx = sql.index("TRY_CAST", when_idx)
    else_idx = sql.index("ELSE", when_idx)
    ok &= check("TRY_CAST only reached via the ELSE branch, not before it", else_idx < try_cast_idx)
    return ok


def test_string_field_no_cast() -> bool:
    field = FixedWidthField("usaf_station_id", 5, 6, FieldType.STRING)
    record_type = RecordType(name="observation", fields=(field,), min_line_length=105)
    sql = build_bronze_insert_sql(record_type, "RAW_T", "BRONZE_T")
    return check("STRING field has no TRY_CAST wrapper", "TRY_CAST" not in sql)


def test_date_field_uses_try_to_date() -> bool:
    field = FixedWidthField("observation_date", 16, 8, FieldType.DATE)
    record_type = RecordType(name="observation", fields=(field,), min_line_length=105)
    sql = build_bronze_insert_sql(record_type, "RAW_T", "BRONZE_T")
    return check("DATE field uses TRY_TO_DATE with YYYYMMDD format", "TRY_TO_DATE" in sql and "YYYYMMDD" in sql)


def test_discriminator_predicate() -> bool:
    field = FixedWidthField("transaction_id", 2, 10, FieldType.STRING)
    record_type = RecordType(
        name="detail", fields=(field,), min_line_length=30, discriminator=(1, 1, "D"),
    )
    sql = build_bronze_insert_sql(record_type, "RAW_T", "BRONZE_T")
    return check(
        "Discriminator predicate present and matches configured position/value",
        "SUBSTRING(line_content, 1, 1) = 'D'" in sql,
    )


def test_min_line_length_guard() -> bool:
    field = FixedWidthField("x", 1, 1, FieldType.STRING)
    record_type = RecordType(name="t", fields=(field,), min_line_length=105)
    sql = build_bronze_insert_sql(record_type, "RAW_T", "BRONZE_T")
    return check("Line-length safety net present", "LENGTH(line_content) >= 105" in sql)


def test_real_registry_entries_generate_cleanly() -> bool:
    ok = True
    for cfg in (NOAA_ISD, DEMO_MULTI_RECORD):
        for rt in cfg.record_types:
            table = bronze_table_name(cfg, rt)
            sql = build_bronze_insert_sql(rt, cfg.raw_table, table)
            ok &= check(f"{cfg.interface_id}/{rt.name}: generates without error ({len(sql)} chars)", bool(sql))
    return ok


def test_metadata_columns_present() -> bool:
    field = FixedWidthField("x", 1, 1, FieldType.STRING)
    record_type = RecordType(name="t", fields=(field,), min_line_length=1)
    sql = build_bronze_insert_sql(record_type, "RAW_T", "BRONZE_T")
    ok = True
    ok &= check("source_file_name metadata column present", "source_file_name" in sql)
    ok &= check("load_ts metadata column present", "load_ts" in sql)
    return ok


if __name__ == "__main__":
    tests = [
        test_substring_position_math,
        test_scaling_factor_applied,
        test_sentinel_checked_pre_cast,
        test_string_field_no_cast,
        test_date_field_uses_try_to_date,
        test_discriminator_predicate,
        test_min_line_length_guard,
        test_metadata_columns_present,
        test_real_registry_entries_generate_cleanly,
    ]
    results = []
    for t in tests:
        print(f"{t.__name__}:")
        results.append(t())
    if all(results):
        print(f"\nAll {len(results)} checks passed.")
        sys.exit(0)
    else:
        print(f"\n{results.count(False)}/{len(results)} checks failed.")
        sys.exit(1)
