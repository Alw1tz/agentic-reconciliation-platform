"""Distributed day-over-day reconciliation diff, reading two CSV snapshots
from S3 (s3a) and writing a JSON diff report back to S3.

This is the Spark-scale version of the EXCEPT-based diff pattern from the
real Rappi reconciliation DAG (and Saturday's DuckDB toy version) — same
three checks (missing rows, duplicate rows, value mismatches), now
expressed as Spark DataFrame operations instead of a single SQL query, so
it scales to however many rows/tables a real nightly run would have.

Usage: spark-submit diff_source_target.py <source_s3a_csv> <target_s3a_csv> <output_s3a_json_path>
"""
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    source_path, target_path, output_path = sys.argv[1], sys.argv[2], sys.argv[3]

    spark = SparkSession.builder.appName("diff_source_target").getOrCreate()

    source = spark.read.option("header", True).option("inferSchema", True).csv(source_path)
    target = spark.read.option("header", True).option("inferSchema", True).csv(target_path)

    source_count = source.count()
    target_count = target.count()

    missing_in_target = (
        source.join(target, on="order_id", how="left_anti").select("order_id").collect()
    )

    duplicates_in_target = (
        target.groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .select("order_id", "count")
        .collect()
    )

    mismatches = (
        source.alias("s")
        .join(target.alias("t"), on="order_id", how="inner")
        .filter(F.col("s.amount_usd") != F.col("t.amount_usd"))
        .select(
            F.col("order_id"),
            F.col("s.amount_usd").alias("source_amount"),
            F.col("t.amount_usd").alias("target_amount"),
        )
        .collect()
    )

    report = {
        "row_count_match": {
            "passed": source_count == target_count,
            "detail": f"source={source_count} rows, target={target_count} rows",
        },
        "no_missing_orders": {
            "passed": len(missing_in_target) == 0,
            "detail": [r["order_id"] for r in missing_in_target],
        },
        "no_duplicate_order_ids": {
            "passed": len(duplicates_in_target) == 0,
            "detail": [{"order_id": r["order_id"], "count": r["count"]} for r in duplicates_in_target],
        },
        "amounts_match": {
            "passed": len(mismatches) == 0,
            "detail": [
                {
                    "order_id": r["order_id"],
                    "source_amount": r["source_amount"],
                    "target_amount": r["target_amount"],
                }
                for r in mismatches
            ],
        },
    }

    # Single JSON file, not Spark's default multi-part output -- this report
    # is small and downstream (the agent pipeline) needs to read one object.
    sc = spark.sparkContext
    rdd = sc.parallelize([json.dumps(report)])
    rdd.coalesce(1).saveAsTextFile(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
