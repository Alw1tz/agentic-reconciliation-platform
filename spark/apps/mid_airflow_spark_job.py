"""A Spark job orchestrated by mid_airflow_spark_dag.py -- tier 2, project 7
of the learning curriculum (projects/mid_airflow_spark_pipeline/README.md).

Reads the real tips.csv dataset from S3 (staged there by the DAG's
seed_and_export task) via s3a, aggregates it, and writes a single JSON
report back to S3 -- same shape as diff_source_target.py's report pattern,
but a groupBy aggregation instead of a source/target diff, and reading from
S3 instead of ez_hello_job.py's local mount, since the point of this
project is Airflow handing Spark a real s3a path, not just running Spark
standalone.

Usage: spark-submit mid_airflow_spark_job.py <input_s3a_csv> <output_s3a_json_path>
"""
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

# Explicit schema instead of inferSchema=True -- inferSchema forces Spark to
# read the file twice (one pass to guess types, one to actually load it) and
# its guesses aren't guaranteed stable across Spark versions. tips.csv's
# columns are fixed and known, so there's nothing to infer.
TIPS_SCHEMA = StructType([
    StructField("total_bill", DecimalType(6, 2), nullable=False),
    StructField("tip", DecimalType(6, 2), nullable=False),
    StructField("sex", StringType(), nullable=False),
    StructField("smoker", StringType(), nullable=False),
    StructField("day", StringType(), nullable=False),
    StructField("time", StringType(), nullable=False),
    StructField("size", IntegerType(), nullable=False),
])


def main() -> None:
    input_path, output_path = sys.argv[1], sys.argv[2]

    spark = SparkSession.builder.appName("mid_airflow_spark_job").getOrCreate()
    # WARN instead of the default INFO -- cuts the block-manager/task-scheduler
    # chatter so only warnings, errors, and the stage progress bar show.
    spark.sparkContext.setLogLevel("WARN")

    try:
        tips = spark.read.option("header", True).schema(TIPS_SCHEMA).csv(input_path)

        # groupBy/agg build a query plan -- .collect() below is the action
        # that triggers the real distributed execution across the workers.
        summary = (
            tips.groupBy("day", "time")
            .agg(
                F.count("*").alias("num_checks"),
                F.round(F.sum("total_bill"), 2).alias("total_bill_sum"),
                F.round(F.avg("tip"), 2).alias("avg_tip"),
                F.round(F.avg(F.col("tip") / F.col("total_bill") * 100), 2).alias("avg_tip_pct"),
            )
            .orderBy("day", "time")
            .collect()
        )

        report = {
            "row_count": tips.count(),
            "by_day_and_time": [
                {
                    "day": r["day"],
                    "time": r["time"],
                    "num_checks": r["num_checks"],
                    "total_bill_sum": float(r["total_bill_sum"]),
                    "avg_tip": float(r["avg_tip"]),
                    "avg_tip_pct": float(r["avg_tip_pct"]),
                }
                for r in summary
            ],
        }

        # Single JSON file, not Spark's default multi-part output -- the
        # downstream Airflow task reads one object, same reasoning as
        # diff_source_target.py.
        sc = spark.sparkContext
        rdd = sc.parallelize([json.dumps(report)])
        rdd.coalesce(1).saveAsTextFile(output_path)
    finally:
        # finally, not a trailing call -- a failure above (e.g. a bad row)
        # would otherwise leak the SparkContext instead of releasing cluster
        # resources.
        spark.stop()


if __name__ == "__main__":
    main()
