"""A standalone Spark job built from scratch -- tier 1, project 2 of the
learning curriculum (projects/ez_spark_hello/README.md). Not the
reconciliation diff job -- the point here is the distributed DataFrame
model in isolation: lazy transformations, a real action triggering
distributed execution across the 2 workers, and what "submitting a job to
a cluster" means versus a local pandas script.

Usage (from repo root, cluster already up via docker compose):
    docker compose exec spark-master /opt/spark/bin/spark-submit \\
        --master spark://spark-master:7077 \\
        /opt/spark/apps/ez_hello_job.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = SparkSession.builder.appName("ez_hello_job").getOrCreate()

    sales = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("/opt/spark/data/ez_hello_sales.csv")
    )

    # groupBy/agg/orderBy build a query plan -- nothing executes yet.
    # .show() below is the *action* that actually triggers the 2 workers
    # to run the plan in parallel and return results.
    summary = (
        sales.groupBy("region")
        .agg(
            F.count("*").alias("num_orders"),
            F.sum("amount").alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
        .orderBy("region")
    )

    print("=== Sales summary by region ===")
    summary.show()

    spark.stop()


if __name__ == "__main__":
    main()
