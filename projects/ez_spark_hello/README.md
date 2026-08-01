# ez_spark_hello

Tier 1, project 2. A standalone Spark job, not the reconciliation diff job
-- the distributed DataFrame model on its own, submitted to the real
2-worker cluster.

## The real files

- `spark/apps/ez_hello_job.py` -- the job. Lives there, not in this
  folder, because only `./spark/apps` (and `./spark/data`) are mounted
  into the Spark containers.
- `spark/data/ez_hello_sales.csv` -- ten rows of toy sales data
  (`region, amount`), also mounted in.

## What it does

Reads the CSV, groups by `region`, and computes count/sum/avg per group --
`sales.groupBy(...).agg(...).orderBy(...)` builds a query plan lazily;
nothing runs until `.show()`, the action, actually triggers the 2 workers
to execute it in parallel.

## Run it

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/apps/ez_hello_job.py
```

## What to look for

- The command's own stdout prints the aggregated table (`region`,
  `num_orders`, `total_amount`, `avg_amount`).
- Spark Master UI (http://localhost:9090) shows `ez_hello_job` as a
  completed application while it's running, with both workers listed as
  having executed tasks for it -- confirms the aggregation actually ran
  distributed across both containers, not just on the master.
- Compare this to a local pandas `groupby()`: same result shape, but here
  Spark had to plan, distribute, and shuffle the aggregation across 2
  separate worker processes to get it -- that coordination overhead is
  exactly why Spark is worth it at data sizes pandas can't hold in one
  process, and *not* worth it below that size (see `CURRICULUM.md`'s note
  on why the 37GB fixed-width file project skips Spark in favor of
  Snowflake-native SQL parsing).
