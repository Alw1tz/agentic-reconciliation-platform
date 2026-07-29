"""Global sandbox settings — same pattern as spark-aws-pipeline's utils/settings.py."""
import os
from copy import deepcopy
from datetime import datetime, timedelta

# ── S3 ────────────────────────────────────────────────────────────────────────
S3_BUCKET = os.getenv("S3_BUCKET", "carlos-agentic-recon-platform")

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_CONN_ID = "spark_default"


def get_default_args(owner: str = "airflow") -> dict:
    """Default DAG args factory — same pattern as spark-aws-pipeline."""
    return deepcopy(
        {
            "owner": owner,
            "depends_on_past": False,
            "start_date": datetime(2025, 1, 1),
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "email_on_failure": False,
            "email_on_retry": False,
        }
    )
