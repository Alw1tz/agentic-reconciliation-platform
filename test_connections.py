"""One-off connectivity check for Snowflake, Slack, and AWS S3.

Run with: uv run python3 test_connections.py

Not part of the pipeline -- just verifies the credentials in .env actually
work before wiring them into the DAG/agents.
"""
import os
import sys

from dotenv import load_dotenv

load_dotenv()


def check_snowflake() -> bool:
    import snowflake.connector

    try:
        con = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ["SNOWFLAKE_ROLE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
        )
        cur = con.cursor()
        cur.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        version, warehouse, role = cur.fetchone()
        con.close()
        print(f"  Snowflake version={version} warehouse={warehouse} role={role}")
        return True
    except Exception as e:
        print(f"  Snowflake connection failed: {e}")
        return False


def check_slack() -> bool:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    try:
        client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
        auth = client.auth_test()
        print(f"  Slack auth ok: bot={auth['user']} team={auth['team']}")

        channel = os.environ["SLACK_CHANNEL_ID"]
        resp = client.chat_postMessage(
            channel=channel,
            text="Connectivity test from agentic-reconciliation-platform (test_connections.py) - safe to ignore/delete.",
        )
        print(f"  Slack post ok: channel={resp['channel']} ts={resp['ts']}")
        return True
    except SlackApiError as e:
        print(f"  Slack connection failed: {e.response.get('error')}")
        return False


def check_s3() -> bool:
    import boto3
    from botocore.exceptions import ClientError

    try:
        s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        bucket = os.environ["S3_BUCKET"]
        s3.head_bucket(Bucket=bucket)
        print(f"  S3 bucket reachable: {bucket}")

        key = "connectivity-test/test_connections.txt"
        s3.put_object(Bucket=bucket, Key=key, Body=b"connectivity test")
        s3.get_object(Bucket=bucket, Key=key)
        s3.delete_object(Bucket=bucket, Key=key)
        print("  S3 put/get/delete round-trip ok")
        return True
    except ClientError as e:
        print(f"  S3 connection failed: {e}")
        return False


def main() -> None:
    checks = {
        "Snowflake": check_snowflake,
        "Slack": check_slack,
        "AWS S3": check_s3,
    }

    results = {}
    for name, fn in checks.items():
        print(f"[{name}]")
        results[name] = fn()

    print("\nSummary:")
    all_ok = True
    for name, ok in results.items():
        print(f"  {name}: {'OK' if ok else 'FAILED'}")
        all_ok = all_ok and ok

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
