"""Real MCP server exposing read-only access to Snowflake.

Same shape as Saturday's mcp_warehouse_server.py (DuckDB), but pointed at
real Snowflake, and with the fix Saturday's red-team pass identified as
correct: the app-level check parses the SQL with sqlglot and rejects
anything that isn't exactly one SELECT statement, instead of a naive
`startswith("select")` string check (which a stacked statement like
"SELECT 1; DROP TABLE ...;" passes). Saturday's demo only survived that
attack because of an unrelated read-only DB connection -- this server
doesn't rely on that accident.
"""
import os

import snowflake.connector
import sqlglot
from dotenv import load_dotenv
from mcp.server.mcpserver import MCPServer

load_dotenv()

mcp = MCPServer("snowflake")


def _connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def _validate_single_select(sql: str) -> None:
    """Reject anything that isn't exactly one read-only SELECT statement."""
    try:
        statements = sqlglot.parse(sql, read="snowflake")
    except sqlglot.errors.ParseError as e:
        raise ValueError(f"Could not parse SQL: {e}") from e

    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        raise ValueError(
            f"Exactly one statement is allowed, found {len(statements)}."
        )
    if statements[0].key.lower() != "select":
        raise ValueError("Only SELECT statements are allowed through this tool.")


@mcp.tool()
def list_tables() -> list[str]:
    """List tables available in the reconciliation schema."""
    con = _connect()
    try:
        cur = con.cursor()
        cur.execute("SHOW TABLES")
        return [row[1] for row in cur.fetchall()]
    finally:
        con.close()


@mcp.tool()
def run_sql_query(sql: str) -> list[dict]:
    """Run a read-only SQL query against Snowflake and return rows as dicts.

    Only a single SELECT statement is permitted, enforced by parsing the
    SQL (not by prefix-matching the string).
    """
    _validate_single_select(sql)
    con = _connect()
    try:
        cur = con.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        con.close()


if __name__ == "__main__":
    mcp.run(transport="stdio")
