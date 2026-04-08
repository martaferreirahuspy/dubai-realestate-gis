#!/usr/bin/env python3
"""
load_snowflake.py
-----------------
Loads the mapped transactions CSV into Snowflake using UPSERT (MERGE).
Safe to re-run — duplicate transactions are skipped, updated rows are refreshed.

Usage:
    python3 load_snowflake.py --input transactions_mapped.csv

Environment variables required:
    SNOWFLAKE_ACCOUNT       e.g. xy12345.eu-west-1
    SNOWFLAKE_USER          your Snowflake username
    SNOWFLAKE_PASSWORD      your Snowflake password (or use key-pair — see below)
    SNOWFLAKE_WAREHOUSE     e.g. COMPUTE_WH
    SNOWFLAKE_DATABASE      e.g. DUBAI_RE
    SNOWFLAKE_SCHEMA        e.g. PUBLIC
    SNOWFLAKE_TABLE         e.g. DLD_TRANSACTIONS  (created automatically if absent)

Optional:
    SNOWFLAKE_ROLE          e.g. SYSADMIN
"""

import csv
import os
import sys
from pathlib import Path

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
except ImportError:
    print("Installing snowflake-connector-python...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install",
                           "snowflake-connector-python[pandas]", "--quiet"])
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

try:
    import pandas as pd
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "--quiet"])
    import pandas as pd

import argparse


# Column renames: CSV name → Snowflake column name (snake_case, all upper)
# Adjust to match actual Dubai Pulse API field names
COLUMN_MAP = {
    "TRANSACTION_NUMBER":       "TRANSACTION_NUMBER",
    "INSTANCE_DATE":            "INSTANCE_DATE",
    "GROUP_EN":                 "GROUP_EN",
    "PROCEDURE_EN":             "PROCEDURE_EN",
    "IS_OFFPLAN_EN":            "IS_OFFPLAN",
    "IS_FREE_HOLD_EN":          "IS_FREE_HOLD",
    "USAGE_EN":                 "USAGE",
    "AREA_EN":                  "AREA_EN",
    "PROP_TYPE_EN":             "PROP_TYPE",
    "PROP_SB_TYPE_EN":          "PROP_SUBTYPE",
    "TRANS_VALUE":              "TRANS_VALUE",
    "PROCEDURE_AREA":           "PROCEDURE_AREA",
    "ACTUAL_AREA":              "ACTUAL_AREA",
    "ROOMS_EN":                 "ROOMS",
    "PARKING":                  "PARKING",
    "NEAREST_METRO_EN":         "NEAREST_METRO",
    "NEAREST_MALL_EN":          "NEAREST_MALL",
    "NEAREST_LANDMARK_EN":      "NEAREST_LANDMARK",
    "TOTAL_BUYER":              "TOTAL_BUYER",
    "TOTAL_SELLER":             "TOTAL_SELLER",
    "MASTER_PROJECT_EN":        "MASTER_PROJECT",
    "PROJECT_EN":               "PROJECT",
    "COMMUNITY":                "COMMUNITY",   # added by map_communities.py
}

PRIMARY_KEY = "TRANSACTION_NUMBER"


def get_conn():
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        sys.exit(f"ERROR: Missing environment variables: {', '.join(missing)}")

    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.environ["SNOWFLAKE_WAREHOUSE"],
        database  = os.environ["SNOWFLAKE_DATABASE"],
        schema    = os.environ["SNOWFLAKE_SCHEMA"],
        role      = os.environ.get("SNOWFLAKE_ROLE"),
    )


def ensure_table(cur, table: str, columns: list[str]):
    """Create the target table if it does not exist."""
    col_defs = ",\n  ".join(
        f"{col} TIMESTAMP_NTZ" if col == "INSTANCE_DATE"
        else f"{col} NUMBER(18,2)" if col in ("TRANS_VALUE", "PROCEDURE_AREA", "ACTUAL_AREA")
        else f"{col} NUMBER(6)" if col in ("PARKING", "TOTAL_BUYER", "TOTAL_SELLER")
        else f"{col} VARCHAR(500)"
        for col in columns
    )
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table} (
      {col_defs},
      LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      PRIMARY KEY ({PRIMARY_KEY})
    )
    """
    cur.execute(ddl)


def main():
    parser = argparse.ArgumentParser(description="Load mapped transactions into Snowflake")
    parser.add_argument("--input", required=True, help="Mapped transactions CSV")
    parser.add_argument("--batch-size", type=int, default=10_000)
    args = parser.parse_args()

    table = os.environ.get("SNOWFLAKE_TABLE", "DLD_TRANSACTIONS")

    print(f"Reading {args.input}...")
    df = pd.read_csv(args.input, dtype=str, low_memory=False)
    print(f"  {len(df):,} rows, {len(df.columns)} columns.")

    # Keep only mapped columns, rename to Snowflake names
    keep = {csv_col: sf_col for csv_col, sf_col in COLUMN_MAP.items() if csv_col in df.columns}
    df = df[list(keep.keys())].rename(columns=keep)

    # Parse date
    if "INSTANCE_DATE" in df.columns:
        df["INSTANCE_DATE"] = pd.to_datetime(df["INSTANCE_DATE"], errors="coerce")

    # Parse numerics
    for col in ["TRANS_VALUE", "PROCEDURE_AREA", "ACTUAL_AREA", "PARKING", "TOTAL_BUYER", "TOTAL_SELLER"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    sf_columns = list(df.columns)

    print("Connecting to Snowflake...")
    conn = get_conn()
    cur  = conn.cursor()

    print(f"Ensuring table {table} exists...")
    ensure_table(cur, table, sf_columns)

    # Stage → MERGE in batches
    stage = f"STAGE_{table}"
    cur.execute(f"CREATE TEMPORARY TABLE IF NOT EXISTS {stage} LIKE {table}")

    print(f"Loading {len(df):,} rows into staging table...")
    success, nchunks, nrows, _ = write_pandas(conn, df, stage, auto_create_table=False, overwrite=True)
    print(f"  Staged {nrows:,} rows in {nchunks} chunks.")

    # MERGE statement — upsert on TRANSACTION_NUMBER
    update_cols = [c for c in sf_columns if c != PRIMARY_KEY]
    update_set  = ", ".join(f"t.{c} = s.{c}" for c in update_cols)
    insert_cols = ", ".join(sf_columns)
    insert_vals = ", ".join(f"s.{c}" for c in sf_columns)

    merge_sql = f"""
    MERGE INTO {table} t
    USING {stage} s
    ON t.{PRIMARY_KEY} = s.{PRIMARY_KEY}
    WHEN MATCHED THEN UPDATE SET {update_set}, t.LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    print("Running MERGE (upsert)...")
    cur.execute(merge_sql)
    result = cur.fetchone()
    print(f"  Inserted: {result[0]:,}  Updated: {result[1]:,}")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
