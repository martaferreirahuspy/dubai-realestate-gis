#!/usr/bin/env python3
"""
fetch_transactions.py
---------------------
Fetches Dubai Land Department transactions from the Dubai Pulse API
for the last N days (default: 3) or N months, and writes to a local CSV.

Default mode is --days 3: fetches the last 3 days on every run.
This is safe to overlap — the Snowflake MERGE upserts on TRANSACTION_NUMBER
so re-fetching a transaction never creates duplicates.

Usage:
    # Daily run (fetch last 3 days, catches any late DLD registrations):
    python3 fetch_transactions.py --days 3 --output transactions_raw.csv

    # Backfill (fetch a longer window):
    python3 fetch_transactions.py --months 3 --output transactions_raw.csv

    # Explicit date range:
    python3 fetch_transactions.py --from 2025-01-01 --to 2025-12-31 --output transactions_raw.csv

Environment variables required:
    DUBAI_PULSE_CLIENT_ID       Your Dubai Pulse API client ID
    DUBAI_PULSE_CLIENT_SECRET   Your Dubai Pulse API client secret

Register for free at: https://data.dubai (click "API Access")
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path

import requests


# ── API constants ──────────────────────────────────────────────────────────────
TOKEN_URL  = "https://api.dubaipulse.gov.ae/oauth/client_credential/accesstoken"
DATA_URL   = "https://api.dubaipulse.gov.ae/open/dld/dld_transactions-open-api"
PAGE_SIZE  = 1000   # max per request
MAX_RETRIES = 3


def get_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        TOKEN_URL,
        params={"grant_type": "client_credentials"},
        data={"client_id": client_id, "client_secret": client_secret},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        sys.exit(f"ERROR: Could not obtain access token. Response: {resp.text}")
    return token


def fetch_page(token: str, date_from: str, date_to: str, offset: int) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "filter": f"instance_date >= '{date_from}' AND instance_date <= '{date_to}'",
        "limit":  PAGE_SIZE,
        "offset": offset,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(DATA_URL, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                raise
            print(f"  [retry {attempt}/{MAX_RETRIES}] {e}", file=sys.stderr)
            time.sleep(2 ** attempt)


def main():
    parser = argparse.ArgumentParser(description="Fetch DLD transactions from Dubai Pulse API")

    # Lookback window — mutually exclusive, in priority order: --from/--to > --days > --months
    window = parser.add_mutually_exclusive_group()
    window.add_argument("--days",   type=int, help="Fetch last N days (default for daily runs: 3)")
    window.add_argument("--months", type=int, help="Fetch last N months (for backfills)")

    parser.add_argument("--from", dest="date_from", default=None, help="Explicit start date YYYY-MM-DD")
    parser.add_argument("--to",   dest="date_to",   default=None, help="Explicit end date YYYY-MM-DD")
    parser.add_argument("--output", default="transactions_raw.csv", help="Output CSV path")
    args = parser.parse_args()

    client_id     = os.environ.get("DUBAI_PULSE_CLIENT_ID")
    client_secret = os.environ.get("DUBAI_PULSE_CLIENT_SECRET")
    if not client_id or not client_secret:
        sys.exit("ERROR: Set DUBAI_PULSE_CLIENT_ID and DUBAI_PULSE_CLIENT_SECRET environment variables.")

    today = datetime.now(timezone.utc).date()

    if args.date_from and args.date_to:
        date_from = args.date_from
        date_to   = args.date_to
    elif args.months:
        date_from = (today - relativedelta(months=args.months)).isoformat()
        date_to   = today.isoformat()
    else:
        # Default: last 3 days (safe overlap for daily runs)
        days      = args.days if args.days else 3
        date_from = (today - timedelta(days=days)).isoformat()
        date_to   = today.isoformat()

    print(f"Fetching DLD transactions: {date_from} → {date_to}")
    print("Authenticating...")
    token = get_token(client_id, client_secret)
    print("  Token obtained.")

    all_rows = []
    offset   = 0
    fieldnames = None

    while True:
        print(f"  Fetching page offset={offset}...", end=" ", flush=True)
        data = fetch_page(token, date_from, date_to, offset)

        # API returns either a list or dict with a data key
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = data.get("data") or data.get("result") or data.get("records") or []
        else:
            rows = []

        if not rows:
            print("done (empty page).")
            break

        print(f"{len(rows)} rows.")
        all_rows.extend(rows)

        if len(rows) < PAGE_SIZE:
            break   # last page
        offset += PAGE_SIZE
        time.sleep(0.1)  # gentle throttle

    if not all_rows:
        print("WARNING: No rows returned. Check your API credentials and date range.")
        return

    # Determine fieldnames from first row
    fieldnames = list(all_rows[0].keys())

    output_path = Path(args.output)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nDone. {len(all_rows):,} rows written to {output_path}")
    print(f"Fields: {', '.join(fieldnames)}")


if __name__ == "__main__":
    main()
