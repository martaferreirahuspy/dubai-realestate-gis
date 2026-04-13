#!/bin/bash
# run_pipeline.sh
# ---------------
# Orchestrates the full pipeline:
#   1. Fetch recent transactions from Dubai Pulse API
#   2. Map AREA_EN → COMMUNITY (DM shapeName)
#   3. UPSERT into Snowflake (safe to re-run — no duplicates)
#
# Usage:
#   bash run_pipeline.sh                        # daily: last 3 days
#   bash run_pipeline.sh --days 7               # last 7 days
#   bash run_pipeline.sh --months 3             # backfill: last 3 months
#   bash run_pipeline.sh --from 2025-01-01 --to 2025-12-31   # full year backfill

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GEOJSON="$SCRIPT_DIR/../dubai_communities.geojson"
RAW_CSV="$SCRIPT_DIR/transactions_raw.csv"
MAPPED_CSV="$SCRIPT_DIR/transactions_mapped.csv"

echo "══════════════════════════════════════════════════"
echo " Dubai RE Pipeline — $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "══════════════════════════════════════════════════"

# Step 1 — Fetch (pass through any args, default to last 3 days)
echo ""
echo "STEP 1 — Fetching transactions..."
python3 "$SCRIPT_DIR/fetch_transactions.py" \
  ${@:---days 3} \
  --output "$RAW_CSV"

# Step 2 — Map communities
echo ""
echo "STEP 2 — Mapping AREA_EN → COMMUNITY..."
python3 "$SCRIPT_DIR/map_communities.py" \
  --input   "$RAW_CSV" \
  --output  "$MAPPED_CSV" \
  --geojson "$GEOJSON" \
  --api-key "${GOOGLE_MAPS_API_KEY:-}"

# Step 3 — Load Snowflake
echo ""
echo "STEP 3 — Upserting into Snowflake..."
python3 "$SCRIPT_DIR/load_snowflake.py" \
  --input "$MAPPED_CSV"

echo ""
echo "══════════════════════════════════════════════════"
echo " Pipeline complete."
echo "══════════════════════════════════════════════════"
