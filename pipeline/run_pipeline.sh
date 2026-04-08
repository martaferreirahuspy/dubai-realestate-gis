#!/bin/bash
# run_pipeline.sh
# ---------------
# Orchestrates the full pipeline:
#   1. Fetch last 3 months from Dubai Pulse API
#   2. Map AREA_EN → COMMUNITY (DM shapeName)
#   3. UPSERT into Snowflake
#
# All secrets come from environment variables (set in GitHub Actions secrets).
# Safe to re-run — idempotent UPSERT means no duplicates.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GEOJSON="$SCRIPT_DIR/../dubai_communities.geojson"
RAW_CSV="$SCRIPT_DIR/transactions_raw.csv"
MAPPED_CSV="$SCRIPT_DIR/transactions_mapped.csv"

echo "══════════════════════════════════════════════════"
echo " Dubai RE Pipeline — $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "══════════════════════════════════════════════════"

# Step 1 — Fetch
echo ""
echo "STEP 1 — Fetching transactions (last 3 months)..."
python3 "$SCRIPT_DIR/fetch_transactions.py" \
  --months 3 \
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
