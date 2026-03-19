#!/bin/bash
# run_pipeline.sh
# Runs tvdata.py (full historical fetch + truncate) first,
# then starts tvdata_update.py (live per-minute update loop).

set -e

echo "=== [$(date -u '+%Y-%m-%d %H:%M:%S')] Starting pipeline ==="

echo "--- Step 1: Full OHLC data refresh (tvdata.py) ---"
python scripts/tvdata.py
echo "--- Step 1 complete ---"

echo "--- Step 2: Live OHLC update loop (tvdata_update.py) ---"
python scripts/tvdata_update.py
echo "--- Step 2 complete ---"

echo "=== Pipeline job finished ==="