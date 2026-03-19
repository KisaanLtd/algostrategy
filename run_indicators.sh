#!/bin/bash
# run_indicators.sh
# Runs indicatordata_all.py (full recalculation + truncate) first,
# then starts indicator_update.py (live per-minute indicator update loop).

set -e

echo "=== [$(date -u '+%Y-%m-%d %H:%M:%S')] Starting indicator pipeline ==="

echo "--- Step 1: Full indicator recalculation (indicatordata_all.py) ---"
python scripts/indicatordata_all.py
echo "--- Step 1 complete ---"

echo "--- Step 2: Live indicator update loop (indicator_update.py) ---"
python scripts/indicator_update.py
echo "--- Step 2 complete ---"

echo "=== Indicator pipeline job finished ==="