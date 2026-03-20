#!/bin/bash
# run_sync_check.sh
# Checks row counts in ohlctick_1mdata vs indicators_data.
# If they differ, triggers the on-demand remediation script
# for whichever table lags behind.

set -e

echo "=== [$(date -u '+%Y-%m-%d %H:%M:%S')] Row-count sync check ==="
python scripts/row_count_sync_check.py
echo "=== Sync check complete ==="