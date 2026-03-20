"""
scripts/row_count_sync_check.py
───────────────────────────────
Parallel row-count monitor for ohlctick_1mdata vs indicators_data.

Logic
─────
1.  Fetch COUNT(*) for both tables in parallel.
2.  If counts are equal  → log and exit (all good).
3.  If ohlctick_1mdata  lags → run tvdata.py       (full OHLC re-fetch).
4.  If indicators_data  lags → run indicatordata_all.py (full indicator re-calc).
5.  Both remediation scripts are run as subprocesses — they are already
    idempotent (TRUNCATE + recalc), so running them on-demand is safe.

Scheduling
──────────
Run as a Cloud Run Job triggered every 5 minutes Mon–Fri during market hours
(09:14–15:35 IST = 03:44–10:05 UTC).  The job exits immediately when counts
match, so idle cost is negligible.

Env vars  (same Secret Manager secrets as the existing jobs)
─────────
DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
"""

import asyncio
import os
import ssl
import subprocess
import sys
from datetime import datetime

import aiomysql
import pytz

# ── Timezone / market window ──────────────────────────────────────────────────
IST           = pytz.timezone('Asia/Kolkata')
MARKET_OPEN   = (9,  14)   # allow a 1-min grace before 09:15
MARKET_CLOSE  = (15, 35)   # allow a 5-min buffer after 15:30

holidays = [
    "2025-02-26","2025-03-14","2025-03-31","2025-04-10","2025-04-14",
    "2025-04-18","2025-05-01","2025-08-15","2025-08-27","2025-10-02",
    "2025-10-21","2025-10-22","2025-11-05","2025-11-15","2025-12-25",
    "2026-01-26","2026-03-03","2026-03-26","2026-03-31","2026-04-03",
    "2026-04-14","2026-05-01","2026-05-28","2026-06-26","2026-09-14",
    "2026-10-02","2026-10-20","2026-11-10","2026-11-24","2026-12-25",
]

# ── DB config (identical pattern to the rest of the project) ─────────────────
db_config = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def now_ist() -> datetime:
    return datetime.now(IST)


def is_business_day(dt: datetime) -> bool:
    return dt.weekday() < 5 and dt.strftime('%Y-%m-%d') not in holidays


def is_within_market_window(dt: datetime) -> bool:
    open_dt  = dt.replace(hour=MARKET_OPEN[0],  minute=MARKET_OPEN[1],  second=0, microsecond=0)
    close_dt = dt.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0)
    return open_dt <= dt <= close_dt


async def get_mysql_pool():
    ssl_ctx = None
    ca_path = os.path.join(os.path.dirname(__file__), '..', 'ca.pem')
    if os.path.exists(ca_path):
        ssl_ctx = ssl.create_default_context(cafile=ca_path)

    return await aiomysql.create_pool(
        host      = db_config['host'],
        port      = db_config['port'],
        user      = db_config['user'],
        password  = db_config['password'],
        db        = db_config['database'],
        autocommit= True,
        ssl       = ssl_ctx,
        minsize   = 1,
        maxsize   = 4,
    )


async def fetch_count(pool, table: str) -> int:
    """Return COUNT(*) for *table*."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            row = await cur.fetchone()
            return row[0] if row else 0


async def fetch_counts_parallel(pool) -> tuple[int, int]:
    """Fetch both counts concurrently."""
    ohlc_count, ind_count = await asyncio.gather(
        fetch_count(pool, 'ohlctick_1mdata'),
        fetch_count(pool, 'indicators_data'),
    )
    return ohlc_count, ind_count


# ── Remediation ───────────────────────────────────────────────────────────────

def scripts_dir() -> str:
    """Absolute path to the scripts/ directory."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scripts')


def run_script(script_name: str) -> int:
    """
    Execute *script_name* (relative to scripts/) as a blocking subprocess.
    Returns the process exit code.
    """
    script_path = os.path.join(scripts_dir(), script_name)
    print(f"[sync_check] Launching remediation: {script_path}")
    result = subprocess.run(
        [sys.executable, script_path],
        cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'),
    )
    return result.returncode


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    ts = now_ist()
    print(f"[sync_check] Started at {ts.strftime('%Y-%m-%d %H:%M:%S')} IST")

    # ── Guard: only run on trading days within market window ─────────────────
    if not is_business_day(ts):
        print("[sync_check] Not a trading day — exiting.")
        return

    if not is_within_market_window(ts):
        print(f"[sync_check] Outside market window ({MARKET_OPEN[0]:02d}:{MARKET_OPEN[1]:02d}"
              f"–{MARKET_CLOSE[0]:02d}:{MARKET_CLOSE[1]:02d} IST) — exiting.")
        return

    # ── Connect & count ───────────────────────────────────────────────────────
    pool = await get_mysql_pool()
    try:
        ohlc_count, ind_count = await fetch_counts_parallel(pool)
    finally:
        pool.close()
        await pool.wait_closed()

    print(f"[sync_check] ohlctick_1mdata = {ohlc_count:,}  |  indicators_data = {ind_count:,}  "
          f"|  diff = {ohlc_count - ind_count:+d}")

    # ── Decision tree ─────────────────────────────────────────────────────────
    LAG_THRESHOLD = 4   # tolerate ≤4 rows (pipeline propagation delay); act only when lag > 4   # tolerate up to 4 rows difference; act only when lag > 4

    lag = ohlc_count - ind_count   # positive → OHLC has more rows → indicators lag
                                   # negative → indicators somehow have more rows → OHLC lags

    if abs(lag) <= LAG_THRESHOLD:
        print(f"[sync_check] ✅ Difference is {abs(lag)} row(s) — within tolerance ({LAG_THRESHOLD}). Nothing to do.")
        return

    if lag > LAG_THRESHOLD:
        # indicators_data is behind — re-run full indicator recalculation
        print(f"[sync_check] ⚠️  indicators_data lags ohlctick_1mdata by {lag} row(s) (threshold={LAG_THRESHOLD}).")
        print("[sync_check] Running indicatordata_all.py (truncate + full recalc)…")
        rc = run_script('indicatordata_all.py')
        if rc == 0:
            print("[sync_check] ✅ indicatordata_all.py completed successfully.")
        else:
            print(f"[sync_check] ❌ indicatordata_all.py exited with code {rc}.")
            sys.exit(rc)

    else:
        # ohlctick_1mdata is behind — re-fetch OHLC data
        abs_lag = abs(lag)
        print(f"[sync_check] ⚠️  ohlctick_1mdata lags indicators_data by {abs_lag} row(s) (threshold={LAG_THRESHOLD}).")
        print("[sync_check] Running tvdata.py (truncate + full OHLC re-fetch)…")
        rc = run_script('tvdata.py')
        if rc == 0:
            print("[sync_check] ✅ tvdata.py completed successfully.")
        else:
            print(f"[sync_check] ❌ tvdata.py exited with code {rc}.")
            sys.exit(rc)

    # ── Post-remediation verification ─────────────────────────────────────────
    print("[sync_check] Re-checking counts after remediation…")
    pool2 = await get_mysql_pool()
    try:
        ohlc_new, ind_new = await fetch_counts_parallel(pool2)
    finally:
        pool2.close()
        await pool2.wait_closed()

    print(f"[sync_check] ohlctick_1mdata = {ohlc_new:,}  |  indicators_data = {ind_new:,}")
    if ohlc_new == ind_new:
        print("[sync_check] ✅ Counts now match after remediation.")
    else:
        remaining = abs(ohlc_new - ind_new)
        print(f"[sync_check] ⚠️  Still {remaining} row(s) apart after remediation — "
              "will re-check on next scheduled run.")


if __name__ == "__main__":
    asyncio.run(main())