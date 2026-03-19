"""
breeze_fetch_test.py
────────────────────
Standalone test for Breeze API BankNifty OHLC fetch.
Run from the algostrategy/ root (so .env is found):

    python scripts/breeze_fetch_test.py            # fetch both 1m and 5m
    python scripts/breeze_fetch_test.py 1m         # fetch 1m only
    python scripts/breeze_fetch_test.py 5m         # fetch 5m only
    python scripts/breeze_fetch_test.py 1m 200     # fetch 200 bars of 1m
    python scripts/breeze_fetch_test.py 5m 30      # fetch 30 bars of 5m

Credentials are read from environment variables (same as the main pipeline):
    API_KEY, API_SECRET, SESSION_TOKEN

Load them via .env or export them in your shell before running.
"""

import os
import sys
import pytz
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from breeze_connect import BreezeConnect

load_dotenv()   # picks up .env in cwd if present

IST = pytz.timezone('Asia/Kolkata')

# ── Constants ─────────────────────────────────────────────────────────────────
MARKET_OPEN_H,  MARKET_OPEN_M  = 9,  15
MARKET_CLOSE_H, MARKET_CLOSE_M = 15, 30
SESSION_MINUTES = 375   # 09:15 – 15:30

# get_historical_data (v1) interval strings
BREEZE_INTERVAL = {
    '1m': '1minute',
    '5m': '5minute',
}

DEFAULT_N_BARS = {
    '1m': 750,
    '5m': 75,
}


# ── Breeze session ────────────────────────────────────────────────────────────

def get_breeze() -> BreezeConnect:
    api_key       = os.getenv("API_KEY")
    api_secret    = os.getenv("API_SECRET")
    session_token = os.getenv("SESSION_TOKEN")

    missing = [k for k, v in {
        "API_KEY": api_key, "API_SECRET": api_secret,
        "SESSION_TOKEN": session_token,
    }.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing environment variables: {', '.join(missing)}\n"
            "Set them in your shell or in a .env file."
        )

    print(f"[Breeze] Connecting with API_KEY={api_key[:6]}...")
    breeze = BreezeConnect(api_key=api_key)
    breeze.generate_session(api_secret=api_secret, session_token=str(session_token))
    print("[Breeze] Session initialised.\n")
    return breeze


def is_business_day(date) -> bool:
    holidays = [
        "2025-02-26","2025-03-14","2025-03-31","2025-04-10","2025-04-14",
        "2025-04-18","2025-05-01","2025-08-15","2025-08-27","2025-10-02",
        "2025-10-21","2025-10-22","2025-11-05","2025-11-15","2025-12-25",
        "2026-01-26","2026-03-03","2026-03-26","2026-03-31","2026-04-03",
        "2026-04-14","2026-05-01","2026-05-28","2026-06-26","2026-09-14",
        "2026-10-02","2026-10-20","2026-11-10","2026-11-24","2026-12-25",
    ]
    return date.weekday() < 5 and date.strftime('%Y-%m-%d') not in holidays


# ── Single-session fetch ──────────────────────────────────────────────────────

def fetch_session(breeze: BreezeConnect,
                  interval: str,
                  from_str: str,
                  to_str: str,
                  timeframe: str) -> pd.DataFrame:
    """
    Fetch one market session from Breeze. Returns clean [datetime, open, high,
    low, close] DataFrame (market-hours only, tz-naive IST), or empty on failure.
    """
    try:
        resp = breeze.get_historical_data(
            interval      = interval,
            from_date     = from_str,
            to_date       = to_str,
            stock_code    = "CNXBAN",
            exchange_code = "NSE",
            product_type  = "cash",
        )
        if not resp or resp.get('Status') != 200:
            print(f"  [ERROR] Status={resp.get('Status') if resp else 'None'} "
                  f"Error={resp.get('Error','unknown') if resp else ''}")
            return pd.DataFrame()

        records = resp.get('Success') or []
        if not records:
            print(f"  [ERROR] Empty Success payload for {from_str}.")
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df['datetime'] = pd.to_datetime(df['datetime'])
        if df['datetime'].dt.tz is not None:
            df['datetime'] = df['datetime'].dt.tz_localize(None)

        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
        df = df[df['open'] != 0].copy()

        # Market hours only
        t = df['datetime'].dt.time
        df = df[(t >= datetime.strptime('09:15','%H:%M').time()) &
                (t <= datetime.strptime('15:30','%H:%M').time())].copy()

        print(f"  → {len(df)} bars from {from_str}")
        return df[['datetime', 'open', 'high', 'low', 'close']]

    except Exception as e:
        print(f"  [ERROR] Session fetch failed: {e}")
        return pd.DataFrame()


# ── Core fetch (multi-day loop) ───────────────────────────────────────────────

def fetch_banknifty(breeze: BreezeConnect,
                    timeframe: str = '1m',
                    n_bars: int    = 750) -> pd.DataFrame:
    """
    Fetch n_bars BankNifty candles from Breeze get_historical_data (v1).

    Fetches today's session first, then walks back one business day at a time
    (up to 10 calendar days) until n_bars is satisfied — same strategy as the
    original IndexHistoricalData.fetch_index_data_with_min_rows().

    stock_code="CNXBAN", exchange_code="NSE", product_type="cash"
    No expiry / right / strike_price required.

    Returns
    -------
    pd.DataFrame  columns: [datetime, open, high, low, close, ohlc4]
    datetime is tz-naive IST, sorted ascending. Empty DataFrame on failure.
    """
    if timeframe not in BREEZE_INTERVAL:
        raise ValueError(f"timeframe must be '1m' or '5m', got '{timeframe}'")

    interval = BREEZE_INTERVAL[timeframe]
    fmt      = '%Y-%m-%d %H:%M:%S'
    now_ist  = datetime.now(IST)
    today    = now_ist.date()

    combined = pd.DataFrame()

    # ── Step 1: today's session ───────────────────────────────────────────────
    today_start = datetime.combine(today, datetime.strptime('09:15','%H:%M').time())
    today_end   = now_ist.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M,
                                   second=0, microsecond=0).replace(tzinfo=None)
    print(f"[Breeze/{timeframe}] Today: {today_start.strftime(fmt)} → {today_end.strftime(fmt)}")
    day_df = fetch_session(breeze, interval,
                           today_start.strftime(fmt), today_end.strftime(fmt), timeframe)
    if not day_df.empty:
        combined = day_df

    # ── Step 2: walk back until n_bars met ────────────────────────────────────
    lookback_day = today - timedelta(days=1)
    cutoff       = today - timedelta(days=10)

    while len(combined) < n_bars and lookback_day >= cutoff:
        if not is_business_day(lookback_day):
            lookback_day -= timedelta(days=1)
            continue

        d_start = datetime.combine(lookback_day, datetime.strptime('09:15','%H:%M').time())
        d_end   = datetime.combine(lookback_day, datetime.strptime('15:30','%H:%M').time())
        print(f"[Breeze/{timeframe}] Prior session ({lookback_day}): "
              f"have {len(combined)}/{n_bars} bars")
        day_df = fetch_session(breeze, interval,
                               d_start.strftime(fmt), d_end.strftime(fmt), timeframe)
        if not day_df.empty:
            combined = pd.concat([day_df, combined], ignore_index=True)

        lookback_day -= timedelta(days=1)

    if combined.empty:
        print(f"[Breeze/{timeframe}] No data collected.")
        return pd.DataFrame()

    if len(combined) < n_bars:
        print(f"[Breeze/{timeframe}] Only {len(combined)} bars available "
              f"(needed {n_bars}) — proceeding with what we have.")

    # ── Finalise ──────────────────────────────────────────────────────────────
    combined.sort_values('datetime', inplace=True)
    combined.drop_duplicates(subset='datetime', keep='last', inplace=True)
    combined.reset_index(drop=True, inplace=True)

    combined['ohlc4'] = ((combined['open'] + combined['high'] +
                          combined['low']  + combined['close']) / 4).round(2)

    combined = combined.tail(n_bars).reset_index(drop=True)

    print(f"[Breeze/{timeframe}] Returning {len(combined)} bars.")
    return combined[['datetime', 'open', 'high', 'low', 'close', 'ohlc4']]


# ── Pretty printer ────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame, timeframe: str, n_bars: int):
    if df.empty:
        print(f"\n[{timeframe}] ✗  No data returned.\n")
        return

    print(f"\n[{timeframe}] ✓  {len(df)} bars returned  (requested {n_bars})")
    print(f"  First : {df['datetime'].iloc[0]}  |  close={df['close'].iloc[0]:.2f}")
    print(f"  Last  : {df['datetime'].iloc[-1]}  |  close={df['close'].iloc[-1]:.2f}")
    print(f"  High  : {df['high'].max():.2f}    Low : {df['low'].min():.2f}")
    print(f"\n--- Last 5 rows ---")
    pd.set_option('display.float_format', '{:.2f}'.format)
    pd.set_option('display.width', 120)
    print(df.tail(5).to_string(index=False))
    print()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    # Parse timeframe argument
    if args and args[0] in ('1m', '5m'):
        timeframes = [args[0]]
        args = args[1:]
    else:
        timeframes = ['1m', '5m']   # default: test both

    # Parse optional n_bars argument
    n_bars_override = None
    if args:
        try:
            n_bars_override = int(args[0])
        except ValueError:
            print(f"[WARN] Ignoring unrecognised argument: {args[0]}")

    breeze = get_breeze()

    for tf in timeframes:
        n_bars = n_bars_override if n_bars_override else DEFAULT_N_BARS[tf]
        print(f"{'='*60}")
        print(f" Fetching {tf} BankNifty  |  n_bars={n_bars}")
        print(f"{'='*60}")

        df = fetch_banknifty(breeze, timeframe=tf, n_bars=n_bars)
        print_summary(df, tf, n_bars)

        # Add hlc3 for 5m — mirrors what the pipeline stores in ohlctick_5mdata
        if tf == '5m' and not df.empty:
            df['hlc3'] = ((df['high'] + df['low'] + df['close']) / 3).round(2)
            print("  hlc3 sample (last 3):", df['hlc3'].tail(3).tolist())
            print()


if __name__ == '__main__':
    main()