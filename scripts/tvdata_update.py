from tvDatafeed import TvDatafeed, Interval
from breeze_connect import BreezeConnect
import pandas as pd
import numpy as np
import asyncio
import aiomysql
import os
import ssl
import pytz
from datetime import datetime, timedelta

IST = pytz.timezone('Asia/Kolkata')

holidays = [
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-11-15", "2025-12-25",
    # 2026
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31",
    "2026-04-03", "2026-04-14", "2026-05-01", "2026-05-28",
    "2026-06-26", "2026-09-14", "2026-10-02", "2026-10-20",
    "2026-11-10", "2026-11-24", "2026-12-25",
]

db_config = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

# ── Breeze interval strings ───────────────────────────────────────────────────
# get_historical_data (v1) accepts: "1minute","5minute","30minute","1day"
BREEZE_INTERVAL = {
    '1m': '1minute',
    '5m': '5minute',
}

# ── Trading session constants ─────────────────────────────────────────────────
MARKET_OPEN_H,  MARKET_OPEN_M  = 9,  15
MARKET_CLOSE_H, MARKET_CLOSE_M = 15, 30
SESSION_MINUTES = 375   # 09:15 – 15:30


class TvDataUpdate:

    def __init__(self):
        self.tv  = None   # TvDatafeed — created once in run()
        self._breeze = None  # BreezeConnect — created lazily on first fallback

    # ── Breeze API (lazy init) ────────────────────────────────────────────────

    def _get_breeze(self):
        """Initialise Breeze API once and reuse for the session."""
        if self._breeze is None:
            api_key       = os.getenv("API_KEY")
            api_secret    = os.getenv("API_SECRET")
            session_token = os.getenv("SESSION_TOKEN")
            if not all([api_key, api_secret, session_token]):
                raise EnvironmentError(
                    "Breeze fallback requires API_KEY, API_SECRET, SESSION_TOKEN env vars."
                )
            self._breeze = BreezeConnect(api_key=api_key)
            self._breeze.generate_session(
                api_secret=api_secret,
                session_token=str(session_token),
            )
            print("[Breeze] Session initialised.")
        return self._breeze

    # ── DB pool ───────────────────────────────────────────────────────────────

    async def get_mysql_pool(self):
        ssl_ctx = None
        ca_path = os.path.join(os.path.dirname(__file__), '..', 'ca.pem')
        if os.path.exists(ca_path):
            ssl_ctx = ssl.create_default_context(cafile=ca_path)

        pool = await aiomysql.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['database'],
            autocommit=True,
            ssl=ssl_ctx,
            minsize=2,
            maxsize=10,
        )
        return pool

    # ── DDL ───────────────────────────────────────────────────────────────────

    async def create_tables_if_not_exists(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ohlctick_1mdata (
                        datetime DATETIME PRIMARY KEY,
                        open     FLOAT,
                        high     FLOAT,
                        low      FLOAT,
                        close    FLOAT,
                        ohlc4    FLOAT
                    )
                ''')
                await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ohlctick_5mdata (
                        datetime DATETIME PRIMARY KEY,
                        open     FLOAT,
                        high     FLOAT,
                        low      FLOAT,
                        close    FLOAT,
                        ohlc4    FLOAT,
                        hlc3     FLOAT
                    )
                ''')
            await conn.commit()

    # ── Gap / duplicate checker ───────────────────────────────────────────────

    async def check_missing_or_duplicate_keys(self, pool, timeframe='1m'):
        table    = 'ohlctick_1mdata' if timeframe == '1m' else 'ohlctick_5mdata'
        interval = 'INTERVAL 1 MINUTE' if timeframe == '1m' else 'INTERVAL 5 MINUTE'

        now             = pd.Timestamp.now(tz='Asia/Kolkata')
        open_time_dt    = now.replace(hour=MARKET_OPEN_H,  minute=MARKET_OPEN_M,  second=0, microsecond=0)
        close_time_dt   = now.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M, second=0, microsecond=0)
        period_now      = pd.Period.now('1min')
        previous_candle = (period_now - 1).start_time.tz_localize('Asia/Kolkata')
        min_datetime    = min(close_time_dt, previous_candle)

        open_str = open_time_dt.strftime('%Y-%m-%d %H:%M:%S')
        min_str  = min_datetime.strftime('%Y-%m-%d %H:%M:%S')

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = f"""
                    WITH RECURSIVE datetime_sequence AS (
                        SELECT '{open_str}' AS dt
                        UNION ALL
                        SELECT DATE_ADD(dt, {interval})
                        FROM datetime_sequence
                        WHERE dt < '{min_str}'
                    )
                    SELECT COUNT(*) AS num_issues
                    FROM (
                        SELECT ds.dt AS missing_or_dup
                        FROM datetime_sequence ds
                        LEFT JOIN (
                            SELECT `datetime`, COUNT(*) AS cnt
                            FROM `{table}`
                            WHERE `datetime` >= '{open_str}' AND `datetime` <= '{min_str}'
                            GROUP BY `datetime`
                        ) t ON ds.dt = t.`datetime`
                        LEFT JOIN `{table}` id ON ds.dt = id.`datetime`
                        WHERE t.`datetime` IS NULL
                           OR t.cnt > 1
                           OR COALESCE(id.open, id.high, id.low, id.close) = 0
                           OR (id.open = id.close AND id.open = id.high AND id.open = id.low)
                    ) AS issues;
                """
                await cursor.execute(query)
                result     = await cursor.fetchone()
                num_issues = result[0] if result else 0
                if num_issues:
                    print(f"[{timeframe}] Missing/duplicate candles in {table}: {num_issues}")
                return num_issues

    # ── Shared DataFrame cleaner ──────────────────────────────────────────────

    def _clean_tv_df(self, raw_data):
        """Convert raw TvDatafeed output to a clean, IST-aware, tz-naive DataFrame."""
        df = pd.DataFrame(raw_data)
        df.index = pd.to_datetime(df.index, errors='coerce')
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'datetime'}, inplace=True)
        df['datetime'] = (
            pd.to_datetime(df['datetime'], utc=True)
              .dt.tz_convert('Asia/Kolkata')
              .dt.tz_localize(None)
        )
        df.rename(columns={
            'Open': 'open', 'High': 'high',
            'Low':  'low',  'Close': 'close',
        }, inplace=True)
        df['ohlc4'] = ((df['open'] + df['high'] +
                        df['low']  + df['close']) / 4).round(2)
        return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4']]

    # ── Breeze single-session fetch ───────────────────────────────────────────

    def _breeze_fetch_session(self, breeze, interval, from_str, to_str, timeframe):
        """
        Fetch one session window from Breeze and return a clean DataFrame.
        Returns empty DataFrame on any failure — never raises.
        Columns: [datetime, open, high, low, close]  (ohlc4 added by caller)
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
                print(f"[Breeze/{timeframe}] Bad response {from_str}: "
                      f"Status={resp.get('Status') if resp else 'None'}, "
                      f"Error={resp.get('Error', 'unknown') if resp else ''}")
                return pd.DataFrame()

            records = resp.get('Success') or []
            if not records:
                print(f"[Breeze/{timeframe}] Empty payload for {from_str}.")
                return pd.DataFrame()

            df = pd.DataFrame(records)
            df['datetime'] = pd.to_datetime(df['datetime'])
            if df['datetime'].dt.tz is not None:
                df['datetime'] = df['datetime'].dt.tz_localize(None)

            for col in ['open', 'high', 'low', 'close']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
            df = df[df['open'] != 0].copy()

            # Keep only market-hours candles (09:15 – 15:30)
            t = df['datetime'].dt.time
            market_open  = datetime.strptime('09:15', '%H:%M').time()
            market_close = datetime.strptime('15:30', '%H:%M').time()
            df = df[(t >= market_open) & (t <= market_close)].copy()

            return df[['datetime', 'open', 'high', 'low', 'close']]

        except Exception as e:
            print(f"[Breeze/{timeframe}] Session fetch error {from_str}: {e}")
            return pd.DataFrame()

    # ── Breeze fallback fetch (multi-day loop) ────────────────────────────────

    def _fetch_breeze(self, n_bars, timeframe='1m'):
        """
        Fetch n_bars candles from Breeze get_historical_data (v1).

        Strategy — mirrors the original IndexHistoricalData logic:
          1. Fetch today's session first.
          2. If still short of n_bars, walk back one business day at a time
             (up to 10 calendar days) fetching each prior session and prepending.
          3. Trim to the last n_bars and return.

        This handles Breeze's per-call session limit cleanly without relying on
        a single large date-window that may be truncated silently.

        BankNifty: stock_code="CNXBAN", exchange_code="NSE", product_type="cash"
        No expiry / right / strike_price required.
        """
        try:
            breeze   = self._get_breeze()
            interval = BREEZE_INTERVAL[timeframe]
            fmt      = '%Y-%m-%d %H:%M:%S'

            now_ist     = datetime.now(IST)
            today       = now_ist.date()
            session_end = now_ist.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M,
                                          second=0, microsecond=0)
            session_open_t = datetime.strptime('09:15', '%H:%M').time()

            combined = pd.DataFrame()

            # ── Step 1: today's session up to now ────────────────────────────
            today_start = datetime.combine(today,
                          datetime.strptime('09:15', '%H:%M').time()).replace(tzinfo=IST)
            from_str = today_start.strftime(fmt)
            to_str   = session_end.strftime(fmt)
            print(f"[Breeze/{timeframe}] Fetching today: {from_str} → {to_str}")
            day_df = self._breeze_fetch_session(breeze, interval, from_str, to_str, timeframe)
            if not day_df.empty:
                combined = day_df

            # ── Step 2: walk back day by day until n_bars satisfied ───────────
            lookback_day  = today - timedelta(days=1)
            cutoff        = today - timedelta(days=10)

            while len(combined) < n_bars and lookback_day >= cutoff:
                if not self.is_business_day(lookback_day):
                    lookback_day -= timedelta(days=1)
                    continue

                d_start  = datetime.combine(lookback_day,
                            datetime.strptime('09:15', '%H:%M').time()).replace(tzinfo=IST)
                d_end    = datetime.combine(lookback_day,
                            datetime.strptime('15:30', '%H:%M').time()).replace(tzinfo=IST)
                from_str = d_start.strftime(fmt)
                to_str   = d_end.strftime(fmt)

                print(f"[Breeze/{timeframe}] Fetching prior session: {from_str} → {to_str}"
                      f"  (have {len(combined)}/{n_bars} bars)")
                day_df = self._breeze_fetch_session(breeze, interval, from_str, to_str, timeframe)

                if not day_df.empty:
                    combined = pd.concat([day_df, combined], ignore_index=True)

                lookback_day -= timedelta(days=1)

            if combined.empty:
                print(f"[Breeze/{timeframe}] No data collected across all sessions.")
                return pd.DataFrame()

            if len(combined) < n_bars:
                print(f"[Breeze/{timeframe}] Only {len(combined)} bars available "
                      f"(needed {n_bars}) — proceeding with what we have.")

            # ── Finalise ─────────────────────────────────────────────────────
            combined.sort_values('datetime', inplace=True)
            combined.drop_duplicates(subset='datetime', keep='last', inplace=True)
            combined.reset_index(drop=True, inplace=True)

            combined['ohlc4'] = ((combined['open'] + combined['high'] +
                                  combined['low']  + combined['close']) / 4).round(2)

            # Trim to last n_bars
            combined = combined.tail(n_bars).reset_index(drop=True)

            print(f"[Breeze/{timeframe}] Returning {len(combined)} bars via fallback.")
            return combined[['datetime', 'open', 'high', 'low', 'close', 'ohlc4']]

        except Exception as e:
            print(f"[Breeze/{timeframe}] Fallback error: {e}")
            return pd.DataFrame()

    # ── Primary TV fetch with fallback ────────────────────────────────────────

    async def _fetch_with_fallback(self, timeframe, n_bars, tv_interval):
        """
        Try tv.get_hist() with a 30-second timeout.
        On timeout, empty result, or any exception, fall back to Breeze API.
        Returns a clean tz-naive IST DataFrame or empty DataFrame.
        """
        raw = None
        try:
            # Run the blocking TvDatafeed call in an executor with a timeout
            loop = asyncio.get_event_loop()
            raw  = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self.tv.get_hist(
                        symbol   = 'BANKNIFTY',
                        exchange = 'NSE',
                        interval = tv_interval,
                        n_bars   = n_bars,
                    )
                ),
                timeout=30.0,
            )
        except asyncio.TimeoutError:
            print(f"[TV/{timeframe}] get_hist() timed out. Switching to Breeze fallback.")
        except Exception as e:
            print(f"[TV/{timeframe}] get_hist() error: {e}. Switching to Breeze fallback.")

        if raw is not None and not raw.empty:
            return self._clean_tv_df(raw)

        # ── Breeze fallback ───────────────────────────────────────────────────
        print(f"[TV/{timeframe}] No data from TvDatafeed. Using Breeze fallback.")
        loop = asyncio.get_event_loop()
        df   = await loop.run_in_executor(
            None,
            lambda: self._fetch_breeze(n_bars, timeframe),
        )
        return df

    # ── Per-timeframe fetch entry points ─────────────────────────────────────

    async def fetch_tv_data_1m(self, pool):
        n_bars = await self.check_missing_or_duplicate_keys(pool, '1m') + 750
        df     = await self._fetch_with_fallback('1m', n_bars, Interval.in_1_minute)
        if df.empty:
            print("[1m] No data from TV or Breeze.")
        return df

    async def fetch_tv_data_5m(self, pool):
        n_bars = await self.check_missing_or_duplicate_keys(pool, '5m') + 75
        df     = await self._fetch_with_fallback('5m', n_bars, Interval.in_5_minute)
        if df.empty:
            print("[5m] No data from TV or Breeze.")
            return df
        df['hlc3'] = ((df['high'] + df['low'] + df['close']) / 3).round(2)
        return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4', 'hlc3']]

    # ── DB insert ─────────────────────────────────────────────────────────────

    async def insert_tick_dataframe(self, pool, df, table):
        if df.empty:
            return
        cols         = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(cols))
        col_names    = ', '.join(f'`{c}`' for c in cols)
        sql          = f'REPLACE INTO `{table}` ({col_names}) VALUES ({placeholders})'

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    records = [tuple(row) for row in df.itertuples(index=False)]
                    await cursor.executemany(sql, records)
                    await conn.commit()
                except Exception as e:
                    print(f"[{table}] Insert error: {e}")
                    await conn.rollback()

    # ── Market calendar helpers ───────────────────────────────────────────────

    def is_market_open(self):
        now = pd.Timestamp.now(tz='Asia/Kolkata')
        return (now.replace(hour=MARKET_OPEN_H,  minute=MARKET_OPEN_M,  second=0, microsecond=0)
                <= now <=
                now.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M, second=0, microsecond=0))

    def is_business_day(self, date):
        return (date.weekday() < 5 and
                date.strftime('%Y-%m-%d') not in holidays)

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self):
        now = datetime.now(IST)
        print(f"TvDataUpdate started at {now.strftime('%Y-%m-%d %H:%M:%S')} IST")

        if not self.is_business_day(now):
            print("Not a trading day. Exiting.")
            return

        market_close = now.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M,
                                   second=0, microsecond=0)
        if now > market_close:
            print(f"Market already closed ({now.strftime('%H:%M:%S')} IST). Exiting.")
            return

        # Wait if started before market open
        market_open = now.replace(hour=MARKET_OPEN_H, minute=MARKET_OPEN_M,
                                  second=0, microsecond=0)
        if now < market_open:
            wait_secs = (market_open - now).total_seconds()
            print(f"Market opens in {wait_secs:.0f}s. Waiting...")
            await asyncio.sleep(wait_secs)

        # Single TvDatafeed instance for the entire session
        self.tv = TvDatafeed()

        pool = await self.get_mysql_pool()
        await self.create_tables_if_not_exists(pool)

        try:
            print("Market open. Starting data pipeline...")
            while self.is_market_open():
                df_1m = await self.fetch_tv_data_1m(pool)
                df_5m = await self.fetch_tv_data_5m(pool)
                await self.insert_tick_dataframe(pool, df_1m, 'ohlctick_1mdata')
                await self.insert_tick_dataframe(pool, df_5m, 'ohlctick_5mdata')

                # Sleep until 5s after the next 1-min candle close
                period_now     = pd.Period.now('1min')
                next_execution = (
                    (period_now + 1).start_time.tz_localize('Asia/Kolkata')
                    + pd.Timedelta(seconds=5)
                )
                sleep_till = (next_execution - pd.Timestamp.now(tz='Asia/Kolkata')).total_seconds()
                if 0 < sleep_till < 61:
                    await asyncio.sleep(sleep_till)

            print(f"Market closed at {datetime.now(IST).strftime('%H:%M:%S')} IST. Exiting.")

        except Exception as e:
            print(f"Pipeline error: {e}")
            raise
        finally:
            pool.close()
            await pool.wait_closed()


if __name__ == "__main__":
    tvdata_update = TvDataUpdate()
    asyncio.run(tvdata_update.run())