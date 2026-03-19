from tvDatafeed import TvDatafeed, Interval
from breeze_connect import BreezeConnect
import pandas as pd
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

MARKET_OPEN_H,  MARKET_OPEN_M  = 9,  15
MARKET_CLOSE_H, MARKET_CLOSE_M = 15, 30

BREEZE_INTERVAL = {
    '1m': '1minute',
    '5m': '5minute',
}


class TvDataAll:

    def __init__(self):
        self.tv      = None   # created lazily — avoid crashing at import time
        self._breeze = None   # created lazily on first fallback

    # ── Breeze session (lazy) ─────────────────────────────────────────────────

    def _get_breeze(self) -> BreezeConnect:
        if self._breeze is None:
            api_key       = os.getenv("API_KEY")
            api_secret    = os.getenv("API_SECRET")
            session_token = os.getenv("SESSION_TOKEN")
            if not all([api_key, api_secret, session_token]):
                raise EnvironmentError(
                    "Breeze fallback requires API_KEY, API_SECRET, SESSION_TOKEN."
                )
            self._breeze = BreezeConnect(api_key=api_key)
            self._breeze.generate_session(
                api_secret=api_secret, session_token=str(session_token))
            print("[Breeze] Session initialised.")
        return self._breeze

    # ── DB pool ───────────────────────────────────────────────────────────────

    async def get_mysql_pool(self):
        ssl_ctx = None
        ca_path = os.path.join(os.path.dirname(__file__), '..', 'ca.pem')
        if os.path.exists(ca_path):
            ssl_ctx = ssl.create_default_context(cafile=ca_path)
        pool = await aiomysql.create_pool(
            host=db_config['host'], port=db_config['port'],
            user=db_config['user'], password=db_config['password'],
            db=db_config['database'],
            autocommit=True, ssl=ssl_ctx, minsize=2, maxsize=10,
        )
        return pool

    # ── DDL ───────────────────────────────────────────────────────────────────

    async def create_tables_if_not_exists(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ohlctick_1mdata (
                        datetime DATETIME PRIMARY KEY,
                        open FLOAT, high FLOAT, low FLOAT, close FLOAT, ohlc4 FLOAT
                    )
                ''')
                await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ohlctick_5mdata (
                        datetime DATETIME PRIMARY KEY,
                        open FLOAT, high FLOAT, low FLOAT, close FLOAT,
                        ohlc4 FLOAT, hlc3 FLOAT
                    )
                ''')
            await conn.commit()

    async def truncate_tables(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('TRUNCATE TABLE ohlctick_1mdata')
                await cursor.execute('TRUNCATE TABLE ohlctick_5mdata')
            await conn.commit()
        print("ohlctick tables truncated.")

    # ── TvDatafeed clean ──────────────────────────────────────────────────────

    def _clean_tv_df(self, raw_data):
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
        if not {'open', 'high', 'low', 'close'}.issubset(df.columns):
            print("Missing OHLC columns. Available:", df.columns.tolist())
            return pd.DataFrame()
        df['ohlc4'] = ((df['open'] + df['high'] +
                        df['low']  + df['close']) / 4).round(2)
        return df

    # ── Breeze helpers (shared with tvdata_update.py pattern) ─────────────────

    def _is_business_day(self, date) -> bool:
        return date.weekday() < 5 and date.strftime('%Y-%m-%d') not in holidays

    def _breeze_fetch_session(self, breeze, interval, from_str, to_str, label):
        """Fetch one session window. Returns [datetime,open,high,low,close] or empty."""
        try:
            resp = breeze.get_historical_data(
                interval=interval, from_date=from_str, to_date=to_str,
                stock_code="CNXBAN", exchange_code="NSE", product_type="cash",
            )
            if not resp or resp.get('Status') != 200:
                print(f"[Breeze/{label}] Bad response {from_str}: "
                      f"Status={resp.get('Status') if resp else 'None'}")
                return pd.DataFrame()
            records = resp.get('Success') or []
            if not records:
                print(f"[Breeze/{label}] Empty payload for {from_str}.")
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
            df = df[(t >= datetime.strptime('09:15', '%H:%M').time()) &
                    (t <= datetime.strptime('15:30', '%H:%M').time())].copy()

            print(f"  [Breeze/{label}] {len(df)} bars from {from_str}")
            return df[['datetime', 'open', 'high', 'low', 'close']]
        except Exception as e:
            print(f"[Breeze/{label}] Session error {from_str}: {e}")
            return pd.DataFrame()

    def _fetch_breeze(self, n_bars, timeframe='1m') -> pd.DataFrame:
        """
        Multi-day Breeze fallback. Fetches today first then walks back day by
        day (up to 10 calendar days) until n_bars is satisfied.
        Returns [datetime, open, high, low, close, ohlc4], tz-naive IST.
        """
        breeze   = self._get_breeze()
        interval = BREEZE_INTERVAL[timeframe]
        fmt      = '%Y-%m-%d %H:%M:%S'
        now_ist  = datetime.now(IST)
        today    = now_ist.date()
        session_end = now_ist.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M,
                                      second=0, microsecond=0)
        combined = pd.DataFrame()

        # Today's session
        today_start = datetime.combine(today, datetime.strptime('09:15', '%H:%M').time())
        print(f"[Breeze/{timeframe}] Fetching today: {today_start.strftime(fmt)}")
        day_df = self._breeze_fetch_session(
            breeze, interval,
            today_start.strftime(fmt), session_end.strftime(fmt), timeframe)
        if not day_df.empty:
            combined = day_df

        # Walk back until n_bars satisfied
        lookback_day = today - timedelta(days=1)
        cutoff       = today - timedelta(days=10)
        while len(combined) < n_bars and lookback_day >= cutoff:
            if not self._is_business_day(lookback_day):
                lookback_day -= timedelta(days=1)
                continue
            d_start = datetime.combine(lookback_day,
                        datetime.strptime('09:15', '%H:%M').time())
            d_end   = datetime.combine(lookback_day,
                        datetime.strptime('15:30', '%H:%M').time())
            print(f"[Breeze/{timeframe}] Prior session ({lookback_day}): "
                  f"have {len(combined)}/{n_bars}")
            day_df = self._breeze_fetch_session(
                breeze, interval,
                d_start.strftime(fmt), d_end.strftime(fmt), timeframe)
            if not day_df.empty:
                combined = pd.concat([day_df, combined], ignore_index=True)
            lookback_day -= timedelta(days=1)

        if combined.empty:
            print(f"[Breeze/{timeframe}] No data collected.")
            return pd.DataFrame()

        combined.sort_values('datetime', inplace=True)
        combined.drop_duplicates(subset='datetime', keep='last', inplace=True)
        combined.reset_index(drop=True, inplace=True)
        combined['ohlc4'] = ((combined['open'] + combined['high'] +
                              combined['low']  + combined['close']) / 4).round(2)
        combined = combined.tail(n_bars).reset_index(drop=True)
        print(f"[Breeze/{timeframe}] Returning {len(combined)} bars.")
        return combined[['datetime', 'open', 'high', 'low', 'close', 'ohlc4']]

    # ── Primary fetch with fallback ───────────────────────────────────────────

    def fetch_1m(self, n_bars=1000) -> pd.DataFrame:
        """Fetch n_bars of 1-min data. TV primary → Breeze fallback."""
        print(f"[TV/1m] Fetching {n_bars} bars...")
        try:
            if self.tv is None:
                self.tv = TvDatafeed()
            raw = self.tv.get_hist(
                symbol='BANKNIFTY', exchange='NSE',
                interval=Interval.in_1_minute, n_bars=n_bars,
            )
            if raw is not None and not raw.empty:
                df = self._clean_tv_df(raw)
                if not df.empty:
                    print(f"[TV/1m] Got {len(df)} bars from TvDatafeed.")
                    return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4']]
        except Exception as e:
            print(f"[TV/1m] TvDatafeed error: {e}")

        print("[TV/1m] Falling back to Breeze...")
        return self._fetch_breeze(n_bars, '1m')

    def fetch_5m(self, n_bars=200) -> pd.DataFrame:
        """Fetch n_bars of 5-min data. TV primary → Breeze fallback."""
        print(f"[TV/5m] Fetching {n_bars} bars...")
        try:
            if self.tv is None:
                self.tv = TvDatafeed()
            raw = self.tv.get_hist(
                symbol='BANKNIFTY', exchange='NSE',
                interval=Interval.in_5_minute, n_bars=n_bars,
            )
            if raw is not None and not raw.empty:
                df = self._clean_tv_df(raw)
                if not df.empty:
                    df['hlc3'] = ((df['high'] + df['low'] + df['close']) / 3).round(2)
                    print(f"[TV/5m] Got {len(df)} bars from TvDatafeed.")
                    return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4', 'hlc3']]
        except Exception as e:
            print(f"[TV/5m] TvDatafeed error: {e}")

        print("[TV/5m] Falling back to Breeze...")
        df = self._fetch_breeze(n_bars, '5m')
        if not df.empty:
            df['hlc3'] = ((df['high'] + df['low'] + df['close']) / 3).round(2)
            return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4', 'hlc3']]
        return df

    # ── DB insert ─────────────────────────────────────────────────────────────

    async def insert_dataframe(self, pool, df, table):
        if df.empty:
            print(f"[{table}] Nothing to insert.")
            return
        df = df[(df[['open', 'high', 'low', 'close']] != 0).all(axis=1)]
        if df.empty:
            print(f"[{table}] All rows were zero-OHLC, skipping.")
            return
        cols         = df.columns.tolist()
        col_names    = ', '.join(f'`{c}`' for c in cols)
        placeholders = ', '.join(['%s'] * len(cols))
        sql          = f'REPLACE INTO `{table}` ({col_names}) VALUES ({placeholders})'
        records      = [tuple(row) for row in df.itertuples(index=False)]
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    await cursor.executemany(sql, records)
                    await conn.commit()
                    print(f"[{table}] Inserted/replaced {len(records)} rows.")
                except Exception as e:
                    print(f"[{table}] Insert error: {e}")
                    await conn.rollback()

    # ── Entry point ───────────────────────────────────────────────────────────

    async def run(self):
        pool = await self.get_mysql_pool()
        await self.create_tables_if_not_exists(pool)
        await self.truncate_tables(pool)

        df_1m = self.fetch_1m(n_bars=1000)
        df_5m = self.fetch_5m(n_bars=200)

        await self.insert_dataframe(pool, df_1m, 'ohlctick_1mdata')
        await self.insert_dataframe(pool, df_5m, 'ohlctick_5mdata')

        pool.close()
        await pool.wait_closed()
        print("tvdata.py complete.")


if __name__ == "__main__":
    tvdata = TvDataAll()
    asyncio.run(tvdata.run())