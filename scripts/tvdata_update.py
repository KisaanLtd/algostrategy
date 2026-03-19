from tvDatafeed import TvDatafeed, Interval
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


class TvDataUpdate:

    def __init__(self):
        self.tv = None  # TvDatafeed instance — created once in run()

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

        now              = pd.Timestamp.now(tz='Asia/Kolkata')
        open_time_dt     = now.replace(hour=9,  minute=15, second=0, microsecond=0)
        close_time_dt    = now.replace(hour=15, minute=30, second=0, microsecond=0)
        period_now       = pd.Period.now('1min')
        previous_candle  = (period_now - 1).start_time.tz_localize('Asia/Kolkata')
        min_datetime     = min(close_time_dt, previous_candle)

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

    # ── TvDatafeed fetch & clean ──────────────────────────────────────────────

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

    async def fetch_tv_data_1m(self, pool):
        num_issues = await self.check_missing_or_duplicate_keys(pool, '1m')
        raw = self.tv.get_hist(
            symbol='BANKNIFTY', exchange='NSE',
            interval=Interval.in_1_minute,
            n_bars=num_issues + 750,
        )
        if raw is None or raw.empty:
            print("[1m] TvDatafeed returned no data.")
            return pd.DataFrame()
        return self._clean_tv_df(raw)

    async def fetch_tv_data_5m(self, pool):
        num_issues = await self.check_missing_or_duplicate_keys(pool, '5m')
        raw = self.tv.get_hist(
            symbol='BANKNIFTY', exchange='NSE',
            interval=Interval.in_5_minute,
            n_bars=num_issues + 75,
        )
        if raw is None or raw.empty:
            print("[5m] TvDatafeed returned no data.")
            return pd.DataFrame()
        df = self._clean_tv_df(raw)
        df['hlc3'] = ((df['high'] + df['low'] + df['close']) / 3).round(2)
        return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4', 'hlc3']]

    # ── DB insert ─────────────────────────────────────────────────────────────

    async def insert_tick_dataframe(self, pool, df, table):
        if df.empty:
            return
        cols        = df.columns.tolist()
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
        return (now.replace(hour=9,  minute=15, second=0, microsecond=0)
                <= now <=
                now.replace(hour=15, minute=30, second=0, microsecond=0))

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

        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now > market_close:
            print(f"Market already closed ({now.strftime('%H:%M:%S')} IST). Exiting.")
            return

        # Wait if started before market open
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
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