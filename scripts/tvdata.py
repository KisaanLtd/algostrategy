from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import asyncio
import aiomysql
import os
import ssl

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


class TvDataAll:

    def __init__(self):
        self.tv = TvDatafeed()

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

    async def truncate_tables(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('TRUNCATE TABLE ohlctick_1mdata')
                await cursor.execute('TRUNCATE TABLE ohlctick_5mdata')
            await conn.commit()
        print("Tables truncated.")

    # ── Fetch & clean ─────────────────────────────────────────────────────────

    def _clean_tv_df(self, raw_data):
        """Convert raw TvDatafeed output to clean IST-aware, tz-naive DataFrame."""
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
            print("Missing expected OHLC columns. Available:", df.columns.tolist())
            return pd.DataFrame()
        df['ohlc4'] = ((df['open'] + df['high'] +
                        df['low']  + df['close']) / 4).round(2)
        return df

    def fetch_1m(self, n_bars=1000):
        print(f"Fetching {n_bars} bars of 1-min data...")
        raw = self.tv.get_hist(
            symbol='BANKNIFTY', exchange='NSE',
            interval=Interval.in_1_minute,
            n_bars=n_bars,
        )
        if raw is None or raw.empty:
            print("1-min fetch returned no data.")
            return pd.DataFrame()
        df = self._clean_tv_df(raw)
        if df.empty:
            return df
        return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4']]

    def fetch_5m(self, n_bars=200):
        print(f"Fetching {n_bars} bars of 5-min data...")
        raw = self.tv.get_hist(
            symbol='BANKNIFTY', exchange='NSE',
            interval=Interval.in_5_minute,
            n_bars=n_bars,
        )
        if raw is None or raw.empty:
            print("5-min fetch returned no data.")
            return pd.DataFrame()
        df = self._clean_tv_df(raw)
        if df.empty:
            return df
        df['hlc3'] = ((df['high'] + df['low'] + df['close']) / 3).round(2)
        return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4', 'hlc3']]

    # ── DB insert ─────────────────────────────────────────────────────────────

    async def insert_dataframe(self, pool, df, table):
        if df.empty:
            print(f"[{table}] Nothing to insert.")
            return
        # Drop rows where all OHLC are zero — bad bars
        df = df[(df[['open', 'high', 'low', 'close']] != 0).all(axis=1)]
        if df.empty:
            print(f"[{table}] All rows were zero-OHLC, skipping.")
            return

        cols         = df.columns.tolist()
        col_names    = ', '.join(f'`{c}`' for c in cols)
        placeholders = ', '.join(['%s'] * len(cols))
        sql          = f'REPLACE INTO `{table}` ({col_names}) VALUES ({placeholders})'

        records = [tuple(row) for row in df.itertuples(index=False)]
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
        print("Done.")


if __name__ == "__main__":
    tvdata = TvDataAll()
    asyncio.run(tvdata.run())