from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import json
import asyncio
import aiomysql
import pytz
from datetime import datetime, time, timedelta

class TvDataAll:
    def __init__(self, config):
        self.config = config
        self.tv_username = config['tvdatafeed']['username']
        self.tv_password = config['tvdatafeed']['password']

    async def get_mysql_pool(self):
        db_config = self.config['db_config']
        port = int(db_config['port'])
        pool = await aiomysql.create_pool(
            host=db_config['host'],
            port=port,
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['database'],
            autocommit=True,
            minsize=5,
            maxsize=20
        )
        return pool

    async def create_tables_if_not_exists(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                create_table_query = '''
                CREATE TABLE IF NOT EXISTS ohlctick_1mdata (
                    datetime DATETIME PRIMARY KEY,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    ohlc4 FLOAT
                )
                '''
                await cursor.execute(create_table_query)
            await conn.commit()

    async def save_indicators_to_db(self, pool, data):
        # Handle NaN values and filter rows with zeroes in specified columns
        data = [[None if pd.isna(x) else x for x in row] for row in data]
        non_zero_data = [
            row for row in data if all(row[i] != 0 for i in [1, 2, 3, 4])
        ]

        replace_query = '''
            REPLACE INTO ohlctick_1mdata (
                datetime, open, high, low, close, ohlc4
            ) VALUES (%s, %s, %s, %s, %s, %s)
        '''
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(replace_query, non_zero_data)
                print(f"Inserted {len(non_zero_data)} rows into the database")

    async def fetch_tv_data(self):
        # tv = TvDatafeed(self.tv_username, self.tv_password)
        tv = TvDatafeed()  # uncomment if using without login
        try:
            data = tv.get_hist(symbol='BANKNIFTY', exchange='NSE',
                               interval=Interval.in_1_minute, n_bars=1000)
            dataf = pd.DataFrame(data)
            dataf.index = pd.to_datetime(dataf.index, errors='coerce')
            dataf.reset_index(inplace=True)
            dataf.rename(columns={'index': 'datetime'}, inplace=True)

            dataf.rename(columns={
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close'
            }, inplace=True)
            if {'open', 'high', 'low', 'close'}.issubset(dataf.columns):
                dataf['ohlc4'] = (dataf['open'] + dataf['high'] +
                                  dataf['low'] + dataf['close']) / 4
                dataf['ohlc4'] = dataf['ohlc4'].round(2)
            else:
                print("Missing expected columns. Available columns:", dataf.columns)
                return pd.DataFrame()
            
            selected_columns = ['datetime', 'open', 'high', 'low', 'close', 'ohlc4']
            tick_df = dataf[selected_columns]
            return tick_df
        except Exception as e:
            print(f"Error fetching TV data: {e}")
            return pd.DataFrame()

    async def run(self):
        pool = await self.get_mysql_pool()
        await self.create_tables_if_not_exists(pool)
        tick_df = await self.fetch_tv_data()
        if not tick_df.empty:
            await self.save_indicators_to_db(pool, tick_df.to_numpy())
        pool.close()
        await pool.wait_closed()

if __name__ == "__main__":
    with open('config.json') as config_file:
        config = json.load(config_file)

    tvdata = TvDataAll(config)
    asyncio.run(tvdata.run())
