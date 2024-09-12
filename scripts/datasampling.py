import os
import json
import asyncio
import aiomysql
import pytz
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from breeze_connect import BreezeConnect

# Get the absolute path of the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Reference to config.json
config_path = os.path.join(project_root, 'config', 'config.json')


IST = pytz.timezone('Asia/Kolkata')


class OnesToOnem:
    def __init__(self, config):
        self.config = config
        self.api = BreezeConnect(api_key=config['api_key'])
        self.api.generate_session(
            api_secret=config['secret_key'], session_token=config['api_session'])
        # self.default_expiry_date = config.get('default_expiry_date', '2024-09-04')

    async def get_mysql_pool(self):
        db_config = self.config['db_config']
        pool = await aiomysql.create_pool(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['database'],
            autocommit=True,
            minsize=5,
            maxsize=20
        )
        return pool

    async def create_tables_if_not_exists(self, pool):
        create_1s_table_query = '''
            CREATE TABLE IF NOT EXISTS ohlctick_1sdata (
                datetime DATETIME,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                ohlc4 FLOAT,
                PRIMARY KEY (datetime)
            )
        '''
        create_1m_table_query = '''
            CREATE TABLE IF NOT EXISTS ohlctick_1mdata (
                datetime DATETIME,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                ohlc4 FLOAT,
                PRIMARY KEY (datetime)
            )
        '''

        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(create_1s_table_query)
                    await cur.execute(create_1m_table_query)
        except Exception as e:
            print(f"Error creating tables: {e}")
            raise

    async def insert_tick_dataframe(self, pool, table_name, tick_df):
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    insert_query = f'''
                        REPLACE INTO {table_name} (datetime, open, high, low, close, ohlc4)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    '''
                    records = tick_df.to_dict(orient='records')
                    for record in records:
                        datetime_value = record['datetime']
                        if isinstance(datetime_value, str):
                            datetime_value = pd.to_datetime(
                                datetime_value, errors='coerce')
                        await cur.execute(insert_query, (
                            datetime_value, record['open'], record['high'],
                            record['low'], record['close'], record['ohlc4']
                        ))
                await conn.commit()
        except Exception as e:
            print(f"Error inserting data into {table_name} table: {e}")
            raise

    async def fetch_and_resample_data(self, pool):
        try:
            now = datetime.now(IST)
            market_open_time = datetime.combine(now.date(), time(9, 15))
            market_close_time = datetime.combine(now.date(), time(15, 30))
            period_now = pd.Period.now('1min')
            open_time_datetime64 = pd.Period(
                market_open_time, '1min').start_time
            close_time_datetime64 = pd.Period(
                market_close_time, '1min').end_time
            period_now_datetime64 = period_now.start_time
            min_datetime = min(close_time_datetime64, period_now_datetime64)

            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = 'SELECT * FROM ohlctick_1sdata ORDER BY datetime'
                    await cur.execute(query)
                    data = await cur.fetchall()
                    columns = [desc[0] for desc in cur.description]

                    ohlc_1s_df = pd.DataFrame(data, columns=columns)
                    ohlc_1s_df['datetime'] = pd.to_datetime(
                        ohlc_1s_df['datetime'], errors='coerce')
                    ohlc_1s_df.set_index('datetime', inplace=True)

                    period_index = pd.date_range(
                        start=open_time_datetime64, end=min_datetime, freq='s')
                    ohlc_1s_df = ohlc_1s_df.reindex(period_index)

                    ohlc_dict = {
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'ohlc4': 'mean'
                    }

                    ohlc_1m_df = ohlc_1s_df.resample(
                        '1min', closed='left', label='left').agg(ohlc_dict).reset_index()
                    ohlc_1m_df.fillna({
                        'open': 0.0,
                        'high': 0.0,
                        'low': 0.0,
                        'close': 0.0,
                        'ohlc4': 0.0
                    }, inplace=True)
                    ohlc_1m_df.rename(
                        columns={'index': 'datetime'}, inplace=True)
                    ohlc_1m_df['ohlc4'] = ohlc_1m_df['ohlc4'].round(2)
                    two_rows = ohlc_1m_df.tail(2)
                    await self.insert_tick_dataframe(pool, 'ohlctick_1mdata', two_rows)
        except Exception as e:
            print(f"Error fetching and resampling data: {e}")

    async def on_ticks(self, tick):
        try:
            if isinstance(tick, dict):
                tick = [tick]
            tick_df = pd.DataFrame(tick)
            if 'datetime' in tick_df.columns:
                tick_df['datetime'] = pd.to_datetime(
                    tick_df['datetime'], errors='coerce')
            else:
                raise ValueError(
                    "'datetime' column is missing in the tick data")

            for col in ['open', 'high', 'low', 'close']:
                tick_df[col] = pd.to_numeric(tick_df[col], errors='coerce')

            if all(col in tick_df.columns for col in ['open', 'high', 'low', 'close']):
                tick_df['ohlc4'] = (
                    tick_df['open'] + tick_df['high'] + tick_df['low'] + tick_df['close']) / 4
                tick_df['ohlc4'] = tick_df['ohlc4'].round(2)
                selected_columns = ['datetime', 'open',
                                    'high', 'low', 'close', 'ohlc4']
                tick_df = tick_df[selected_columns]
                pool = await self.get_mysql_pool()
                await self.insert_tick_dataframe(pool, 'ohlctick_1sdata', tick_df)
                pool.close()
                await pool.wait_closed()
            else:
                print("Error: Missing required columns in tick data")
        except Exception as e:
            print(f"Error processing tick data: {e}")

    def async_on_ticks(self, tick):
        asyncio.run(self.on_ticks(tick))

    async def connect_to_websocket(self):
        print("Connecting to WebSocket...")
        self.api.ws_connect()
        self.api.on_ticks = self.async_on_ticks
        self.api.subscribe_feeds(
            stock_token='4.1!NIFTY BANK', interval="1second")
        print("Subscribed to data feed")

    async def disconnect_from_websocket(self):
        print("Unsubscribing from data feed...")
        self.api.unsubscribe_feeds(
            stock_token='4.1!NIFTY BANK', interval="1second")
        disconnected = self.api.ws_disconnect()
        if disconnected:
            print("WebSocket disconnected")
        else:
            print("Error disconnecting WebSocket")

    def is_business_day(self, date):
        return np.is_busday(date.date())

    def is_market_open(self):
        # now = pd.Timestamp.now(IST)
        now = datetime.now(IST)
        market_open_time = datetime.combine(
            now.date(), time(9, 15)).replace(tzinfo=IST)
        market_close_time = datetime.combine(
            now.date(), time(15, 30)).replace(tzinfo=IST)
        return market_open_time <= now <= market_close_time

    async def run(self):
        pool = await self.get_mysql_pool()
        await self.create_tables_if_not_exists(pool)
        try:
            while True:
                now = datetime.now(IST)
                if self.is_market_open() and self.is_business_day(now):
                    await self.connect_to_websocket()
                    while self.is_market_open():
                        await self.fetch_and_resample_data(pool)
                        # current_time = pd.Timestamp.now(IST)
                        current_time = datetime.now(IST)
                        period_now = pd.Period.now('1min')
                        # period_now_start_time = period_now.start_time.replace(tzinfo=IST)
                        next_period_start = (period_now + 1).start_time.replace(tzinfo=IST)
                        next_execution = (next_period_start + pd.Timedelta(seconds=3))
                        sleep_duration = (next_execution - current_time).total_seconds()
                        if sleep_duration > 0 and sleep_duration < 61:
                            await asyncio.sleep(sleep_duration)
                    await self.disconnect_from_websocket()
                else:
                    now = datetime.now(IST)
                    next_market_open = (
                        now + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
                    # sleep_duration = (next_market_open - now).total_seconds()
                    time_until_open = (next_market_open - now).total_seconds()
                    print(
                        f"Market closed. Sleeping for {time_until_open} seconds.")
                    await asyncio.sleep(time_until_open)
        except Exception as e:
            print(f"Error in run loop: {e}")
        finally:
            pool.close()
            await pool.wait_closed()

if __name__ == "__main__":
    with open(config_path, 'r') as f:
        config = json.load(f)

# if __name__ == "__main__":
#     with open('config.json') as config_file:
#         config = json.load(config_file)

    onestoonem = OnesToOnem(config)
    asyncio.run(onestoonem.run())
