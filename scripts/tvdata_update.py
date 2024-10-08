import os
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import json
import pytz
import asyncio
import aiomysql
import numpy as np
from datetime import datetime, time, timedelta

# Get the absolute path of the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Reference to config.json
config_path = os.path.join(project_root, 'config', 'config.json')

IST = pytz.timezone('Asia/Kolkata')

class TvDataUpdate:
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

    async def fetch_ohlctick_data(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = 'SELECT * FROM ohlctick_1mdata ORDER BY datetime'
                await cur.execute(query)
                data = await cur.fetchall()
                df = pd.DataFrame(
                    data, columns=['datetime', 'open', 'high', 'low', 'close', 'ohlc4'])
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.sort_values(by='datetime', inplace=True)
                return df

    async def create_tables_if_not_exists(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                create_table_query = '''CREATE TABLE IF NOT EXISTS ohlctick_1mdata (
                datetime DATETIME,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                ohlc4 FLOAT,
                PRIMARY KEY (datetime)
            )'''
                await cursor.execute(create_table_query)
            await conn.commit()

    async def check_missing_or_duplicate_keys(self, pool):
        now = pd.Timestamp.now()
        market_open_time = datetime.strptime('09:15', '%H:%M').time()
        market_close_time = datetime.strptime('15:30', '%H:%M').time()

        # Create Period object for the current minute
        period_now = pd.Period.now('1min')

        # Get the start time and end time of the market in datetime64 format
        open_time_datetime64 = pd.Timestamp.combine(now.date(), market_open_time)
        close_time_datetime64 = pd.Timestamp.combine(now.date(), market_close_time)

        # Get the start time of the current period
        period_now_datetime64 = period_now.start_time
        previous_candle = (period_now - 1).start_time

        # Calculate the sleep duration
        sleep_duration = (now - previous_candle).total_seconds()
        min_datetime = min(close_time_datetime64, previous_candle)
        

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = f"""
                    WITH RECURSIVE datetime_sequence AS (
                        SELECT '{open_time_datetime64}' AS dt
                        UNION ALL
                        SELECT DATE_ADD(dt, INTERVAL 1 MINUTE)
                        FROM datetime_sequence
                        WHERE dt < '{min_datetime}'
                    )
                    SELECT COUNT(*) AS num_issues
                    FROM (
                        SELECT ds.dt AS datetime_missing_or_duplicate
                        FROM datetime_sequence ds
                        LEFT JOIN (
                            SELECT `datetime`, COUNT(*) AS cnt
                            FROM ohlctick_1mdata
                            WHERE `datetime` >= '{open_time_datetime64}' AND `datetime` <= '{min_datetime}'
                            GROUP BY `datetime`
                        ) t ON ds.dt = t.`datetime`
                        LEFT JOIN ohlctick_1mdata id ON ds.dt = id.`datetime`
                        WHERE t.`datetime` IS NULL 
                           OR t.cnt > 1
                           OR COALESCE(id.open, id.high, id.low, id.close, id.ohlc4) = 0
                    ) AS issues;
                    """

                await cursor.execute(query)
                result = await cursor.fetchone()
                num_issues = result[0] if result else 0
                if num_issues:
                    print("Number of missing or duplicate datetime entries found:", num_issues)
                else:
                    print("No gaps or duplicates found.")
                return num_issues

    async def insert_tick_dataframe(self, pool, tick_df):
        # past_tick_df = tick_df.iloc[:-1]
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    insert_query = '''REPLACE INTO ohlctick_1mdata (datetime, open, high, low, close, ohlc4)
                    VALUES (%s, %s, %s, %s, %s, %s)'''
                    records = tick_df.to_dict(orient='records')
                    # records = past_tick_df.to_dict(orient='records')
                    for record in records:
                        await cursor.execute(insert_query, (
                            record['datetime'], record['open'], record['high'],
                            record['low'], record['close'], record['ohlc4']
                        ))
                    await conn.commit()
                    print(
                        f"Successfully inserted/updated {len(records)} rows into the database.")
                except Exception as e:
                    print(f"Error inserting data into database: {e}")
                    await conn.rollback()

    async def fetch_tv_data(self):
        tv = TvDatafeed()  # Use without login
        try:
            pool = await self.get_mysql_pool()
            num_issues = await self.check_missing_or_duplicate_keys(pool)
            data = tv.get_hist(symbol='BANKNIFTY', exchange='NSE',
                               interval=Interval.in_1_minute, n_bars=num_issues+100)
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
            selected_columns = ['datetime', 'open',
                                'high', 'low', 'close', 'ohlc4']
            tick_df = dataf[selected_columns]
            return tick_df
        except Exception as e:
            print(f"Error fetching TV data: {e}")
            return pd.DataFrame()

    def is_market_open(self):
        now = pd.Timestamp.now()
        market_open_time = now.replace(
            hour=9, minute=15, second=0, microsecond=0)
        market_close_time = now.replace(
            hour=15, minute=30, second=0, microsecond=0)
        return market_open_time <= now <= market_close_time

    def is_business_day(self, date):
        is_business = date.weekday() < 5 and date.strftime(
            '%Y-%m-%d') not in self.config['holidays']
        print(f"Date {date.strftime('%Y-%m-%d')} is business day: {is_business}")
        return is_business

    def get_sleep_duration(self):
        current_datetime = datetime.now(IST)
        current_time = current_datetime.time()
        next_market_open_datetime = current_datetime.replace(
            hour=9, minute=15, second=0, microsecond=0)

        if current_time >= next_market_open_datetime.time():
            next_market_open_datetime += timedelta(days=1)

        while not self.is_business_day(next_market_open_datetime):
            next_market_open_datetime += timedelta(days=1)

        sleep_duration = (next_market_open_datetime -
                          current_datetime).total_seconds()
        # print(f"Sleep duration until next market open: {sleep_duration} seconds")
        return sleep_duration

    async def run(self):
        pool = await self.get_mysql_pool()
        try:
            while True:
                if self.is_market_open() and self.is_business_day(datetime.now(IST)):
                    while self.is_market_open():
                        num_issues = await self.check_missing_or_duplicate_keys(pool)
                        if num_issues:
                            tick_df = await self.fetch_tv_data()
                            if not tick_df.empty:
                                await self.insert_tick_dataframe(pool, tick_df)
                        current_time = pd.Timestamp.now(IST)
                        period_now = pd.Period.now('1min')
                        period_now_start_time = period_now.start_time.replace(tzinfo=IST)
                        # next_execution = (period_now_start_time + pd.Timedelta(seconds=61))
                        next_period_start = (period_now + 1).start_time.replace(tzinfo=IST)
                        next_execution = (next_period_start + pd.Timedelta(seconds=2))
                        sleep_duration = (next_execution - current_time).total_seconds()
                        if sleep_duration > 0 and sleep_duration < 59:
                            await asyncio.sleep(sleep_duration)
                else:
                    time_until_open = self.get_sleep_duration()
                    # time_until_open = (next_market_open - now).total_seconds()
                    print(f"Market closed. Sleeping for {time_until_open} seconds.")
                    await asyncio.sleep(time_until_open)
        except KeyboardInterrupt:
            print("Process interrupted")
        finally:
            pool.close()
            await pool.wait_closed()

if __name__ == "__main__":
    with open(config_path, 'r') as f:
        config = json.load(f)

# if __name__ == "__main__":
#     with open('config.json') as config_file:
#         config = json.load(config_file)

    tvdata_update = TvDataUpdate(config)
    asyncio.run(tvdata_update.run())
