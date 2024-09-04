from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import json
import asyncio
import aiomysql
import pytz
from datetime import datetime, time, timedelta

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
    def is_market_open(self):
        current_time = datetime.now(IST).time()
        open_time = datetime.strptime('09:15', '%H:%M').time()
        close_time = datetime.strptime('15:30', '%H:%M').time()
        is_open = open_time <= current_time < close_time
        print(f"Market open status: {is_open}")
        return is_open

    def is_business_day(self, date):
        is_business = date.weekday() < 5 and date.strftime('%Y-%m-%d') not in self.config['holidays']
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

        sleep_duration = (next_market_open_datetime - current_datetime).total_seconds()
        print(f"Sleep duration until next market open: {sleep_duration} seconds")
        return sleep_duration
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
    async def check_missing_or_duplicate_keys(self, pool):
        now = pd.Timestamp.now()
        market_open_time = datetime.combine(now.date(), time(9, 15))
        market_close_time = datetime.combine(now.date(), time(15, 28))
        period_now = pd.Period.now('1T')
        open_time_datetime64 = pd.Period(market_open_time, '1T').start_time
        close_time_datetime64 = pd.Period(market_close_time, '1T').end_time
        period_now_datetime64 = period_now.start_time - pd.Timedelta(minutes=1)
        # period_now_datetime64 = period_now.end_time - pd.Timedelta(minutes=1)
        min_datetime = min(close_time_datetime64, period_now_datetime64)

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
                    WHERE t.`datetime` IS NULL OR t.cnt > 1
                ) AS issues;
                """
                await cursor.execute(query)
                result = await cursor.fetchone()
                num_issues = result[0] if result else 0
                if num_issues:
                    print(
                        "Number of missing or duplicate datetime entries found:", num_issues)
                else:
                    print("No gaps or duplicates found.")
                return num_issues
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
        tv = TvDatafeed()  # Use without login
        try:
            pool = await self.get_mysql_pool()
            num_issues = await self.check_missing_or_duplicate_keys(pool)
            data = tv.get_hist(symbol='BANKNIFTY', exchange='NSE',
                               interval=Interval.in_1_minute, n_bars=num_issues+2)
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

    # async def run(self):
    #     pool = await self.get_mysql_pool()
    #     await self.create_tables_if_not_exists(pool)
    #     tick_df = await self.fetch_tv_data()
    #     if not tick_df.empty:
    #         await self.save_indicators_to_db(pool, tick_df.to_numpy())
    #     pool.close()
    #     await pool.wait_closed()
    async def run(self):
        pool = await self.get_mysql_pool()
        try:
            while True:
                now = pd.Timestamp.now()
                if self.is_business_day(now) and self.is_market_open():
                    while self.is_market_open():
                        num_issues = await self.check_missing_or_duplicate_keys(pool)
                        if num_issues:
                            tick_df = await self.fetch_tv_data()
                            if not tick_df.empty:
                                await self.save_indicators_to_db(pool, tick_df.to_numpy())
                        current_time = pd.Timestamp.now()
                        period_now = pd.Period.now('1T')
                        period_now_start_time = period_now.start_time
                        # next_execution = (period_now_start_time + pd.Timedelta(seconds=61))
                        next_execution = (period_now.end_time + pd.Timedelta(seconds=6))
                        sleep_duration = (next_execution - current_time).total_seconds()
                        await asyncio.sleep(sleep_duration)
                else:
                    # now = pd.Timestamp.now()
                    # next_market_open = datetime.combine(
                    #     now.date(), time(9, 15))
                    # if now.time() > time(15, 30):
                    #     next_market_open += timedelta(days=1)
                    # time_until_open = (next_market_open - now).total_seconds()
                    time_until_open = self.get_sleep_duration()
                
                    print(f"Market closed. Sleeping for {time_until_open} seconds.")
                    await asyncio.sleep(time_until_open)
                    # await asyncio.sleep(time_until_open)
        except KeyboardInterrupt:
            print("Process interrupted")
        finally:
            pool.close()
            await pool.wait_closed()

if __name__ == "__main__":
    with open('config.json') as config_file:
        config = json.load(config_file)

    tvdata_update = TvDataUpdate(config)
    asyncio.run(tvdata_update.run())
