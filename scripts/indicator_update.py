import os
import json
import asyncio
import aiomysql
import pytz
import pandas as pd
import talib
import numpy as np
from datetime import datetime, timedelta

# Get the absolute path of the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Reference to config.json
config_path = os.path.join(project_root, 'config', 'config.json')


IST = pytz.timezone('Asia/Kolkata')


class IndicatorUpdate:
    def __init__(self, config):
        self.config = config

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

    def is_market_open(self):
        current_time = datetime.now(IST).time()
        open_time = datetime.strptime('09:15', '%H:%M').time()
        close_time = datetime.strptime('15:30', '%H:%M').time()
        is_open = open_time <= current_time < close_time
        print(f"Market open status: {is_open}")
        return is_open

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
        print(
            f"Sleep duration until next market open: {sleep_duration} seconds")
        return sleep_duration

    async def create_tables_if_not_exists(self, pool):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS indicators_data (
                datetime DATETIME PRIMARY KEY,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                ohlc4 DOUBLE,
                ohlc4_sma5 DOUBLE,
                highsma5 DOUBLE,
                lowsma5 DOUBLE,
                closesma26 DOUBLE,
                closesma5 DOUBLE,
                highsma5_off3 DOUBLE,
                lowsma5_off3 DOUBLE,
                KST DOUBLE,
                KST26 DOUBLE,
                BuyCall INTEGER,
                BuyPut INTEGER,
                ATR DOUBLE,
                VStop2 DOUBLE,
                VStop3 DOUBLE,
                TrendUp2 INTEGER,
                TrendUp3 INTEGER,
                Max DOUBLE,
                Min DOUBLE
            )
        '''
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(create_table_query)
                print("Tables created if not exist")

    async def fetch_ohlctick_1mdata(self, pool):
        query = "SELECT * FROM ohlctick_1mdata ORDER BY datetime"
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                result = await cur.fetchall()
                columns = [col[0] for col in cur.description]
                data = pd.DataFrame(result, columns=columns)
                data.sort_values(by='datetime', inplace=True)
                print(f"Fetched OHLC data with {len(data)} rows")
                return data

    async def calculate_additional_indicators(self, data):
        data['ohlc4_sma5'] = talib.SMA(data['ohlc4'], timeperiod=5)
        data['highsma5'] = talib.SMA(data['high'], timeperiod=5)
        data['lowsma5'] = talib.SMA(data['low'], timeperiod=5)
        data['closesma26'] = talib.SMA(data['close'], timeperiod=26)
        data['closesma5'] = talib.SMA(data['close'], timeperiod=5)
        data['highsma5_off3'] = data['highsma5'].shift(3)
        data['lowsma5_off3'] = data['lowsma5'].shift(3)

        sma1 = talib.SMA(
            talib.ROC(data['close'], timeperiod=20), timeperiod=20)
        sma2 = talib.SMA(
            talib.ROC(data['close'], timeperiod=30), timeperiod=20)
        sma3 = talib.SMA(
            talib.ROC(data['close'], timeperiod=40), timeperiod=20)
        sma4 = talib.SMA(
            talib.ROC(data['close'], timeperiod=60), timeperiod=30)
        data['KST'] = (sma1 + sma2 * 2 + sma3 * 3 + sma4 * 4)
        data['KST26'] = talib.SMA(data['KST'], timeperiod=26)

        data['BuyCall'] = (data['KST'] > data['KST26']).astype(int)
        data['BuyPut'] = (data['KST'] < data['KST26']).astype(int)

        columns_to_round = ['ohlc4_sma5', 'highsma5', 'lowsma5',
                            'closesma26', 'closesma5', 'highsma5_off3', 'lowsma5_off3', 'KST', 'KST26']
        data[columns_to_round] = data[columns_to_round].round(2)
        print("Calculated additional indicators")
        return data

    async def calculate_vstop(self, data):
        data['ATR'] = talib.ATR(data['high'], data['low'],
                                data['close'], timeperiod=252)
        data['VStop2'] = np.nan
        data['VStop3'] = np.nan
        data['TrendUp2'] = True
        data['TrendUp3'] = True
        data['Max'] = data['close']
        data['Min'] = data['close']

        for i in range(252, len(data)):
            src = data['close'].iloc[i]
            atr_m2 = data['ATR'].iloc[i] * 2
            atr_m3 = data['ATR'].iloc[i] * 3

            data.at[i, 'Max'] = max(data['Max'].iloc[i-1], src)
            data.at[i, 'Min'] = min(data['Min'].iloc[i-1], src)

            if data['TrendUp2'].iloc[i-1]:
                data.at[i, 'VStop2'] = max(data['VStop2'].iloc[i-1] if not np.isnan(
                    data['VStop2'].iloc[i-1]) else src, data['Max'].iloc[i] - atr_m2)
            else:
                data.at[i, 'VStop2'] = min(data['VStop2'].iloc[i-1] if not np.isnan(
                    data['VStop2'].iloc[i-1]) else src, data['Min'].iloc[i] + atr_m2)

            data.at[i, 'TrendUp2'] = src >= data['VStop2'].iloc[i]

            if data['TrendUp2'].iloc[i] != data['TrendUp2'].iloc[i-1]:
                data.at[i, 'Max'] = src
                data.at[i, 'Min'] = src
                data.at[i, 'VStop2'] = data['Max'].iloc[i] - \
                    atr_m2 if data['TrendUp2'].iloc[i] else data['Min'].iloc[i] + atr_m2

            if data['TrendUp3'].iloc[i-1]:
                data.at[i, 'VStop3'] = max(data['VStop3'].iloc[i-1] if not np.isnan(
                    data['VStop3'].iloc[i-1]) else src, data['Max'].iloc[i] - atr_m3)
            else:
                data.at[i, 'VStop3'] = min(data['VStop3'].iloc[i-1] if not np.isnan(
                    data['VStop3'].iloc[i-1]) else src, data['Min'].iloc[i] + atr_m3)

            data.at[i, 'TrendUp3'] = src >= data['VStop3'].iloc[i]

            if data['TrendUp3'].iloc[i] != data['TrendUp3'].iloc[i-1]:
                data.at[i, 'Max'] = src
                data.at[i, 'Min'] = src
                data.at[i, 'VStop3'] = data['Max'].iloc[i] - \
                    atr_m3 if data['TrendUp3'].iloc[i] else data['Min'].iloc[i] + atr_m3

        columns_to_round = ['ATR', 'VStop2', 'VStop3']
        data[columns_to_round] = data[columns_to_round].round(2)
        return data

    async def save_indicators_to_db(self, pool, data):
        data = [[None if pd.isna(x) else x for x in row] for row in data]
        non_zero_data = [row for row in data if any(
            row[i] not in (0, None) for i in [1, 2, 3, 4])]
        non_zero_data = non_zero_data[-min(len(non_zero_data), 10):]

        replace_query = '''
            REPLACE INTO indicators_data (
                datetime, open, high, low, close, ohlc4, ohlc4_sma5, highsma5, lowsma5, closesma26, closesma5, 
                highsma5_off3, lowsma5_off3, KST, KST26, BuyCall, BuyPut, ATR, VStop2, VStop3, TrendUp2, TrendUp3, Max, Min
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(replace_query, non_zero_data)
                print(f"Inserted {len(non_zero_data)} rows into the database")

    async def get_signal(self):
        pool = await self.get_mysql_pool()  # Await the pool creation
        try:
            # await self.create_tables_if_not_exists(pool)
            ohlc_data = await self.fetch_ohlctick_1mdata(pool)
            if len(ohlc_data) < 252:
                print("Not enough data to calculate indicators")
                return

            # ohlc_data['ohlc4'] = ohlc_data[['open', 'high', 'low', 'close']].mean(axis=1)
            indicator_data = await self.calculate_additional_indicators(ohlc_data)
            indicator_data = await self.calculate_vstop(indicator_data)
            await self.save_indicators_to_db(pool, indicator_data.to_numpy())
        finally:
            pool.close()  # Close the pool after usage
            await pool.wait_closed()  # Wait until the pool is fully closed

    async def main(self):
        while True:
            if self.is_market_open() and self.is_business_day(datetime.now(IST)):
                await self.get_signal()
                current_time = datetime.now(IST)
                period_now = pd.Period.now('1min')
                period_now_start_time = period_now.start_time.replace(
                    tzinfo=IST)
                next_execution = (period_now_start_time +
                                  pd.Timedelta(seconds=61))
                sleep_till = (next_execution - current_time).total_seconds()
                await asyncio.sleep(sleep_till)
                # await asyncio.sleep(60)
            else:
                sleep_duration = self.get_sleep_duration()
                await asyncio.sleep(sleep_duration)

if __name__ == "__main__":
    with open(config_path, 'r') as f:
        config = json.load(f)

# if __name__ == "__main__":
#     with open('config.json') as config_file:
#         config = json.load(config_file)

    indicator_update = IndicatorUpdate(config)
    asyncio.run(indicator_update.main())
