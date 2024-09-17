import os
import json
import asyncio
import aiomysql
import pytz
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
from datetime import datetime, time, timedelta
from breeze_connect import BreezeConnect
import logging

# Get the absolute path of the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Reference to config.json
config_path = os.path.join(project_root, 'config', 'config.json')
log_path = os.path.join(project_root, 'logs', 'optionbuying.log')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
IST = pytz.timezone('Asia/Kolkata')

class OptionBuying:
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
    def is_market_open(self):
            # now = pd.Timestamp.now(IST)
            now = datetime.now(IST)
            market_open_time = datetime.combine(now.date(), time(9, 15)).replace(tzinfo=IST)
            market_close_time = datetime.combine(now.date(), time(15, 30)).replace(tzinfo=IST)
            return market_open_time <= now <= market_close_time
            print(f"now:{now}")
            print(f"market_open_time:{market_open_time}")
            print(f"market_close_time:{market_close_time}")
    def is_business_day(self, date):
        is_business = date.weekday() < 5 and date.strftime(
            '%Y-%m-%d') not in self.config['holidays']
        ## print(f"Date {date.strftime('%Y-%m-%d')} is business day: {is_business}")
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
        return sleep_duration
    async def fetch_indicators_data(self, pool):
        query = f'SELECT * FROM `indicators_data` ORDER BY `datetime`'
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query)
                result = await cur.fetchall()
                data = pd.DataFrame(result)
                data['datetime'] = pd.to_datetime(data['datetime'])
                data = data.sort_values(by='datetime', ascending=True)
        return data

    async def get_sma_cross_data(self, data):
        # Ensure all required columns are present
        required_columns = [
            'KST', 'KST26', 'highsma5_off3','ohlc4_sma5','lowsma5_off3', 'TrendUp2', 'TrendUp3','BuyCall','BuyPut'
        ]
        if not all(col in data.columns for col in required_columns):
            raise ValueError("Data must contain all required columns: 'KST', 'KST26', 'ohlc4_sma5', "
                "'highsma5_off3', 'lowsma5_off3', 'TrendUp2', 'TrendUp3','BuyCall','BuyPut'"
            )

        # Calculate conditions for crossover and crossunder
        sma_crossover = (
            (data['ohlc4_sma5'] > data['highsma5_off3']) & 
            (data['ohlc4_sma5'].shift(1) <= data['highsma5_off3'].shift(1)) & (data['TrendUp2'] ==1) &
            ((data['BuyCall'] == 1) | (data['KST26'].diff() > 0))
        )
        sma_crossunder = (
            (data['ohlc4_sma5'] < data['lowsma5_off3']) & 
            (data['ohlc4_sma5'].shift(1) >= data['lowsma5_off3'].shift(1)) & (data['TrendUp2'] ==0) &
            ((data['BuyPut'] == 1) | (data['KST26'].diff() < 0))
        )
        KST_crossover = (
            (data['BuyCall'] ==1) & (data['BuyCall'].shift(1)==0) & (data['TrendUp2'] ==1) & (data['ohlc4_sma5'] > data['highsma5_off3'])
        )
        KST_crossunder = ((data['BuyPut'] ==1) & (data['BuyPut'].shift(1) == 0) & (data['TrendUp2'] ==0) & (data['ohlc4_sma5'] < data['lowsma5_off3'])
        )
        vstopcrossover = (
            (data['TrendUp2'] == 1) & (data['TrendUp3'] == 1) & (data['BuyCall'] == 1) & ((data['TrendUp2'].shift(1)==0) | (data['TrendUp3'].shift(1)==0) | (data['BuyCall'].shift() == 0))
        )
        vstopcrossunder = (
            (data['TrendUp2'] == 0) & (data['TrendUp3'] == 0) & (data['BuyPut'] == 1) & ((data['TrendUp2'].shift(1)==1) | (data['TrendUp3'].shift(1)==1) | (data['BuyPut'].shift() == 0))
        )

        # Filter data based on conditions
        sma_crossover_data = data.loc[sma_crossover]
        sma_crossunder_data = data.loc[sma_crossunder]
        smakst_crossover_data = data.loc[KST_crossover]
        smakst_crossunder_data = data.loc[KST_crossunder]
        vstopcrossover_data = data.loc[vstopcrossover]
        vstopcrossunder_data = data.loc[vstopcrossunder]

        return sma_crossover_data, sma_crossunder_data, smakst_crossover_data, smakst_crossunder_data, vstopcrossover_data, vstopcrossunder_data

    async def get_entry_trigger(self, sma_crossover_data, sma_crossunder_data, smakst_crossover_data, smakst_crossunder_data, vstop_crossover_data, vstop_crossunder_data):
        if sma_crossover_data.empty or sma_crossunder_data.empty or smakst_crossover_data.empty or smakst_crossunder_data.empty or vstop_crossover_data.empty or vstop_crossunder_data.empty:
            logging.error("One or more dataframes are empty.")
            return None, None

        sma_crossover_data_row = sma_crossover_data.iloc[-1]
        sma_crossunder_data_row = sma_crossunder_data.iloc[-1]
        smakst_crossover_data_row = smakst_crossover_data.iloc[-1]
        smakst_crossunder_data_row = smakst_crossunder_data.iloc[-1]
        vstop_crossover_data_row = vstop_crossover_data.iloc[-1]
        vstop_crossunder_data_row = vstop_crossunder_data.iloc[-1]

        # Determine the latest crossover time
        max_trigger_datetime = max(
            sma_crossover_data_row['datetime'], sma_crossunder_data_row['datetime'],
            smakst_crossover_data_row['datetime'], smakst_crossunder_data_row['datetime'],
            vstop_crossover_data_row['datetime'], vstop_crossunder_data_row['datetime']
        )

        call_entry_trigger, put_entry_trigger = None, None

        # Determine the correct entry triggers
        if max_trigger_datetime == sma_crossover_data_row['datetime']:
            call_entry_trigger = sma_crossover_data_row['ohlc4_sma5'] if sma_crossover_data_row['close'] >= sma_crossover_data_row['highsma5'] else sma_crossover_data_row['VStop2']
        if max_trigger_datetime == smakst_crossover_data_row['datetime']:
            call_entry_trigger = smakst_crossover_data_row['ohlc4_sma5'] if smakst_crossover_data_row['close'] >= smakst_crossover_data_row['highsma5'] else smakst_crossover_data_row['VStop2']
        if max_trigger_datetime == vstop_crossover_data_row['datetime']:
            call_entry_trigger = vstop_crossover_data_row['ohlc4_sma5'] if vstop_crossover_data_row['close'] >= vstop_crossover_data_row['highsma5'] else vstop_crossover_data_row['VStop2']
        if max_trigger_datetime == sma_crossunder_data_row['datetime']:
            put_entry_trigger = sma_crossunder_data_row['ohlc4_sma5'] if sma_crossunder_data_row['close'] <= sma_crossunder_data_row['lowsma5'] else sma_crossunder_data_row['VStop2']
        if max_trigger_datetime == smakst_crossunder_data_row['datetime']:
            put_entry_trigger = smakst_crossunder_data_row['ohlc4_sma5'] if smakst_crossunder_data_row['close'] <= smakst_crossunder_data_row['lowsma5'] else smakst_crossunder_data_row['VStop2']
        if max_trigger_datetime == vstop_crossunder_data_row['datetime']:
            put_entry_trigger = vstop_crossunder_data_row['ohlc4_sma5'] if vstop_crossunder_data_row['close'] <= vstop_crossunder_data_row['lowsma5'] else vstop_crossunder_data_row['VStop2']

        return call_entry_trigger, put_entry_trigger


    async def get_strike_prices(self, call_entry_trigger, put_entry_trigger):
        strike_price = None
        option_type = None

        # Calculate strike prices if input values are valid
        if call_entry_trigger:
            # Calculate the nearest higher strike price for call
            strike_price = int((call_entry_trigger - call_entry_trigger % 100) + 100)
            option_type = "call"
        
        if put_entry_trigger:
            # Calculate the nearest lower strike price for put
            strike_price = int((put_entry_trigger - put_entry_trigger % 100) - 100)
            if option_type is None:
                option_type = "put"

        return strike_price, option_type

    async def place_order(self, strike_price, option_type):
        if not strike_price or not option_type:
            logging.error("Invalid option_type or strike_price for placing order.")
            return

        transaction_type = "BUY"
        # expiry_date = self.default_expiry_date  # Default expiry date
        expiry_date = self.config['expiry_date']

        response = self.api.place_order(
            stock_code="CNXBAN",
            exchange_code="NFO",
            product="options",
            action=transaction_type,
            order_type="market",
            stoploss="",
            quantity="15",
            price="",
            validity="day",
            validity_date=datetime.now(pytz.timezone(
                'Asia/Kolkata')).strftime('%Y-%m-%d'),
            disclosed_quantity='0',
            expiry_date=expiry_date,
            right=option_type,
            strike_price=strike_price
        )
        return response

        logging.info(f"Order placed for {option_type} {strike_price} : {response}")
        logging.info(f"Order placed for {option_type} {strike_price} at price {entry_trigger_price}: {response}")

    async def run(self):
        # Get the MySQL connection pool
        pool = await self.get_mysql_pool()
        try:
            # Fetch the indicator data
            data = await self.fetch_indicators_data(pool)
            sma_crossover_data, sma_crossunder_data, smakst_crossover_data, smakst_crossunder_data, vstopcrossover_data, vstopcrossunder_data = await self.get_sma_cross_data(data)

            # Get the triggers for entry
            call_entry_trigger, put_entry_trigger = await self.get_entry_trigger(
                sma_crossover_data, sma_crossunder_data, smakst_crossover_data, smakst_crossunder_data, vstopcrossover_data, vstopcrossunder_data)

            # Calculate strike price and option type
            strike_price, option_type = await self.get_strike_prices(call_entry_trigger, put_entry_trigger)
            logging.info(f"call_entry_trigger: {call_entry_trigger}")
            logging.info(f"put_entry_trigger: {put_entry_trigger}")
            logging.info(f"strike_price: {strike_price}")
            logging.info(f"option_type: {option_type}")

            # Check the conditions to place an order
            if option_type == 'call' and strike_price is not None:
                if call_entry_trigger is not None and (call_entry_trigger >= data['low'].iloc[-1]):
                    await self.place_order(strike_price, option_type)
            elif option_type == 'put' and strike_price is not None:
                if put_entry_trigger is not None and (put_entry_trigger <= data['high'].iloc[-1]):
                    await self.place_order(strike_price, option_type)

        finally:
            pool.close()
            await pool.wait_closed()

    async def run_scheduled(self):
        while True:
            now = datetime.now(IST)
            # Check if the market is open
            if self.is_market_open() and self.is_business_day(now):
                await self.run()
                current_time = datetime.now(IST)
                
                # Calculate the next period start and add an 8-second buffer
                period_now = pd.Period.now('1min')
                next_period_start = (period_now + 1).start_time.replace(tzinfo=IST)
                next_execution_time = next_period_start + pd.Timedelta(seconds=8)

                # Calculate sleep duration until the next execution
                sleep_duration = (next_execution_time - current_time).total_seconds()
                logging.info(f"Sleeping for {sleep_duration} seconds until next execution at {next_execution_time}")
                
                if 0 < sleep_duration < 64:
                    await asyncio.sleep(sleep_duration)
            else:
                # If the market is closed, calculate sleep duration until the next market open
                logging.info(f"Market is closed. Current time: {now}")
                sleep_duration = self.get_sleep_duration()
                logging.info(f"Sleeping for {sleep_duration} seconds until market opens.")
                await asyncio.sleep(sleep_duration)

if __name__ == "__main__":
    with open(config_path, 'r') as f:
        config = json.load(f)

    optionbuying = OptionBuying(config)
    # asyncio.run(optionbuying.run())
    asyncio.run(optionbuying.run_scheduled())