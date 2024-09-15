import os
import json
import asyncio
import aiomysql
import pytz
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from scipy.signal import find_peaks
from breeze_connect import BreezeConnect
import logging

# Get the absolute path of the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Reference to config.json
config_path = os.path.join(project_root, 'config', 'config.json')
# Reference to option_buying.log
log_path = os.path.join(project_root, 'logs', 'option_buying.log')


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


class TradingBot:
    def __init__(self, config):
        self.config = config
        self.api = BreezeConnect(api_key=config['api_key'])
        self.api.generate_session(
            api_secret=config['secret_key'], session_token=config['api_session'])
        # expiry_date = self.config['expiry_date']

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
        ## print(f"Market open status: {is_open}")
        return is_open

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
        ## print(
        ##     f"Sleep duration until next market open: {sleep_duration} seconds")
        return sleep_duration

    async def fetch_indicators_data(self, pool, table_name):
        query = f'SELECT * FROM `{table_name}` ORDER BY `datetime`'
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query)
                result = await cur.fetchall()
                data = pd.DataFrame(result)
                data['datetime'] = pd.to_datetime(data['datetime'])
                data = data.sort_values(by='datetime', ascending=True)
        return data

    async def get_peak_trough(self, data):
        data['datetime'] = pd.to_datetime(data['datetime'])
        highs_array = data['highsma5'].to_numpy()
        lows_array = data['lowsma5'].to_numpy()

        peaks, properties_peaks = find_peaks(highs_array, prominence=True)
        troughs, properties_troughs = find_peaks(-lows_array, prominence=True)

        peak_df = pd.DataFrame({
            'Datetime': data['datetime'].to_numpy()[peaks],
            'PeakValue': highs_array[peaks],
            'PeakProm': properties_peaks['prominences']
        })
        trough_df = pd.DataFrame({
            'Datetime': data['datetime'].to_numpy()[troughs],
            'TroughValue': lows_array[troughs],
            'TroughProm': properties_troughs['prominences']
        })

        peak_df_filtered = peak_df[peak_df['PeakProm'] > 80]
        trough_df_filtered = trough_df[trough_df['TroughProm'] > 80]

        ##peak_sorted_df = peak_df_filtered.sort_values(by='Datetime', ascending=True)
        ##trough_sorted_df = trough_df_filtered.sort_values(by='Datetime', ascending=True)

        peak_sorted_df = peak_df.sort_values(by='Datetime', ascending=True)
        trough_sorted_df = trough_df.sort_values(by='Datetime', ascending=True)

        latest_peak_row = peak_sorted_df.iloc[-1] if not peak_sorted_df.empty else pd.Series()
        latest_trough_row = trough_sorted_df.iloc[-1] if not trough_sorted_df.empty else pd.Series()

        return latest_peak_row, latest_trough_row, peak_sorted_df, trough_sorted_df

    async def get_strike_prices(self, latest_peak_row, latest_trough_row):
        if latest_peak_row.empty or latest_trough_row.empty:
            return None, None, None, None

        latest_peak_prom = latest_peak_row['PeakProm']
        latest_peak_value = latest_peak_row['PeakValue']
        latest_peak_datetime = latest_peak_row['Datetime']
        latest_trough_prom = latest_trough_row['TroughProm']
        latest_trough_value = latest_trough_row['TroughValue']
        latest_trough_datetime = latest_trough_row['Datetime']

        strike_price_ce = int(
            (latest_trough_value - latest_trough_value % 100) + 100)
        strike_price_pe = int(
            (latest_peak_value - latest_peak_value % 100) - 100)

        max_peak_trough_datetime = max(
            latest_peak_datetime, latest_trough_datetime)
        ##peak_trough_range = (latest_peak_value - latest_trough_value).round(2)

        ##if max_peak_trough_datetime == latest_peak_datetime and peak_trough_range >= 70:
        if max_peak_trough_datetime == latest_peak_datetime:
            if latest_peak_prom > latest_trough_prom:
                ## return strike_price_pe, "put", max_peak_trough_datetime, peak_trough_range
                return strike_price_pe, "put", max_peak_trough_datetime
        ##elif max_peak_trough_datetime == latest_trough_datetime and peak_trough_range >= 70:
        elif max_peak_trough_datetime == latest_trough_datetime:
            if latest_peak_prom < latest_trough_prom:
                ## return strike_price_ce, "call", max_peak_trough_datetime, peak_trough_range
                return strike_price_ce, "call", max_peak_trough_datetime

        # return None, None, max_peak_trough_datetime, peak_trough_range
        return None, None, max_peak_trough_datetime

    async def TrendUp2_cross(self, data):
        TrendUp2crossover = (data['TrendUp2'] == 1) & (data['TrendUp2'].shift(1) == 0)
        TrendUp2crossunder = (data['TrendUp2'] == 0) & (data['TrendUp2'].shift(1) == 1)
        return data.loc[TrendUp2crossover], data.loc[TrendUp2crossunder]

    async def get_entry_trigger(self, latest_peak_row, latest_trough_row, TrendUp2crossover, TrendUp2crossunder):
        if latest_peak_row.empty or latest_trough_row.empty:
            return None, None

        latest_peak_datetime = latest_peak_row['Datetime']
        latest_trough_datetime = latest_trough_row['Datetime']
        latest_trendup2_crossover_datetime = TrendUp2crossover['datetime'].max() if not TrendUp2crossover.empty else pd.Timestamp.min
        latest_trendup2_crossunder_datetime = TrendUp2crossunder['datetime'].max() if not TrendUp2crossunder.empty else pd.Timestamp.min
        max_trendup2cross_datetime = max(latest_trendup2_crossover_datetime, latest_trendup2_crossunder_datetime)
        max_peak_trough_datetime = max(latest_peak_datetime, latest_trough_datetime)

        trendup2_crossunder_vstop2 = TrendUp2crossunder.iloc[-1]['VStop2'] if not TrendUp2crossunder.empty else float('inf')
        trendup2_crossunder_highsma5 = TrendUp2crossunder.iloc[-1]['highsma5'] if not TrendUp2crossunder.empty else float('inf')
        trendup2_crossunder_highsma5_off3 = TrendUp2crossunder.iloc[-1]['highsma5_off3'] if not TrendUp2crossunder.empty else float('inf')

        trendup2_crossover_vstop2 = TrendUp2crossover.iloc[-1]['VStop2'] if not TrendUp2crossover.empty else -float('inf')
        trendup2_crossover_lowsma5 = TrendUp2crossover.iloc[-1]['lowsma5'] if not TrendUp2crossover.empty else -float('inf')
        trendup2_crossover_lowsma5_off3 = TrendUp2crossover.iloc[-1]['lowsma5_off3'] if not TrendUp2crossover.empty else -float('inf')

        call_entry_trigger = None
        put_entry_trigger = None

        if not latest_peak_row.empty and not latest_trough_row.empty:
            if (max_peak_trough_datetime == latest_peak_datetime) and (max_trendup2cross_datetime > max_peak_trough_datetime):
                if not TrendUp2crossunder.empty and (max_trendup2cross_datetime == latest_trendup2_crossunder_datetime):
                    if trendup2_crossunder_highsma5 < trendup2_crossunder_highsma5_off3 < trendup2_crossunder_vstop2:
                        put_entry_trigger = trendup2_crossunder_highsma5_off3
                        if trendup2_crossunder_highsma5 < trendup2_crossunder_vstop2:
                            put_entry_trigger = trendup2_crossunder_highsma5
                    else:
                        put_entry_trigger = trendup2_crossunder_vstop2

            elif (max_peak_trough_datetime == latest_trough_datetime) and (max_trendup2cross_datetime > max_peak_trough_datetime):
                if not TrendUp2crossover.empty and (max_trendup2cross_datetime == latest_trendup2_crossover_datetime):
                    if trendup2_crossover_lowsma5 > trendup2_crossover_lowsma5_off3 > trendup2_crossover_vstop2:
                        call_entry_trigger = trendup2_crossover_lowsma5_off3
                        if trendup2_crossover_lowsma5 > trendup2_crossover_vstop2:
                            call_entry_trigger = trendup2_crossover_lowsma5
                    else:
                        call_entry_trigger = trendup2_crossover_vstop2

        # return call_entry_trigger, put_entry_trigger
        return call_entry_trigger, put_entry_trigger, max_trendup2cross_datetime

    async def place_order(self, option_type, strike_price, max_peak_trough_datetime, entry_trigger_price):
        if not strike_price or not option_type:
            logging.error(
                "Invalid option_type or strike_price for placing order.")
            return

        transaction_type = "BUY"  # Assuming you want to place a BUY order
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
        logging.info(
            f"Order placed for {option_type} {strike_price} at price {entry_trigger_price}: {response}")

    async def run(self):
        table_name = "indicators_data"
        pool = await self.get_mysql_pool()
        try:
            data = await self.fetch_indicators_data(pool, table_name)
            latest_peak_row, latest_trough_row, _, _ = await self.get_peak_trough(data)
            TrendUp2crossover, TrendUp2crossunder = await self.TrendUp2_cross(data)
            call_entry_trigger, put_entry_trigger, max_trendup2cross_datetime = await self.get_entry_trigger(latest_peak_row, latest_trough_row, TrendUp2crossover, TrendUp2crossunder)
            logging.info("call_entry_trigger: %s", call_entry_trigger)
            logging.info("put_entry_trigger: %s", put_entry_trigger)
            logging.info("max_trendup2cross_datetime: %s",
                         max_trendup2cross_datetime)

            # strike_price, option_type, max_peak_trough_datetime, peak_trough_range = await self.get_strike_prices(latest_peak_row, latest_trough_row)
            strike_price, option_type, max_peak_trough_datetime = await self.get_strike_prices(latest_peak_row, latest_trough_row)
            logging.info("strike_price: %s", strike_price)
            logging.info("option_type: %s", option_type)
            logging.info("max_peak_trough_datetime: %s",
                         max_peak_trough_datetime)
            # logging.info("peak_trough_range: %s", peak_trough_range)

            if option_type == 'call' and call_entry_trigger and strike_price is not None:
                if (call_entry_trigger > data['low'].iloc[-1]):
                    await self.place_order(option_type, strike_price, max_peak_trough_datetime, call_entry_trigger)
            elif option_type == 'put' and put_entry_trigger and strike_price is not None:
                if (put_entry_trigger < data['high'].iloc[-1]):
                    await self.place_order(option_type, strike_price, max_peak_trough_datetime, put_entry_trigger)
        finally:
            pool.close()
            await pool.wait_closed()

    async def run_scheduled(self):
        # ist = pytz.timezone('Asia/Kolkata')
        # start_time = time(9, 15)
        # end_time = time(15, 30)

        while True:
            now = datetime.now(IST)
            if self.is_market_open():
                if self.is_business_day(datetime.now(IST)):
                    # logging.info("Starting execution at: %s", now)
                    await self.run()
                    # logging.info("Execution completed at: %s", now)
                else:
                    print("Today is not a business day: %s", now)

                current_time = datetime.now(IST)
                period_now = pd.Period.now('1min')
                # period_now_start_time = period_now.start_time.replace(tzinfo=IST)
                next_period_start = (period_now + 1).start_time.replace(tzinfo=IST)
                next_execution = (next_period_start +  pd.Timedelta(seconds=8))
                sleep_till = (next_execution - current_time).total_seconds()
                if sleep_till > 0 and sleep_till < 64:
                    await asyncio.sleep(sleep_till)
                # next_run = (now + timedelta(minutes=1)).replace(second=10, microsecond=0)
                # sleep_duration = (next_run - now).total_seconds()
                # await asyncio.sleep(sleep_duration)
            else:
                print("Outside trading hours: %s", now)
                sleep_duration = self.get_sleep_duration()
                await asyncio.sleep(sleep_duration)
                # next_run = (now + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
                # sleep_duration = (next_run - now).total_seconds()
                # await asyncio.sleep(sleep_duration)

    # def is_business_day(self, date):
    #     return np.is_busday(date.date())  # Simple business day check


if __name__ == "__main__":
    with open(config_path, 'r') as f:
        config = json.load(f)

    bot = TradingBot(config)
    asyncio.run(bot.run_scheduled())
