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

# Get the absolute path of the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Reference to config.json
config_path = os.path.join(project_root, 'config', 'config.json')

IST = pytz.timezone('Asia/Kolkata')

class HistoricalData:
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

        peak_sorted_df = peak_df_filtered.sort_values(by='Datetime', ascending=True)
        trough_sorted_df = trough_df_filtered.sort_values(by='Datetime', ascending=True)

        ## peak_sorted_df = peak_df.sort_values(by='Datetime', ascending=True)
        ## trough_sorted_df = trough_df.sort_values(by='Datetime', ascending=True)

        latest_peak_row = peak_sorted_df.iloc[-1] if not peak_sorted_df.empty else pd.Series()
        latest_trough_row = trough_sorted_df.iloc[-1] if not trough_sorted_df.empty else pd.Series()

        return latest_peak_row, latest_trough_row, peak_sorted_df, trough_sorted_df
    # async def KST_KST26_cross(self, data):
    #     KST_KST26_crossover = (data['KST'] > data['KST26']) & (data['KST'].shift(1) <= data['KST26'].shift(1))
    #     KST_KST26_crossunder = (data['KST'] < data['KST26']) & (data['KST'].shift(1) >= data['KST26'].shift(1))
    #     return data.loc[KST_KST26_crossover], data.loc[KST_KST26_crossunder]

    async def KST_KST26_cross(self, data):
        # Ensure 'KST' and 'KST26' columns are present
        if 'KST' not in data.columns or 'KST26' not in data.columns:
            raise ValueError("Data must contain 'KST' and 'KST26' columns")

        # Compute crossover: KST crosses above KST26
        KST_KST26_crossover = (
            (data['KST'] > data['KST26']) & 
            (data['KST'].shift(1) <= data['KST26'].shift(1))
        )
        
        # Compute crossunder: KST crosses below KST26
        KST_KST26_crossunder = (
            (data['KST'] < data['KST26']) & 
            (data['KST'].shift(1) >= data['KST26'].shift(1))
        )
        
        # Return filtered data for crossover and crossunder events
        crossover_data = data.loc[KST_KST26_crossover]
        crossunder_data = data.loc[KST_KST26_crossunder]
        
        return crossover_data, crossunder_data



    async def run(self):
        # Get the MySQL connection pool
        pool = await self.get_mysql_pool()
        
        # Fetch indicators data
        data = await self.fetch_indicators_data(pool)
        
        # Get peak and trough information
        latest_peak_row, latest_trough_row, peak_sorted_df, trough_sorted_df = await self.get_peak_trough(data)
        
        # Calculate KST and KST26 crossovers
        crossover_data, crossunder_data = await self.KST_KST26_cross(data)
        # KST_KST26_crossunder = await self.KST_KST26_cross(data)
        
        # Print KST-KST26 crossover results
        print(crossover_data)
        
        # Define the current time and period
        now = datetime.now()
        period_now = pd.Period.now('1min')
        
        # Define market open and close times
        market_open_time = datetime.combine(now.date(), time(9, 15))
        market_close_time = datetime.combine(now.date(), time(15, 30))
        
        # Convert to pandas Timestamps
        open_time_timestamp = pd.Timestamp(market_open_time)
        close_time_timestamp = pd.Timestamp(market_close_time)
        now_timestamp = pd.Timestamp.now()
        period_now_timestamp = period_now.start_time
        print(f"open_time_timestamp:{open_time_timestamp}")
        print(f"close_time_timestamp:{close_time_timestamp}")
        print(f"now_timestamp:{now_timestamp}")
        print(f"period_now_timestamp:{period_now_timestamp}")
        
        # Determine the minimum time between market close and current time
        min_datetime = min(close_time_timestamp, period_now_timestamp)
        
        # Print sorted peak DataFrame
        # print(peak_sorted_df)
        
        # Create a pandas Index from the 'datetime' column
        period_index = data['datetime']
        idx = pd.Index(period_index)
        
        # Get slice locations for filtering the DataFrame
        start_loc, end_loc = idx.slice_locs(start=open_time_timestamp, end=min_datetime)
        
        # Slice the DataFrame using the start and stop positions
        filtered_data = data.iloc[start_loc:end_loc]
        # print(filtered_data)

        
        # Close the database connection pool
        pool.close()
        await pool.wait_closed()


if __name__ == "__main__":
    with open(config_path, 'r') as f:
        config = json.load(f)

# if __name__ == "__main__":
#     with open('config.json') as config_file:
#         config = json.load(config_file)

    histdata = HistoricalData(config)
    asyncio.run(histdata.run())
