import json
import asyncio
import aiomysql
import pytz
import pandas as pd
# import numpy as np
# import logging
from datetime import datetime, time, timedelta
from breeze_connect import BreezeConnect

# Define timezone
IST = pytz.timezone('Asia/Kolkata')

# Load configuration from config.json
with open('config.json', 'r') as f:
    config = json.load(f)

api_key = config['api_key']
api_secret = config['api_secret']
secret_key = config['secret_key']
api_session = config['api_session']

# Database configuration
db_config = config['db_config']

# Holidays list
HOLIDAYS = set(config['holidays'])

# Initialize BreezeConnect with credentials
api = BreezeConnect(api_key=api_key)
api.generate_session(api_secret=secret_key, session_token=api_session)

async def get_mysql_pool():
    """Create and return a connection pool to the MySQL database."""
    port = int(db_config['port'])
    pool = await aiomysql.create_pool(
        host=db_config['host'],
        port=port,
        user=db_config['user'],
        password=db_config['password'],
        db=db_config['database'],
        autocommit=True
    )
    return pool

async def create_tables_if_not_exists(pool):
    """Create the necessary tables if they do not exist."""
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

async def insert_tick_dataframe(pool, table_name, tick_df):
    """Insert a DataFrame into the database and print a success message."""
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
                        datetime_value = pd.to_datetime(datetime_value, errors='coerce')
                    await cur.execute(insert_query, (
                        datetime_value, record['open'], record['high'],
                        record['low'], record['close'], record['ohlc4']
                    ))
            
            await conn.commit()

    except Exception as e:
        print(f"Error inserting data into {table_name} table: {e}")
        raise

async def fetch_and_resample_data(pool):
    """Fetch data from ohlctick_1sdata, resample it, and insert new indices into ohlctick_1mdata."""
    try:
        # Get current time in IST
        now = datetime.now(IST)
        
        # Define market open and close times
        market_open_time = datetime.combine(now.date(), time(9, 15))
        market_close_time = datetime.combine(now.date(), time(15, 30))
    
        ## Create a period range for the market hours with 1-minute frequency
        period_index = pd.period_range(market_open_time, market_close_time, freq='1T')
        period_now=pd.Period.now('1T')
        open_time_datetime64 = pd.Period(market_open_time, '1T').start_time
        close_time_datetime64 = pd.Period(market_close_time, '1T').end_time
        period_now_datetime64 = period_now.start_time
        periods=len(period_index)
        min_datetime = min(close_time_datetime64, period_now_datetime64)

        # Fetch data from the MySQL database
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = 'SELECT * FROM ohlctick_1sdata ORDER BY datetime'
                await cur.execute(query)
                data = await cur.fetchall()
                columns = [desc[0] for desc in cur.description]  # Get column names from cursor description

                ### Convert the fetched data into a DataFrame
                ohlc_1s_df = pd.DataFrame(data, columns=columns)

                ohlc_dict = {                                                                                                             
                            'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'ohlc4': 'mean'
                    }
                
                # print(ohlc_dict)
                ### Ensure datetime is parsed correctly and set as index
                ohlc_1s_df['datetime'] = pd.to_datetime(ohlc_1s_df['datetime'], errors='coerce')
                
                ### Set datetime column as index
                ohlc_1s_df.set_index('datetime', inplace=True)
                period_now=pd.Period.now('1T')
                period_index = pd.date_range(start=open_time_datetime64, end=min_datetime, freq='S')
                periods = len(period_index)
                ohlc_1s_df = ohlc_1s_df.reindex(period_index)
                # print(ohlc_1s_df)
                ohlc_1m_df = ohlc_1s_df.resample('1T').agg(ohlc_dict).reset_index()
                # Handle NaN values: fill or drop as necessary
                ohlc_1m_df.fillna({
                    'open': 0.0, 
                    'high': 0.0, 
                    'low': 0.0, 
                    'close': 0.0, 
                    'ohlc4': 0.0
                 }, inplace=True)

                ohlc_1m_df.reset_index(inplace=False)
                ohlc_1m_df.rename(columns={'index': 'datetime'}, inplace=True)
                ohlc_1m_df['ohlc4'] = ohlc_1m_df['ohlc4'].round(2)
                ###Insert resampled data into the 1-minute table
                await insert_tick_dataframe(pool, 'ohlctick_1mdata', ohlc_1m_df)
    except Exception as e:
        logger.error(f"Error fetching and resampling data: {e}")

async def on_ticks(tick):
    """Callback function to process received ticks."""
    print("Received ticks:", tick)
    try:
        # Convert tick to a list of dictionaries if it's a single dictionary
        if isinstance(tick, dict):
            tick = [tick]

        # Create a DataFrame from tick data
        tick_df = pd.DataFrame(tick)

        # Check if 'datetime' is present and convert it to a datetime object
        if 'datetime' in tick_df.columns:
            tick_df['datetime'] = pd.to_datetime(tick_df['datetime'], errors='coerce')
        else:
            raise ValueError("'datetime' column is missing in the tick data")

        # Convert necessary columns to numeric types
        for col in ['open', 'high', 'low', 'close']:
            tick_df[col] = pd.to_numeric(tick_df[col], errors='coerce')

        # Check and calculate 'ohlc4' if necessary columns are present
        if all(col in tick_df.columns for col in ['open', 'high', 'low', 'close']):
            tick_df['ohlc4'] = (tick_df['open'] + tick_df['high'] + tick_df['low'] + tick_df['close']) / 4
            tick_df['ohlc4'] = tick_df['ohlc4'].round(2)

            # Select columns to insert
            selected_columns = ['datetime', 'open', 'high', 'low', 'close', 'ohlc4']
            tick_df = tick_df[selected_columns]

            # Establish a connection pool and insert the data
            pool = await get_mysql_pool()
            await insert_tick_dataframe(pool, 'ohlctick_1sdata', tick_df)
            pool.close()
            await pool.wait_closed()
        else:
            print("Error: Missing required columns in tick data")

    except Exception as e:
        print(f"Error processing tick data: {e}")


def async_on_ticks(tick):
    """Wrap the async on_ticks function to be used as a callback."""
    asyncio.run(on_ticks(tick))

async def connect_to_websocket():
    """Connect to the WebSocket and subscribe to feeds."""
    print("Connecting to WebSocket...")
    api.ws_connect()
    api.on_ticks = async_on_ticks
    api.subscribe_feeds(stock_token='4.1!NIFTY BANK', interval="1second")
    print("Subscribed to data feed")

async def disconnect_from_websocket():
    """Disconnect from the WebSocket."""
    print("Unsubscribing from data feed...")
    api.unsubscribe_feeds(stock_token='4.1!NIFTY BANK', interval="1second")
    disconnected = api.ws_disconnect()
    if disconnected:
        print("WebSocket disconnected")
    else:
        print("Error disconnecting WebSocket")

def is_market_open():
    """Check if the market is currently open."""
    now = datetime.now(IST)
    market_open_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open_time <= now <= market_close_time

def is_business_day(date):
    """Check if the given date is a business day."""
    return date.weekday() < 5 and date.date() not in HOLIDAYS

async def main():
    """Main function to control the flow of the program."""
    pool = await get_mysql_pool()
    await create_tables_if_not_exists(pool)

    try:
        while True:
            now = datetime.now(IST)
            if is_business_day(now) and is_market_open():
                await connect_to_websocket()
                while is_market_open():
                    await fetch_and_resample_data(pool)
                    await asyncio.sleep(60)  # Wait for 1 minute between each fetch
                await disconnect_from_websocket()
            else:
                # Wait until the next market open
                now = datetime.now(IST)
                next_market_open = datetime.combine(now.date(), time(9, 15), tzinfo=IST)
                if now.time() > time(15, 30):
                    next_market_open += timedelta(days=1)
                time_until_open = (next_market_open - now).total_seconds()
                print(f"Market closed. Sleeping for {time_until_open} seconds.")
                await asyncio.sleep(time_until_open)
    except KeyboardInterrupt:
        print("Process interrupted")
    finally:
        pool.close()
        await pool.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
