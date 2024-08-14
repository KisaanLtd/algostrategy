import json
import asyncio
import aiomysql
import pytz
import pandas as pd
from datetime import datetime, timedelta
from breeze_connect import BreezeConnect
import threading

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
HOLIDAYS = config['holidays']

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
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS ohlctick_data (
                datetime DATETIME,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                ohlc4 FLOAT,
                PRIMARY KEY (datetime)
            )
            '''
            await cursor.execute(create_table_query)

async def insert_tick_dataframe(pool, tick_df):
    """Insert a DataFrame into the database and print a success message."""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Use an alias in the INSERT INTO statement
                insert_query = '''
                INSERT INTO ohlctick_data (datetime, open, high, low, close, ohlc4)
                VALUES (%s, %s, %s, %s, %s, %s) AS new_data
                ON DUPLICATE KEY UPDATE
                    open = new_data.open,
                    high = new_data.high,
                    low = new_data.low,
                    close = new_data.close,
                    ohlc4 = new_data.ohlc4
                '''
                # Track the number of rows inserted or updated
                row_count = 0
                for _, row in tick_df.iterrows():
                    result = await cursor.execute(insert_query, tuple(row))
                    row_count += result
                
                # Commit the transaction
                await conn.commit()

                if row_count > 0:
                    print(f"Successfully inserted {row_count} rows into the database.")
                else:
                    print("No rows were inserted or updated.")

    except Exception as e:
        print(f"Error inserting data into database: {e}")


async def on_ticks(tick):
    """Callback function to process received ticks."""
    print("Received ticks:", tick)
    try:
        tick_df = pd.DataFrame([tick])
        # Convert columns to numeric
        tick_df[['open', 'high', 'low', 'close']] = tick_df[['open', 'high', 'low', 'close']].apply(pd.to_numeric, errors='coerce')
        # Convert 'datetime' column to ISO 8601 format
        tick_df['datetime'] = pd.to_datetime(tick_df['datetime'], errors='coerce')
        tick_df['datetime'] = tick_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

        if all(col in tick_df.columns for col in ['open', 'high', 'low', 'close']):
            tick_df['ohlc4'] = (tick_df['open'] + tick_df['high'] + tick_df['low'] + tick_df['close']) / 4
            tick_df['ohlc4'] = tick_df['ohlc4'].round(2)
            selected_columns = ['datetime', 'open', 'high', 'low', 'close', 'ohlc4']
            pool = await get_mysql_pool()
            await insert_tick_dataframe(pool, tick_df[selected_columns])
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
    api.on_ticks = async_on_ticks  # Use the wrapped async function
    api.subscribe_feeds(stock_token='4.1!NIFTY BANK', interval="1minute")
    print("Subscribed to data feed")

async def disconnect_from_websocket():
    """Disconnect from the WebSocket."""
    print("Unsubscribing from data feed...")
    api.unsubscribe_feeds(stock_token="4.1!NIFTY BANK")
    disconnected = api.ws_disconnect()
    if disconnected:
        print("WebSocket disconnected")
    else:
        print("Error in disconnecting WebSocket")

def is_market_open():
    now = datetime.now(IST)
    market_open_time = now.replace(hour=9, minute=14, second=0, microsecond=0)
    market_close_time = now.replace(hour=15, minute=31, second=0, microsecond=0)
    return market_open_time <= now <= market_close_time

def is_business_day(date):
    """Check if a given date is a business day."""
    if date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False
    return date.strftime('%Y-%m-%d') not in HOLIDAYS

async def main():
    """Main function to control the flow of the program."""
    pool = await get_mysql_pool()
    await create_tables_if_not_exists(pool)

    while True:
        now = datetime.now(IST)
        if is_market_open():
            print("Market is open")
            await connect_to_websocket()
            while is_market_open():
                await asyncio.sleep(5)  # Sleep while the market is open to process ticks
            await disconnect_from_websocket()
        else:
            # Calculate the sleep duration until the next market open
            next_market_open_time = datetime.now(IST).replace(hour=9, minute=14, second=0, microsecond=0)
            if datetime.now(IST) > next_market_open_time:
                next_market_open_time += timedelta(days=1)
            # Adjust if it's a weekend or holiday
            while not is_business_day(next_market_open_time):
                next_market_open_time += timedelta(days=1)
            sleep_duration = (next_market_open_time - datetime.now(IST)).total_seconds()
            print(f"Sleeping for {sleep_duration / 3600:.2f} hours until next market open")
            await asyncio.sleep(sleep_duration)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        print(f"RuntimeError: {e}")
