import json
import pytz
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from breeze_connect import BreezeConnect
import time

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

def get_mysql_connection():
    """Establish and return a connection to the MySQL database."""
    return mysql.connector.connect(**db_config)

def get_sqlalchemy_engine():
    """Create and return an SQLAlchemy engine."""
    from urllib.parse import quote_plus
    user = quote_plus(db_config['user'])
    password = quote_plus(db_config['password'])
    host = db_config['host']
    database = db_config['database']
    
    connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
    return create_engine(connection_string)

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

def create_tables_if_not_exists(conn):
    """Create the necessary tables if they do not exist."""
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
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()

def insert_tick_dataframe(tick_df):
    """Insert a DataFrame into the database."""
    #print("Start inserting tick DataFrame")
    engine = get_sqlalchemy_engine()
    
    try:
        tick_df.to_sql('ohlctick_data', engine, if_exists='append', index=False, method='multi')
        print("Inserted DataFrame into database")
    except Exception as e:
        print(f"Error inserting DataFrame into database: {e}")

def on_ticks(tick):
    """Callback function to process received ticks."""
    #print("Received ticks:", tick)
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
            insert_tick_dataframe(tick_df[selected_columns])
        else:
            print("Error: Missing required columns in tick data")
    except Exception as e:
        print(f"Error processing tick data: {e}")

def connect_to_websocket():
    """Connect to the WebSocket and subscribe to feeds."""
    print("Connecting to WebSocket...")
    api.ws_connect()
    api.on_ticks = on_ticks
    api.subscribe_feeds(stock_token='4.1!NIFTY BANK', interval="1minute")
    print("Subscribed to data feed")

def disconnect_from_websocket():
    """Disconnect from the WebSocket."""
    print("Unsubscribing from data feed...")
    api.unsubscribe_feeds(stock_token="4.1!NIFTY BANK")
    disconnected = api.ws_disconnect()
    if disconnected:
        print("WebSocket disconnected")
    else:
        print("Error in disconnecting WebSocket")

def main():
    """Main function to control the flow of the program."""
    while True:
        now = datetime.now(IST)
        if is_market_open():
            print("Market is open")
            connect_to_websocket()
            while is_market_open():
                time.sleep(60)  # Sleep while the market is open to process ticks
            disconnect_from_websocket()
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
            time.sleep(sleep_duration)

if __name__ == "__main__":
    main()
