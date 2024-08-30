import json
import asyncio
import aiomysql
import pytz
import pandas as pd
import talib
import numpy as np
from datetime import datetime, timedelta
import logging

# Define timezone
IST = pytz.timezone('Asia/Kolkata')

# Configure logging
logging.basicConfig(
    filename='trading_script.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load configuration from config.json
with open('config.json', 'r') as f:
    config = json.load(f)

# Database configuration
db_config = config['db_config']
# Convert holidays to a set for faster lookup
HOLIDAYS = set(config['holidays'])

async def get_mysql_pool():
    """Create and return a connection pool to the MySQL database using aiomysql."""
    pool = await aiomysql.create_pool(
        user=db_config['user'],
        db=db_config['database'],
        host=db_config['host'],
        port=int(db_config['port']),
        password=db_config['password'],
        autocommit=True,
        minsize=5,
        maxsize=20
    )
    logging.debug("MySQL connection pool created")
    return pool

async def get_latest_timestamp(pool, table_name):
    """Retrieve the latest timestamp from the specified table."""
    query = f"SELECT MAX(datetime) AS latest_timestamp FROM {table_name}"
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query)
            row = await cur.fetchone()
            latest_timestamp = row[0] if row else None
            logging.debug(f"Latest timestamp from {table_name}: {latest_timestamp}")
            return latest_timestamp

def is_market_open():
    """Check if the current time is within market hours."""
    current_time = datetime.now(IST).time()
    open_time = datetime.strptime('09:15', '%H:%M').time()
    close_time = datetime.strptime('15:30', '%H:%M').time()
    is_open = open_time <= current_time < close_time
    logging.debug(f"Market open status: {is_open}")
    return is_open

def is_business_day(date):
    is_business = date.weekday() < 5 and date.strftime('%Y-%m-%d') not in HOLIDAYS
    logging.debug(f"Date {date.strftime('%Y-%m-%d')} is business day: {is_business}")
    return is_business

def get_sleep_duration():
    """Calculate the sleep duration until the next market open."""
    current_datetime = datetime.now(IST)
    current_time = current_datetime.time()

    # Set the next market open time at 09:15 AM
    next_market_open_datetime = current_datetime.replace(hour=9, minute=15, second=0, microsecond=0)

    # If the market is already closed today, calculate the next market open datetime
    if current_time >= next_market_open_datetime.time():
        next_market_open_datetime += timedelta(days=1)
    
    # Skip to the next business day if necessary
    while not is_business_day(next_market_open_datetime):
        next_market_open_datetime += timedelta(days=1)

    # Calculate the difference in seconds
    sleep_duration = (next_market_open_datetime - current_datetime).total_seconds()
    logging.debug(f"Sleep duration until next market open: {sleep_duration} seconds")
    return sleep_duration

async def create_tables_if_not_exists(pool):
    """Create the necessary tables if they do not exist."""
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
            ATR DOUBLE,
            VStop2 DOUBLE,
            VStop3 DOUBLE,
            `Call` INTEGER,
            `Put` INTEGER,
            BuyCall INTEGER,
            BuyPut INTEGER,
            TrendUp2 INTEGER,
            TrendUp3 INTEGER,
            Max DOUBLE,
            Min DOUBLE
        )
    '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(create_table_query)
            logging.debug("Tables created if not exist")

async def fetch_ohlctick_1mdata(pool):
    """Fetch at least 260 rows of data from ohlctick_1mdata table."""
    query = "SELECT * FROM ohlctick_1mdata ORDER BY datetime"
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query)
            result = await cur.fetchall()
            columns = [col[0] for col in cur.description]
            data = pd.DataFrame(result, columns=columns)
            # Sort data in ascending order
            data.sort_values(by='datetime', inplace=True)
            logging.debug(f"Fetched OHLC data with {len(data)} rows")
            return data

def calculate_additional_indicators(data):
    data['ohlc4_sma5'] = talib.SMA(data['ohlc4'], timeperiod=5)
    data['highsma5'] = talib.SMA(data['high'], timeperiod=5)
    data['lowsma5'] = talib.SMA(data['low'], timeperiod=5)
    data['closesma26'] = talib.SMA(data['close'], timeperiod=26)
    data['closesma5'] = talib.SMA(data['close'], timeperiod=5)
    data['highsma5_off3'] = data['highsma5'].shift(3)
    data['lowsma5_off3'] = data['lowsma5'].shift(3)
    data['Call'] = (data['closesma5'] > data['ohlc4_sma5']).astype(int)
    data['Put'] = (data['closesma5'] < data['ohlc4_sma5']).astype(int)
    data['BuyCall'] = (data['ohlc4_sma5'] > data['highsma5_off3']).astype(int)
    data['BuyPut'] = (data['ohlc4_sma5'] < data['lowsma5_off3']).astype(int)
    
    # Round the output indicators to two decimal places
    columns_to_round = ['ohlc4_sma5', 'highsma5', 'lowsma5', 'closesma26', 'closesma5', 'highsma5_off3', 'lowsma5_off3']
    data[columns_to_round] = data[columns_to_round].round(2)
    logging.debug("Calculated additional indicators")
    return data

def calculate_vstop(data):
    """Calculate VStop indicators from data."""
    # data['ATR'] = ta.atr(data['high'], data['low'], data['close'], timeperiod=252)
    data['ATR'] = talib.ATR(data['high'], data['low'], data['close'], timeperiod=252)
    
    # Initialize columns
    data['VStop2'] = np.nan
    data['VStop3'] = np.nan
    data['TrendUp2'] = True
    data['TrendUp3'] = True
    data['Max'] = data['close']
    data['Min'] = data['close']

    # Calculate VStop2
    for i in range(252, len(data)):
        src = data['close'].iloc[i]
        atr_m2 = data['ATR'].iloc[i] * 2
        
        # Update max and min values
        data.at[i, 'Max'] = max(data['Max'].iloc[i-1], src)
        data.at[i, 'Min'] = min(data['Min'].iloc[i-1], src)
        
        if data['TrendUp2'].iloc[i-1]:
            data.at[i, 'VStop2'] = max(
                data['VStop2'].iloc[i-1] if not np.isnan(data['VStop2'].iloc[i-1]) else src, 
                data['Max'].iloc[i] - atr_m2
            )
        else:
            data.at[i, 'VStop2'] = min(
                data['VStop2'].iloc[i-1] if not np.isnan(data['VStop2'].iloc[i-1]) else src, 
                data['Min'].iloc[i] + atr_m2
            )
        
        # Determine trend change
        data.at[i, 'TrendUp2'] = src >= data['VStop2'].iloc[i]
        
        if data['TrendUp2'].iloc[i] != data['TrendUp2'].iloc[i-1]:
            data.at[i, 'Max'] = src
            data.at[i, 'Min'] = src
            data.at[i, 'VStop2'] = data['Max'].iloc[i] - atr_m2 if data['TrendUp2'].iloc[i] else data['Min'].iloc[i] + atr_m2
    
    # Reset Max and Min values for second calculation
    data['Max'] = data['close']
    data['Min'] = data['close']
    
    # Calculate VStop3
    for i in range(252, len(data)):
        src = data['close'].iloc[i]
        atr_m3 = data['ATR'].iloc[i] * 3
        
        # Update max and min values
        data.at[i, 'Max'] = max(data['Max'].iloc[i-1], src)
        data.at[i, 'Min'] = min(data['Min'].iloc[i-1], src)
        
        if data['TrendUp3'].iloc[i-1]:
            data.at[i, 'VStop3'] = max(
                data['VStop3'].iloc[i-1] if not np.isnan(data['VStop3'].iloc[i-1]) else src, 
                data['Max'].iloc[i] - atr_m3
            )
        else:
            data.at[i, 'VStop3'] = min(
                data['VStop3'].iloc[i-1] if not np.isnan(data['VStop3'].iloc[i-1]) else src, 
                data['Min'].iloc[i] + atr_m3
            )
        
        # Determine trend change
        data.at[i, 'TrendUp3'] = src >= data['VStop3'].iloc[i]
        
        if data['TrendUp3'].iloc[i] != data['TrendUp3'].iloc[i-1]:
            data.at[i, 'Max'] = src
            data.at[i, 'Min'] = src
            data.at[i, 'VStop3'] = data['Max'].iloc[i] - atr_m3 if data['TrendUp3'].iloc[i] else data['Min'].iloc[i] + atr_m3

    # Round the output indicators to two decimal places
    columns_to_round = ['ATR', 'VStop2', 'VStop3']
    data[columns_to_round] = data[columns_to_round].round(2)

    return data

async def save_indicators_to_db(pool, data):
    """Insert or update the latest five non-zero rows of calculated indicator values into the database."""
    # Replace NaN values with None, which translates to NULL in MySQL
    data = [[None if pd.isna(x) else x for x in row] for row in data]

    # Filter out rows where all indicator values are zero
    non_zero_data = [row for row in data if any(x not in (0, None) for x in row[1:])]  # Skip datetime column (index 0)

    # Keep only the latest five rows
    non_zero_data = non_zero_data[-5:]

    query = '''
        REPLACE INTO indicators_data 
        (datetime, open, high, low, close, ohlc4, ohlc4_sma5, highsma5, lowsma5, closesma26, closesma5, highsma5_off3, lowsma5_off3, ATR, VStop2, VStop3, `Call`, `Put`, BuyCall, BuyPut, TrendUp2, TrendUp3, `Max`, `Min`) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(query, non_zero_data)
            logging.debug("Latest five non-zero indicator data rows inserted/updated into the database")


async def get_signal():
    """Main function to calculate indicators and save them to the database."""
    pool = await get_mysql_pool()
    await create_tables_if_not_exists(pool)
    
    ohlc_data = await fetch_ohlctick_1mdata(pool)
    # if len(ohlc_data) >= 260:
        # ohlc_data['ohlc4'] = (ohlc_data['open'] + ohlc_data['high'] + ohlc_data['low'] + ohlc_data['close']) / 4
    calculated_data = calculate_additional_indicators(ohlc_data)
    calculated_data = calculate_vstop(calculated_data)
    
    # Prepare data for insertion
    data_to_insert = calculated_data[['datetime', 'open', 'high', 'low', 'close', 'ohlc4', 'ohlc4_sma5', 'highsma5', 'lowsma5', 'closesma26', 'closesma5', 'highsma5_off3', 'lowsma5_off3', 'ATR', 'VStop2', 'VStop3', 'Call', 'Put', 'BuyCall', 'BuyPut', 'TrendUp2', 'TrendUp3', 'Max', 'Min']].values.tolist()
    
    await save_indicators_to_db(pool, data_to_insert)
    # print(data_to_insert[-1:])
async def main():
    while True:
        if is_market_open() and is_business_day(datetime.now(IST)):
            await get_signal()
            current_time = pd.Timestamp.now()
            next_minute_end_time = pd.Period.now(freq='1T').end_time
            sleep_time = (next_minute_end_time - current_time).total_seconds()
        await asyncio.sleep(sleep_time)
    else:
        sleep_duration = get_sleep_duration()
    await asyncio.sleep(sleep_duration)  # Sleep until the next market open

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
