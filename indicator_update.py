import json
import asyncio
import aiomysql
import pytz
import pandas as pd
import talib
import numpy as np
from datetime import datetime, timedelta
import time
import logging


# Define timezone
IST = pytz.timezone('Asia/Kolkata')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("retrivedata.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load configuration from config.json
with open('config.json', 'r') as f:
    config = json.load(f)

api_key = config['api_key']
api_secret = config['api_secret']
secret_key = config['secret_key']
api_session = config['api_session']

# Database configuration
db_config = config['db_config']
# Convert holidays to a set for faster lookup
HOLIDAYS = set(config['holidays'])

async def get_mysql_pool():
    """Create and return a connection pool to the MySQL database."""
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


async def get_latest_timestamp(pool, table_name):
    """Retrieve the latest timestamp from the specified table."""
    query = f"SELECT MAX(datetime) AS latest_timestamp FROM {table_name}"
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(query)
            result = await cursor.fetchone()
            # latest_timestamp = result['latest_timestamp']
            return result['latest_timestamp']


def is_market_open():
    """Check if the current time is within market hours."""
    current_time = datetime.now(IST).time()
    return datetime.strptime('09:15', '%H:%M').time() <= current_time < datetime.strptime('15:30', '%H:%M').time()


def is_business_day(date):
    """Check if a given date is a business day."""
    return date.weekday() < 5 and date.strftime('%Y-%m-%d') not in HOLIDAYS


def get_sleep_duration():
    """Calculate the sleep duration until the next market open."""
    # now = datetime.now(IST)
    current_time = datetime.now(IST).time()
    next_market_open_time = current_time.replace(
        hour=9, minute=14, second=0, microsecond=0)
    if current_time > next_market_open_time:
        next_market_open_time += timedelta(days=1)
        while not is_business_day(next_market_open_time):
            next_market_open_time += timedelta(days=1)
    sleep_duration = (next_market_open_time - current_time).total_seconds()
    hours, remainder = divmod(sleep_duration, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(
        f"Market is closed. Sleeping for {int(hours)}h {int(minutes)}m {int(seconds)}s.")
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
                closesma9 DOUBLE,
                highsma5_off3 DOUBLE,
                lowsma5_off3 DOUBLE,
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
        async with conn.cursor() as cursor:
            await cursor.execute(create_table_query)


async def fetch_ohlctick_data(pool):
    """Fetch at least 260 rows of data from ohlctick_data table."""
    query = "SELECT * FROM ohlctick_data ORDER BY datetime DESC LIMIT 260;"
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query)
            data = pd.DataFrame(await cur.fetchall())
            # Sort data in ascending order
            data.sort_values(by='datetime', inplace=True)
            return data

def calculate_additional_indicators(data):
    """Calculate additional indicators and add them to the DataFrame."""
    data['ohlc4_sma5'] = talib.SMA(data['ohlc4'], timeperiod=5).round(2)
    data['highsma5'] = talib.SMA(data['high'], timeperiod=5).round(2)
    data['lowsma5'] = talib.SMA(data['low'], timeperiod=5).round(2)
    data['closesma26'] = talib.SMA(data['close'], timeperiod=26).round(2)
    data['closesma9'] = talib.SMA(data['close'], timeperiod=9).round(2)
    data['highsma5_off3'] = data['highsma5'].shift(3).round(2)
    data['lowsma5_off3'] = data['lowsma5'].shift(3).round(2)
    return data

def calculate_vstop(data):
    """Calculate VStop indicators from data."""
    # Calculate ATR using TA-Lib
    data['ATR'] = talib.ATR(data['high'], data['low'], data['close'], timeperiod=252).round(2)
    
    # Initialize columns
    data['VStop2'] = np.nan
    data['VStop3'] = np.nan
    data['TrendUp2'] = 1  # Start with trend up (1)
    data['TrendUp3'] = 1  # Start with trend up (1)
    data['Max'] = data['close']
    data['Min'] = data['close']
    # data['RangeDiff']=data['Max']-data['Min']

    # Calculate VStop2
    for i in range(252, len(data)):
        src = data['close'].iloc[i]
        atr_m2 = data['ATR'].iloc[i] * 2
        
        # Update max and min values
        data.at[i, 'Max'] = max(data['Max'].iloc[i-1], src)
        data.at[i, 'Min'] = min(data['Min'].iloc[i-1], src)
        
        if data['TrendUp2'].iloc[i-1] == 1:
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
        data.at[i, 'TrendUp2'] = 1 if src >= data['VStop2'].iloc[i] else 0
        
        if data['TrendUp2'].iloc[i] != data['TrendUp2'].iloc[i-1]:
            data.at[i, 'Max'] = src
            data.at[i, 'Min'] = src
            data.at[i, 'VStop2'] = data['Max'].iloc[i] - atr_m2 if data['TrendUp2'].iloc[i] == 1 else data['Min'].iloc[i] + atr_m2
    
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
        
        if data['TrendUp3'].iloc[i-1] == 1:
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
        data.at[i, 'TrendUp3'] = 1 if src >= data['VStop3'].iloc[i] else 0
        
        if data['TrendUp3'].iloc[i] != data['TrendUp3'].iloc[i-1]:
            data.at[i, 'Max'] = src
            data.at[i, 'Min'] = src
            data.at[i, 'VStop3'] = data['Max'].iloc[i] - atr_m3 if data['TrendUp3'].iloc[i] == 1 else data['Min'].iloc[i] + atr_m3

    # Round the output indicators to two decimal places
    columns_to_round = ['ATR', 'VStop2', 'VStop3']
    data[columns_to_round] = data[columns_to_round].round(2)

    return data

async def insert_latest_data(pool, data):
    """Insert only the latest row of data into the database."""
    latest_row = data.iloc[-1]

    latest_row = latest_row.where(pd.notnull(latest_row), None)

    insert_query = """
    INSERT INTO indicators_data (datetime, open, high, low, close, ohlc4, ohlc4_sma5, highsma5, lowsma5, closesma26, closesma9, highsma5_off3, lowsma5_off3, ATR, VStop2, VStop3, TrendUp2, TrendUp3, Max, Min)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    AS new
    ON DUPLICATE KEY UPDATE
                    open=new.open, high=new.high, low=new.low, close=new.close, 
                    ohlc4=new.ohlc4, ohlc4_sma5=new.ohlc4_sma5, highsma5=new.highsma5, lowsma5=new.lowsma5, 
                    closesma26=new.closesma26, closesma9=new.closesma9, highsma5_off3=new.highsma5_off3, 
                    lowsma5_off3=new.lowsma5_off3, ATR=new.ATR, VStop2=new.VStop2, VStop3=new.VStop3, 
                    TrendUp2=new.TrendUp2, TrendUp3=new.TrendUp3, Max=new.Max, Min=new.Min
    """

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(insert_query, (
                latest_row['datetime'], latest_row['open'], latest_row['high'], latest_row['low'],
                latest_row['close'], latest_row['ohlc4'], latest_row['ohlc4_sma5'], latest_row['highsma5'],
                latest_row['lowsma5'], latest_row['closesma26'], latest_row['closesma9'], latest_row['highsma5_off3'],
                latest_row['lowsma5_off3'], latest_row['ATR'], latest_row['VStop2'], latest_row['VStop3'],
                latest_row['TrendUp2'], latest_row['TrendUp3'], latest_row['Max'], latest_row['Min']
            ))
            logger.info(f"Inserted/Updated data for {latest_row['datetime']}")

async def main():
    pool = await get_mysql_pool()
    try:
        while True:
            if is_market_open() and is_business_day(datetime.now(IST).date()):
                logger.info("Market is open. Fetching data...")
                data = await fetch_ohlctick_data(pool)
                latest_timestamp_ohlctick = await get_latest_timestamp(pool, 'ohlctick_data')
                latest_timestamp_indicators = await get_latest_timestamp(pool, 'indicators_data')
                if len(data) >= 260:
                    # data['ohlc4'] = data[['open', 'high', 'low', 'close']].mean(axis=1).round(2)
                    data = calculate_additional_indicators(data)
                    data = calculate_vstop(data)
                    await insert_latest_data(pool, data)
                    #await insert_latest_data(pool, data[data['datetime'] == latest_timestamp_ohlctick])
                else:
                    logger.warning("Insufficient data to calculate indicators.")
                await asyncio.sleep(60)  # Wait 60 seconds before fetching data again
            else:
                sleep_duration = get_sleep_duration()
                logger.info(f"Market is closed. Sleeping for {sleep_duration/3600:.2f} hours.")
                await asyncio.sleep(sleep_duration)  # Sleep until the next market open
    finally:
        pool.close()
        await pool.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
