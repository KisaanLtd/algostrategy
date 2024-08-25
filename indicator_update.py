import json
import asyncio
import aiomysql
import pytz
import pandas as pd
import talib
import numpy as np
from datetime import datetime, timedelta
import sqlalchemy as sa
from aiomysql.sa import create_engine

# Define timezone
IST = pytz.timezone('Asia/Kolkata')

# Load configuration from config.json
with open('config.json', 'r') as f:
    config = json.load(f)

# Database configuration
db_config = config['db_config']
# Convert holidays to a set for faster lookup
HOLIDAYS = set(config['holidays'])

async def get_mysql_pool():
    """Create and return a connection pool to the MySQL database using aiomysql.sa."""
    port = int(db_config['port'])
    engine = await create_engine(
        user=db_config['user'],
        db=db_config['database'],
        host=db_config['host'],
        port=port,
        password=db_config['password'],
        autocommit=True,
        minsize=5,
        maxsize=20
    )
    return engine

async def get_latest_timestamp(engine, table_name):
    """Retrieve the latest timestamp from the specified table."""
    query = sa.text(f"SELECT MAX(datetime) AS latest_timestamp FROM {table_name}")
    async with engine.acquire() as conn:
        result = await conn.execute(query)
        row = await result.fetchone()
        return row['latest_timestamp']

def is_market_open():
    """Check if the current time is within market hours."""
    current_time = datetime.now(IST).time()
    return datetime.strptime('09:15', '%H:%M').time() <= current_time < datetime.strptime('15:30', '%H:%M').time()

def is_business_day(date):
    """Check if a given date is a business day."""
    return date.weekday() < 5 and date.strftime('%Y-%m-%d') not in HOLIDAYS

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
    return sleep_duration

async def create_tables_if_not_exists(engine):
    """Create the necessary tables if they do not exist."""
    create_table_query = sa.text('''
        CREATE TABLE IF NOT EXISTS indicators_data (
            datetime DATETIME,
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
            BuyCall INTEGER,
            BuyPut INTEGER,
            TrendUp2 INTEGER,
            TrendUp3 INTEGER,
            Max DOUBLE,
            Min DOUBLE,
            PRIMARY KEY (datetime)
        )
    ''')
    async with engine.acquire() as conn:
        await conn.execute(create_table_query)

async def fetch_ohlctick_data(engine):
    """Fetch at least 260 rows of data from ohlctick_data table."""
    query = sa.text("SELECT * FROM ohlctick_data ORDER BY datetime")
    async with engine.acquire() as conn:
        result = await conn.execute(query)
        data = pd.DataFrame(await result.fetchall(), columns=result.keys())
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
    data['BuyCall'] = (data['ohlc4_sma5'] > data['highsma5_off3']).astype(int)
    data['BuyPut'] = (data['ohlc4_sma5'] < data['lowsma5_off3']).astype(int)
    return data

def calculate_vstop(data):
    """Calculate VStop indicators from data."""
    data['ATR'] = talib.ATR(data['high'], data['low'], data['close'], timeperiod=252).round(2)
    
    data['VStop2'] = np.nan
    data['VStop3'] = np.nan
    data['TrendUp2'] = 1
    data['TrendUp3'] = 1
    data['Max'] = data['close']
    data['Min'] = data['close']

    for i in range(252, len(data)):
        src = data['close'].iloc[i]
        atr_m2 = data['ATR'].iloc[i] * 2
        data.at[i, 'Max'] = max(data['Max'].iloc[i-1], src)
        data.at[i, 'Min'] = min(data['Min'].iloc[i-1], src)
        
        if data['TrendUp2'].iloc[i-1] == 1:
            vstop2 = max(data['VStop2'].iloc[i-1] if not np.isnan(data['VStop2'].iloc[i-1]) else src, 
                         data['Max'].iloc[i] - atr_m2)
        else:
            vstop2 = min(data['VStop2'].iloc[i-1] if not np.isnan(data['VStop2'].iloc[i-1]) else src, 
                         data['Min'].iloc[i] + atr_m2)
        data.at[i, 'VStop2'] = vstop2
        trend_up2 = 1 if src >= vstop2 else 0
        data.at[i, 'TrendUp2'] = trend_up2
        
        if trend_up2 != data['TrendUp2'].iloc[i-1]:
            data.at[i, 'Max'] = src
            data.at[i, 'Min'] = src
            data.at[i, 'VStop2'] = data['Max'].iloc[i] - atr_m2 if trend_up2 == 1 else data['Min'].iloc[i] + atr_m2
    
    data['Max'] = data['close']
    data['Min'] = data['close']
    
    for i in range(252, len(data)):
        src = data['close'].iloc[i]
        atr_m3 = data['ATR'].iloc[i] * 3
        data.at[i, 'Max'] = max(data['Max'].iloc[i-1], src)
        data.at[i, 'Min'] = min(data['Min'].iloc[i-1], src)
        
        if data['TrendUp3'].iloc[i-1] == 1:
            vstop3 = max(data['VStop3'].iloc[i-1] if not np.isnan(data['VStop3'].iloc[i-1]) else src, 
                         data['Max'].iloc[i] - atr_m3)
        else:
            vstop3 = min(data['VStop3'].iloc[i-1] if not np.isnan(data['VStop3'].iloc[i-1]) else src, 
                         data['Min'].iloc[i] + atr_m3)
        data.at[i, 'VStop3'] = vstop3
        trend_up3 = 1 if src >= vstop3 else 0
        data.at[i, 'TrendUp3'] = trend_up3
        
        if trend_up3 != data['TrendUp3'].iloc[i-1]:
            data.at[i, 'Max'] = src
            data.at[i, 'Min'] = src
            data.at[i, 'VStop3'] = data['Max'].iloc[i] - atr_m3 if trend_up3 == 1 else data['Min'].iloc[i] + atr_m3
    
    columns_to_round = ['ATR', 'VStop2', 'VStop3']
    data[columns_to_round] = data[columns_to_round].round(2)

    return data

async def insert_all_data(engine, data):
    """Insert the latest row of data into the database or update if it already exists."""
    table_name = "indicators_data"
    latest_data = data.iloc[-1].to_dict()

    # Define the replace query
    query = sa.text(f"""
    REPLACE INTO {table_name} (
        datetime, open, high, low, close, ohlc4, ohlc4_sma5, highsma5, lowsma5, 
        closesma26, closesma9, highsma5_off3, lowsma5_off3, ATR, VStop2, VStop3, 
        BuyCall, BuyPut, TrendUp2, TrendUp3, Max, Min
    ) VALUES (
        :datetime, :open, :high, :low, :close, :ohlc4, :ohlc4_sma5, :highsma5, 
        :lowsma5, :closesma26, :closesma9, :highsma5_off3, :lowsma5_off3, :ATR, 
        :VStop2, :VStop3, :BuyCall, :BuyPut, :TrendUp2, :TrendUp3, :Max, :Min
    )
    """)

    async with engine.acquire() as conn:
        try:
            await conn.execute(query, latest_data)
        except Exception as e:
            # Handle the error or raise an alert
            pass

async def main():
    engine = await get_mysql_pool()
    # await create_tables_if_not_exists(engine)

    while True:
        latest_timestamp_ohlctick = await get_latest_timestamp(engine, 'ohlctick_data')
        latest_timestamp_indicators = await get_latest_timestamp(engine, 'indicators_data')

        if latest_timestamp_ohlctick and latest_timestamp_indicators and latest_timestamp_ohlctick > latest_timestamp_indicators:
            ohlctick_data = await fetch_ohlctick_data(engine)
            indicators_data = calculate_additional_indicators(ohlctick_data)
            indicators_data = calculate_vstop(indicators_data)
            await insert_all_data(engine, indicators_data)
        
        sleep_duration = 60  # Fetch data every minute
        await asyncio.sleep(sleep_duration)

if __name__ == '__main__':
    asyncio.run(main())
