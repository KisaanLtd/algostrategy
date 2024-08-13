import json
import pandas as pd
import numpy as np
import pandas_ta as ta
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Float, DateTime, text
from sqlalchemy.orm import declarative_base, sessionmaker
import pytz
import time

# Load configuration from config.json
def load_config():
    """Load configuration from config.json."""
    with open('config.json', 'r') as file:
        return json.load(file)

config = load_config()

# Extract configuration details
db_config = config['db_config']
tv_username = config['tvdatafeed']['username']
tv_password = config['tvdatafeed']['password']
# HOLIDAYS = config.get('holidays', [])  # Load holidays from config

# Holidays list
HOLIDAYS = config['holidays']


# Define the timezone for IST
IST = pytz.timezone('Asia/Kolkata')

def is_market_open():
    """Check if the market is currently open."""
    now = datetime.now(IST)
    market_open_time = now.replace(hour=9, minute=14, second=0, microsecond=0)
    market_close_time = now.replace(hour=15, minute=31, second=0, microsecond=0)
    return market_open_time <= now <= market_close_time

def is_business_day(date):
    """Check if a given date is a business day."""
    if date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False
    return date.strftime('%Y-%m-%d') not in HOLIDAYS

def get_sqlalchemy_engine():
    """Create and return an SQLAlchemy engine."""
    from urllib.parse import quote_plus
    user = quote_plus(db_config['user'])
    password = quote_plus(db_config['password'])
    host = db_config['host']
    database = db_config['database']

    connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
    return create_engine(connection_string)

# Define SQLAlchemy base and models
Base = declarative_base()

class OHLCData(Base):
    __tablename__ = 'ohlctick_data'
    datetime = Column(DateTime, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    ohlc4 = Column(Float)

class IndicatorsData(Base):
    __tablename__ = 'indicators_data'
    datetime = Column(DateTime, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    ohlc4 = Column(Float)
    ohlc4_sma5 = Column(Float)
    highsma5 = Column(Float)
    lowsma5 = Column(Float)
    closesma26 = Column(Float)
    closesma52 = Column(Float)
    highsma5_off3 = Column(Float)
    lowsma5_off3 = Column(Float)
    ATR = Column(Float)
    VStop2 = Column(Float)
    VStop3 = Column(Float)
    BuyCall = Column(Float)
    BuyPut = Column(Float)

def calculate_additional_indicators(data):
    """Calculate additional indicators and add them to the DataFrame."""
    data['ohlc4'] = (data['high'] + data['low'] + data['close']) / 3
    data['ohlc4_sma5'] = ta.sma(data['ohlc4'], length=5)
    data['highsma5'] = ta.sma(data['high'], length=5)
    data['lowsma5'] = ta.sma(data['low'], length=5)
    data['closesma26'] = ta.sma(data['close'], length=26)
    data['closesma9'] = ta.sma(data['close'], length=9)
    data['highsma5_off3'] = data['highsma5'].shift(3)
    data['lowsma5_off3'] = data['lowsma5'].shift(3)

    # Calculate previous values
    data['closesma9_prev'] = data['closesma9'].shift(1)
    data['closesma26_prev'] = data['closesma26'].shift(1)

    # Calculate crossover and crossunder
    data['BuyCall'] = ((data['closesma9'] > data['closesma26']) & 
                       (data['closesma9_prev'] <= data['closesma26_prev'])).astype(int)

    data['BuyPut'] = ((data['closesma9'] < data['closesma26']) & 
                      (data['closesma9_prev'] >= data['closesma26_prev'])).astype(int)

    return data

def calculate_vstop(data):
    """Calculate VStop indicators from data."""
    data['ATR'] = ta.atr(data['high'], data['low'], data['close'], length=252)

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

    return data

def vstop():
    """
    Fetch, calculate, and update indicators.
    """
    engine = get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Fetch the latest datetime from ohlctick_data
    def fetch_latest_datetime(table_name):
        with engine.connect() as conn:
            query = text(f"SELECT MAX(datetime) AS latest_datetime FROM {table_name}")
            result = conn.execute(query).fetchone()
            return result[0] if result else None

    latest_datetime_ohlctick = fetch_latest_datetime('ohlctick_data')

    if not latest_datetime_ohlctick:
        print("No data in ohlctick_data. Exiting.")
        return

    # Fetch all data from ohlctick_data
    query = "SELECT * FROM ohlctick_data"
    with engine.connect() as conn:
        data = pd.read_sql_query(query, conn)

    if data.empty:
        print("No data retrieved from ohlctick_data. Exiting.")
        return

    # Convert columns to numeric
    data['high'] = pd.to_numeric(data['high'], errors='coerce')
    data['low'] = pd.to_numeric(data['low'], errors='coerce')
    data['close'] = pd.to_numeric(data['close'], errors='coerce')
    data['datetime'] = pd.to_datetime(data['datetime'], errors='coerce')

    # Drop rows with NaN values in crucial columns
    data = data.dropna(subset=['high', 'low', 'close'])

    # Calculate additional indicators
    data = calculate_additional_indicators(data)

    # Calculate VStop indicators
    data = calculate_vstop(data)

    # Filter to keep only the latest 750 rows
    data = data.sort_values('datetime', ascending=False).head(750)

    # Save the updated data back to the database
    with engine.connect() as conn:
        data.to_sql('indicators_data', conn, if_exists='replace', index=False)
    print("Indicators data updated.")

if __name__ == "__main__":
    while True:
        now = datetime.now(IST)
        if is_market_open():
            vstop()
            # Sleep for 60 seconds if the market is open
            time.sleep(60)
        else:
            # Calculate the sleep duration until the next market open
            next_market_open_time = now.replace(hour=9, minute=14, second=0, microsecond=0)
            if datetime.now(IST) > next_market_open_time:
                next_market_open_time += timedelta(days=1)
                # Adjust if it's a weekend or holiday
                while not is_business_day(next_market_open_time):
                    next_market_open_time += timedelta(days=1)
            sleep_duration = (next_market_open_time - datetime.now(IST)).total_seconds()
            print(f"Sleeping for {sleep_duration / 3600:.2f} hours until next market open")
            time.sleep(sleep_duration)
