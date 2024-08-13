import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
from scipy.stats import linregress
import json
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from breeze_connect import BreezeConnect
from datetime import datetime
import pytz
import mysql.connector

# Load configuration from config.json
with open('config.json', 'r') as f:
    config = json.load(f)

# Extract configuration settings
api_key = config['api_key']
api_secret = config['api_secret']
secret_key = config['secret_key']
api_session = config['api_session']
db_config = config['db_config']
HOLIDAYS = config['holidays']
expiry_date = config['expiry_date']

# Initialize BreezeConnect API
api = BreezeConnect(api_key=api_key)
api.generate_session(api_secret=secret_key, session_token=api_session)

def get_sqlalchemy_engine():
    """Create and return an SQLAlchemy engine."""
    user = db_config['user']
    password = db_config['password']
    host = db_config['host']
    database = db_config['database']
    
    connection_string = URL.create(
        drivername='mysql+mysqlconnector',
        username=user,
        password=password,
        host=host,
        database=database
    )
    
    return create_engine(connection_string)

def get_mysql_connection():
    """Establish and return a connection to the MySQL database."""
    return mysql.connector.connect(**db_config)

def is_market_open():
    """Check if the market is currently open."""
    now = datetime.now(pytz.timezone('Asia/Kolkata'))  # Set the timezone to IST
    market_open_time = now.replace(hour=9, minute=14, second=0, microsecond=0)
    market_close_time = now.replace(hour=15, minute=31, second=0, microsecond=0)
    return market_open_time <= now <= market_close_time

IST = pytz.timezone('Asia/Kolkata')

def is_business_day(date):
    """Check if a given date is a business day."""
    if date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False
    return date.strftime('%Y-%m-%d') not in HOLIDAYS

# Database connection
engine = get_sqlalchemy_engine()
connection = get_mysql_connection()

# Load data
df = pd.read_sql('SELECT datetime, highsma5, lowsma5, closesma9, ATR, BuyCall, BuyPut FROM indicators_data', engine)

# Extract the latest peaks and troughs from the DataFrame.
df['datetime'] = pd.to_datetime(df['datetime'])
highs_array = df['highsma5'].to_numpy()
lows_array = df['lowsma5'].to_numpy()
sma9_array = df['closesma9'].to_numpy()
atr_array = df['ATR'].to_numpy()
datetime_series = df['datetime'].to_numpy()

# Find local maxima and minima
order = 5  # You can adjust this value based on your data and requirements
# Find local maxima (peaks)
local_maxima_indices = argrelextrema(highs_array, np.greater, order=order)[0]
local_maxima_indice = argrelextrema(highs_array, np.greater, order=order)[0][1]
local_minima_indices = argrelextrema(lows_array, np.less, order=order)[0]

maxima_values = highs_array[local_maxima_indices]
maxima_datetime = datetime_series[local_maxima_indices]
minima_values = lows_array[local_minima_indices]
minima_datetime = datetime_series[local_minima_indices]
maxima_df = pd.DataFrame({
    'Datetime': maxima_datetime,
    'MaximaValues': maxima_values
})

minima_df = pd.DataFrame({
    'Datetime': minima_datetime,
    'MinimaValues': minima_values
})

# Print the indices of the maxima and minima
print(minima_df)
print(maxima_df)
