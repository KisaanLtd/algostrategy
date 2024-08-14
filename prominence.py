import pandas as pd
import numpy as np
from scipy.signal import find_peaks
import json
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from breeze_connect import BreezeConnect
from datetime import datetime, timedelta
import pytz
import mysql.connector  # Import MySQL connector
import time

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

connection = get_mysql_connection()
cursor = connection.cursor()
engine = get_sqlalchemy_engine()
df = pd.read_sql('SELECT datetime, highsma5, lowsma5, closesma9, ATR, BuyCall, BuyPut FROM indicators_data', engine)

# Extract the latest peaks and troughs from the DataFrame.
df['datetime'] = pd.to_datetime(df['datetime'])
highs_array = df['highsma5'].to_numpy()
lows_array = df['lowsma5'].to_numpy()
sma9_array = df['closesma9'].to_numpy()
atr_array = df['ATR'].to_numpy()
datetime_series = df['datetime'].to_numpy()

peaks, properties = find_peaks(highs_array, prominence=True)
troughs, properties_troughs = find_peaks(-lows_array, prominence=True)

peak_values = highs_array[peaks]
trough_values = lows_array[troughs]

peak_dates = datetime_series[peaks]
trough_dates = datetime_series[troughs]
peak_sma9 = sma9_array[peaks]
peak_atr = atr_array[peaks]
trough_atr = atr_array[troughs]
trough_sma9 = sma9_array[troughs]

peak_prominences = properties['prominences']
trough_prominences = properties_troughs['prominences']

# Create DataFrames
peak_df = pd.DataFrame({
    'Datetime': peak_dates,
    'Peak_sma9': peak_sma9,
    'PeakValue': peak_values,
    'PeakProminence': peak_prominences,
    'Peak_atr': peak_atr
})

trough_df = pd.DataFrame({
    'Datetime': trough_dates,
    'Trough_sma9': trough_sma9,
    'TroughValue': trough_values,
    'TroughProminence': trough_prominences,
    'Trough_atr': trough_atr
})

# Round numeric columns to 2 decimal places
numeric_columns_peak_df = ['Peak_sma9', 'PeakValue', 'PeakProminence', 'Peak_atr']
numeric_columns_trough_df = ['Trough_sma9', 'TroughValue', 'TroughProminence', 'Trough_atr']

peak_df[numeric_columns_peak_df] = peak_df[numeric_columns_peak_df].round(2)
trough_df[numeric_columns_trough_df] = trough_df[numeric_columns_trough_df].round(2)

# Filter and sort DataFrames
peak_df_filtered = peak_df[peak_df['PeakProminence'] > 100]
trough_df_filtered = trough_df[trough_df['TroughProminence'] > 100]
peak_df_sorted = peak_df_filtered.sort_values(by='Datetime', ascending=False)
trough_df_sorted = trough_df_filtered.sort_values(by='Datetime', ascending=False)

latest_peak_row = peak_df_sorted.iloc[0] if not peak_df_sorted.empty else pd.Series(dtype='float64')
latest_trough_row = trough_df_sorted.iloc[0] if not trough_df_sorted.empty else pd.Series(dtype='float64')

print(peak_df_sorted)
print(trough_df_sorted)
