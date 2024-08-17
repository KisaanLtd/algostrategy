import json
import asyncio
import aiomysql
import pytz
import pandas as pd
import numpy as np
from breeze_connect import BreezeConnect
from datetime import datetime, timedelta
from scipy.signal import find_peaks
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
HOLIDAYS = config['holidays']
default_expiry_date = config['expiry_date']  # Load default expiry date

# Initialize BreezeConnect API
api = BreezeConnect(api_key=api_key)
api.generate_session(api_secret=secret_key, session_token=api_session)


async def get_mysql_pool():
    """Create and return a connection pool to the MySQL database."""
    pool = await aiomysql.create_pool(
        host=db_config['host'],
        port=int(db_config['port']),
        user=db_config['user'],
        password=db_config['password'],
        db=db_config['database'],
        autocommit=True
    )
    return pool


async def fetch_indicators_data(pool):
    """Fetch data from ohlctick_data table."""
    query = "SELECT * FROM indicators_data;"
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(query)
            result = await cur.fetchall()
            return pd.DataFrame(result)


def detect_crossovers(data):
    """Detect crossover and crossunder between ohlc4_sma5 and highsma5_off3, lowsma5_off3."""
    crossover = (data['ohlc4_sma5'] > data['highsma5_off3']) & (
        data['ohlc4_sma5'].shift(-1) <= data['highsma5_off3'].shift(-1))
    crossunder = (data['ohlc4_sma5'] < data['lowsma5_off3']) & (
        data['ohlc4_sma5'].shift(-1) >= data['lowsma5_off3'].shift(-1))

    return data.loc[crossover], data.loc[crossunder]


def TrendUp2_cross(data):
    """Detect crossovers and crossunders for TrendUp2."""
    TrendUp2crossover = (data['TrendUp2'] == 1) & (
        data['TrendUp2'].shift(-1) == 0)
    TrendUp2crossunder = (data['TrendUp2'] == 0) & (
        data['TrendUp2'].shift(-1) == 1)

    return (
        data.loc[TrendUp2crossover].sort_values(
            by='datetime', ascending=False),
        data.loc[TrendUp2crossunder].sort_values(
            by='datetime', ascending=False)
    )


def get_peak_trough(data):
    """Extract the latest peaks and troughs from the DataFrame."""
    data['datetime'] = pd.to_datetime(data['datetime'])
    highs_array = data['highsma5'].to_numpy()
    lows_array = data['lowsma5'].to_numpy()

    peaks, properties = find_peaks(highs_array, width=5, prominence=True)
    troughs, properties_troughs = find_peaks(
        -lows_array, width=5, prominence=True)

    peak_df = pd.DataFrame({
        'Datetime': data['datetime'].to_numpy()[peaks],
        'PeakValue': highs_array[peaks],
        'PeakProm': properties['prominences']
    })

    trough_df = pd.DataFrame({
        'Datetime': data['datetime'].to_numpy()[troughs],
        'TroughValue': lows_array[troughs],
        'TroughProm': properties_troughs['prominences']
    })

    latest_peak_row = peak_df.sort_values(
        by='Datetime', ascending=False).iloc[0] if not peak_df.empty else pd.Series(dtype='float64')
    latest_trough_row = trough_df.sort_values(
        by='Datetime', ascending=False).iloc[0] if not trough_df.empty else pd.Series(dtype='float64')

    return latest_peak_row, latest_trough_row, peak_df, trough_df


def filter_prominent_peaks_troughs(peak_df, trough_df, threshold=100):
    """Filter peaks and troughs where PeakProm > 100 or TroughProm > 100."""
    filtered_peak_df = peak_df[peak_df['PeakProm'] > threshold]
    filtered_trough_df = trough_df[trough_df['TroughProm'] > threshold]
    latest_filtered_peak_row = filtered_peak_df.sort_values(
        by='Datetime', ascending=False).iloc[0] if not filtered_peak_df.empty else pd.Series(dtype='float64')
    latest_filtered_trough_row = filtered_trough_df.sort_values(
        by='Datetime', ascending=False).iloc[0] if not filtered_trough_df.empty else pd.Series(dtype='float64')
    return latest_filtered_peak_row, latest_filtered_trough_row


def get_strike_prices(latest_peak_row, latest_trough_row, latest_filtered_peak_row, latest_filtered_trough_row):
    """Calculate strike prices based on the latest peak and trough rows."""
    latest_peak_value = latest_peak_row['PeakValue'] if not latest_peak_row.empty else None
    latest_trough_value = latest_trough_row['TroughValue'] if not latest_trough_row.empty else None
    latest_filtered_peak_value = latest_filtered_peak_row[
        'PeakValue'] if not latest_filtered_peak_row.empty else None
    latest_filtered_trough_value = latest_filtered_trough_row[
        'TroughValue'] if not latest_filtered_trough_row.empty else None

    strike_price_ce = int((latest_filtered_trough_value - latest_filtered_trough_value %
                          100) + 100) if latest_filtered_trough_value is not None else None
    strike_price_pe = int((latest_filtered_peak_value - latest_filtered_peak_value %
                          100) - 100) if latest_filtered_peak_value is not None else None

    if latest_peak_value is not None and latest_trough_value is not None:
        if (latest_peak_row['PeakProm'] < 100) & (latest_peak_row['PeakProm'] > latest_trough_row['TroughProm']):
            return strike_price_pe, "put"
        if (latest_filtered_peak_value > latest_trough_value):
            return strike_price_pe, "put"
        elif (latest_peak_row['PeakProm'] < latest_trough_row['TroughProm']) & (100 < latest_trough_row['TroughProm']):
            return strike_price_ce, "call"
        elif (latest_peak_value < latest_filtered_trough_value):
            return strike_price_ce, "call"
    return None, None


def get_entry_trigger(latest_peak_row, latest_trough_row, crossover_points, crossunder_points, TrendUp2crossover_sorted, TrendUp2crossunder_sorted):
    """Determine the entry trigger based on crossover and crossunder points."""
    if not latest_peak_row.empty and not latest_trough_row.empty:
        if (latest_peak_row['PeakProm'] > 100) & (latest_peak_row['PeakProm'] > latest_trough_row['TroughProm']):
            if not TrendUp2crossunder_sorted.empty and not crossunder_points.empty:
                if TrendUp2crossunder_sorted.iloc[0]['datetime'] < crossunder_points.iloc[0]['datetime']:
                    return TrendUp2crossunder_sorted.iloc[0]['lowsma5_off3']
                else:
                    return crossunder_points.iloc[0]['lowsma5_off3']
        elif (latest_peak_row['PeakProm'] < latest_trough_row['TroughProm']) & (100 < latest_trough_row['TroughProm']):
            if not TrendUp2crossover_sorted.empty and not crossover_points.empty:
                if TrendUp2crossover_sorted.iloc[0]['datetime'] < crossover_points.iloc[0]['datetime']:
                    return TrendUp2crossover_sorted.iloc[0]['highsma5_off3']
                else:
                    return crossover_points.iloc[0]['highsma5_off3']
    return None


def get_current_expiry_date(api, stock_code, exchange_code, product, right, strike_price):
    """Fetch the current expiry date for the given option."""
    try:
        expiry_dates = api.get_option_chain_quotes(
            stock_code=stock_code,
            exchange_code=exchange_code,
            product_type=product,
            expiry_date="",
            right=right,
            strike_price=strike_price
        )
        expiry_dates_df = pd.DataFrame(expiry_dates["Success"])
        if not expiry_dates_df.empty:
            expiry_date = expiry_dates_df.expiry_date.iloc[0]
            return expiry_date
        else:
            return default_expiry_date
    except Exception as e:
        return default_expiry_date


def place_order(api, stock_code, exchange_code, product, action, order_type, stoploss, quantity, price, validity, validity_date, disclosed_quantity, expiry_date, right, strike_price):
    """Place an order through the BreezeConnect API."""
    try:
        order_id = api.place_order(
            stock_code=stock_code,
            exchange_code=exchange_code,
            product=product,
            action=action,
            order_type=order_type,
            stoploss=stoploss,
            quantity=quantity,
            price=price,
            validity=validity,
            validity_date=validity_date,
            disclosed_quantity=disclosed_quantity,
            expiry_date=expiry_date,
            right=right,
            strike_price=strike_price
        )
        print(f"Order ID: {order_id}")
    except Exception as e:
        print(f"Error placing order: {str(e)}")


def time_check():
    """Check if the current time is within market hours."""
    current_time = datetime.now(IST).time()
    market_open = current_time >= datetime.strptime('09:15', '%H:%M').time()
    market_close = current_time < datetime.strptime('15:30', '%H:%M').time()
    return market_open and market_close


def is_business_day(date):
    """Check if a given date is a business day."""
    if date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False
    return date.strftime('%Y-%m-%d') not in HOLIDAYS


def get_sleep_duration():
    """Calculate the sleep duration until the next market open."""
    now = datetime.now(IST)
    next_market_open_time = now.replace(
        hour=9, minute=14, second=0, microsecond=0)
    if now > next_market_open_time:
        next_market_open_time += timedelta(days=1)
        while not is_business_day(next_market_open_time):
            next_market_open_time += timedelta(days=1)
    sleep_duration = (next_market_open_time - now).total_seconds()
    print(
        f"Sleeping for {sleep_duration / 3600:.2f} hours until next market open")
    time.sleep(sleep_duration)


async def process_data():
    """Main process to run every 5 seconds during market hours."""
    pool = await get_mysql_pool()
    while time_check() and is_business_day(datetime.now(IST).date()):
        data = await fetch_indicators_data(pool)

        crossover_points, crossunder_points = detect_crossovers(data)
        TrendUp2crossover_sorted, TrendUp2crossunder_sorted = TrendUp2_cross(
            data)
        latest_peak_row, latest_trough_row, peak_df, trough_df = get_peak_trough(
            data)
        latest_filtered_peak_row, latest_filtered_trough_row = filter_prominent_peaks_troughs(
            peak_df, trough_df)
        strike_price, option_type = get_strike_prices(
            latest_peak_row, latest_trough_row, latest_filtered_peak_row, latest_filtered_trough_row)

        if strike_price and option_type:
            entry_trigger = get_entry_trigger(
                latest_peak_row, latest_trough_row, crossover_points, crossunder_points, TrendUp2crossover_sorted, TrendUp2crossunder_sorted)
            if entry_trigger:
                # expiry_date = get_current_expiry_date(
                # api, config['stock_code'], config['exchange_code'], config['product'], option_type, strike_price)
                expiry_date = get_current_expiry_date(
                    api, "CNXBAN", "NFO", 'options', option_type, strike_price)
                place_order(api, stock_code="CNXBAN", exchange_code="NFO", product="options", action="buy", order_type="market", stoploss='', quantity="15", price="", validity="day",
                            validity_date=datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d'), disclosed_quantity="0", expiry_date=expiry_date, right=option_type, strike_price=strike_price)

        await asyncio.sleep(5)  # Sleep for 5 seconds

    get_sleep_duration()  # Sleep until next market open

if __name__ == "__main__":
    asyncio.run(process_data())
