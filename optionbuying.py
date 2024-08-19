import json
import asyncio
import aiomysql
import pytz
import pandas as pd
import numpy as np
from breeze_connect import BreezeConnect
from datetime import datetime, timedelta
from scipy.signal import find_peaks

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
# api = BreezeConnect(api_key=api_key)
# api.generate_session(api_secret=secret_key, session_token=api_session)

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
    crossover = (data['ohlc4_sma5'] > data['highsma5_off3']) & (data['ohlc4_sma5'].shift(-1) <= data['highsma5_off3'].shift(-1))
    crossunder = (data['ohlc4_sma5'] < data['lowsma5_off3']) & (data['ohlc4_sma5'].shift(-1) >= data['lowsma5_off3'].shift(-1))
    return data.loc[crossover], data.loc[crossunder]

def TrendUp2_cross(data):
    """Detect crossovers and crossunders for TrendUp2."""
    TrendUp2crossover = (data['TrendUp2'] == 1) & (data['TrendUp2'].shift(-1) == 0)
    TrendUp2crossunder = (data['TrendUp2'] == 0) & (data['TrendUp2'].shift(-1) == 1)

    return (
        data.loc[TrendUp2crossover].sort_values(by='datetime', ascending=False),
        data.loc[TrendUp2crossunder].sort_values(by='datetime', ascending=False)
    )

def get_peak_trough(data):
    """Extract the latest peaks and troughs from the DataFrame."""
    data['datetime'] = pd.to_datetime(data['datetime'])
    highs_array = data['highsma5'].to_numpy()
    lows_array = data['lowsma5'].to_numpy()

    peaks, properties = find_peaks(highs_array, width=5, prominence=True)
    troughs, properties_troughs = find_peaks(-lows_array, width=5, prominence=True)

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

    latest_peak_row = peak_df.sort_values(by='Datetime', ascending=False).iloc[0] if not peak_df.empty else pd.Series(dtype='float64')
    latest_trough_row = trough_df.sort_values(by='Datetime', ascending=False).iloc[0] if not trough_df.empty else pd.Series(dtype='float64')

    return latest_peak_row, latest_trough_row, peak_df, trough_df

def filter_prominent_peaks_troughs(peak_df, trough_df, threshold=100):
    """Filter peaks and troughs where PeakProm > 100 or TroughProm > 100."""
    filtered_peak_df = peak_df[peak_df['PeakProm'] > threshold]
    filtered_trough_df = trough_df[trough_df['TroughProm'] > threshold]
    latest_filtered_peak_row = filtered_peak_df.sort_values(by='Datetime', ascending=False).iloc[0] if not filtered_peak_df.empty else pd.Series(dtype='float64')
    latest_filtered_trough_row = filtered_trough_df.sort_values(by='Datetime', ascending=False).iloc[0] if not filtered_trough_df.empty else pd.Series(dtype='float64')
    return latest_filtered_peak_row, latest_filtered_trough_row

def get_strike_prices(latest_peak_row, latest_trough_row):
    latest_peakprom = latest_peak_row['PeakProm']
    latest_peak_value = latest_peak_row['PeakValue']
    latest_peak_datetime = latest_peak_row['Datetime']
    latest_troughprom = latest_trough_row['TroughProm']
    latest_trough_value = latest_trough_row['TroughValue']
    latest_trough_datetime = latest_trough_row['Datetime']
    strike_price_ce = int((latest_trough_value - latest_trough_value % 100) + 100)
    strike_price_pe = int((latest_peak_value - latest_peak_value % 100) - 100)

    if (latest_peakprom > 100) & (latest_peakprom > latest_troughprom):
        return strike_price_pe, "put"
    elif (latest_peakprom < latest_troughprom) & (100 < latest_troughprom):
        return strike_price_ce, "call"
    return None, None

def get_entry_trigger(latest_peak_row, latest_trough_row, crossover_points, crossunder_points, TrendUp2crossover_sorted, TrendUp2crossunder_sorted):
    """Determine the entry trigger based on crossover and crossunder points."""
    if not latest_peak_row.empty and not latest_trough_row.empty:
        if (latest_peak_row['PeakProm'] > 100) & (latest_peak_row['PeakProm'] > latest_trough_row['TroughProm']):
            if not TrendUp2crossunder_sorted.empty and not crossunder_points.empty:
                if TrendUp2crossunder_sorted.iloc[0]['datetime'] < crossunder_points.iloc[0]['datetime']:
                    return TrendUp2crossunder_sorted.iloc[0]['lowsma5']
                else:
                    return crossunder_points.iloc[0]['lowsma5']
        elif (latest_peak_row['PeakProm'] < latest_trough_row['TroughProm']) & (100 < latest_trough_row['TroughProm']):
            if not TrendUp2crossover_sorted.empty and not crossover_points.empty:
                if TrendUp2crossover_sorted.iloc[0]['datetime'] < crossover_points.iloc[0]['datetime']:
                    return TrendUp2crossover_sorted.iloc[0]['highsma5']
                else:
                    return crossover_points.iloc[0]['highsma5']
    return None

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
        print(f"Order placed successfully! Order ID: {order_id}")
    except Exception as e:
        print(f"Failed to place order: {str(e)}")

async def get_signal():
    pool = await get_mysql_pool()
    indicators_data = await fetch_indicators_data(pool)
    crossover_points, crossunder_points = detect_crossovers(indicators_data)
    TrendUp2crossover_sorted, TrendUp2crossunder_sorted = TrendUp2_cross(indicators_data)
    latest_peak_row, latest_trough_row, peak_df, trough_df = get_peak_trough(indicators_data)
    strike_price, option_type = get_strike_prices(latest_peak_row, latest_trough_row)
    entry_trigger = get_entry_trigger(latest_peak_row, latest_trough_row, crossover_points, crossunder_points, TrendUp2crossover_sorted, TrendUp2crossunder_sorted)

    if strike_price is not None and entry_trigger is not None and option_type is not None:
        signal_buycall = (entry_trigger > indicators_data['low'].iloc[0]).astype(int)  # Fetch the latest tick data
        signal_buyput = (entry_trigger < indicators_data['high'].iloc[0]).astype(int)
        return signal_buycall, signal_buyput, strike_price, option_type
    return None, None, None, None

async def main():
    signal_buycall, signal_buyput, strike_price, option_type = await get_signal()
    print(signal_buycall)
    print(signal_buyput)
    print(strike_price)
    print(option_type)

    if signal_buycall == 1 and option_type == "call":
        print(f"Call: Entry triggered at {datetime.now(IST)} at strike price {strike_price}")
        # You can uncomment and adjust the API call parameters when integrating with BreezeConnect
        # place_order(api, stock_code='BANKNIFTY', exchange_code='NFO', product='options', action='buy',
        #             order_type='limit', stoploss='100', quantity='25', price=entry_trigger,
        #             validity='DAY', validity_date=None, disclosed_quantity=None, expiry_date=default_expiry_date,
        #             right='call', strike_price=str(strike_price)))
    elif signal_buyput == 1 and option_type == "put":
        print(f"Put: Entry triggered at {datetime.now(IST)} at strike price {strike_price}")
        # You can uncomment and adjust the API call parameters when integrating with BreezeConnect
        # place_order(api, stock_code='BANKNIFTY', exchange_code='NFO', product='options', action='buy',
        #             order_type='limit', stoploss='100', quantity='25', price=entry_trigger,
        #             validity='DAY', validity_date=None, disclosed_quantity=None, expiry_date=default_expiry_date,
        #             right='put', strike_price=str(strike_price)))

    # Closing the connection pool after the process is done
    pool = await get_mysql_pool()
    pool.close()
    await pool.wait_closed()

# # Run the async main loop
# asyncio.run(main())
if __name__ == "__main__":
    asyncio.run(main())
