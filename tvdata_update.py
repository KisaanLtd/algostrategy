from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import json
import asyncio
import aiomysql
import pytz
from datetime import datetime, time

# Load configuration from config.json
def load_config():
    """Load configuration from config.json."""
    with open('config.json', 'r') as file:
        return json.load(file)

config = load_config()

# Define timezone
IST = pytz.timezone('Asia/Kolkata')

# Database configuration
db_config = config['db_config']
tv_username = config['tvdatafeed']['username']
tv_password = config['tvdatafeed']['password']

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

async def fetch_ohlctick_data(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            query = 'SELECT * FROM ohlctick_data ORDER BY datetime'
            await cur.execute(query)
            data = await cur.fetchall()
            df = pd.DataFrame(data, columns=['datetime', 'open', 'high', 'low', 'close', 'ohlc4'])
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.sort_values(by='datetime', inplace=True)
            return df

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

async def check_missing_or_duplicate_keys(pool):
    now = datetime.now(IST)
    market_open_time = datetime.combine(now.date(), time(9, 15))
    market_close_time = datetime.combine(now.date(), time(15, 28))
    period_now = pd.Period.now('2T')
    open_time_datetime64 = pd.Period(market_open_time, '1T').start_time
    close_time_datetime64 = pd.Period(market_close_time, '1T').end_time
    period_now_datetime64 = period_now.start_time
    min_datetime = min(close_time_datetime64, period_now_datetime64)

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            query = f"""
            WITH RECURSIVE datetime_sequence AS (
                SELECT '{open_time_datetime64}' AS dt
                UNION ALL
                SELECT DATE_ADD(dt, INTERVAL 1 MINUTE)
                FROM datetime_sequence
                WHERE dt < '{min_datetime}'
            )
            SELECT ds.dt AS datetime_missing_or_duplicate
            FROM datetime_sequence ds
            LEFT JOIN (
                SELECT `datetime`, COUNT(*) AS cnt
                FROM ohlctick_data
                WHERE `datetime` >= '{open_time_datetime64}' AND `datetime` <= '{min_datetime}'
                GROUP BY `datetime`
            ) t ON ds.dt = t.`datetime`
            WHERE t.`datetime` IS NULL OR t.cnt > 1;
            """
            await cursor.execute(query)
            result = await cursor.fetchall()
            if result:
                print("Missing or duplicate datetime found:", result)
            else:
                print("No gaps or duplicates found.")
            return result

async def insert_tick_dataframe(pool, tick_df):
    """Insert a DataFrame into the database and print a success message."""
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                insert_query = '''
                REPLACE INTO ohlctick_data (datetime, open, high, low, close, ohlc4)
                VALUES (%s, %s, %s, %s, %s, %s) 
                '''
                records = tick_df.to_dict(orient='records')
                for record in records:
                    await cursor.execute(insert_query, (
                        record['datetime'], record['open'], record['high'],
                        record['low'], record['close'], record['ohlc4']
                    ))
            await conn.commit()
            print(f"Successfully inserted/updated {len(records)} rows into the database.")
    except Exception as e:
        print(f"Error inserting data into database: {e}")

async def fetch_tv_data():
    """Fetch data from TVDatafeed and prepare it for database insertion."""
    tv = TvDatafeed(tv_username, tv_password)

    try:
        data = tv.get_hist(symbol='BANKNIFTY', exchange='NSE', interval=Interval.in_1_minute, n_bars=100)
        dataf = pd.DataFrame(data)
        dataf.index = pd.to_datetime(dataf.index, errors='coerce')
        dataf.reset_index(inplace=True)
        dataf.rename(columns={'index': 'datetime'}, inplace=True)

        dataf.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close'
        }, inplace=True)

        if {'open', 'high', 'low', 'close'}.issubset(dataf.columns):
            dataf['ohlc4'] = (dataf['open'] + dataf['high'] + dataf['low'] + dataf['close']) / 4
            dataf['ohlc4'] = dataf['ohlc4'].round(2)
        else:
            print("Missing expected columns. Available columns:", dataf.columns)
            return pd.DataFrame()

        selected_columns = ['datetime', 'open', 'high', 'low', 'close', 'ohlc4']
        tick_df = dataf[selected_columns]

        return tick_df
    except Exception as e:
        print(f"Error fetching TV data: {e}")
        return pd.DataFrame()

async def main():
    pool = await get_mysql_pool()
    # await create_tables_if_not_exists(pool)
    result = await check_missing_or_duplicate_keys(pool)
    
    if result:
        tick_df = await fetch_tv_data()
        if not tick_df.empty:
            await insert_tick_dataframe(pool, tick_df)
    pool.close()
    await pool.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        print(f"RuntimeError: {e}")
