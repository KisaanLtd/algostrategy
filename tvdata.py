from tvDatafeed import TvDatafeed, Interval
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd
import json
import mysql.connector

def load_config():
    """Load configuration from config.json."""
    with open('config.json', 'r') as file:
        return json.load(file)

config = load_config()

# Extract configuration details
db_config = config['db_config']
tv_username = config['tvdatafeed']['username']
tv_password = config['tvdatafeed']['password']

def get_mysql_connection():
    """Establish and return a connection to the MySQL database."""
    return mysql.connector.connect(**db_config)

def get_sqlalchemy_engine():
    """Create and return an SQLAlchemy engine."""
    user = quote_plus(db_config['user'])
    password = quote_plus(db_config['password'])
    host = db_config['host']
    database = db_config['database']

    connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
    return create_engine(connection_string)

def fetch_and_store_data():
    """Fetch data from TVDatafeed and store it in the MySQL database."""
    # Initialize TvDatafeed with credentials
    tv = TvDatafeed(tv_username, tv_password)

    # Fetch historical data
    data = tv.get_hist(symbol='BANKNIFTY', exchange='NSE', interval=Interval.in_1_minute, n_bars=1000)

    # Convert data to DataFrame
    dataf = pd.DataFrame(data)

    # Ensure 'datetime' column is properly converted to datetime format
    dataf.index = pd.to_datetime(dataf.index, errors='coerce')
    dataf.reset_index(inplace=True)
    dataf.rename(columns={'index': 'datetime'}, inplace=True)

    # Calculate 'ohlc4'
    dataf['ohlc4'] = (dataf['open'] + dataf['high'] + dataf['low'] + dataf['close']) / 4
    dataf['ohlc4'] = dataf['ohlc4'].round(2)

    # Define the columns to be selected, excluding 'symbol'
    selected_columns = ['datetime', 'open', 'high', 'low', 'close', 'ohlc4']

    # Create a table and insert data into MySQL table using SQLAlchemy
    engine = get_sqlalchemy_engine()

    with get_mysql_connection() as connection:
        cursor = connection.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ohlctick_data (
            datetime DATETIME,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            ohlc4 FLOAT,
            PRIMARY KEY (datetime)
        )
        """)

        # Commit the table creation
        connection.commit()

    # Insert data into MySQL table
    dataf[selected_columns].to_sql('ohlctick_data', con=engine, if_exists='replace', index=False)

def main():
    """Main function to execute data fetching and storing."""
    fetch_and_store_data()

if __name__ == "__main__":
    main()
