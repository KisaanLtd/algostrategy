import json
import pymysql
import time
from datetime import datetime

def load_config():
    """Load configuration from config.json."""
    with open('config.json', 'r') as file:
        return json.load(file)

config = load_config()

db_config = config['db_config']

def check_missing_or_duplicate_keys():
    connection = pymysql.connect(
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        database=db_config['database']
    )

    try:
        with connection.cursor() as cursor:
            current_date = datetime.now().strftime('%Y-%m-%d')
            query = f"""
            WITH RECURSIVE datetime_sequence AS (
                SELECT '{current_date} 09:15:00' AS dt
                UNION ALL
                SELECT DATE_ADD(dt, INTERVAL 1 MINUTE)
                FROM datetime_sequence
                WHERE dt < '{current_date} 15:30:00'
            )
            SELECT ds.dt AS datetime_missing_or_duplicate
            FROM datetime_sequence ds
            LEFT JOIN (
                SELECT `datetime`, COUNT(*) AS cnt
                FROM ohlctick_data
                WHERE `datetime` >= '{current_date} 09:15:00' AND `datetime` <= '{current_date} 15:30:00'
                GROUP BY `datetime`
            ) t ON ds.dt = t.`datetime`
            WHERE t.`datetime` IS NULL OR t.cnt > 1;
            """
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                print("Missing or duplicate datetime found:", result)
            else:
                print("No gaps or duplicates found.")
    finally:
        connection.close()

# Uncomment the code below to enable continuous checking during market hours
# while True:
#     current_time = datetime.now().time()
#     market_start = datetime.strptime("09:15", "%H:%M").time()
#     market_end = datetime.strptime("15:30", "%H:%M").time()

#     if market_start <= current_time <= market_end:
#         check_missing_or_duplicate_keys()
#         time.sleep(60)  # Wait for 1 minute before checking again
#     else:
#         print("Market is closed.")
#         time.sleep(60)  # Still wait for 1 minute, but skip checking

print(check_missing_or_duplicate_keys())
