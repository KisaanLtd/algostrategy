import os
from dotenv import load_dotenv
from breeze_connect import BreezeConnect
from datetime import datetime,date,timedelta
import pytz

load_dotenv()

# My credentials
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")
session_token = os.getenv("SESSION_TOKEN")

api = BreezeConnect(api_key=api_key)
api.generate_session(api_secret=api_secret, session_token=str(session_token))

db_config = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "autocommit": True,
    "minsize": 5,
    "maxsize": 50
}

holidays=[
        "2024-12-25",
        "2025-02-26",
        "2025-03-14",
		"2025-03-31",
		"2025-04-10",
		"2025-04-14",
        "2025-04-18",
        "2025-05-01",
        "2025-08-15",
        "2025-08-27",
        "2025-10-02",
        "2025-10-21",
        "2025-10-22",
        "2025-11-05",
		"2025-12-25"
    ]
# expiry_date="2024-11-13T06:00:00.000Z"
stock_code ="CNXBAN"
options_basket = [{"stock_code": "CNXBAN", "strike_price": "48800", "right": "put"},
    {"stock_code": "CNXBAN", "strike_price": "51700", "right": "call"},]

######################################################

def get_last_wednesday(year, month, timezone):
    """
    Helper function to calculate the last Wednesday of a given month.
    """
    if month == 12:
        last_day = timezone.localize(datetime(year, month, 31))
    else:
        first_day_next_month = timezone.localize(datetime(year, month + 1, 1))
        last_day = first_day_next_month - timedelta(days=1)

    # Calculate the last Wednesday of the month
    offset = (last_day.weekday() - 3) % 7  # 2 represents thursday
    return last_day - timedelta(days=offset)

def get_monthly_expiry():
    """
    Function to calculate the monthly expiry date based on the last Wednesday of the month.
    """
    timezone = pytz.timezone('Asia/Kolkata')
    today_date = datetime.now(timezone)
    year = today_date.year
    month = today_date.month

    # Get last Wednesday of the current month
    current_month_last_wednesday = get_last_wednesday(year, month, timezone)
    while current_month_last_wednesday.strftime('%Y-%m-%d') in holidays:
            current_month_last_wednesday -= timedelta(days=1)

    # Check if today is later than the current month's last Wednesday
    if today_date.date() > current_month_last_wednesday.date():
        # Move to the next month
        if month == 12:  # Handle year transition
            year += 1
            month = 1
        else:
            month += 1

        # Get last Wednesday of the next month
        next_month_last_wednesday = get_last_wednesday(year, month, timezone)

        # Adjust if the calculated date falls on a holiday
        while next_month_last_wednesday.strftime('%Y-%m-%d') in holidays:
            next_month_last_wednesday -= timedelta(days=1)

        return next_month_last_wednesday.strftime('%Y-%m-%d')

    # Default: return the last Wednesday of the current month
    return current_month_last_wednesday.strftime('%Y-%m-%d')

# Get and print the expiry date
expiry_date = get_monthly_expiry()

###########################

# source .venv/bin/activate
# python scripts/indicator_update.py
