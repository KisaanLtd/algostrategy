import os
import ssl
import asyncio
import aiomysql
import pytz
import pandas as pd
import numpy as np
import math
from datetime import datetime, time, timedelta
from breeze_connect import BreezeConnect

IST = pytz.timezone('Asia/Kolkata')

# ── NSE Holidays ────────────────────────────────────────────────────────────
holidays = [
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10", "2025-04-14",
    "2025-04-18", "2025-05-01", "2025-08-15", "2025-08-27", "2025-10-02",
    "2025-10-21", "2025-10-22", "2025-11-05", "2025-11-15", "2025-12-25",
    # 2026 — NSE Circular NSE/CMTR/71775
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31", "2026-04-03",
    "2026-04-14", "2026-05-01", "2026-05-28", "2026-06-26", "2026-09-14",
    "2026-10-02", "2026-10-20", "2026-11-10", "2026-11-24", "2026-12-25",
]

# ── Config from environment ──────────────────────────────────────────────────
db_config = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

def get_last_wednesday(year, month, timezone):
    """Last Wednesday of a given month."""
    if month == 12:
        last_day = timezone.localize(datetime(year, month, 31))
    else:
        first_day_next_month = timezone.localize(datetime(year, month + 1, 1))
        last_day = first_day_next_month - timedelta(days=1)
    offset = (last_day.weekday() - 2) % 7  # 2 = Wednesday
    return last_day - timedelta(days=offset)


def get_monthly_expiry():
    """
    Auto-calculate Bank Nifty monthly expiry.
    Last Wednesday of current month; rolls to next month if today is past it.
    Rolls back one day if Wednesday falls on an NSE holiday.
    """
    today = datetime.now(IST)
    year, month = today.year, today.month

    expiry = get_last_wednesday(year, month, IST)
    while expiry.strftime('%Y-%m-%d') in holidays:
        expiry -= timedelta(days=1)

    if today.date() > expiry.date():
        month = month + 1 if month < 12 else 1
        year  = year + 1 if month == 1 else year
        expiry = get_last_wednesday(year, month, IST)
        while expiry.strftime('%Y-%m-%d') in holidays:
            expiry -= timedelta(days=1)

    return expiry.strftime('%Y-%m-%d')


# Auto-calculated at job startup — no manual EXPIRY_DATE env var needed
expiry_date = get_monthly_expiry()


def get_breeze_api():
    """Initialise Breeze API using credentials from environment."""
    api_key       = os.getenv("API_KEY")
    api_secret    = os.getenv("API_SECRET")
    session_token = os.getenv("SESSION_TOKEN")

    if not all([api_key, api_secret, session_token]):
        raise EnvironmentError(
            "Missing Breeze credentials: API_KEY, API_SECRET, SESSION_TOKEN must be set."
        )

    api = BreezeConnect(api_key=api_key)
    api.generate_session(api_secret=api_secret, session_token=str(session_token))
    return api


# ── MySQL pool ───────────────────────────────────────────────────────────────
async def get_mysql_pool():
    ssl_ctx = None
    ca_path = os.path.join(os.path.dirname(__file__), '..', 'ca.pem')
    if os.path.exists(ca_path):
        ssl_ctx = ssl.create_default_context(cafile=ca_path)

    pool = await aiomysql.create_pool(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=db_config['password'],
        db=db_config['database'],
        autocommit=True,
        ssl=ssl_ctx,
        minsize=2,
        maxsize=10,
    )
    return pool


def safe(v):
    """Replace NaN/inf with None for MySQL compatibility."""
    if v is None:
        return None
    try:
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
    except Exception:
        pass
    return v


# ════════════════════════════════════════════════════════════════════════════
class OptionBuying:

    def __init__(self):
        self.api = None   # Initialised lazily — only when placing orders

    # ── Market helpers ───────────────────────────────────────────────────────
    def is_market_open(self):
        now = datetime.now(IST)
        open_time  = datetime.combine(now.date(), time(9, 15)).replace(tzinfo=IST)
        close_time = datetime.combine(now.date(), time(15, 30)).replace(tzinfo=IST)
        return open_time <= now <= close_time

    def is_business_day(self, date):
        return date.weekday() < 5 and date.strftime('%Y-%m-%d') not in holidays

    # ── Data fetch ───────────────────────────────────────────────────────────
    async def fetch_indicators_data(self, pool):
        """
        Fetch last 1 day of indicators_data.
        dayhigh/daylow are pre-calculated per candle by indicator_update.py
        as cumulative high/low since 09:15 — no recalculation needed here.
        """
        query = (
            "SELECT * FROM `indicators_data` "
            "WHERE `datetime` >= NOW() - INTERVAL 1 DAY "
            "ORDER BY `datetime`"
        )
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query)
                result = await cur.fetchall()
        if not result:
            return pd.DataFrame()
        data = pd.DataFrame(result)
        data['datetime'] = pd.to_datetime(data['datetime'])
        data = data.sort_values(by='datetime', ascending=True).reset_index(drop=True)
        return data

    # ── Signal calculation ───────────────────────────────────────────────────
    async def get_sma_cross_data(self, data):
        """
        Identify long-entry (crossover) and short-entry (crossunder) candles.

        BuyCall and BuyPut already encode the full signal logic computed by
        indicator_update.py:
          BuyCall = (linearreg > hma26_5m OR ema26 > hma26_5m)
                    AND st_dir == 1
                    AND lri_angle is rising
          BuyPut  = (linearreg < hma26_5m OR ema26 < hma26_5m)
                    AND st_dir == 0
                    AND lri_angle is falling

        They are used here directly as the qualifying gate on VStop crossovers —
        no separate lri_slope check is needed.
        """
        required = [
            'lri_angle', 'TrendUp2', 'TrendUp3',
            'BuyCall', 'BuyPut', 'close', 'open',
        ]
        missing = [c for c in required if c not in data.columns]
        if missing:
            raise ValueError(f"Missing columns in indicators_data: {missing}")

        # ── Long-entry crossover ─────────────────────────────────────────────
        # VStop2 crosses above its previous level AND BuyCall confirms long bias,
        # OR VStop3 just turned up, OR both VStops simultaneously turned up.
        trendup_crossover = (
            ((data['TrendUp2'] == 1) & (data['TrendUp2'].shift(1) == 0) &
             (data['BuyCall'] == 1)) |
            ((data['TrendUp3'] == 1) & (data['TrendUp3'].shift(1) == 0)) |
            ((data['TrendUp2'] == 1) & (data['TrendUp3'] == 1) &
             (data['TrendUp3'].shift(1) == 0) & (data['TrendUp2'].shift(1) == 0))
        )

        # ── Short-entry crossunder ───────────────────────────────────────────
        # VStop2 crosses below its previous level AND BuyPut confirms short bias,
        # OR VStop3 just turned down, OR both VStops simultaneously turned down.
        trendup_crossunder = (
            ((data['TrendUp2'] == 0) & (data['TrendUp2'].shift(1) == 1) &
             (data['BuyPut'] == 1)) |
            ((data['TrendUp3'] == 0) & (data['TrendUp3'].shift(1) == 1)) |
            ((data['TrendUp2'] == 0) & (data['TrendUp3'] == 0) &
             (data['TrendUp3'].shift(1) == 1) & (data['TrendUp2'].shift(1) == 1))
        )

        bullish_trend = (data['close'] > data['open']) & (data['TrendUp3'] == 1) & (data['TrendUp2'] == 1)
        bearish_trend = (data['close'] < data['open']) & (data['TrendUp3'] == 0) & (data['TrendUp2'] == 0)

        crossover_data  = data.loc[trendup_crossover & bullish_trend].copy()
        crossunder_data = data.loc[trendup_crossunder & bearish_trend].copy()

        return crossover_data, crossunder_data

    async def get_entry_trigger(self, crossover_data, crossunder_data):
        if crossover_data.empty or crossunder_data.empty:
            return None, None, None, None

        if len(crossover_data) < 2 or len(crossunder_data) < 2:
            return None, None, None, None

        co_row  = crossover_data.iloc[-1]
        cu_row  = crossunder_data.iloc[-1]
        co_pre  = crossover_data.iloc[-2]
        cu_pre  = crossunder_data.iloc[-2]

        call_entry_trigger = call_sl_trigger = None
        put_entry_trigger  = put_sl_trigger  = None

        max_dt     = max(co_row['datetime'],  cu_row['datetime'])
        max_pre_dt = max(co_pre['datetime'],  cu_pre['datetime'])

        if (max_dt - max_pre_dt) <= timedelta(minutes=2):
            if max_pre_dt == co_pre['datetime']:
                call_entry_trigger = co_pre['close']
                call_sl_trigger    = min(co_pre['VStop2'], co_pre['VStop3'])
            if max_pre_dt == cu_pre['datetime']:
                put_entry_trigger  = cu_pre['close']
                put_sl_trigger     = max(cu_pre['VStop2'], cu_pre['VStop3'])

        if max_dt == co_row['datetime']:
            call_entry_trigger = co_row['close']
            call_sl_trigger    = min(co_row['VStop2'], co_row['VStop3'])

        if max_dt == cu_row['datetime']:
            put_entry_trigger  = cu_row['close']
            put_sl_trigger     = max(cu_row['VStop2'], cu_row['VStop3'])

        return call_entry_trigger, call_sl_trigger, put_entry_trigger, put_sl_trigger

    async def get_strike_prices(self, call_entry_trigger, put_entry_trigger):
        strike_price = None
        option_type  = None
        if call_entry_trigger:
            strike_price = int((call_entry_trigger - call_entry_trigger % 100) - 100)
            option_type  = "call"
        if put_entry_trigger:
            strike_price = int((put_entry_trigger - put_entry_trigger % 100) + 100)
            if option_type is None:
                option_type = "put"
        return strike_price, option_type

    async def get_entry_signal(self, call_entry_trigger, put_entry_trigger, data):
        callbuy_signal = putbuy_signal = None

        close_last  = data['close'].iloc[-1]
        last_bull   = data['Bull'].iloc[-1]
        last_bear   = data['Bear'].iloc[-1]
        ohlc4_last  = data['ohlc4'].iloc[-1]

        now              = datetime.now(IST)
        trade_start      = datetime.combine(now.date(), time(9, 20)).replace(tzinfo=IST)
        trade_end        = datetime.combine(now.date(), time(15, 7)).replace(tzinfo=IST)
        in_trading_hours = trade_start < now < trade_end

        if (call_entry_trigger is not None and put_entry_trigger is None and
                last_bull == 1 and last_bear == 0 and in_trading_hours and
                (ohlc4_last - call_entry_trigger) <= 100):
            callbuy_signal = int(close_last > call_entry_trigger)

        if (put_entry_trigger is not None and call_entry_trigger is None and
                last_bear == 1 and last_bull == 0 and in_trading_hours and
                (put_entry_trigger - ohlc4_last) <= 100):
            putbuy_signal = int(close_last < put_entry_trigger)

        return callbuy_signal, putbuy_signal

    # ── Order placement ──────────────────────────────────────────────────────
    async def fetch_order_placed_flag(self, pool):
        today = datetime.now(IST).strftime('%Y-%m-%d')
        query = """
            SELECT * FROM `order_notification`
            WHERE `orderReference` IS NOT NULL
              AND DATE(`datetime`) = %s
            ORDER BY `datetime` DESC
        """
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (today,))
                result = await cur.fetchall()
                if not result:
                    return False
                columns    = [col[0] for col in cur.description]
                order_data = pd.DataFrame(result, columns=columns)
                order_data.sort_values(by='datetime', inplace=True)
                valid      = order_data[order_data['orderReference'].notna()]
                last_flow  = valid['orderFlow'].iloc[-1]
                last_qty   = valid['executedQuantity'].iloc[-1]
                flag = (last_flow == 'Buy' and last_qty > 0) or \
                       (last_flow == 'Sell' and last_qty == 0)
                return flag

    async def place_order(self, strike_price, option_type,
                          callbuy_signal, putbuy_signal, order_placed_flag):
        if not strike_price or not option_type:
            return None
        if order_placed_flag or not (callbuy_signal or putbuy_signal):
            return None

        # Initialise Breeze API only when actually placing an order
        if self.api is None:
            self.api = get_breeze_api()

        try:
            response = self.api.place_order(
                stock_code="CNXBAN",
                exchange_code="NFO",
                product="options",
                action="BUY",
                order_type="market",
                stoploss="",
                quantity="15",
                price="",
                validity="day",
                validity_date=datetime.now(IST).strftime('%Y-%m-%d'),
                disclosed_quantity='0',
                expiry_date=expiry_date,
                right=option_type,
                strike_price=strike_price,
            )
            if response.get('Status') == 200:
                order_id = response['Success']['order_id']
                print(f"Order placed successfully: order_id={order_id}")
                return order_id
            else:
                print(f"Order failed: {response}")
                return None
        except Exception as e:
            print(f"Order placement error: {e}")
            return None

    # ── DB write ─────────────────────────────────────────────────────────────
    async def create_placeorder_track_table(self, pool):
        query = """
            CREATE TABLE IF NOT EXISTS `placeorder_track` (
                `datetime`           DATETIME     PRIMARY KEY,
                `order_id`           BIGINT       DEFAULT NULL,
                `call_entry_trigger` DOUBLE       DEFAULT NULL,
                `call_sl_trigger`    DOUBLE       DEFAULT NULL,
                `put_entry_trigger`  DOUBLE       DEFAULT NULL,
                `put_sl_trigger`     DOUBLE       DEFAULT NULL,
                `strike_price`       INT          DEFAULT NULL,
                `option_type`        VARCHAR(10)  DEFAULT NULL,
                `Bull`               INT          DEFAULT NULL,
                `Bear`               INT          DEFAULT NULL,
                `callbuy_signal`     INT          DEFAULT NULL,
                `putbuy_signal`      INT          DEFAULT NULL,
                `dayhigh`            DOUBLE       DEFAULT NULL,
                `daylow`             DOUBLE       DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                await conn.commit()

    async def insert_order_data(self, pool, row: dict):
        query = """
            INSERT INTO placeorder_track (
                datetime, order_id,
                call_entry_trigger, call_sl_trigger,
                put_entry_trigger,  put_sl_trigger,
                strike_price, option_type,
                Bull, Bear, callbuy_signal, putbuy_signal,
                dayhigh, daylow
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                order_id            = VALUES(order_id),
                call_entry_trigger  = VALUES(call_entry_trigger),
                call_sl_trigger     = VALUES(call_sl_trigger),
                put_entry_trigger   = VALUES(put_entry_trigger),
                put_sl_trigger      = VALUES(put_sl_trigger),
                strike_price        = VALUES(strike_price),
                option_type         = VALUES(option_type),
                Bull                = VALUES(Bull),
                Bear                = VALUES(Bear),
                callbuy_signal      = VALUES(callbuy_signal),
                putbuy_signal       = VALUES(putbuy_signal),
                dayhigh             = VALUES(dayhigh),
                daylow              = VALUES(daylow);
        """
        values = (
            row['datetime'], row['order_id'],
            row['call_entry_trigger'], row['call_sl_trigger'],
            row['put_entry_trigger'],  row['put_sl_trigger'],
            row['strike_price'],       row['option_type'],
            row['Bull'],               row['Bear'],
            row['callbuy_signal'],     row['putbuy_signal'],
            row['dayhigh'],            row['daylow'],
        )
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, values)
                await conn.commit()

    # ── Core run cycle ───────────────────────────────────────────────────────
    async def run_once(self, pool):
        """One full evaluation cycle — fetch signals, check entry, place order."""
        data = await self.fetch_indicators_data(pool)
        if data.empty or len(data) < 2:
            return

        crossover_data, crossunder_data = await self.get_sma_cross_data(data)
        order_placed_flag = await self.fetch_order_placed_flag(pool)

        call_entry, call_sl, put_entry, put_sl = await self.get_entry_trigger(
            crossover_data, crossunder_data)
        callbuy_signal, putbuy_signal = await self.get_entry_signal(
            call_entry, put_entry, data)
        strike_price, option_type = await self.get_strike_prices(call_entry, put_entry)

        order_id = await self.place_order(
            strike_price, option_type,
            callbuy_signal, putbuy_signal, order_placed_flag)

        # dayhigh/daylow: read from last row of indicators_data.
        # Pre-calculated per candle as cumulative max/min since 09:15 by indicator_update.py.
        last_row = data.iloc[-1]
        dayhigh  = last_row.get('dayhigh') if 'dayhigh' in data.columns else None
        daylow   = last_row.get('daylow')  if 'daylow'  in data.columns else None

        # Use the last closed candle's datetime as the row key.
        # This ensures ON DUPLICATE KEY UPDATE deduplicates correctly —
        # one row per candle regardless of how many times run_once() fires
        # within the same minute (retries, overlaps, etc.).
        candle_dt = data['datetime'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')

        row = {
            'datetime':           candle_dt,
            'order_id':           order_id,
            'call_entry_trigger': call_entry,
            'call_sl_trigger':    call_sl,
            'put_entry_trigger':  put_entry,
            'put_sl_trigger':     put_sl,
            'strike_price':       strike_price,
            'option_type':        option_type,
            'Bull':               int(data['Bull'].iloc[-1]),
            'Bear':               int(data['Bear'].iloc[-1]),
            'callbuy_signal':     callbuy_signal,
            'putbuy_signal':      putbuy_signal,
            'dayhigh':            dayhigh,
            'daylow':             daylow,
        }

        row = {k: safe(v) for k, v in row.items()}
        await self.insert_order_data(pool, row)

    # ── Entry points ─────────────────────────────────────────────────────────
    async def run(self):
        """On-demand: single evaluation cycle."""
        print(f"OptionBuying on-demand run at {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")
        pool = await get_mysql_pool()
        await self.create_placeorder_track_table(pool)
        try:
            await self.run_once(pool)
        except Exception as e:
            print(f"Error: {e}")
            raise
        finally:
            pool.close()
            await pool.wait_closed()

    async def run_scheduled(self):
        """Scheduled: runs every minute during market hours, exits at close."""
        print(f"OptionBuying scheduled job started at "
              f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")

        now = datetime.now(IST)

        if not self.is_business_day(now):
            print("Not a trading day. Exiting.")
            return

        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now > market_close:
            print(f"Market already closed ({now.strftime('%H:%M:%S')} IST). Exiting.")
            return

        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if now < market_open:
            wait_secs = (market_open - now).total_seconds()
            print(f"Market opens in {wait_secs:.0f}s. Waiting...")
            await asyncio.sleep(wait_secs)

        pool = await get_mysql_pool()
        await self.create_placeorder_track_table(pool)

        try:
            print("Market open. Starting option buying loop...")
            while self.is_market_open():
                await self.run_once(pool)

                current_time      = pd.Timestamp.now(tz='Asia/Kolkata')
                period_now        = pd.Period.now('1min')
                next_period_start = (period_now + 1).start_time.tz_localize('Asia/Kolkata')
                next_execution    = next_period_start + pd.Timedelta(seconds=6)
                sleep_secs        = (next_execution - current_time).total_seconds()

                if 0 < sleep_secs < 64:
                    print(f"Sleeping {sleep_secs:.1f}s until next candle...")
                    await asyncio.sleep(sleep_secs)

            print(f"Market closed at {datetime.now(IST).strftime('%H:%M:%S')} IST. "
                  f"OptionBuying job complete. Exiting.")
        except Exception as e:
            print(f"Error in option buying loop: {e}")
            raise
        finally:
            pool.close()
            await pool.wait_closed()


# ── Run mode ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "scheduled"

    ob = OptionBuying()
    if mode == "once":
        asyncio.run(ob.run())
    else:
        asyncio.run(ob.run_scheduled())