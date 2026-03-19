import os
import ssl
import asyncio
import aiomysql
import pytz
import pandas as pd
from datetime import datetime, time, timedelta
from breeze_connect import BreezeConnect

IST = pytz.timezone('Asia/Kolkata')

# ── NSE Holidays ─────────────────────────────────────────────────────────────
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

# ── Config from environment ───────────────────────────────────────────────────
db_config = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

REQUIRED_COLUMNS = [
    'datetime', 'orderExchangeCode', 'stockCode', 'productType', 'optionType',
    'strikePrice', 'expiryDate', 'orderValidDate', 'orderFlow', 'limitMarketFlag',
    'orderType', 'limitRate', 'orderStatus', 'orderReference', 'executedQuantity',
]


# ════════════════════════════════════════════════════════════════════════════
class OrderNotification:

    def __init__(self):
        self.api  = None   # Initialised in run() after env vars confirmed
        self.pool = None   # Shared pool — created once in run()
        self._loop = None  # Event loop reference for WebSocket callback

    # ── Market helpers ────────────────────────────────────────────────────────
    def is_market_open(self):
        now        = datetime.now(IST)
        open_time  = datetime.combine(now.date(), time(9, 15)).replace(tzinfo=IST)
        close_time = datetime.combine(now.date(), time(15, 30)).replace(tzinfo=IST)
        is_open    = open_time <= now <= close_time
        # print(f"Market open: {is_open} | IST: {now.strftime('%H:%M:%S')}")
        return is_open

    def is_business_day(self, date):
        return date.weekday() < 5 and date.strftime('%Y-%m-%d') not in holidays

    # ── Breeze API ────────────────────────────────────────────────────────────
    def init_breeze(self):
        api_key       = os.getenv("API_KEY")
        api_secret    = os.getenv("API_SECRET")
        session_token = os.getenv("SESSION_TOKEN")

        if not all([api_key, api_secret, session_token]):
            raise EnvironmentError(
                "Missing Breeze credentials: API_KEY, API_SECRET, SESSION_TOKEN must be set."
            )

        self.api = BreezeConnect(api_key=api_key)
        self.api.generate_session(
            api_secret=api_secret,
            session_token=str(session_token)
        )
        # print("Breeze API session initialised.")

    # ── MySQL pool ────────────────────────────────────────────────────────────
    async def init_pool(self):
        ssl_ctx = None
        ca_path = os.path.join(os.path.dirname(__file__), '..', 'ca.pem')
        if os.path.exists(ca_path):
            ssl_ctx = ssl.create_default_context(cafile=ca_path)

        self.pool = await aiomysql.create_pool(
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
        # print("MySQL pool created.")

    async def create_tables_if_not_exists(self):
        query = '''
            CREATE TABLE IF NOT EXISTS `order_notification` (
                `datetime`          DATETIME     NOT NULL,
                `orderExchangeCode` TEXT,
                `stockCode`         TEXT,
                `productType`       TEXT,
                `optionType`        TEXT,
                `strikePrice`       INT          DEFAULT NULL,
                `expiryDate`        TEXT,
                `orderValidDate`    TEXT,
                `orderFlow`         TEXT,
                `limitMarketFlag`   TEXT,
                `orderType`         TEXT,
                `limitRate`         DOUBLE       DEFAULT NULL,
                `orderStatus`       TEXT,
                `orderReference`    BIGINT       DEFAULT NULL,
                `executedQuantity`  INT          DEFAULT NULL,
                PRIMARY KEY (`datetime`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
        # print("Table verified: order_notification")

    # ── Tick processing ───────────────────────────────────────────────────────
    async def insert_order_notification(self, record: dict):
        """Insert a single order notification record into the DB."""
        query = '''
            REPLACE INTO `order_notification` (
                `datetime`, `orderExchangeCode`, `stockCode`,
                `productType`, `optionType`, `strikePrice`, `expiryDate`,
                `orderValidDate`, `orderFlow`, `limitMarketFlag`,
                `orderType`, `limitRate`, `orderStatus`,
                `orderReference`, `executedQuantity`
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        try:
            # ICICI sends strikePrice and limitRate multiplied by 100
            strike = int(float(record.get('strikePrice', 0) or 0) / 100)
            rate   = float(float(record.get('limitRate', 0) or 0) / 100)

            values = (
                datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
                record.get('orderExchangeCode'),
                record.get('stockCode'),
                record.get('productType'),
                record.get('optionType'),
                strike,
                record.get('expiryDate'),
                record.get('orderValidDate'),
                record.get('orderFlow'),
                record.get('limitMarketFlag'),
                record.get('orderType'),
                rate,
                record.get('orderStatus'),
                record.get('orderReference'),
                record.get('executedQuantity'),
            )

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, values)
                    await conn.commit()
            # print(f"Order notification saved: {record.get('orderReference')} "
            #       f"| {record.get('orderFlow')} | {record.get('orderStatus')}")

        except Exception as e:
            print(f"Error inserting order notification: {e} | record: {record}")

    async def process_tick(self, tick):
        """Validate and insert a single tick from the WebSocket callback."""
        print(f"Tick received: {tick}")
        if not tick:
            return

        # Normalise to list
        records = [tick] if isinstance(tick, dict) else tick

        for record in records:
            if not isinstance(record, dict):
                # print(f"Unexpected tick format: {record}")
                continue

            # Check required columns
            missing = [c for c in REQUIRED_COLUMNS if c not in record and c != 'datetime']
            if missing:
                # print(f"Skipping tick — missing columns: {missing}")
                continue

            await self.insert_order_notification(record)

    def on_ticks(self, tick):
        """
        Sync callback registered with Breeze WebSocket.
        Schedules the async handler onto the running event loop.
        """
        if self._loop and not self._loop.is_closed():
            asyncio.run_coroutine_threadsafe(self.process_tick(tick), self._loop)
        else:
            print("Event loop not available — tick dropped.")

    # ── WebSocket connection ──────────────────────────────────────────────────
    async def connect_websocket(self):
        print("Connecting to Breeze WebSocket...")
        self.api.ws_connect()
        self.api.on_ticks = self.on_ticks
        self.api.subscribe_feeds(get_order_notification=True)
        print("Subscribed to order notifications feed.")

    async def disconnect_websocket(self):
        print("Disconnecting from Breeze WebSocket...")
        try:
            self.api.unsubscribe_feeds(get_order_notification=True)
            disconnected = self.api.ws_disconnect()
            print("WebSocket disconnected." if disconnected else "WebSocket disconnect error.")
        except Exception as e:
            print(f"Error during WebSocket disconnect: {e}")

    # ── Main run ──────────────────────────────────────────────────────────────
    async def run(self):
        print(f"OrderNotification job started at "
              f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")

        now = datetime.now(IST)

        # Exit if not a business day
        if not self.is_business_day(now):
            print("Not a trading day. Exiting.")
            return

        # Exit if market already closed
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now > market_close:
            print(f"Market already closed ({now.strftime('%H:%M:%S')} IST). Exiting.")
            return

        # Wait if before market open
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if now < market_open:
            wait_secs = (market_open - now).total_seconds()
            print(f"Market opens in {wait_secs:.0f}s. Waiting...")
            await asyncio.sleep(wait_secs)

        # Initialise Breeze API and DB pool
        self.init_breeze()
        await self.init_pool()
        await self.create_tables_if_not_exists()

        # Store event loop reference for WebSocket callback thread
        self._loop = asyncio.get_event_loop()

        try:
            await self.connect_websocket()
            # print("Listening for order notifications...")

            # Keep alive until market closes — check every second
            while self.is_market_open():
                await asyncio.sleep(1)

            await self.disconnect_websocket()
            print(f"Market closed at {datetime.now(IST).strftime('%H:%M:%S')} IST. "
                  f"OrderNotification job complete. Exiting.")

        except Exception as e:
            print(f"Error in run loop: {e}")
            raise
        finally:
            await self.disconnect_websocket()
            if self.pool:
                self.pool.close()
                await self.pool.wait_closed()


# ── Entry point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    order_stream = OrderNotification()
    asyncio.run(order_stream.run())