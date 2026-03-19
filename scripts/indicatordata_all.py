import asyncio
import aiomysql
import pytz
import pandas as pd
import talib
import numpy as np
import os
import ssl
from datetime import datetime

IST = pytz.timezone('Asia/Kolkata')

holidays = [
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-11-15", "2025-12-25",
    # 2026
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31",
    "2026-04-03", "2026-04-14", "2026-05-01", "2026-05-28",
    "2026-06-26", "2026-09-14", "2026-10-02", "2026-10-20",
    "2026-11-10", "2026-11-24", "2026-12-25",
]

db_config = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}


class IndicatorAllData:

    def __init__(self):
        pass

    # ── DB pool ───────────────────────────────────────────────────────────────

    async def get_mysql_pool(self):
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

    # ── DDL ───────────────────────────────────────────────────────────────────

    async def create_tables_if_not_exists(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('''
                    CREATE TABLE IF NOT EXISTS indicators_data (
                        datetime      DATETIME PRIMARY KEY,
                        open          DOUBLE, high  DOUBLE, low   DOUBLE, close DOUBLE,
                        ohlc4         DOUBLE,
                        linearreg     DOUBLE,
                        lri_intercept DOUBLE,
                        lri_curve     DOUBLE, lri_angle     DOUBLE,
                        lri_angle_diff DOUBLE,
                        ema26         DOUBLE,
                        wma_half      DOUBLE,  wma_full     DOUBLE,
                        hma26_5m      DOUBLE,
                        BuyCall       INTEGER, BuyPut  INTEGER,
                        Bull          INTEGER, Bear    INTEGER,
                        ATR           DOUBLE,
                        VStop2        DOUBLE,  VStop3        DOUBLE,
                        TrendUp2      INTEGER, TrendUp3      INTEGER,
                        Max           DOUBLE,  Min           DOUBLE,
                        cup_vstop2    DOUBLE,  cup_vstop3    DOUBLE,
                        cdn_vstop2    DOUBLE,  cdn_vstop3    DOUBLE,
                        dayhigh       DOUBLE,  daylow        DOUBLE,
                        st_avg        DOUBLE,  st_max        DOUBLE,
                        st_min        DOUBLE,  supertrend    DOUBLE,
                        st_dir        INTEGER
                    )
                ''')

    async def truncate_table(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('TRUNCATE TABLE indicators_data')
            await conn.commit()
        print("indicators_data truncated.")

    # ── Data fetch ────────────────────────────────────────────────────────────

    async def fetch_ohlctick_1mdata(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM ohlctick_1mdata ORDER BY datetime")
                result  = await cur.fetchall()
                columns = [col[0] for col in cur.description]
                data    = pd.DataFrame(result, columns=columns)
                data.sort_values(by='datetime', inplace=True)
                return data

    async def fetch_ohlctick_5mdata(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM ohlctick_5mdata ORDER BY datetime")
                result  = await cur.fetchall()
                columns = [col[0] for col in cur.description]
                data    = pd.DataFrame(result, columns=columns)
                data.sort_values(by='datetime', inplace=True)
                return data

    # ── HMA (5-min) → forward-fill onto 1-min index ──────────────────────────

    def _hma(self, series: pd.Series, length: int):
        """Returns (hma, wma_half, wma_full) as pd.Series — all on the same index."""
        half      = max(1, length // 2)
        sqrtn     = max(1, int(np.floor(np.sqrt(length))))
        wma_half  = talib.WMA(series, timeperiod=half)
        wma_full  = talib.WMA(series, timeperiod=length)
        diff      = 2 * wma_half - wma_full
        hma       = talib.WMA(diff, timeperiod=sqrtn)
        return hma, wma_half, wma_full

    def merge_hma26_5m(self, data_1m, data_5m):
        df5 = data_5m.copy()
        df5['datetime'] = pd.to_datetime(df5['datetime'])
        df5.set_index('datetime', inplace=True)

        hma, wma_half, wma_full = self._hma(df5['hlc3'], 26)
        df5['hma26_5m'] = hma.round(2)
        df5['wma_half'] = wma_half.round(2)   # wma(13) of hlc3 on 5m bars
        df5['wma_full'] = wma_full.round(2)   # wma(26) of hlc3 on 5m bars

        df1 = data_1m.copy()
        df1['datetime'] = pd.to_datetime(df1['datetime'])
        df1.set_index('datetime', inplace=True)

        # Forward-fill all three 5-min series onto the 1-min index
        df1['hma26_5m'] = df5['hma26_5m'].reindex(df1.index, method='ffill')
        df1['wma_half'] = df5['wma_half'].reindex(df1.index, method='ffill')
        df1['wma_full'] = df5['wma_full'].reindex(df1.index, method='ffill')

        df1.reset_index(inplace=True)
        return df1

    # ── Indicator calculations ────────────────────────────────────────────────

    async def calculate_vstop(self, data):
        data['ATR']      = talib.ATR(data['high'], data['low'], data['close'], timeperiod=252)
        data['VStop2']   = np.nan; data['VStop3']   = np.nan
        data['TrendUp2'] = True;   data['TrendUp3'] = True
        data['Max']      = data['close']; data['Min'] = data['close']

        cup_vstop2 = np.nan; cup_vstop3 = np.nan
        cdn_vstop2 = np.nan; cdn_vstop3 = np.nan
        cup_vstop2_arr = np.full(len(data), np.nan)
        cup_vstop3_arr = np.full(len(data), np.nan)
        cdn_vstop2_arr = np.full(len(data), np.nan)
        cdn_vstop3_arr = np.full(len(data), np.nan)

        for i in range(252, len(data)):
            src    = data['close'].iloc[i]
            atr_m2 = data['ATR'].iloc[i] * 2
            atr_m3 = data['ATR'].iloc[i] * 3

            data.at[i, 'Max'] = max(data['Max'].iloc[i-1], src)
            data.at[i, 'Min'] = min(data['Min'].iloc[i-1], src)

            prev2 = data['VStop2'].iloc[i-1]
            if data['TrendUp2'].iloc[i-1]:
                data.at[i, 'VStop2'] = max(prev2 if not np.isnan(prev2) else src,
                                           data['Max'].iloc[i] - atr_m2)
            else:
                data.at[i, 'VStop2'] = min(prev2 if not np.isnan(prev2) else src,
                                           data['Min'].iloc[i] + atr_m2)
            data.at[i, 'TrendUp2'] = src >= data['VStop2'].iloc[i]
            if data['TrendUp2'].iloc[i] != data['TrendUp2'].iloc[i-1]:
                data.at[i, 'Max']    = src; data.at[i, 'Min']    = src
                data.at[i, 'VStop2'] = (data['Max'].iloc[i] - atr_m2
                                        if data['TrendUp2'].iloc[i]
                                        else data['Min'].iloc[i] + atr_m2)

            prev3 = data['VStop3'].iloc[i-1]
            if data['TrendUp3'].iloc[i-1]:
                data.at[i, 'VStop3'] = max(prev3 if not np.isnan(prev3) else src,
                                           data['Max'].iloc[i] - atr_m3)
            else:
                data.at[i, 'VStop3'] = min(prev3 if not np.isnan(prev3) else src,
                                           data['Min'].iloc[i] + atr_m3)
            data.at[i, 'TrendUp3'] = src >= data['VStop3'].iloc[i]
            if data['TrendUp3'].iloc[i] != data['TrendUp3'].iloc[i-1]:
                data.at[i, 'Max']    = src; data.at[i, 'Min']    = src
                data.at[i, 'VStop3'] = (data['Max'].iloc[i] - atr_m3
                                        if data['TrendUp3'].iloc[i]
                                        else data['Min'].iloc[i] + atr_m3)

            v2_now = data['VStop2'].iloc[i]; v3_now = data['VStop3'].iloc[i]
            v2_prv = data['VStop2'].iloc[i-1]; v3_prv = data['VStop3'].iloc[i-1]

            cross_up   = (not np.isnan(v2_prv) and not np.isnan(v3_prv) and
                          v2_prv <= v3_prv and v2_now > v3_now)
            cross_down = (not np.isnan(v2_prv) and not np.isnan(v3_prv) and
                          v2_prv >= v3_prv and v2_now < v3_now)

            if cross_up:
                cup_vstop2 = v2_now; cup_vstop3 = v3_now
            if cross_down:
                cdn_vstop2 = v2_now; cdn_vstop3 = v3_now

            cup_vstop2_arr[i] = cup_vstop2; cup_vstop3_arr[i] = cup_vstop3
            cdn_vstop2_arr[i] = cdn_vstop2; cdn_vstop3_arr[i] = cdn_vstop3

        data['cup_vstop2'] = cup_vstop2_arr; data['cup_vstop3'] = cup_vstop3_arr
        data['cdn_vstop2'] = cdn_vstop2_arr; data['cdn_vstop3'] = cdn_vstop3_arr

        data[['ATR','VStop2','VStop3',
              'cup_vstop2','cup_vstop3',
              'cdn_vstop2','cdn_vstop3']] = \
            data[['ATR','VStop2','VStop3',
                  'cup_vstop2','cup_vstop3',
                  'cdn_vstop2','cdn_vstop3']].round(2)
        return data

    async def calculate_supertrend(self, data, atr_period=63, atr_mult=3.0):
        atr  = talib.ATR(data['high'], data['low'], data['close'],
                         timeperiod=atr_period) * atr_mult
        hl2  = (data['high'] + data['low']) / 2
        n    = len(data)
        upper_arr  = np.full(n, np.nan); lower_arr  = np.full(n, np.nan)
        os_arr     = np.zeros(n, dtype=int)
        spt_arr    = np.full(n, np.nan)
        st_max_arr = np.full(n, np.nan); st_min_arr = np.full(n, np.nan)

        close = data['close'].to_numpy()
        hl2v  = hl2.to_numpy()
        atrv  = atr.to_numpy()

        for i in range(1, n):
            if np.isnan(atrv[i]):
                continue
            up = hl2v[i] + atrv[i]; dn = hl2v[i] - atrv[i]

            prev_upper = upper_arr[i-1]
            upper_arr[i] = (min(up, prev_upper) if not np.isnan(prev_upper)
                            and close[i-1] < prev_upper else up)

            prev_lower = lower_arr[i-1]
            lower_arr[i] = (max(dn, prev_lower) if not np.isnan(prev_lower)
                            and close[i-1] > prev_lower else dn)

            if   close[i] > upper_arr[i]: os_arr[i] = 1
            elif close[i] < lower_arr[i]: os_arr[i] = 0
            else:                         os_arr[i] = os_arr[i-1]

            spt_arr[i] = lower_arr[i] if os_arr[i] == 1 else upper_arr[i]

            prev_spt = spt_arr[i-1]
            crossed  = (not np.isnan(prev_spt) and
                        ((close[i-1] <= prev_spt and close[i] > spt_arr[i]) or
                         (close[i-1] >= prev_spt and close[i] < spt_arr[i])))

            prev_stmax = st_max_arr[i-1]
            if   np.isnan(prev_stmax):  st_max_arr[i] = close[i]
            elif crossed:               st_max_arr[i] = max(prev_stmax, close[i])
            elif os_arr[i] == 1:        st_max_arr[i] = max(close[i], prev_stmax)
            else:                       st_max_arr[i] = min(spt_arr[i], prev_stmax)

            prev_stmin = st_min_arr[i-1]
            if   np.isnan(prev_stmin):  st_min_arr[i] = close[i]
            elif crossed:               st_min_arr[i] = min(prev_stmin, close[i])
            elif os_arr[i] == 0:        st_min_arr[i] = min(close[i], prev_stmin)
            else:                       st_min_arr[i] = max(spt_arr[i], prev_stmin)

        data['supertrend'] = spt_arr;  data['st_dir']  = os_arr
        data['st_max']     = st_max_arr; data['st_min'] = st_min_arr
        data['st_avg']     = np.where(
            ~np.isnan(st_max_arr) & ~np.isnan(st_min_arr),
            (st_max_arr + st_min_arr) / 2, np.nan)
        data[['supertrend','st_max','st_min','st_avg']] = \
            data[['supertrend','st_max','st_min','st_avg']].round(2)
        return data

    async def calculate_additional_indicators(self, data):
        """Compute pure price-derived indicators that do NOT depend on hma26_5m."""
        data['linearreg']     = talib.LINEARREG(data['close'], timeperiod=63)
        data['lri_intercept'] = talib.LINEARREG_INTERCEPT(data['close'], timeperiod=63)
        data['lri_curve']     = data['linearreg'] - data['lri_intercept']
        data['lri_angle']      = talib.LINEARREG_ANGLE(data['close'], timeperiod=63)
        data['lri_angle_diff'] = (data['lri_angle'] - data['lri_angle'].shift(1))
        data['ema26']          = talib.EMA(data['close'], timeperiod=26)

        data[['linearreg','lri_intercept',
              'lri_curve','lri_angle','lri_angle_diff','ema26']] = \
            data[['linearreg','lri_intercept',
                  'lri_curve','lri_angle','lri_angle_diff','ema26']].round(4)

        # dayhigh / daylow — correct multi-day cumulative per calendar day
        data['_dt']       = pd.to_datetime(data['datetime'])
        data['_day_open'] = data['_dt'].dt.normalize() + pd.Timedelta(hours=9, minutes=15)
        mask = data['_dt'] >= data['_day_open']
        data['dayhigh'] = data['high'].where(mask).groupby(data['_dt'].dt.date).cummax()
        data['daylow']  = data['low'].where(mask).groupby(data['_dt'].dt.date).cummin()
        data[['dayhigh','daylow']] = data[['dayhigh','daylow']].round(2)
        data.drop(columns=['_dt','_day_open'], inplace=True)
        return data

    def calculate_signals(self, data):
        """
        Compute BuyCall / BuyPut / Bull / Bear.
        Must be called AFTER merge_hma26_5m so that hma26_5m is available.

        Long  (BuyCall): (linearreg > hma26_5m OR ema26 > hma26_5m)
                          AND st_dir == 1
                          AND lri_angle is rising (lri_angle > prev lri_angle)

        Short (BuyPut):  (linearreg < hma26_5m OR ema26 < hma26_5m)
                          AND st_dir == 0
                          AND lri_angle is falling (lri_angle < prev lri_angle)
        """
        angle_rising  = data['lri_angle_diff'] > 0
        angle_falling = data['lri_angle_diff'] < 0

        price_above_hma = (data['linearreg'] > data['hma26_5m']) | (data['ema26'] > data['hma26_5m'])
        price_below_hma = (data['linearreg'] < data['hma26_5m']) | (data['ema26'] < data['hma26_5m'])

        data['BuyCall'] = (price_above_hma & (data['st_dir'] == 1) & angle_rising).astype(int)
        data['BuyPut']  = (price_below_hma & (data['st_dir'] == 0) & angle_falling).astype(int)

        data['Bull'] = (
            (data['BuyCall'] == 1) &
            (data['TrendUp3'] == 1) &
            (data['close'] > data['open'])
        ).astype(int)

        data['Bear'] = (
            (data['BuyPut'] == 1) &
            (data['TrendUp3'] == 0) &
            (data['close'] < data['open'])
        ).astype(int)

        return data

    # ── DB save ───────────────────────────────────────────────────────────────

    async def save_indicators_to_db(self, pool, data):
        data = [[None if pd.isna(x) else x for x in row] for row in data]
        non_zero_data = [row for row in data
                         if any(row[i] not in (0, None) for i in [1, 2, 3, 4])]

        replace_query = '''
            REPLACE INTO indicators_data (
                datetime, open, high, low, close, ohlc4,
                linearreg, lri_intercept, lri_curve, lri_angle, lri_angle_diff, ema26,
                wma_half, wma_full,
                hma26_5m,
                BuyCall, BuyPut, Bull, Bear,
                ATR, VStop2, VStop3, TrendUp2, TrendUp3, Max, Min,
                cup_vstop2, cup_vstop3, cdn_vstop2, cdn_vstop3,
                dayhigh, daylow,
                st_avg, st_max, st_min, supertrend, st_dir
            ) VALUES (
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,
                %s,%s,%s,%s,%s
            )
        '''
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(replace_query, non_zero_data)

    # ── Entry point ───────────────────────────────────────────────────────────

    async def run(self):
        pool = await self.get_mysql_pool()
        await self.create_tables_if_not_exists(pool)
        await self.truncate_table(pool)

        try:
            ohlc_1m = await self.fetch_ohlctick_1mdata(pool)
            if len(ohlc_1m) < 252:
                print(f"Not enough 1-min data: {len(ohlc_1m)} rows (need 252+). Exiting.")
                return

            ohlc_5m = await self.fetch_ohlctick_5mdata(pool)
            if len(ohlc_5m) < 26:
                print(f"Not enough 5-min data: {len(ohlc_5m)} rows (need 26+). Exiting.")
                return

            indicator_data = await self.calculate_vstop(ohlc_1m)
            indicator_data = await self.calculate_supertrend(indicator_data)
            indicator_data = await self.calculate_additional_indicators(indicator_data)

            # Merge 5-min HMA (and wma_half / wma_full) onto 1-min frame
            indicator_data = self.merge_hma26_5m(indicator_data, ohlc_5m)

            # Signals depend on hma26_5m — compute AFTER the merge
            indicator_data = self.calculate_signals(indicator_data)

            cols = [
                'datetime', 'open', 'high', 'low', 'close', 'ohlc4',
                'linearreg', 'lri_intercept', 'lri_curve', 'lri_angle', 'lri_angle_diff', 'ema26',
                'wma_half', 'wma_full',
                'hma26_5m',
                'BuyCall', 'BuyPut', 'Bull', 'Bear',
                'ATR', 'VStop2', 'VStop3', 'TrendUp2', 'TrendUp3', 'Max', 'Min',
                'cup_vstop2', 'cup_vstop3', 'cdn_vstop2', 'cdn_vstop3',
                'dayhigh', 'daylow',
                'st_avg', 'st_max', 'st_min', 'supertrend', 'st_dir',
            ]
            await self.save_indicators_to_db(pool, indicator_data[cols].to_numpy())
            print(f"Indicator calculation complete — "
                  f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")

        except Exception as e:
            print(f"Error during indicator calculation: {e}")
            raise
        finally:
            pool.close()
            await pool.wait_closed()


if __name__ == "__main__":
    indicator_all = IndicatorAllData()
    asyncio.run(indicator_all.run())