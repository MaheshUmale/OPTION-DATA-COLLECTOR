"""
Backfill historical option chain data from Trendlyne SmartOptions API.
This populates a local SQLite database with 1-minute interval historical data.
"""
import requests
import time
import sqlite3
import os
import json
import argparse
from datetime import datetime, timedelta, date
 

# Upstox SDK
try:
    import upstox_client
    from upstox_client.rest import ApiException
   
    UPSTOX_AVAILABLE = True
except ImportError:
    UPSTOX_AVAILABLE = False
    print("[WARN] Upstox SDK not found. Option Chain will rely on Trendlyne only.")

# ==========================================================================
# 1. DATABASE LAYER (SQLite)
# ==========================================================================
class OptionDatabase:
    def __init__(self, master_db_path="sos_master_data.db"):
        self.master_db_path = master_db_path
        self._init_master_db()

    def _get_timeseries_db_path(self):
        """Returns the DB path for the current month, e.g., 'sos_timeseries_2023_12.db'"""
        return f"sos_timeseries_{datetime.now().strftime('%Y_%m')}.db"

    def _get_connection(self, db_path):
        """Establishes a connection and enables WAL mode."""
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_master_db(self):
        conn = self._get_connection(self.master_db_path)
        cursor = conn.cursor()
        # This table is for static, slowly changing data
        cursor.execute('''CREATE TABLE IF NOT EXISTS pcr_history (
                            symbol TEXT,
                            date TEXT,
                            pcr REAL,
                            call_oi INTEGER,
                            put_oi INTEGER,
                            PRIMARY KEY (symbol, date)
                          )''')
        conn.commit()
        conn.close()

    def _init_timeseries_db(self, db_path):
        """Initializes tables in the monthly timeseries database."""
        conn = self._get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS option_aggregates (
                            symbol TEXT, date TEXT, timestamp TEXT, expiry TEXT,
                            call_oi INTEGER, put_oi INTEGER, pcr REAL,
                            PRIMARY KEY (symbol, date, timestamp)
                          )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS option_chain_details (
                            symbol TEXT, date TEXT, timestamp TEXT, strike REAL,
                            call_oi INTEGER, put_oi INTEGER, call_oi_chg INTEGER, put_oi_chg INTEGER,
                            PRIMARY KEY (symbol, date, timestamp, strike)
                          )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS market_depth_history (
                            timestamp INTEGER, symbol TEXT, rvol REAL, pcr REAL, adv_dec_ratio REAL,
                            PRIMARY KEY (timestamp, symbol)
                          )''')
        conn.commit()
        conn.close()

    def save_snapshot(self, symbol, trading_date, timestamp, expiry, aggregates, details):
        db_path = self._get_timeseries_db_path()
        self._init_timeseries_db(db_path) # Ensures table exists
        conn = self._get_connection(db_path)
        cursor = conn.cursor()
        try:
            # Save Aggregates
            cursor.execute("""INSERT OR REPLACE INTO option_aggregates
                              VALUES (?, ?, ?, ?, ?, ?, ?)""",
                           (symbol, trading_date, timestamp, expiry,
                            aggregates['call_oi'], aggregates['put_oi'], aggregates['pcr']))

            # Save Details
            for strike, d in details.items():
                cursor.execute("""INSERT OR REPLACE INTO option_chain_details
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                               (symbol, trading_date, timestamp, float(strike),
                                d['call_oi'], d['put_oi'], d['call_oi_chg'], d['put_oi_chg']))

            conn.commit()
        except Exception as e:
            print(f"[DB ERROR] {e}")
            conn.rollback()
        finally:
            conn.close()

    def save_market_depth(self, ts, symbol, rvol, pcr, ratio):
        db_path = self._get_timeseries_db_path()
        self._init_timeseries_db(db_path)
        conn = self._get_connection(db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT OR REPLACE INTO market_depth_history VALUES (?, ?, ?, ?, ?)",
                           (ts, symbol, rvol, pcr, ratio))
            conn.commit()
        except Exception as e:
            print(f"[DB DEPTH ERROR] {e}")
            conn.rollback()
        finally:
            conn.close()

    def save_breadth(self, trading_date, timestamp, data):
        # This data is transient and might not need historical persistence in this context
        # If required, it should go into the timeseries DB.
        # For now, this can be a no-op or write to a log.
        pass

    def get_latest_breadth(self):
        # This would require querying the latest timeseries DB, which is complex.
        # Market breadth is better handled live.
        return None

    def get_latest_aggregates(self, symbol):
        db_path = self._get_timeseries_db_path()
        if not os.path.exists(db_path): return None
        conn = self._get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("""SELECT * FROM option_aggregates
                          WHERE symbol=?
                          ORDER BY date DESC, timestamp DESC LIMIT 1""", (symbol,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return {
                'symbol': row[0],
                'date': row[1],
                'timestamp': row[2],
                'expiry': row[3],
                'call_oi': row[4],
                'put_oi': row[5],
                'pcr': row[6]
            }
        return None

    def get_latest_chain(self, symbol):
        db_path = self._get_timeseries_db_path()
        if not os.path.exists(db_path): return []
        conn = self._get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("""SELECT date, timestamp FROM option_chain_details
                          WHERE symbol=?
                          ORDER BY date DESC, timestamp DESC LIMIT 1""", (symbol,))
        last = cursor.fetchone()
        if not last:
            conn.close()
            return []

        d, ts = last
        cursor.execute("""SELECT * FROM option_chain_details
                          WHERE symbol=? AND date=? AND timestamp=?""", (symbol, d, ts))
        rows = cursor.fetchall()
        conn.close()

        return [{
            'strike': r[3], 'call_oi': r[4], 'put_oi': r[5],
            'call_oi_chg': r[6], 'put_oi_chg': r[7]
        } for r in rows]

    def save_daily_stats(self, symbol, trading_date, pcr, call_oi, put_oi):
        conn = self._get_connection(self.master_db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT OR REPLACE INTO pcr_history VALUES (?, ?, ?, ?, ?)",
                           (symbol, trading_date, pcr, call_oi, put_oi))
            conn.commit()
        except Exception as e:
            print(f"[DB STATS ERROR] {e}")
        finally:
            conn.close()

    def get_pcr_history(self, symbol, days=30):
        conn = self._get_connection(self.master_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pcr_history WHERE symbol=? ORDER BY date DESC LIMIT ?", (symbol, days))
        rows = cursor.fetchall()
        conn.close()
        return rows

# Keep a cache to avoid repeated API calls
STOCK_ID_CACHE = {}
EXPIRY_CACHE = {}  # Cache for expiry dates
DB = OptionDatabase()

def get_stock_id_for_symbol(symbol):
    """Automatically lookup Trendlyne stock ID for a given symbol"""
    if symbol in STOCK_ID_CACHE:
        return STOCK_ID_CACHE[symbol]

    search_url = "https://smartoptions.trendlyne.com/phoenix/api/search-contract-stock/"
    params = {'query': symbol.lower()}

    try:
        response = requests.get(search_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data and 'body' in data and 'data' in data['body'] and len(data['body']['data']) > 0:
            # Match strictly or take first
            for item in data['body']['data']:
                if item.get('stock_code', '').upper() == symbol.upper():
                    stock_id = item['stock_id']
                    STOCK_ID_CACHE[symbol] = stock_id
                    return stock_id

            stock_id = data['body']['data'][0]['stock_id']
            STOCK_ID_CACHE[symbol] = stock_id
            return stock_id
        return None
    except Exception as e:
        print(f"[ERROR] Stock Lookup {symbol}: {e}")
        return None

def backfill_from_trendlyne(symbol, stock_id, expiry_date_str, timestamp_snapshot):
    """Fetch and save historical OI data from Trendlyne for a specific timestamp snapshot"""

    url = f"https://smartoptions.trendlyne.com/phoenix/api/live-oi-data/"
    params = {
        'stockId': stock_id,
        'expDateList': expiry_date_str,
        'minTime': "09:15",
        'maxTime': timestamp_snapshot
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"Fetched data for {symbol} at {timestamp_snapshot} from Trendlyne.")
        # print(data)
        if data['head']['status'] != '0':
            return False

        body = data['body']
        oi_data = body.get('oiData', {})
        input_data = body.get('inputData', {})

        trading_date = input_data.get('tradingDate', date.today().strftime("%Y-%m-%d"))
        expiry = input_data.get('expDateList', [expiry_date_str])[0]

        total_call_oi = 0
        total_put_oi = 0
        details = {}

        for strike_str, strike_data in oi_data.items():
            c_oi = int(strike_data.get('callOi', 0))
            p_oi = int(strike_data.get('putOi', 0))
            total_call_oi += c_oi
            total_put_oi += p_oi

            details[strike_str] = {
                'call_oi': c_oi,
                'put_oi': p_oi,
                'call_oi_chg': int(strike_data.get('callOiChange', 0)),
                'put_oi_chg': int(strike_data.get('putOiChange', 0))
            }

            print(f"Strike {strike_str}: Call OI={c_oi}, Put OI={p_oi}")
        pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 1.0

        aggregates = {
            'call_oi': total_call_oi,
            'put_oi': total_put_oi,
            'pcr': pcr
        }

        DB.save_snapshot(symbol, trading_date, timestamp_snapshot, expiry, aggregates, details)
        return True

    except Exception as e:
        print(f"[ERROR] Fetch {symbol} @ {timestamp_snapshot}: {e}")
        return False
 
def fetch_live_snapshot(symbol):

    """
    Fetches live data for symbol, saves to DB, and returns the chain.
    Priority: Upstox -> Trendlyne.
    """
     
    # 2. Fallback to Trendlyne
    stock_id = get_stock_id_for_symbol(symbol)
    if not stock_id:
        return []

    # Get Expiry (cached)
    expiry = EXPIRY_CACHE.get(symbol)
    # Simple validation: if expiry is in the past, refresh
    if expiry:
        try:
             exp_date = datetime.strptime(expiry, "%Y-%m-%d").date()
             if date.today() > exp_date:
                 expiry = None
        except:
             expiry = None

    if not expiry:
        try:
             expiry_url = f"https://smartoptions.trendlyne.com/phoenix/api/fno/get-expiry-dates/?mtype=options&stock_id={stock_id}"
             resp = requests.get(expiry_url, timeout=5)
             expiry_list = resp.json().get('body', {}).get('expiryDates', [])
             if expiry_list:
                 expiry = expiry_list[0]
                 EXPIRY_CACHE[symbol] = expiry
        except Exception as e:
             print(f"[WARN] Failed to fetch expiry for {symbol}: {e}")
             pass

    if not expiry:
        return DB.get_latest_chain(symbol)

    # Timestamp
    ts = datetime.now().strftime("%H:%M")

    # Fetch and Save
    # This calls the existing backfill logic which SAVES to DB
    success = backfill_from_trendlyne(symbol, stock_id, expiry, ts)

    # Return latest from DB (whether update succeeded or not, we return best available)
    return DB.get_latest_chain(symbol)

def generate_time_intervals(start_time="09:15", end_time="15:30", interval_minutes=1):
    """Generate time strings in HH:MM format with 1-minute default"""
    start = datetime.strptime(start_time, "%H:%M")
    end = datetime.strptime(end_time, "%H:%M")
    current = start
    times = []
    while current <= end:
        times.append(current.strftime("%H:%M"))
        current += timedelta(minutes=interval_minutes)
    return times

def run_backfill(symbols_list=None, full_run=False):
    if not symbols_list:
        symbols_list = ["NIFTY", "BANKNIFTY", "RELIANCE", "SBIN", "HDFCBANK"]

    print("=" * 60)
    if full_run:
        print("STARTING TRENDLYNE BACKFILL (FULL DAY)")
    else:
        print("STARTING TRENDLYNE BACKFILL (LAST 15 MINS)")
    print("=" * 60)

    now = datetime.now()
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)

    start_time_str = "09:15"

    if now < market_open:
        end_time_str = "15:30"
    elif now > now.replace(hour=15, minute=30, second=0, microsecond=0):
        end_time_str = "15:30"
    else:
        end_time_str = now.strftime("%H:%M")

    if not full_run:
        end_dt = datetime.strptime(end_time_str, "%H:%M")
        start_dt = end_dt - timedelta(minutes=15)
        start_time_str = start_dt.strftime("%H:%M")

        # Ensure we don't request data from before market open
        if start_dt.time() < market_open.time():
            start_time_str = "09:15"

    time_slots = generate_time_intervals(start_time=start_time_str, end_time=end_time_str)
    print(f"Time Slots: {len(time_slots)} ({start_time_str} to {end_time_str}) | Symbols: {len(symbols_list)}")

    for symbol in symbols_list:
        stock_id = get_stock_id_for_symbol(symbol)
        if not stock_id:
            print(f"[SKIP] No Stock ID for {symbol}")
            continue

        try:
            # Fetch Expiry
            expiry_url = f"https://smartoptions.trendlyne.com/phoenix/api/fno/get-expiry-dates/?mtype=options&stock_id={stock_id}"
            resp = requests.get(expiry_url, timeout=10)
            expiry_list = resp.json().get('body', {}).get('expiryDates', [])
            if not expiry_list:
                print(f"[SKIP] No Expiry for {symbol}")
                continue

            nearest_expiry = expiry_list[0]
            print(f"Syncing {symbol} | Expiry: {nearest_expiry}...")

            success_count = 0
            for ts in time_slots:
                if backfill_from_trendlyne(symbol, stock_id, nearest_expiry, ts):
                    success_count += 1

                # Sleep briefly to avoid rate limits
                if success_count % 10 == 0:
                    time.sleep(0.1)

            print(f"[OK] {symbol}: Captured {success_count}/{len(time_slots)} points")
        except Exception as e:
            print(f"[FAIL] {symbol}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trendlyne Data Backfill Script")
    parser.add_argument('--full', action='store_true', help='Perform a full-day backfill instead of the default last 15 minutes.')
    args = parser.parse_args()

    target_symbols = ["NIFTY", "BANKNIFTY", "RELIANCE"]
    run_backfill(target_symbols, full_run=args.full)
    print("\n[DB PATH]:", os.path.abspath("sos_master_data.db"))
