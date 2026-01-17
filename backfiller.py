import time
import json
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from clients import TrendlyneClient, TVClient, NSEClient
from database import Database
import sys
import os

class Backfiller:
    def __init__(self, config_path="config.json"):
        if not os.path.exists(config_path):
            print(f"Config file not found: {config_path}")
            sys.exit(1)

        with open(config_path, "r") as f:
            self.config = json.load(f)

        self.tv = TVClient()
        self.tl = TrendlyneClient()
        self.nse = NSEClient()
        db_name = self.config.get("db_name", "options_data.db")
        self.db = Database(db_name)
        self.symbols = self.config.get("symbols", ["NSE|INDEX|NIFTY", "NSE|INDEX|BANKNIFTY"])
        print(f"Using database: {os.path.abspath(db_name)}")

    def get_clean_symbol(self, symbol):
        return symbol.split('|')[-1] if '|' in symbol else symbol

    def get_strike_gap(self, clean_symbol):
        gaps = self.config.get("strike_gaps", {})
        return gaps.get(clean_symbol, 100)

    def backfill_date(self, date_str):
        print(f"--- Starting Backfill for {date_str} ---")

        for symbol in self.symbols:
            clean_symbol = self.get_clean_symbol(symbol)

            # Skip if already in DB for this date
            with self.db._get_connection() as conn:
                count = conn.execute("SELECT COUNT(*) FROM market_data WHERE symbol=? AND timestamp LIKE ?", (symbol, f"{date_str}%")).fetchone()[0]
                if count >= 370: # Roughly full day
                    print(f"\n[Skipping {symbol}] Already has {count} records for {date_str}")
                    continue

            print(f"\n[Processing {symbol}]")

            # 1. Fetch OHLCV from TV
            print(f"Fetching OHLCV for {clean_symbol} from TradingView...")
            ohlcv_df = self.tv.get_ohlcv(clean_symbol, n_bars=5000)

            if ohlcv_df is not None and not ohlcv_df.empty:
                ohlcv_df = ohlcv_df[ohlcv_df.index.strftime('%Y-%m-%d') == date_str]
                print(f"Bars after filtering for {date_str}: {len(ohlcv_df)}")
            else:
                print("No data returned from TradingView.")
                continue

            if ohlcv_df.empty:
                print(f"No OHLCV data for {date_str}")
                continue

            # 2. Get Stock ID and Expiry
            stock_id = self.tl.get_stock_id_for_symbol(clean_symbol)
            if not stock_id:
                print(f"Could not find stock ID for {clean_symbol}")
                continue

            expiries = self.tl.get_expiry_dates(stock_id)
            current_expiry = None
            for exp in expiries:
                if exp >= date_str:
                    current_expiry = exp
                    break
            if not current_expiry:
                print(f"Could not determine expiry for {date_str}")
                continue

            print(f"Date: {date_str}, Expiry: {current_expiry}, Stock ID: {stock_id}")

            # 3. Generate time slots (every minute)
            start_dt = datetime.strptime(f"{date_str} 09:15", "%Y-%m-%d %H:%M")
            end_dt = datetime.strptime(f"{date_str} 15:30", "%Y-%m-%d %H:%M")
            time_slots = []
            curr = start_dt
            while curr <= end_dt:
                time_slots.append(curr.strftime("%H:%M"))
                curr += timedelta(minutes=1)

            # 4. Fetch Snapshots and collect data
            all_market_records = []
            all_option_records = []

            # Map OHLCV by time for easy lookup
            ohlcv_map = {ts.strftime("%H:%M"): row for ts, row in ohlcv_df.iterrows()}

            prev_pcr = None

            for ts_hhmm in time_slots:
                snapshot = self.tl.get_oi_snapshot(stock_id, current_expiry, ts_hhmm)

                if snapshot:
                    oi_data = snapshot.get('oiData', {})
                    total_call_oi = 0
                    total_put_oi = 0

                    timestamp_full = f"{date_str} {ts_hhmm}:00"

                    for strike_str, strike_data in oi_data.items():
                        c_oi = float(strike_data.get('callOi', 0))
                        p_oi = float(strike_data.get('putOi', 0))
                        total_call_oi += c_oi
                        total_put_oi += p_oi

                        # Add option records
                        all_option_records.append({
                            'timestamp': timestamp_full,
                            'symbol': symbol,
                            'strike_price': float(strike_str),
                            'expiry_date': current_expiry,
                            'option_type': 'CE',
                            'price': strike_data.get('callClose', 0),
                            'oi': c_oi,
                            'oi_change': float(strike_data.get('callOiChange', 0))
                        })
                        all_option_records.append({
                            'timestamp': timestamp_full,
                            'symbol': symbol,
                            'strike_price': float(strike_str),
                            'expiry_date': current_expiry,
                            'option_type': 'PE',
                            'price': strike_data.get('putClose', 0),
                            'oi': p_oi,
                            'oi_change': float(strike_data.get('putOiChange', 0))
                        })

                    current_pcr = round(total_put_oi / total_call_oi, 4) if total_call_oi > 0 else 1.0
                    pcr_change = (current_pcr - prev_pcr) if prev_pcr is not None else 0
                    prev_pcr = current_pcr

                    # Match with OHLCV
                    ohlc = ohlcv_map.get(ts_hhmm)
                    market_record = {
                        'timestamp': timestamp_full,
                        'symbol': symbol,
                        'spot_price': ohlc['close'] if ohlc is not None else None,
                        'open': ohlc['open'] if ohlc is not None else None,
                        'high': ohlc['high'] if ohlc is not None else None,
                        'low': ohlc['low'] if ohlc is not None else None,
                        'close': ohlc['close'] if ohlc is not None else None,
                        'volume': ohlc['volume'] if ohlc is not None else None,
                        'total_pcr': current_pcr,
                        'pcr_change': pcr_change
                    }
                    all_market_records.append(market_record)
                else:
                    # If snapshot fails, still try to save OHLCV if available
                    ohlc = ohlcv_map.get(ts_hhmm)
                    if ohlc is not None:
                        timestamp_full = f"{date_str} {ts_hhmm}:00"
                        all_market_records.append({
                            'timestamp': timestamp_full,
                            'symbol': symbol,
                            'spot_price': ohlc['close'],
                            'open': ohlc['open'],
                            'high': ohlc['high'],
                            'low': ohlc['low'],
                            'close': ohlc['close'],
                            'volume': ohlc['volume'],
                            'total_pcr': None,
                            'pcr_change': None
                        })

                # Small delay to prevent hitting rate limits
                time.sleep(0.05)

            # 5. Bulk Save
            if all_market_records:
                print(f"Saving {len(all_market_records)} market data records...")
                for record in all_market_records:
                    self.db.save_market_data(record)

            if all_option_records:
                print(f"Saving {len(all_option_records)} option records...")
                self.db.save_option_data(all_option_records)

        print(f"\n--- Backfill complete for {date_str} ---")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backfiller.py YYYY-MM-DD")
    else:
        date = sys.argv[1]
        bf = Backfiller()
        bf.backfill_date(date)
