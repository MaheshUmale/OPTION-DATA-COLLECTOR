import time
import json
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from clients import TrendlyneClient, TVClient
from database import Database
import sys

class Backfiller:
    def __init__(self, config_path="config.json"):
        with open(config_path, "r") as f:
            self.config = json.load(f)

        self.tv = TVClient()
        self.tl = TrendlyneClient()
        self.db = Database(self.config.get("db_name", "options_data.db"))
        self.symbols = self.config.get("symbols", ["NSE|INDEX|NIFTY", "NSE|INDEX|BANKNIFTY"])

    def get_clean_symbol(self, symbol):
        return symbol.split('|')[-1] if '|' in symbol else symbol

    def check_data_exists(self, symbol, date_str):
        with sqlite3.connect(self.db.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM market_data WHERE symbol = ? AND timestamp LIKE ?", (symbol, f"{date_str}%"))
            count = cursor.fetchone()[0]
            return count > 0

    def backfill_date(self, date_str):
        print(f"Checking data for {date_str}...")

        for symbol in self.symbols:
            # We allow re-running to fill missing PCR if market data exists but PCR is null
            # or just skip if any data exists (user choice). For now, let's process if not fully complete.

            clean_symbol = self.get_clean_symbol(symbol)
            print(f"Backfilling {symbol} for {date_str}...")

            # 1. Fetch OHLCV from TV
            ohlcv_df = self.tv.get_ohlcv(clean_symbol, n_bars=5000)
            if ohlcv_df is not None and not ohlcv_df.empty:
                ohlcv_df = ohlcv_df[ohlcv_df.index.strftime('%Y-%m-%d') == date_str]

            # 2. Fetch Options/OI from Trendlyne
            tl_data = self.tl.get_historical_oi(clean_symbol, date_str)

            if (ohlcv_df is None or ohlcv_df.empty) and not tl_data:
                print(f"No historical data found for {symbol} on {date_str}")
                continue

            # Process Options/OI and PCR Data from Trendlyne first
            # because we want to use PCR data in market_data table
            timestamp_pcr_map = {}
            option_records = []

            if tl_data and 'body' in tl_data:
                # Body usually contains a list under 'data' or 'strikeWiseData' or 'pcrData'
                # Based on Trendlyne F&O API structure:
                pcr_list = tl_data['body'].get('pcrData', [])
                if not pcr_list and 'data' in tl_data['body']:
                    # Fallback to general data list
                    pcr_list = tl_data['body']['data']

                # First pass: Extract PCR per timestamp
                prev_pcr = None
                pcr_list.sort(key=lambda x: x.get('time', 0) or x.get('timestamp', 0))

                for entry in pcr_list:
                    try:
                        ts_val = entry.get('time') or entry.get('timestamp')
                        if not ts_val: continue
                        ts_str = datetime.fromtimestamp(ts_val / 1000.0).strftime("%Y-%m-%d %H:%M:%S")

                        curr_pcr = entry.get('pcr') or entry.get('total_pcr')
                        if curr_pcr is not None:
                            pcr_change = 0
                            if prev_pcr is not None:
                                pcr_change = curr_pcr - prev_pcr

                            timestamp_pcr_map[ts_str] = {
                                'total_pcr': curr_pcr,
                                'pcr_change': pcr_change
                            }
                            prev_pcr = curr_pcr
                    except:
                        continue

                # Second pass: Extract Option Strike data
                strike_data = tl_data['body'].get('strikeWiseData', [])
                for entry in strike_data:
                    try:
                        ts_val = entry.get('time') or entry.get('timestamp')
                        if not ts_val: continue
                        ts_str = datetime.fromtimestamp(ts_val / 1000.0).strftime("%Y-%m-%d %H:%M:%S")

                        option_records.append({
                            'timestamp': ts_str,
                            'symbol': symbol,
                            'strike_price': entry.get('strike') or entry.get('strike_price'),
                            'expiry_date': entry.get('expiry_date') or entry.get('expiryDate', ''),
                            'option_type': entry.get('option_type') or entry.get('optionType'),
                            'price': entry.get('ltp') or entry.get('price'),
                            'oi': entry.get('oi') or entry.get('open_interest'),
                            'oi_change': entry.get('oi_change') or entry.get('change_in_oi')
                        })
                    except:
                        continue

                if option_records:
                    self.db.save_option_data(option_records)

            # Process Market Data and merge PCR
            if ohlcv_df is not None and not ohlcv_df.empty:
                for ts, row in ohlcv_df.iterrows():
                    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

                    pcr_info = timestamp_pcr_map.get(ts_str, {'total_pcr': None, 'pcr_change': None})

                    market_record = {
                        'timestamp': ts_str,
                        'symbol': symbol,
                        'spot_price': row['close'],
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume'],
                        'total_pcr': pcr_info['total_pcr'],
                        'pcr_change': pcr_info['pcr_change']
                    }
                    self.db.save_market_data(market_record)
            elif timestamp_pcr_map:
                # If we have PCR data but no OHLCV from TV, still save PCR
                for ts_str, pcr_info in timestamp_pcr_map.items():
                    market_record = {
                        'timestamp': ts_str,
                        'symbol': symbol,
                        'spot_price': None,
                        'open': None,
                        'high': None,
                        'low': None,
                        'close': None,
                        'volume': None,
                        'total_pcr': pcr_info['total_pcr'],
                        'pcr_change': pcr_info['pcr_change']
                    }
                    self.db.save_market_data(market_record)

        print(f"Backfill complete for {date_str}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backfiller.py YYYY-MM-DD")
    else:
        date = sys.argv[1]
        bf = Backfiller()
        bf.backfill_date(date)
