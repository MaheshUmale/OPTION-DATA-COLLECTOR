import time
import json
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from clients import TrendlyneClient, TVClient
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
        db_name = self.config.get("db_name", "options_data.db")
        self.db = Database(db_name)
        self.symbols = self.config.get("symbols", ["NSE|INDEX|NIFTY", "NSE|INDEX|BANKNIFTY"])
        print(f"Using database: {os.path.abspath(db_name)}")

    def get_clean_symbol(self, symbol):
        return symbol.split('|')[-1] if '|' in symbol else symbol

    def check_data_exists(self, symbol, date_str):
        with sqlite3.connect(self.db.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM market_data WHERE symbol = ? AND timestamp LIKE ?", (symbol, f"{date_str}%"))
            count = cursor.fetchone()[0]
            return count > 0

    def backfill_date(self, date_str):
        print(f"--- Starting Backfill for {date_str} ---")

        for symbol in self.symbols:
            clean_symbol = self.get_clean_symbol(symbol)
            print(f"\n[Processing {symbol}]")

            # 1. Fetch OHLCV from TV
            print(f"Fetching OHLCV for {clean_symbol} from TradingView...")
            ohlcv_df = self.tv.get_ohlcv(clean_symbol, n_bars=5000)

            if ohlcv_df is not None and not ohlcv_df.empty:
                print(f"Fetched {len(ohlcv_df)} total bars from TV.")
                # Localize and filter
                ohlcv_df = ohlcv_df[ohlcv_df.index.strftime('%Y-%m-%d') == date_str]
                print(f"Bars after filtering for {date_str}: {len(ohlcv_df)}")
            else:
                print("No data returned from TradingView.")

            # 2. Fetch Options/OI from Trendlyne
            print(f"Fetching Options/OI for {clean_symbol} from Trendlyne...")
            tl_data = self.tl.get_historical_oi(clean_symbol, date_str)

            timestamp_pcr_map = {}
            option_records = []

            if tl_data and 'body' in tl_data:
                body = tl_data['body']
                print("Successfully received data from Trendlyne.")

                # Extract PCR data
                pcr_data = body.get('overallData', {})
                if pcr_data and 'totalPCR' in pcr_data:
                    # If it's single point (live data), we might need historical series
                    # check for pcrData or chartData if available
                    pcr_list = body.get('pcrData', [])
                    if not pcr_list:
                        # If Trendlyne returns a single overall snapshot
                        print("Single PCR record found.")
                        ts_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Placeholder if no time
                        timestamp_pcr_map[ts_now] = {'total_pcr': pcr_data.get('totalPCR'), 'pcr_change': 0}
                    else:
                        print(f"Found {len(pcr_list)} historical PCR records.")
                        pcr_list.sort(key=lambda x: x.get('time', 0))
                        prev_pcr = None
                        for entry in pcr_list:
                            ts = datetime.fromtimestamp(entry['time'] / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
                            curr_pcr = entry.get('pcr')
                            pcr_change = (curr_pcr - prev_pcr) if prev_pcr is not None else 0
                            timestamp_pcr_map[ts] = {'total_pcr': curr_pcr, 'pcr_change': pcr_change}
                            prev_pcr = curr_pcr

                # Extract Strike-wise data
                strike_list = body.get('strikePriceList', [])
                # Trendlyne usually returns strikeWiseData in a list or within overallData
                strike_wise = body.get('strikeWiseData', [])
                if strike_wise:
                    print(f"Found {len(strike_wise)} strike-wise records.")
                    for entry in strike_wise:
                        try:
                            ts = datetime.fromtimestamp(entry['time'] / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
                            option_records.append({
                                'timestamp': ts,
                                'symbol': symbol,
                                'strike_price': entry.get('strike'),
                                'expiry_date': entry.get('expiryDate', ''),
                                'option_type': entry.get('optionType'),
                                'price': entry.get('ltp'),
                                'oi': entry.get('oi'),
                                'oi_change': entry.get('oiChange')
                            })
                        except: continue
            else:
                print("No data returned from Trendlyne.")

            if (ohlcv_df is None or ohlcv_df.empty) and not timestamp_pcr_map:
                print(f"Skipping {symbol} due to missing data.")
                continue

            # Save Market Data
            market_records_saved = 0
            if ohlcv_df is not None and not ohlcv_df.empty:
                for ts, row in ohlcv_df.iterrows():
                    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
                    pcr_info = timestamp_pcr_map.get(ts_str, {'total_pcr': None, 'pcr_change': None})
                    # If we only have one PCR record, maybe apply it to the whole day or last minute?
                    # For now, just exact match.

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
                    market_records_saved += 1
            elif timestamp_pcr_map:
                for ts_str, pcr_info in timestamp_pcr_map.items():
                    market_record = {
                        'timestamp': ts_str, 'symbol': symbol, 'spot_price': None,
                        'open': None, 'high': None, 'low': None, 'close': None, 'volume': None,
                        'total_pcr': pcr_info['total_pcr'], 'pcr_change': pcr_info['pcr_change']
                    }
                    self.db.save_market_data(market_record)
                    market_records_saved += 1
            print(f"Saved {market_records_saved} market data records.")

            if option_records:
                self.db.save_option_data(option_records)
                print(f"Saved {len(option_records)} option data records.")

        print(f"\n--- Backfill complete for {date_str} ---")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backfiller.py YYYY-MM-DD")
    else:
        date = sys.argv[1]
        bf = Backfiller()
        bf.backfill_date(date)
