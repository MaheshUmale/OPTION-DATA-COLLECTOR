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

            # 2. Get Expiry and Spot Price for ATM calculation
            # Use current NSE client to get near expiry (might not be exact for deep history)
            stock_id = self.tl.get_stock_id_for_symbol(clean_symbol)
            expiries = self.tl.get_expiry_dates(stock_id) if stock_id else []

            current_expiry = None
            for exp in expiries:
                if exp >= date_str:
                    current_expiry = exp
                    break
            if not current_expiry:
                print(f"Could not determine expiry for {date_str}")
                continue

            # ATM Strike from first OHLCV bar or last? Let's use opening of the day.
            spot_price = ohlcv_df['open'].iloc[0]
            strike_gap = self.get_strike_gap(clean_symbol)
            atm_strike = round(spot_price / strike_gap) * strike_gap
            relevant_strikes = [atm_strike + i * strike_gap for i in range(-7, 8)]

            print(f"Date: {date_str}, Spot: {spot_price}, ATM: {atm_strike}, Expiry: {current_expiry}")

            # 3. Fetch Buildup data for each strike
            all_option_records = []
            for strike in relevant_strikes:
                for opt_type in ['call', 'put']:
                    # print(f"  Fetching buildup for {strike} {opt_type}...")
                    buildup_data = self.tl.get_options_buildup(clean_symbol, current_expiry, strike, opt_type)

                    if buildup_data and 'body' in buildup_data and 'data_v2' in buildup_data['body']:
                        data_v2 = buildup_data['body']['data_v2']
                        # print(f"    Found {len(data_v2)} records.")

                        for entry in data_v2:
                            # interval: "15:25 TO 15:30"
                            interval_str = entry.get('interval')
                            if not interval_str: continue

                            try:
                                end_time_str = interval_str.split(" TO ")[1]
                                timestamp_str = f"{date_str} {end_time_str}:00"

                                all_option_records.append({
                                    'timestamp': timestamp_str,
                                    'symbol': symbol,
                                    'strike_price': strike,
                                    'expiry_date': current_expiry,
                                    'option_type': 'CE' if opt_type == 'call' else 'PE',
                                    'price': entry.get('close_price'),
                                    'oi': entry.get('open_interest'),
                                    'oi_change': entry.get('oi_change_gross')
                                })
                            except Exception as e:
                                # print(f"Error parsing interval {interval_str}: {e}")
                                continue
                    time.sleep(0.1) # Be nice to API

            # 4. Fetch PCR Data (Using historical OI endpoint as it often has PCR)
            tl_oi_data = self.tl.get_historical_oi(clean_symbol, date_str)
            timestamp_pcr_map = {}
            if tl_oi_data and 'body' in tl_oi_data:
                body = tl_oi_data['body']
                # Try overallData first
                overall = body.get('overallData', {})
                if overall and 'totalPCR' in overall:
                    # If single point, apply to all market records or just last?
                    pass

                # Check for historical pcr series if available
                pcr_list = body.get('pcrData', [])
                if pcr_list:
                    pcr_list.sort(key=lambda x: x.get('time', 0))
                    prev_pcr = None
                    for entry in pcr_list:
                        ts = datetime.fromtimestamp(entry['time'] / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
                        curr_pcr = entry.get('pcr')
                        pcr_change = (curr_pcr - prev_pcr) if prev_pcr is not None else 0
                        timestamp_pcr_map[ts] = {'total_pcr': curr_pcr, 'pcr_change': pcr_change}
                        prev_pcr = curr_pcr

            # 5. Save everything
            print(f"Saving {len(ohlcv_df)} market data records...")
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
