import time
import pandas as pd
from datetime import datetime, time as dtime
from clients import NSEClient, TVClient
from database import Database
import math

class DataCollector:
    def __init__(self):
        self.nse = NSEClient()
        self.tv = TVClient()
        self.db = Database()
        self.symbols = ["NIFTY", "BANKNIFTY"]
        self.tv_symbols = {"NIFTY": "NIFTY", "BANKNIFTY": "BANKNIFTY"}
        self.previous_pcr = {"NIFTY": None, "BANKNIFTY": None}

    def get_atm_strike(self, spot_price, strike_gap):
        return round(spot_price / strike_gap) * strike_gap

    def get_strike_gap(self, symbol):
        if symbol == "NIFTY":
            return 50
        if symbol == "BANKNIFTY":
            return 100
        return 100

    def process_symbol(self, symbol):
        print(f"[{datetime.now()}] Processing {symbol}...")

        # 1. Fetch Option Chain from NSE
        oc_data = self.nse.get_option_chain(symbol)
        if not oc_data:
            print(f"Failed to fetch option chain for {symbol}")
            return

        # 2. Get Spot Price from Option Chain data (most reliable for the chain)
        spot_price = oc_data.get('records', {}).get('underlyingValue')
        if not spot_price:
            print(f"No spot price found for {symbol}")
            return

        # 3. Fetch OHLCV from TradingView
        tv_symbol = self.tv_symbols.get(symbol)
        ohlcv_df = self.tv.get_ohlcv(tv_symbol)
        ohlcv = {}
        if ohlcv_df is not None and not ohlcv_df.empty:
            last_row = ohlcv_df.iloc[-1]
            ohlcv = {
                'open': last_row['open'],
                'high': last_row['high'],
                'low': last_row['low'],
                'close': last_row['close'],
                'volume': last_row['volume']
            }
        else:
            print(f"No OHLCV data from TV for {symbol}")

        # 4. Filter Option Chain for +/- 7 strikes around ATM
        strike_gap = self.get_strike_gap(symbol)
        atm_strike = self.get_atm_strike(spot_price, strike_gap)

        relevant_strikes = [atm_strike + i * strike_gap for i in range(-7, 8)]

        records = oc_data.get('records', {}).get('data', [])
        filtered_records = [r for r in records if r['strikePrice'] in relevant_strikes]

        # Get current expiry (first one available)
        expiry_dates = oc_data.get('records', {}).get('expiryDates', [])
        if not expiry_dates:
            print(f"No expiry dates found for {symbol}")
            return
        current_expiry = expiry_dates[0]

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        option_entries = []
        for r in filtered_records:
            if r['expiryDate'] != current_expiry:
                continue

            strike = r['strikePrice']

            # CE
            if 'CE' in r:
                ce = r['CE']
                option_entries.append({
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'strike_price': strike,
                    'expiry_date': current_expiry,
                    'option_type': 'CE',
                    'price': ce.get('lastPrice'),
                    'oi': ce.get('openInterest'),
                    'oi_change': ce.get('changeinOpenInterest')
                })

            # PE
            if 'PE' in r:
                pe = r['PE']
                option_entries.append({
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'strike_price': strike,
                    'expiry_date': current_expiry,
                    'option_type': 'PE',
                    'price': pe.get('lastPrice'),
                    'oi': pe.get('openInterest'),
                    'oi_change': pe.get('changeinOpenInterest')
                })

        # 5. Calculate PCR
        # Use filtered records or all records for total PCR? README says "TOTAL PCR"
        total_pe_oi = oc_data.get('filtered', {}).get('PE', {}).get('totOI', 0)
        total_ce_oi = oc_data.get('filtered', {}).get('CE', {}).get('totOI', 0)

        total_pcr = total_pe_oi / total_ce_oi if total_ce_oi != 0 else 0
        pcr_change = 0
        if self.previous_pcr[symbol] is not None:
            pcr_change = total_pcr - self.previous_pcr[symbol]
        self.previous_pcr[symbol] = total_pcr

        # 6. Save to DB
        market_data_record = {
            'timestamp': timestamp,
            'symbol': symbol,
            'spot_price': spot_price,
            'open': ohlcv.get('open'),
            'high': ohlcv.get('high'),
            'low': ohlcv.get('low'),
            'close': ohlcv.get('close'),
            'volume': ohlcv.get('volume'),
            'total_pcr': total_pcr,
            'pcr_change': pcr_change
        }

        self.db.save_market_data(market_data_record)
        self.db.save_option_data(option_entries)
        print(f"Saved data for {symbol} at {timestamp}")

    def is_market_open(self):
        # Indian Market Hours: 9:15 AM to 3:30 PM (9:15 to 15:30)
        now = datetime.now()
        # Market is closed on Saturday and Sunday
        if now.weekday() >= 5:
            return False

        start_time = dtime(9, 15)
        end_time = dtime(15, 30)
        return start_time <= now.time() <= end_time

    def run(self):
        print("Starting Data Collector...")
        # Get holidays once
        holidays = self.nse.get_holiday_list()

        while True:
            if self.is_market_open():
                today_str = datetime.now().strftime("%d-%b-%Y")
                if today_str in holidays:
                    print(f"Today ({today_str}) is a holiday. Sleeping...")
                    time.sleep(3600)
                    continue

                for symbol in self.symbols:
                    try:
                        self.process_symbol(symbol)
                    except Exception as e:
                        print(f"Error processing {symbol}: {e}")

                # Sleep until next minute
                time.sleep(60)
            else:
                # print("Market is closed. Waiting...")
                time.sleep(60)

if __name__ == "__main__":
    collector = DataCollector()
    collector.run()
