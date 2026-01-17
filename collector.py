import time
import json
import os
import pandas as pd
from datetime import datetime, time as dtime
from clients import NSEClient, TVClient
from database import Database

class DataCollector:
    def __init__(self, config_path="config.json"):
        with open(config_path, "r") as f:
            self.config = json.load(f)

        self.nse = NSEClient()
        self.tv = TVClient()
        self.db = Database(self.config.get("db_name", "options_data.db"))
        self.symbols = self.config.get("symbols", ["NSE|INDEX|NIFTY", "NSE|INDEX|BANKNIFTY"])
        self.previous_pcr = {s: None for s in self.symbols}

    def get_clean_symbol(self, symbol):
        if '|' in symbol:
            return symbol.split('|')[-1]
        return symbol

    def get_atm_strike(self, spot_price, strike_gap):
        return round(spot_price / strike_gap) * strike_gap

    def get_strike_gap(self, clean_symbol):
        gaps = self.config.get("strike_gaps", {})
        return gaps.get(clean_symbol, 100)

    def process_symbol(self, full_symbol):
        clean_symbol = self.get_clean_symbol(full_symbol)
        print(f"[{datetime.now()}] Processing {full_symbol}...")

        # 1. Fetch Option Chain from NSE
        oc_data = self.nse.get_option_chain(clean_symbol)
        if not oc_data:
            print(f"Failed to fetch option chain for {clean_symbol}")
            return

        # 2. Get Spot Price
        spot_price = oc_data.get('records', {}).get('underlyingValue')
        if not spot_price:
            print(f"No spot price found for {clean_symbol}")
            return

        # 3. Fetch OHLCV from TradingView
        # TradingView usually uses just NIFTY or BANKNIFTY for NSE
        ohlcv_df = self.tv.get_ohlcv(clean_symbol)
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

        # 4. Filter Option Chain for +/- 7 strikes around ATM
        strike_gap = self.get_strike_gap(clean_symbol)
        atm_strike = self.get_atm_strike(spot_price, strike_gap)

        relevant_strikes = [atm_strike + i * strike_gap for i in range(-7, 8)]

        records = oc_data.get('records', {}).get('data', [])
        expiry_dates = oc_data.get('records', {}).get('expiryDates', [])
        if not expiry_dates:
            return
        current_expiry = expiry_dates[0]

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        option_entries = []
        for r in records:
            if r['strikePrice'] in relevant_strikes and r['expiryDate'] == current_expiry:
                strike = r['strikePrice']
                for opt_type in ['CE', 'PE']:
                    if opt_type in r:
                        opt = r[opt_type]
                        option_entries.append({
                            'timestamp': timestamp,
                            'symbol': full_symbol,
                            'strike_price': strike,
                            'expiry_date': current_expiry,
                            'option_type': opt_type,
                            'price': opt.get('lastPrice'),
                            'oi': opt.get('openInterest'),
                            'oi_change': opt.get('changeinOpenInterest')
                        })

        # 5. Calculate PCR
        total_pe_oi = oc_data.get('filtered', {}).get('PE', {}).get('totOI', 0)
        total_ce_oi = oc_data.get('filtered', {}).get('CE', {}).get('totOI', 0)

        total_pcr = total_pe_oi / total_ce_oi if total_ce_oi != 0 else 0
        pcr_change = 0
        if self.previous_pcr[full_symbol] is not None:
            pcr_change = total_pcr - self.previous_pcr[full_symbol]
        self.previous_pcr[full_symbol] = total_pcr

        # 6. Save to DB
        market_data_record = {
            'timestamp': timestamp,
            'symbol': full_symbol,
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
        print(f"Saved data for {full_symbol} at {timestamp}")

    def is_market_open(self):
        now = datetime.now()
        if now.weekday() >= 5: return False

        m_hours = self.config.get("market_hours", {"start": "09:15", "end": "15:30"})
        start_h, start_m = map(int, m_hours["start"].split(":"))
        end_h, end_m = map(int, m_hours["end"].split(":"))

        return dtime(start_h, start_m) <= now.time() <= dtime(end_h, end_m)

    def run(self):
        print(f"Starting Data Collector with symbols: {self.symbols}")
        holidays = self.nse.get_holiday_list()

        while True:
            if self.is_market_open():
                today_str = datetime.now().strftime("%d-%b-%Y")
                if today_str in holidays:
                    time.sleep(3600)
                    continue

                for symbol in self.symbols:
                    try:
                        self.process_symbol(symbol)
                    except Exception as e:
                        print(f"Error processing {symbol}: {e}")

                time.sleep(60)
            else:
                time.sleep(60)

if __name__ == "__main__":
    collector = DataCollector()
    collector.run()
