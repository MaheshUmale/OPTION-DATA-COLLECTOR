import requests
import time
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime

class NSEClient:
    def __init__(self):
        self.base_url = "https://www.nseindia.com"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self._init_session()

    def _init_session(self):
        try:
            self.session.get(self.base_url, timeout=15)
            self.session.get(f"{self.base_url}/market-data/live-market-indices", timeout=15)
        except Exception as e:
            print(f"[NSE] Failed to initialize session: {e}")

    def _make_get_request(self, url, params=None, referer=None):
        time.sleep(2.0)
        headers = self.headers.copy()
        headers["Referer"] = referer if referer else self.base_url
        try:
            response = self.session.get(url, params=params, headers=headers, timeout=15)
            if response.status_code in [401, 403]:
                self.session.cookies.clear()
                self._init_session()
                response = self.session.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception:
            return None

    def get_option_chain(self, symbol, indices=True):
        url = f"{self.base_url}/api/option-chain-v3"
        params = {"type": "Indices" if indices else "Equities", "symbol": symbol}
        referer = f"{self.base_url}/get-quotes/derivatives?symbol={symbol}"
        data = self._make_get_request(url, params=params, referer=referer)
        if not data:
            url = f"{self.base_url}/api/option-chain-indices" if indices else f"{self.base_url}/api/option-chain-equities"
            data = self._make_get_request(url, params={"symbol": symbol}, referer=referer)
        return data

    def get_holiday_list(self):
        data = self._make_get_request(f"{self.base_url}/api/holiday-master")
        return [h['tradingDate'] for h in data['trading']] if data and 'trading' in data else []

class TVClient:
    def __init__(self):
        try:
            self.tv = TvDatafeed()
        except Exception:
            self.tv = None

    def get_ohlcv(self, symbol, exchange='NSE', interval=Interval.in_1_minute, n_bars=1):
        if not self.tv: return None
        try:
            return self.tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)
        except Exception:
            return None

class TrendlyneClient:
    def __init__(self):
        self.base_url = "https://smartoptions.trendlyne.com/phoenix/api"

    def get_stock_id_for_symbol(self, symbol):
        s = symbol.upper().split('|')[-1] if '|' in symbol else symbol.upper()
        if "NIFTY 50" in s or s == "NIFTY": s = "NIFTY"
        elif "NIFTY BANK" in s or s == "BANKNIFTY": s = "BANKNIFTY"

        try:
            response = requests.get(f"{self.base_url}/search-contract-stock/", params={'query': s.lower()}, timeout=10)
            data = response.json()
            if data and 'body' in data and 'data' in data['body']:
                for item in data['body']['data']:
                    if item.get('stock_code', '').upper() == s: return item['stock_id']
                return data['body']['data'][0]['stock_id']
        except Exception:
            pass
        return None

    def get_live_oi_data(self, stock_id, expiry_date, min_time, max_time):
        params = {'stockId': stock_id, 'expDateList': expiry_date, 'minTime': min_time, 'maxTime': max_time}
        try:
            response = requests.get(f"{self.base_url}/live-oi-data/", params=params, timeout=10)
            return response.json()
        except Exception:
            return None
