import requests
import time
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime, timedelta

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
        time.sleep(1.0)
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
        nse_symbol = symbol
        if "NIFTY" in symbol and "BANK" not in symbol: nse_symbol = "NIFTY"
        elif "BANK" in symbol: nse_symbol = "BANKNIFTY"

        url = f"{self.base_url}/api/option-chain-v3"
        params = {"type": "Indices" if indices else "Equities", "symbol": nse_symbol}
        referer = f"{self.base_url}/get-quotes/derivatives?symbol={nse_symbol}"
        data = self._make_get_request(url, params=params, referer=referer)
        if not data:
            url = f"{self.base_url}/api/option-chain-indices" if indices else f"{self.base_url}/api/option-chain-equities"
            data = self._make_get_request(url, params={"symbol": nse_symbol}, referer=referer)
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
        tv_symbol = symbol
        if "NIFTY" in symbol and "BANK" not in symbol: tv_symbol = "NIFTY"
        elif "BANK" in symbol: tv_symbol = "BANKNIFTY"

        try:
            return self.tv.get_hist(symbol=tv_symbol, exchange=exchange, interval=interval, n_bars=n_bars)
        except Exception:
            return None

class TrendlyneClient:
    def __init__(self):
        self.base_url = "https://smartoptions.trendlyne.com/phoenix/api"

    def format_expiry_for_url(self, expiry_date):
        """
        expiry_date: 'YYYY-MM-DD' -> 'DD-mon-YYYY-near'
        """
        dt = datetime.strptime(expiry_date, "%Y-%m-%d")
        return dt.strftime("%d-%b-%Y").lower() + "-near"

    def get_stock_id_for_symbol(self, symbol):
        s = symbol.upper().split('|')[-1] if '|' in symbol else symbol.upper()
        if "NIFTY" in s and "BANK" not in s: search_query = "NIFTY"
        elif "BANK" in s: search_query = "BANKNIFTY"
        else: search_query = s

        try:
            response = requests.get(f"{self.base_url}/search-contract-stock/", params={'query': search_query.lower()}, timeout=10)
            data = response.json()
            if data and 'body' in data and 'data' in data['body']:
                for item in data['body']['data']:
                    stock_code = item.get('stock_code', '').upper()
                    if stock_code == search_query: return item['stock_id']
                return data['body']['data'][0]['stock_id']
        except Exception:
            pass
        return None

    def get_expiry_dates(self, stock_id):
        url = f"{self.base_url}/fno/get-expiry-dates/?mtype=options&stock_id={stock_id}"
        try:
            response = requests.get(url, timeout=5)
            return response.json().get('body', {}).get('expiryDates', [])
        except Exception:
            return []

    def get_options_buildup(self, symbol, expiry_date, strike, option_type, interval=5):
        """
        symbol: 'NIFTY' or 'BANKNIFTY'
        expiry_date: 'YYYY-MM-DD'
        strike: 25700
        option_type: 'call' or 'put'
        """
        clean_symbol = symbol.upper()
        if "NIFTY" in clean_symbol and "BANK" not in clean_symbol: clean_symbol = "NIFTY"
        elif "BANK" in clean_symbol: clean_symbol = "BANKNIFTY"

        expiry_formatted = self.format_expiry_for_url(expiry_date)

        url = f"{self.base_url}/fno/buildup-{interval}/{expiry_formatted}/{clean_symbol}/"
        params = {
            'fno_mtype': 'options',
            'strikePrice': strike,
            'option_type': option_type
        }
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            # print(f"[Trendlyne] Error fetching buildup: {e}")
            return None

    def get_historical_oi(self, symbol, date_str):
        # Kept for compatibility, but buildup is preferred for strike-wise history
        stock_id = self.get_stock_id_for_symbol(symbol)
        if not stock_id: return None
        expiries = self.get_expiry_dates(stock_id)
        if not expiries: return None

        current_expiry = None
        for exp in expiries:
            if exp >= date_str:
                current_expiry = exp
                break
        if not current_expiry: current_expiry = expiries[0]

        dt = datetime.strptime(date_str, "%Y-%m-%d")
        min_time = int(dt.replace(hour=9, minute=15).timestamp() * 1000)
        max_time = int(dt.replace(hour=15, minute=30).timestamp() * 1000)

        params = {
            'stockId': stock_id,
            'expDateList': current_expiry,
            'minTime': min_time,
            'maxTime': max_time
        }
        try:
            response = requests.get(f"{self.base_url}/live-oi-data/", params=params, timeout=10)
            return response.json()
        except Exception:
            return None
