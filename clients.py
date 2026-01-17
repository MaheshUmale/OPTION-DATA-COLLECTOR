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
            # First hit homepage to get cookies
            resp = self.session.get(self.base_url, timeout=15)
            # print(f"Homepage status: {resp.status_code}")

            # Then hit a page that sets more cookies
            resp = self.session.get(f"{self.base_url}/market-data/live-market-indices", timeout=15)
            # print(f"Indices page status: {resp.status_code}")

        except Exception as e:
            print(f"[NSE] Failed to initialize session: {e}")

    def _make_get_request(self, url, params=None, referer=None):
        time.sleep(2.0) # Be more conservative
        headers = self.headers.copy()
        if referer:
            headers["Referer"] = referer
        else:
            headers["Referer"] = self.base_url

        try:
            response = self.session.get(url, params=params, headers=headers, timeout=15)
            if response.status_code == 401 or response.status_code == 403:
                # print(f"[NSE] Session expired or blocked ({response.status_code}). Re-initializing...")
                self.session.cookies.clear()
                self._init_session()
                response = self.session.get(url, params=params, headers=headers, timeout=15)

            response.raise_for_status()
            return response.json()
        except Exception as e:
            # print(f"[NSE] Request failed for {url}: {e}")
            pass
        return None

    def get_option_chain(self, symbol, indices=True):
        instrument_type = "Indices" if indices else "Equities"
        # Try both v3 and v2 if one fails
        url = f"{self.base_url}/api/option-chain-v3"
        params = {"type": instrument_type, "symbol": symbol}
        referer = f"{self.base_url}/get-quotes/derivatives?symbol={symbol}"
        data = self._make_get_request(url, params=params, referer=referer)

        if not data:
            # Fallback to older API if v3 is not responding
            url = f"{self.base_url}/api/option-chain-indices" if indices else f"{self.base_url}/api/option-chain-equities"
            params = {"symbol": symbol}
            data = self._make_get_request(url, params=params, referer=referer)

        return data

    def get_indices(self):
        url = f"{self.base_url}/api/allIndices"
        return self._make_get_request(url)

    def get_holiday_list(self):
        url = f"{self.base_url}/api/holiday-master"
        data = self._make_get_request(url)
        if data and 'trading' in data:
            return [h['tradingDate'] for h in data['trading']]
        return []

class TVClient:
    def __init__(self):
        try:
            # nologin method
            self.tv = TvDatafeed()
        except Exception as e:
            # print(f"[TV] Error initializing TvDatafeed: {e}")
            self.tv = None

    def get_ohlcv(self, symbol, exchange='NSE', interval=Interval.in_1_minute, n_bars=1):
        if not self.tv:
            return None
        try:
            data = self.tv.get_hist(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                n_bars=n_bars
            )
            return data
        except Exception as e:
            # print(f"[TV] Error fetching OHLCV for {symbol}: {e}")
            pass
        return None

class TrendlyneClient:
    def __init__(self):
        self.base_url = "https://smartoptions.trendlyne.com/phoenix/api"

    def get_stock_id_for_symbol(self, symbol):
        s = symbol.upper()
        if '|' in s:
            s = s.split('|')[-1]
        if "NIFTY 50" in s or s == "NIFTY":
            s = "NIFTY"
        elif "NIFTY BANK" in s or s == "BANKNIFTY":
            s = "BANKNIFTY"

        search_url = f"{self.base_url}/search-contract-stock/"
        params = {'query': s.lower()}
        try:
            response = requests.get(search_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data and 'body' in data and 'data' in data['body'] and len(data['body']['data']) > 0:
                for item in data['body']['data']:
                    target_code = item.get('stock_code', '').upper()
                    if target_code == s:
                        return item['stock_id']
                return data['body']['data'][0]['stock_id']
            return None
        except Exception as e:
            # print(f"[Trendlyne] Error fetching stock ID for {symbol}: {e}")
            pass
        return None

    def get_expiry_dates(self, stock_id):
        expiry_url = f"{self.base_url}/fno/get-expiry-dates/?mtype=options&stock_id={stock_id}"
        try:
            response = requests.get(expiry_url, timeout=5)
            response.raise_for_status()
            return response.json().get('body', {}).get('expiryDates', [])
        except Exception as e:
            # print(f"[Trendlyne] Error fetching expiry dates: {e}")
            pass
        return []

    def get_live_oi_data(self, stock_id, expiry_date, min_time, max_time):
        url = f"{self.base_url}/live-oi-data/"
        params = {
            'stockId': stock_id,
            'expDateList': expiry_date,
            'minTime': min_time,
            'maxTime': max_time
        }
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            # print(f"[Trendlyne] Error fetching live OI data: {e}")
            pass
        return None
