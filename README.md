# OPTION-DATA-COLLECTOR
CREATE PYTHON CODE AND SQLITE DB WHICH WILL COLLECT FOLLOWIND DATA FOR NIFTY AND BANKNIFTY PER MINUTE

1) SPOT PRICE 
2) OHLCV PER MINUTE USING tvDataFeed lib
3) SPOT(ATM Strike ) +/- 7 Strike PE and CE OPTIONS PRICES , TOTAL OPEN INTEREST , CHANGE IN OI per minute , AND 
4) TOTAL PCR  anc  CHANGE IN PCR per minute 

THIS SCRIPT CAN USE NSE API , TVDATAFEED API , smartoptions.trendlyne API to FETCH AND CONSOLIDATE DATA INTO DATABASE.


""""""""""""""""
import requests

class TrendlyneClient:
    def __init__(self):
        self.base_url = "https://smartoptions.trendlyne.com/phoenix/api"

    def get_stock_id_for_symbol(self, symbol):
        # Strip common prefixes
        s = symbol.upper()
        if '|' in s:
            s = s.split('|')[-1]
        
        # Map indices to Trendlyne ticker codes
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
            print(f"[Trendlyne] Error fetching stock ID for {symbol}: {e}")
            return None

    def get_expiry_dates(self, stock_id):
        expiry_url = f"{self.base_url}/fno/get-expiry-dates/?mtype=options&stock_id={stock_id}"
        try:
            response = requests.get(expiry_url, timeout=5)
            response.raise_for_status()
            return response.json().get('body', {}).get('expiryDates', [])
        except Exception as e:
            print(f"[Trendlyne] Error fetching expiry dates: {e}")
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
            print(f"[Trendlyne] Error fetching live OI data: {e}")
            return None

""""""""""
from tvDatafeed import TvDatafeed, Interval
self.tv = TvDatafeed()

tv.get_hist(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                n_bars=n_bars
            )

""""""""""""""""""""""""""""""""""""
import requests
import time

class NSEClient:
    def __init__(self):
        self.base_url = "https://www.nseindia.com"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "www.nseindia.com",
            "Connection": "keep-alive"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self._init_session()

    def _init_session(self):
        if not self.session.cookies:
            try:
                # First hit homepage
                self.session.get(self.base_url, timeout=15)
                # Then hit a subpage to ensure cookies are fully set
                self.session.get(f"{self.base_url}/market-data/live-equity-market", timeout=15)
            except Exception as e:
                print(f"[NSE] Failed to initialize session: {e}")

    def _make_get_request(self, url, params=None):
        time.sleep(1.0) # Be more conservative with NSE
        try:
            response = self.session.get(url, params=params, timeout=15)
            if response.status_code == 401 or response.status_code == 403:
                print(f"[NSE] Session expired or blocked. Re-initializing...")
                self.session.cookies.clear()
                self._init_session()
                response = self.session.get(url, params=params, timeout=15)
            
            response.raise_for_status()
            try:
                return response.json()
            except ValueError:
                print(f"[NSE] Failed to decode JSON from {url}. Response started with: {response.text[:100]}")
                return None
        except requests.exceptions.HTTPError as e:
            print(f"[NSE] HTTP error: {e.response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[NSE] Request failed: {e}")
        return None

    def get_option_chain(self, symbol, indices=True):
        instrument_type = "Indices" if indices else "Equities"
        url = f"{self.base_url}/api/option-chain-v3"
        params = {"type": instrument_type, "symbol": symbol}
        headers = self.headers.copy()
        headers["Referer"] = f"{self.base_url}/get-quotes/derivatives?symbol={symbol}"
        self.session.headers.update(headers)
        return self._make_get_request(url, params=params)

    def get_market_breadth(self):
        url = f"{self.base_url}/api/live-analysis-advance"
        headers = self.headers.copy()
        headers["Referer"] = f"{self.base_url}/market-data/live-equity-market"
        self.session.headers.update(headers)
        return self._make_get_request(url)

    def get_holiday_list(self):
        url = f"{self.base_url}/api/holiday-master"
        headers = self.headers.copy()
        headers["Referer"] = f"{self.base_url}/resources/exchange-communication-holidays"
        self.session.headers.update(headers)
        data = self._make_get_request(url)
        if data and 'trading' in data:
            return [h['tradingDate'] for h in data['trading']]
        return []

    def get_indices(self):
        """
        Fetches the current data for all NSE indices.
        URL: https://www.nseindia.com/api/allIndices
        """
        url = f"{self.base_url}/api/allIndices"
        headers = self.headers.copy()
        headers["Referer"] = f"{self.base_url}/market-data/live-equity-market"
        self.session.headers.update(headers)
        return self._make_get_request(url)

"""""""""""""""""""


