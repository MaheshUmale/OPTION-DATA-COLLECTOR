import sqlite3
from datetime import datetime

class Database:
    def __init__(self, db_name="options_data.db"):
        self.db_name = db_name
        self._create_tables()

    def _get_connection(self):
        return sqlite3.connect(self.db_name)

    def _create_tables(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Table for Index spot and OHLCV + Summary data (PCR)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    symbol TEXT NOT NULL,
                    spot_price REAL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    total_pcr REAL,
                    pcr_change REAL,
                    UNIQUE(timestamp, symbol)
                )
            ''')

            # Table for individual option strikes data
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS option_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    symbol TEXT NOT NULL,
                    strike_price REAL NOT NULL,
                    expiry_date TEXT NOT NULL,
                    option_type TEXT NOT NULL, -- 'CE' or 'PE'
                    price REAL,
                    oi REAL,
                    oi_change REAL,
                    UNIQUE(timestamp, symbol, strike_price, option_type, expiry_date)
                )
            ''')

            conn.commit()

    def save_market_data(self, data):
        """
        data: dict with keys matching market_data columns
        """
        query = '''
            INSERT OR REPLACE INTO market_data
            (timestamp, symbol, spot_price, open, high, low, close, volume, total_pcr, pcr_change)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        with self._get_connection() as conn:
            conn.execute(query, (
                data['timestamp'], data['symbol'], data.get('spot_price'),
                data.get('open'), data.get('high'), data.get('low'),
                data.get('close'), data.get('volume'), data.get('total_pcr'),
                data.get('pcr_change')
            ))

    def save_option_data(self, option_records):
        """
        option_records: list of dicts with keys matching option_data columns
        """
        query = '''
            INSERT OR REPLACE INTO option_data
            (timestamp, symbol, strike_price, expiry_date, option_type, price, oi, oi_change)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        with self._get_connection() as conn:
            conn.executemany(query, [
                (r['timestamp'], r['symbol'], r['strike_price'], r['expiry_date'],
                 r['option_type'], r.get('price'), r.get('oi'), r.get('oi_change'))
                for r in option_records
            ])
