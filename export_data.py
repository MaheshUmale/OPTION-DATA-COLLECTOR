import sqlite3
import pandas as pd
import sys
from datetime import datetime

def export_to_csv(date_str, output_file=None):
    """
    date_str: 'YYYY-MM-DD'
    """
    if output_file is None:
        output_file = f"options_data_{date_str}.csv"

    db_file = "options_data.db"

    try:
        conn = sqlite3.connect(db_file)

        # Query to join market data and option data for a unified view
        query = f"""
        SELECT
            m.timestamp,
            m.symbol,
            m.spot_price,
            m.open as index_open,
            m.high as index_high,
            m.low as index_low,
            m.close as index_close,
            m.volume as index_volume,
            m.total_pcr,
            o.strike_price,
            o.expiry_date,
            o.option_type,
            o.price as option_price,
            o.oi as option_oi,
            o.oi_change as option_oi_change
        FROM market_data m
        JOIN option_data o ON m.timestamp = o.timestamp AND m.symbol = o.symbol
        WHERE m.timestamp LIKE '{date_str}%'
        ORDER BY m.timestamp ASC, o.strike_price ASC
        """

        df = pd.read_sql_query(query, conn)

        if df.empty:
            print(f"No data found for date: {date_str}")
            return

        df.to_csv(output_file, index=False)
        print(f"Successfully exported {len(df)} rows to {output_file}")

    except Exception as e:
        print(f"Error exporting data: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python export_data.py YYYY-MM-DD [output_filename.csv]")
    else:
        date = sys.argv[1]
        out = sys.argv[2] if len(sys.argv) > 2 else None
        export_to_csv(date, out)
