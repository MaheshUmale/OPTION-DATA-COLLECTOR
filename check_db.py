import sqlite3
import pandas as pd

def check_db():
    conn = sqlite3.connect("options_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(f"Tables: {tables}")

    for table in tables:
        t_name = table[0]
        if t_name == 'sqlite_sequence': continue
        cursor.execute(f"SELECT COUNT(*) FROM {t_name}")
        count = cursor.fetchone()[0]
        print(f"Table {t_name} has {count} rows")

    conn.close()

if __name__ == "__main__":
    check_db()
