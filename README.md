# OPTION-DATA-COLLECTOR

A robust Python-based system for collecting, unifying, and storing per-minute options data for NIFTY and BANKNIFTY.

## Overview

This collector consolidates data from multiple sources into a single, structured SQLite database, specifically designed for algorithmic trading and backtesting. It captures the index spot price, OHLCV, and the option chain premiums and Open Interest (OI) for the ATM and surrounding strikes.

## Key Features

- **Unified Data Stream**: Merges NSE Option Chain data with TradingView OHLCV data in real-time.
- **ATM +/- 7 Strikes**: Automatically calculates the At-The-Money (ATM) strike and captures data for 15 strikes (ATM and 7 above/below).
- **PCR Analytics**: Computes Total Put-Call Ratio (PCR) and its change per minute.
- **Market Awareness**:
    - Automatically handles Indian Market Hours (09:15 to 15:30 IST).
    - Fetches and respects the official NSE holiday calendar.
- **Datewise Organization**: All data is timestamped and indexed, allowing for easy retrieval and analysis on a per-day basis.

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd OPTION-DATA-COLLECTOR
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   *The `tvdatafeed` library is automatically installed from its GitHub source.*

## Usage

### 1. Data Collection
Start the minute-by-minute collection loop:
```bash
python collector.py
```
The script will run continuously, entering a sleep state outside of market hours or on holidays.

### 2. Exporting Data
To export data for a specific date (e.g., for use in Excel or Pandas):
```bash
python export_data.py 2026-01-16
```
This will generate a `options_data_2026-01-16.csv` file with a unified view of index and option data.

## Project Architecture

- `collector.py`: Orchestrates the collection loop, ATM calculation, and data merging.
- `clients.py`: Contains optimized API clients:
    - `NSEClient`: Fetches option chains with session/cookie handling.
    - `TVClient`: Fetches minute-wise index OHLCV.
    - `TrendlyneClient`: Optional client for alternative OI data.
- `database.py`: Defines the SQLite schema and handles atomic data inserts.
- `export_data.py`: Utility to pull unified data into CSV format.

## Database Schema

The system uses `options_data.db` with two related tables:

### `market_data`
| Column | Type | Description |
| --- | --- | --- |
| `timestamp` | DATETIME | ISO format timestamp (YYYY-MM-DD HH:MM:SS) |
| `symbol` | TEXT | Index symbol (NIFTY/BANKNIFTY) |
| `spot_price` | REAL | Current underlying index price |
| `open`, `high`, `low`, `close` | REAL | Minute OHLCV for the index |
| `volume` | REAL | Index trading volume |
| `total_pcr` | REAL | PCR for the entire option chain |
| `pcr_change` | REAL | Change in PCR since the last minute |

### `option_data`
| Column | Type | Description |
| --- | --- | --- |
| `timestamp` | DATETIME | Matches `market_data.timestamp` |
| `strike_price` | REAL | The strike price of the contract |
| `expiry_date` | TEXT | Contract expiry (DD-MMM-YYYY) |
| `option_type` | TEXT | CE or PE |
| `price` | REAL | Last Traded Price (LTP) |
| `oi` | REAL | Open Interest |
| `oi_change` | REAL | Change in OI since previous record |

## Troubleshooting

- **NSE Connectivity**: If the NSE API blocks your IP, the script will automatically attempt to re-initialize the session. Ensure you are not running multiple instances against the same API.
- **TradingView Volume**: TradingView volume for indices can sometimes be zero; the system captures it as provided by the `tvDatafeed`.
- **Database Access**: You can view the data using any SQLite browser (e.g., DB Browser for SQLite).

---
*Developed for integration with Scalping Orchestration System (SOS).*
