# OPTION-DATA-COLLECTOR

A robust Python-based system for collecting, unifying, and storing per-minute options data for NIFTY and BANKNIFTY, designed for integration with the Scalping Orchestration System (SOS).

## Overview

This collector consolidates data from multiple sources into a single, structured SQLite database. It captures index spot prices, OHLCV data, and the option chain premiums/Open Interest (OI) for the ATM and surrounding strikes.

## Key Features

- **Canonical Symbol Support**: Uses `NSE|INDEX|NIFTY` and `NSE|INDEX|BANKNIFTY` as per SOS standards.
- **Unified Data Stream**: Merges NSE Option Chain data with TradingView OHLCV data in real-time.
- **ATM +/- 7 Strikes**: Captures data for 15 strikes (ATM and 7 above/below).
- **PCR Analytics**: Computes Total Put-Call Ratio (PCR) and its change per minute.
- **Market Awareness**: Handles Indian Market Hours (09:15 to 15:30 IST) and respects official NSE holidays.
- **Configurable**: Uses `config.json` for easy adjustment of symbols, strike gaps, and database settings.

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
   *Note: The `tvdatafeed` library is installed from GitHub to ensure compatibility.*

3. **Configure the system**:
   Edit `config.json` to customize the symbols or database name.

## Usage

### 1. Data Collection
Start the minute-by-minute collection loop:
```bash
python collector.py
```
The script will run continuously, entering a sleep state outside of market hours or on holidays.

### 2. Exporting Data
To export unified data for a specific date:
```bash
python export_data.py YYYY-MM-DD
```

## Project Architecture

- `collector.py`: Main execution loop, ATM calculation, and data merging.
- `clients.py`: API clients for NSE, TradingView (`tvDatafeed`), and Trendlyne.
- `database.py`: SQLite database management.
- `config.json`: System configuration.
- `export_data.py`: Data export utility.

## Database Schema

The system uses `options_data.db` with two related tables:

### `market_data`
| Column | Description |
| --- | --- |
| `timestamp` | ISO format timestamp (YYYY-MM-DD HH:MM:SS) |
| `symbol` | Canonical symbol (e.g., `NSE|INDEX|NIFTY`) |
| `spot_price` | Current underlying index price |
| `open`, `high`, `low`, `close` | Minute OHLCV for the index |
| `total_pcr` | PCR for the entire option chain |

### `option_data`
| Column | Description |
| --- | --- |
| `strike_price` | The strike price of the contract |
| `option_type` | CE or PE |
| `price` | Last Traded Price (LTP) |
| `oi` | Open Interest |

---
*Developed for Scalping Orchestration System (SOS).*
