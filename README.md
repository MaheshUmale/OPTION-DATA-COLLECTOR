# OPTION-DATA-COLLECTOR

This project provides a Python-based solution for collecting and consolidating per-minute options data for NIFTY and BANKNIFTY indices into a unified SQLite database.

## Features

- **Spot Price**: Fetches real-time spot prices for NIFTY and BANKNIFTY.
- **OHLCV**: Collects per-minute OHLCV data using the `tvDatafeed` library.
- **Option Chain Data**:
  - Tracks ATM Strike and +/- 7 strikes.
  - Records PE and CE prices, Total Open Interest (OI), and Change in OI per minute.
- **PCR Statistics**: Calculates and stores Total PCR and Change in PCR per minute.
- **Market Awareness**: Includes logic for Indian market hours (9:15 AM - 3:30 PM) and holiday checks.

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd OPTION-DATA-COLLECTOR
   ```

2. **Install dependencies**:
   It is recommended to use a virtual environment.
   ```bash
   pip install -r requirements.txt
   ```
   *Note: `tvdatafeed` is installed directly from its GitHub repository as specified in `requirements.txt`.*

## Usage

To start the data collection process, run the `collector.py` script:

```bash
python collector.py
```

The script will:
- Check if the market is open and it's not a holiday.
- Fetch data every minute.
- Consolidate and save data into `options_data.db`.

## Project Structure

- `collector.py`: The main entry point that orchestrates data collection.
- `clients.py`: Contains API clients for NSE, TradingView (`tvDatafeed`), and Trendlyne.
- `database.py`: Manages the SQLite database schema and data persistence.
- `requirements.txt`: Lists the necessary Python packages.

## Database Schema

The data is stored in `options_data.db` with the following tables:

### `market_data`
| Column | Description |
| --- | --- |
| `timestamp` | Date and time of the record |
| `symbol` | NIFTY or BANKNIFTY |
| `spot_price` | Current underlying price |
| `open`, `high`, `low`, `close` | Minute-wise OHLCV data |
| `volume` | Trading volume |
| `total_pcr` | Put-Call Ratio |
| `pcr_change` | Change in PCR from previous minute |

### `option_data`
| Column | Description |
| --- | --- |
| `timestamp` | Date and time of the record |
| `symbol` | NIFTY or BANKNIFTY |
| `strike_price` | The strike price of the option |
| `expiry_date` | Option expiry date |
| `option_type` | CE or PE |
| `price` | Last traded price of the option |
| `oi` | Open Interest |
| `oi_change` | Change in Open Interest |

---
*Developed for Scalping Orchestration System (SOS).*
