# Stock Market Data Pipeline

A Python ETL pipeline that retrieves, cleans, and analyzes stock market data for S&P 500 companies. The pipeline handles missing values from weekends/holidays and calculates technical indicators including moving averages and RSI.

## Features

- **Automated Data Retrieval**: Fetches daily stock data for S&P 500 companies using the Yahoo Finance API
- **Data Cleaning**: Handles missing values, validates price relationships, and ensures data quality
- **Technical Indicators**: Calculates Simple Moving Averages (20 & 50 day) and Relative Strength Index (RSI-14)
- **Data Storage**: Saves processed data to SQLite database and CSV files
- **Logging**: Comprehensive logging for monitoring pipeline runs and debugging
- **Error Handling**: Gracefully handles API failures and data inconsistencies

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/stock-market-pipeline.git
cd stock-market-pipeline
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

Run the pipeline to fetch the last 30 days of data:

```python
from pipeline import StockDataPipeline

# Initialize pipeline
pipeline = StockDataPipeline()

# Run ETL process
pipeline.run_pipeline(days_back=30)
```

### Command Line

Run directly from command line:

```bash
python pipeline.py
```

### Retrieve Data

Query processed data from the database:

```python
# Get latest data for a specific stock
df = pipeline.get_latest_data('AAPL', days=30)
print(df.head())
```

## Project Structure

```
stock-market-pipeline/
│
├── pipeline.py          # Main pipeline code
├── requirements.txt     # Python dependencies
├── README.md           # This file
├── .gitignore          # Git ignore file
├── data/               # CSV exports directory (created automatically)
├── stocks.db           # SQLite database (created automatically)
└── pipeline.log        # Log file (created automatically)
```

## Technical Indicators

### Simple Moving Average (SMA)
- **SMA-20**: 20-day moving average of closing prices
- **SMA-50**: 50-day moving average of closing prices

### Relative Strength Index (RSI)
- **RSI-14**: 14-day RSI to identify overbought/oversold conditions
  - RSI > 70: Potentially overbought
  - RSI < 30: Potentially oversold

## Database Schema

### stocks table
- `symbol`: Stock ticker symbol
- `date`: Trading date
- `open`: Opening price
- `high`: Highest price of the day
- `low`: Lowest price of the day
- `close`: Closing price
- `volume`: Trading volume
- `sma_20`: 20-day Simple Moving Average
- `sma_50`: 50-day Simple Moving Average
- `rsi_14`: 14-day Relative Strength Index

### pipeline_runs table
- `run_id`: Unique identifier for each pipeline run
- `run_date`: Timestamp of pipeline execution
- `symbols_processed`: Number of symbols processed
- `errors`: Number of errors encountered
- `duration_seconds`: Pipeline execution time

## Configuration

The pipeline can be customized through the `StockDataPipeline` initialization:

```python
pipeline = StockDataPipeline(
    db_path="custom_stocks.db",  # Custom database path
    data_dir="output"            # Custom output directory
)
```

## Error Handling

The pipeline includes robust error handling:
- Continues processing if individual stock downloads fail
- Validates data integrity (e.g., High >= Low)
- Logs all errors for debugging
- Records error counts in pipeline_runs table

## Performance Notes

- By default, limited to 10 S&P 500 symbols for demonstration
- To process all S&P 500 stocks, remove the limit in `get_sp500_symbols()`
- Consider implementing rate limiting for production use
- Database indexes on (symbol, date) for optimal query performance

## Future Enhancements

- Add more technical indicators (MACD, Bollinger Bands)
- Implement real-time data streaming
- Add data visualization capabilities
- Support for multiple exchanges
- Automated daily scheduling
- Email notifications for pipeline status

## License

MIT License

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.