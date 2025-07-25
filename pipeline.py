# Stock Market Data Pipeline

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import logging
from typing import List, Dict, Optional
import json
import sqlite3
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StockDataPipeline:
    """ETL Pipeline for S&P 500 stock market data."""
    
    def __init__(self, db_path: str = "stocks.db", data_dir: str = "data"):
        """
        Initialize the pipeline.
        
        Args:
            db_path: Path to SQLite database
            data_dir: Directory to store CSV exports
        """
        self.db_path = db_path
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Initialize database
        self._init_database()
        
    def _init_database(self):
        """Create database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create stocks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                symbol TEXT,
                date DATE,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                sma_20 REAL,
                sma_50 REAL,
                rsi_14 REAL,
                PRIMARY KEY (symbol, date)
            )
        ''')
        
        # Create metadata table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TIMESTAMP,
                symbols_processed INTEGER,
                errors INTEGER,
                duration_seconds REAL
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized")
        
    def get_sp500_symbols(self) -> List[str]:
        """
        Fetch current S&P 500 symbols.
        
        Returns:
            List of S&P 500 ticker symbols
        """
        try:
            # Get S&P 500 symbols from Wikipedia
            url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            tables = pd.read_html(url)
            sp500_table = tables[0]
            symbols = sp500_table['Symbol'].tolist()
            
            # Clean symbols (remove any special characters)
            symbols = [s.replace('.', '-') for s in symbols]
            
            logger.info(f"Retrieved {len(symbols)} S&P 500 symbols")
            return symbols[:10]  # Limit to 10 for demo purposes
            
        except Exception as e:
            logger.error(f"Error fetching S&P 500 symbols: {e}")
            # Return a small default list if Wikipedia fails
            return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
    
    def calculate_sma(self, df: pd.DataFrame, period: int) -> pd.Series:
        """
        Calculate Simple Moving Average.
        
        Args:
            df: DataFrame with price data
            period: Period for SMA calculation
            
        Returns:
            Series with SMA values
        """
        return df['Close'].rolling(window=period, min_periods=1).mean()
    
    def calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Calculate Relative Strength Index.
        
        Args:
            df: DataFrame with price data
            period: Period for RSI calculation
            
        Returns:
            Series with RSI values
        """
        delta = df['Close'].diff()
        delta = pd.to_numeric(delta, errors='coerce')
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        # Fill NaN values with 50 (neutral RSI)
        rsi = rsi.fillna(50)
        
        return rsi
    
    def fetch_stock_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        Fetch stock data for a single symbol.
        
        Args:
            symbol: Stock ticker symbol
            start_date: Start date for data fetch
            end_date: End date for data fetch
            
        Returns:
            DataFrame with stock data or None if error
        """
        try:
            logger.info(f"Fetching data for {symbol}")
            
            # Download data
            stock = yf.Ticker(symbol)
            df = stock.history(start=start_date, end=end_date)
            
            if df.empty:
                logger.warning(f"No data retrieved for {symbol}")
                return None
            
            # Reset index to have Date as a column
            df.reset_index(inplace=True)
            
            # Calculate technical indicators
            df['SMA_20'] = self.calculate_sma(df, 20)
            df['SMA_50'] = self.calculate_sma(df, 50)
            df['RSI_14'] = self.calculate_rsi(df, 14)
            
            # Add symbol column
            df['Symbol'] = symbol
            
            logger.info(f"Successfully processed {len(df)} days of data for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate stock data.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        # Remove any rows with all NaN values
        df = df.dropna(how='all')
        
        # Forward fill missing values for price data (weekends/holidays)
        price_columns = ['Open', 'High', 'Low', 'Close']
        df[price_columns] = df[price_columns].ffill()
        
        # Ensure volume is not negative
        df['Volume'] = df['Volume'].abs()
        
        # Remove any remaining rows with NaN in critical columns
        df = df.dropna(subset=['Close', 'Volume'])
        
        # Validate price relationships
        # High should be >= Low
        invalid_prices = df['High'] < df['Low']
        if invalid_prices.any():
            logger.warning(f"Found {invalid_prices.sum()} rows with High < Low, correcting...")
            df.loc[invalid_prices, 'High'] = df.loc[invalid_prices, 'Low']
        
        return df
    
    def save_to_database(self, df: pd.DataFrame):
        """
        Save cleaned data to SQLite database.
        
        Args:
            df: Cleaned DataFrame with stock data
        """
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Prepare data for insertion
            df_to_save = df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 
                           'Volume', 'SMA_20', 'SMA_50', 'RSI_14']].copy()
            
            # Rename columns to match database schema
            df_to_save.columns = ['symbol', 'date', 'open', 'high', 'low', 
                                 'close', 'volume', 'sma_20', 'sma_50', 'rsi_14']
            
            # Convert date to string format
            df_to_save['date'] = df_to_save['date'].dt.strftime('%Y-%m-%d')
            
            # Save to database
            df_to_save.to_sql('stocks', conn, if_exists='replace', index=False)
            
            conn.commit()
            logger.info(f"Saved {len(df_to_save)} records to database")
            
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def export_to_csv(self, df: pd.DataFrame, filename: str):
        """
        Export data to CSV file.
        
        Args:
            df: DataFrame to export
            filename: Output filename
        """
        filepath = self.data_dir / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Exported data to {filepath}")
    
    def run_pipeline(self, days_back: int = 30):
        """
        Run the complete ETL pipeline.
        
        Args:
            days_back: Number of days of historical data to fetch
        """
        start_time = datetime.now()
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"Starting pipeline run for date range: {start_date.date()} to {end_date.date()}")
        
        # Get S&P 500 symbols
        symbols = self.get_sp500_symbols()
        
        # Process each symbol
        all_data = []
        errors = 0
        
        for symbol in symbols:
            df = self.fetch_stock_data(
                symbol, 
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            
            if df is not None:
                df_clean = self.clean_data(df)
                all_data.append(df_clean)
            else:
                errors += 1
        
        # Combine all data
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Save to database
            self.save_to_database(combined_df)
            
            # Export to CSV
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.export_to_csv(combined_df, f'stock_data_{timestamp}.csv')
            
            # Record pipeline run
            duration = (datetime.now() - start_time).total_seconds()
            self._record_pipeline_run(len(symbols), errors, duration)
            
            logger.info(f"Pipeline completed successfully. Processed {len(symbols)} symbols with {errors} errors")
        else:
            logger.error("No data was successfully retrieved")
    
    def _record_pipeline_run(self, symbols_processed: int, errors: int, duration: float):
        """Record pipeline run metadata."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO pipeline_runs (run_date, symbols_processed, errors, duration_seconds)
            VALUES (?, ?, ?, ?)
        ''', (datetime.now(), symbols_processed, errors, duration))
        
        conn.commit()
        conn.close()
    
    def get_latest_data(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """
        Retrieve latest data for a symbol from database.
        
        Args:
            symbol: Stock ticker symbol
            days: Number of days to retrieve
            
        Returns:
            DataFrame with stock data
        """
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT * FROM stocks 
            WHERE symbol = ? 
            ORDER BY date DESC 
            LIMIT ?
        '''
        
        df = pd.read_sql_query(query, conn, params=(symbol, days))
        conn.close()
        
        return df


def main():
    """Main function to run the pipeline."""
    pipeline = StockDataPipeline()
    
    # Run the pipeline
    pipeline.run_pipeline(days_back=30)
    
    # Example: Retrieve and display data for Apple
    print("\nSample data for AAPL:")
    aapl_data = pipeline.get_latest_data('AAPL', days=5)
    print(aapl_data)


if __name__ == "__main__":
    main()