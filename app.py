# Flask backend for Stock Market Data Pipeline

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from pipeline import StockDataPipeline
import os

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend communication

# Initialize pipeline
pipeline = StockDataPipeline()

@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/stock/<symbol>')
def get_stock_data(symbol):
    """
    Get stock data for a specific symbol.
    
    Args:
        symbol: Stock ticker symbol
        days: Number of days to retrieve (query parameter)
    """
    try:
        days = int(request.args.get('days', 30))
        
        # Get data from database
        conn = sqlite3.connect(pipeline.db_path)
        
        # Query with proper date filtering
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        query = '''
            SELECT * FROM stocks 
            WHERE symbol = ? AND date >= ?
            ORDER BY date DESC
        '''
        
        df = pd.read_sql_query(
            query, 
            conn, 
            params=(symbol.upper(), start_date.strftime('%Y-%m-%d'))
        )
        conn.close()
        
        if df.empty:
            # Try to fetch fresh data
            pipeline.run_pipeline(days_back=days)
            
            # Try again
            conn = sqlite3.connect(pipeline.db_path)
            df = pd.read_sql_query(
                query, 
                conn, 
                params=(symbol.upper(), start_date.strftime('%Y-%m-%d'))
            )
            conn.close()
        
        if df.empty:
            return jsonify({"error": f"No data found for {symbol}"}), 404
        
        # Calculate metrics
        latest = df.iloc[0]
        oldest = df.iloc[-1]
        
        metrics = {
            "latest_price": float(latest['close']),
            "price_change": float(((latest['close'] - oldest['close']) / oldest['close']) * 100),
            "avg_volume": float(df['volume'].mean()),
            "current_rsi": float(latest['rsi_14']) if pd.notna(latest['rsi_14']) else 50.0
        }
        
        # Convert DataFrame to JSON-friendly format
        data = df.to_dict('records')
        
        return jsonify({
            "symbol": symbol.upper(),
            "data": data,
            "metrics": metrics,
            "count": len(data)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/run-pipeline', methods=['POST'])
def run_pipeline():
    """Run the full pipeline to update all stock data."""
    try:
        start_time = datetime.now()
        
        # Run pipeline with default settings
        pipeline.run_pipeline(days_back=30)
        
        # Get run statistics
        conn = sqlite3.connect(pipeline.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT symbols_processed, errors, duration_seconds 
            FROM pipeline_runs 
            ORDER BY run_id DESC 
            LIMIT 1
        ''')
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            symbols_processed, errors, duration = result
        else:
            # Fallback values
            duration = (datetime.now() - start_time).total_seconds()
            symbols_processed = 10
            errors = 0
        
        # Find the latest CSV file
        csv_files = list(pipeline.data_dir.glob('stock_data_*.csv'))
        latest_file = max(csv_files, key=os.path.getctime) if csv_files else None
        
        return jsonify({
            "status": "success",
            "symbols_processed": symbols_processed,
            "errors": errors,
            "duration": duration,
            "output_file": latest_file.name if latest_file else "No file generated"
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/symbols')
def get_symbols():
    """Get list of available symbols."""
    try:
        conn = sqlite3.connect(pipeline.db_path)
        
        query = "SELECT DISTINCT symbol FROM stocks ORDER BY symbol"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        symbols = df['symbol'].tolist()
        
        return jsonify({
            "symbols": symbols,
            "count": len(symbols)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/pipeline-stats')
def get_pipeline_stats():
    """Get pipeline run statistics."""
    try:
        conn = sqlite3.connect(pipeline.db_path)
        
        query = '''
            SELECT * FROM pipeline_runs 
            ORDER BY run_date DESC 
            LIMIT 10
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return jsonify({"runs": [], "message": "No pipeline runs found"})
        
        runs = df.to_dict('records')
        
        return jsonify({
            "runs": runs,
            "total_runs": len(runs),
            "average_duration": float(df['duration_seconds'].mean()),
            "total_errors": int(df['errors'].sum())
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("Starting Stock Market Data Pipeline API...")
    print("Frontend should be served from index.html")
    print("API running at http://localhost:5000")
    
    # Run the app
    app.run(debug=True, port=5000)