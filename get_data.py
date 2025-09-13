import yfinance as yf
import pandas as pd
import os
import zipfile
import time

# --- CONFIGURATION ---
EQUITY_TICKERS = [
    "SPY", "AAPL", "META", "NVDA", "TSM", "INTU", "BX", "TSLA", "PWR", "NUE", 
    "ZM", "NIO", "BRKR", "AXON", "ODFL", "PINS", "EFX", "BLDR", "ENPH", "PLTR"
]

CRYPTO_TICKERS = {
    "BTC-USD": "btcusd",
    "ETH-USD": "ethusd"
}

# Yahoo Finance uses "=X" for currency pairs.
FOREX_TICKERS = {
    "GBPUSD=X": "gbpusd",
    "EURUSD=X": "eurusd",
    "USDJPY=X": "usdjpy"
}

START_DATE = "2018-12-01" 
END_DATE = "2025-01-02"
DATA_DIR = "data"

# --- SCRIPT ---

def format_and_save_data(df, lean_ticker, output_folder, is_forex=False):
    """A helper function to format and save data in the LEAN format."""
    df.reset_index(inplace=True)
    df.rename(columns={'Date': 'time', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
    
    if 'Adj Close' in df.columns:
        df = df.drop(columns=['Adj Close'])
        
    df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y%m%d 00:00')

    # LEAN expects Forex files to be named _quote.zip, even if it's trade data inside.
    file_suffix = "quote" if is_forex else "trade"
    
    os.makedirs(output_folder, exist_ok=True)
    csv_filename = f"{lean_ticker}_{file_suffix}.csv"
    zip_filename = os.path.join(output_folder, f"{lean_ticker}_{file_suffix}.zip")

    # For Forex quote data, LEAN expects extra bid/ask columns. We'll use the OHLC for them.
    if is_forex:
        df['bidopen'] = df['open']
        df['bidhigh'] = df['high']
        df['bidlow'] = df['low']
        df['bidclose'] = df['close']
        df['askopen'] = df['open']
        df['askhigh'] = df['high']
        df['asklow'] = df['low']
        df['askclose'] = df['close']
        df = df[['time', 'bidopen', 'bidhigh', 'bidlow', 'bidclose', 'askopen', 'askhigh', 'asklow', 'askclose']]
    else:
        df = df[['time', 'open', 'high', 'low', 'close', 'volume']]


    df.to_csv(csv_filename, index=False, header=False)
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(csv_filename)
    
    os.remove(csv_filename)
    print(f"    SUCCESS: Saved LEAN data to {zip_filename}")

def download_data(ticker_map, asset_class, market, is_forex=False):
    """Main function to download data for a given asset class."""
    print(f"\n--- Downloading {asset_class.upper()} Data ---")
    for yf_ticker, lean_ticker in ticker_map.items():
        print(f"--> Fetching {yf_ticker} from Yahoo Finance...")
        try:
            df = yf.download(yf_ticker, start=START_DATE, end=END_DATE, progress=False)
            if df.empty:
                print(f"    ERROR: No data returned for {yf_ticker}.")
                continue
            
            output_dir = os.path.join(DATA_DIR, asset_class, market, 'daily')
            format_and_save_data(df, lean_ticker, output_dir, is_forex)
            
        except Exception as e:
            print(f"    ERROR: Failed to process {yf_ticker}. Reason: {e}")
        
        time.sleep(1) 

if __name__ == "__main__":
    equity_ticker_map = {ticker: ticker.lower() for ticker in EQUITY_TICKERS}
    
    download_data(equity_ticker_map, "equity", "usa")
    download_data(CRYPTO_TICKERS, "crypto", "coinbase")
    download_data(FOREX_TICKERS, "forex", "oanda", is_forex=True)
    
    print("\n\nAll data downloads are complete! You are ready to backtest.")
