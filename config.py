"""
Configuration file for MEXC Trade Data Streamer
Change the SYMBOL here to stream different trading pairs
"""

# Trading pair symbols - LIST ALL SYMBOLS YOU WANT TO STREAM
SYMBOLS = [
    "BTCUSDT",    # Bitcoin
    "ETHUSDT",    # Ethereum
    "UMAUSDT",    # UMA
    "ADAUSDT",    # Cardano
    "DOTUSDT",    # Polkadot
    "LINKUSDT",   # Chainlink
    "LTCUSDT",    # Litecoin
    "XRPUSDT",    # Ripple
    "BNBUSDT",    # Binance Coin
    "SOLUSDT",    # Solana, 
    "AVAXUSDT"    # Avalanche
]

# Alternative: Use single symbol (for backward compatibility)
# SYMBOL = "BTCUSDT"  # Uncomment this and comment SYMBOLS above for single symbol mode

# WebSocket settings
WS_URL = "wss://wbs-api.mexc.com/ws"

# Data saving settings
SAVE_INTERVAL_MINUTES = 1  # Save data every X minutes (60 = 1 hour, 15 = 15 minutes, 1 = 1 minute)
PING_INTERVAL_SECONDS = 30  # Send ping every X seconds
STATS_INTERVAL_SECONDS = 60  # Print statistics every X seconds

# Output settings
OUTPUT_DIRECTORY = "trade_data"  # Directory for CSV files

# Debug settings
DEBUG_MODE = True  # Set to False to reduce console output
SHOW_RAW_MESSAGES = True  # Set to True to see raw protobuf messages
