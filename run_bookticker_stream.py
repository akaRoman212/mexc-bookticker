"""
Runner script for MEXC Book Ticker Data Streamer
Streams real-time best bid/ask prices and quantities
"""

import sys
import os

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mexc_bookticker_streamer import MEXCBookTickerStreamer
from bookticker_config import SYMBOLS, SAVE_INTERVAL_MINUTES
from datetime import datetime

def main():
    """Main function to run the book ticker streamer"""
    print("🚀 Starting MEXC Book Ticker Data Streamer...")
    print(f"📊 Streaming {len(SYMBOLS)} symbols: {', '.join(SYMBOLS)}")
    print(f"💾 Save interval: {SAVE_INTERVAL_MINUTES} minutes")
    print(f"📁 Output directory: bookticker_data/")
    print("-" * 60)
    
    try:
        # Create and start the streamer
        streamer = MEXCBookTickerStreamer()
        streamer.connect()
    except KeyboardInterrupt:
        print("\n🛑 Streamer stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
