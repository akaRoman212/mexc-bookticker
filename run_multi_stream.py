#!/usr/bin/env python3
"""
MEXC Multi-Symbol Trade Data Streamer
- Streams multiple trading pairs simultaneously
- Saves to different tabs in the same Excel file
- Configurable save intervals
- All symbols configurable in config.py
"""
from mexc_multi_streamer import MEXCMultiStreamer
from config import SYMBOLS, SAVE_INTERVAL_MINUTES, DEBUG_MODE


def main():
    print("🚀 MEXC Multi-Symbol Trade Data Streamer")
    print("=" * 60)
    print(f"📊 Symbols ({len(SYMBOLS)}): {', '.join(SYMBOLS)}")
    if SAVE_INTERVAL_MINUTES < 60:
        print(f"⏰ Save interval: {SAVE_INTERVAL_MINUTES} minute(s)")
    else:
        print(f"⏰ Save interval: {SAVE_INTERVAL_MINUTES//60} hour(s)")
    print(f"📁 Excel file: mexc_multi_trades_YYYYMMDD.xlsx")
    print("📋 Each symbol gets its own tab in the Excel file")
    print("🔄 Auto-reconnection on connection issues")
    print("🔧 Debug mode: ON" if DEBUG_MODE else "🔧 Debug mode: OFF")
    print("Press Ctrl+C to stop (data saves automatically)")
    print("-" * 60)
    print("💡 To change symbols or save interval, edit config.py")
    print("-" * 60)
    
    # Create client (symbols from config.py)
    client = MEXCMultiStreamer()
    
    try:
        client.connect()
    except KeyboardInterrupt:
        print("\n🛑 Stopping data stream...")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()

