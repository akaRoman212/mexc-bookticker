"""
MEXC Book Ticker Data Streamer
Streams real-time best bid/ask prices and quantities for multiple symbols
"""

import websocket
import json
import ssl
import threading
import time
import csv
import os
from datetime import datetime, timedelta
import bookticker_config as config
from PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper
from PublicAggreBookTickerV3Api_pb2 import PublicAggreBookTickerV3Api

# Import configuration
SYMBOLS = config.SYMBOLS
WS_URL = config.WS_URL
SAVE_INTERVAL_MINUTES = config.SAVE_INTERVAL_MINUTES
PING_INTERVAL_SECONDS = config.PING_INTERVAL_SECONDS
STATS_INTERVAL_SECONDS = config.STATS_INTERVAL_SECONDS
DEBUG_MODE = config.DEBUG_MODE
SHOW_RAW_MESSAGES = config.SHOW_RAW_MESSAGES

class MEXCBookTickerStreamer:
    def __init__(self, symbols: list = None, output_dir: str = "bookticker_data"):
        """
        Initialize MEXC WebSocket client for book ticker data
        
        Args:
            symbols: List of trading pair symbols (default: from bookticker_config.py)
            output_dir: Directory to save CSV files (default: bookticker_data)
        """
        self.symbols = [s.upper() for s in (symbols or SYMBOLS)]
        self.ws_url = WS_URL
        self.ws = None
        self.is_connected = False
        self.is_running = False
        
        # Create output directory
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        # CSV file setup - one file per symbol per day
        self.csv_files = {}
        self._initialize_csv_files()
        
        # Data storage - separate buffer for each symbol
        self.ticker_data_buffers = {symbol: [] for symbol in self.symbols}
        self.buffer_locks = {symbol: threading.Lock() for symbol in self.symbols}
        self.total_updates = {symbol: 0 for symbol in self.symbols}
        self.start_time = None
        self.last_save_time = datetime.now()
        self.save_interval = timedelta(minutes=SAVE_INTERVAL_MINUTES)
        
    def _initialize_csv_files(self):
        """Initialize CSV files for each symbol"""
        timestamp = datetime.now().strftime("%Y%m%d")
        
        for symbol in self.symbols:
            filename = f"{symbol}_bookticker_{timestamp}.csv"
            filepath = os.path.join(self.output_dir, filename)
            
            # Create CSV file with headers
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                headers = [
                    'timestamp', 'datetime', 'symbol', 'bid_price', 'bid_quantity', 
                    'ask_price', 'ask_quantity', 'spread', 'spread_percent', 'channel', 'event_type'
                ]
                writer.writerow(headers)
            
            self.csv_files[symbol] = filepath
            print(f"📊 CSV file initialized: {filename}")
        
        print(f"📋 Created CSV files for {len(self.symbols)} symbols: {', '.join(self.symbols)}")
    
    def _parse_protobuf_message(self, data, symbol=None):
        """Parse protobuf message for book ticker data"""
        try:
            # Parse the wrapper message
            wrapper = PushDataV3ApiWrapper()
            wrapper.ParseFromString(data)
            
            # Extract book ticker data
            if hasattr(wrapper, 'publicAggreBookTicker') and wrapper.publicAggreBookTicker:
                book_ticker = wrapper.publicAggreBookTicker
                
                # Extract data
                bid_price = float(book_ticker.bidPrice) if book_ticker.bidPrice else 0.0
                bid_quantity = float(book_ticker.bidQuantity) if book_ticker.bidQuantity else 0.0
                ask_price = float(book_ticker.askPrice) if book_ticker.askPrice else 0.0
                ask_quantity = float(book_ticker.askQuantity) if book_ticker.askQuantity else 0.0
                
                # Calculate spread
                spread = ask_price - bid_price if ask_price > 0 and bid_price > 0 else 0.0
                spread_percent = (spread / bid_price * 100) if bid_price > 0 else 0.0
                
                # Validate data
                if bid_price > 0 and ask_price > 0 and bid_quantity > 0 and ask_quantity > 0:
                    return {
                        'symbol': wrapper.symbol,
                        'bid_price': bid_price,
                        'bid_quantity': bid_quantity,
                        'ask_price': ask_price,
                        'ask_quantity': ask_quantity,
                        'spread': spread,
                        'spread_percent': spread_percent,
                        'channel': wrapper.channel,
                        'timestamp': wrapper.sendTime
                    }
            
            return None
            
        except Exception as e:
            if DEBUG_MODE:
                print(f"❌ Error parsing book ticker protobuf: {e}")
            return None
    
    def _on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            # Check if it's a JSON message (subscription response)
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                    if 'code' in data and 'msg' in data:
                        print(f"✅ Subscription response: {data}")
                        return
                    if data.get('msg') == 'PONG':
                        if DEBUG_MODE:
                            print("🏓 Received PONG")
                        return
                except json.JSONDecodeError:
                    pass

            # Handle protobuf messages for all symbols
            if isinstance(message, bytes) or (isinstance(message, str) and len(message) > 100):
                if SHOW_RAW_MESSAGES:
                    print(f"📨 Processing book ticker protobuf message ({len(message)} bytes)")

                # Parse the message once to extract the symbol
                ticker_data = self._parse_protobuf_message(message, None)

                if ticker_data:
                    symbol = ticker_data.get('symbol')
                    if symbol and symbol in self.symbols:
                        with self.buffer_locks[symbol]:
                            self._process_ticker_data(ticker_data, symbol)

        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def _process_ticker_data(self, ticker_data, symbol):
        """Process and store ticker data"""
        try:
            # Convert timestamp to datetime
            ticker_timestamp = ticker_data.get('timestamp', 0)
            if ticker_timestamp > 0:
                ticker_datetime = datetime.fromtimestamp(ticker_timestamp / 1000)
            else:
                ticker_datetime = datetime.now()
            
            # Create data row
            data_row = {
                'timestamp': ticker_timestamp,
                'datetime': ticker_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                'symbol': symbol,
                'bid_price': ticker_data.get('bid_price', 0.0),
                'bid_quantity': ticker_data.get('bid_quantity', 0.0),
                'ask_price': ticker_data.get('ask_price', 0.0),
                'ask_quantity': ticker_data.get('ask_quantity', 0.0),
                'spread': ticker_data.get('spread', 0.0),
                'spread_percent': ticker_data.get('spread_percent', 0.0),
                'channel': ticker_data.get('channel', ''),
                'event_type': 'spot@public.aggre.bookTicker.v3.api.pb@100ms'
            }
            
            self.ticker_data_buffers[symbol].append(data_row)
            self.total_updates[symbol] += 1
            
            # Print ticker info
            if DEBUG_MODE:
                print(f"📊 [{ticker_datetime.strftime('%H:%M:%S')}] {symbol}: "
                      f"Bid: {ticker_data.get('bid_price', 0):.2f}@{ticker_data.get('bid_quantity', 0):.4f} | "
                      f"Ask: {ticker_data.get('ask_price', 0):.2f}@{ticker_data.get('ask_quantity', 0):.4f} | "
                      f"Spread: {ticker_data.get('spread', 0):.2f} ({ticker_data.get('spread_percent', 0):.3f}%) "
                      f"[Buffer: {len(self.ticker_data_buffers[symbol])}]")
            
        except Exception as e:
            print(f"❌ Error processing ticker data for {symbol}: {e}")
    
    def _on_error(self, ws, error):
        """Handle WebSocket errors"""
        print(f"❌ WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        print(f"🔌 WebSocket connection closed: {close_status_code} - {close_msg}")
        self.is_connected = False
    
    def _on_open(self, ws):
        """Handle WebSocket connection open"""
        print("✅ WebSocket connection opened")
        self.is_connected = True
        
        # Subscribe to book ticker data streams for all symbols
        for symbol in self.symbols:
            subscription_message = {
                "method": "SUBSCRIPTION",
                "params": [f"spot@public.aggre.bookTicker.v3.api.pb@100ms@{symbol}"]
            }
            ws.send(json.dumps(subscription_message))
            print(f"📡 Subscribed to {symbol} book ticker data stream")
    
    def save_data_to_csv(self):
        """Save buffered data to CSV files"""
        try:
            total_saved = 0
            for symbol in self.symbols:
                with self.buffer_locks[symbol]:
                    if self.ticker_data_buffers[symbol]:
                        # Convert ticker data to CSV rows
                        csv_rows = []
                        for ticker_data in self.ticker_data_buffers[symbol]:
                            csv_rows.append([
                                ticker_data['timestamp'],
                                ticker_data['datetime'],
                                ticker_data['symbol'],
                                ticker_data['bid_price'],
                                ticker_data['bid_quantity'],
                                ticker_data['ask_price'],
                                ticker_data['ask_quantity'],
                                ticker_data['spread'],
                                ticker_data['spread_percent'],
                                ticker_data['channel'],
                                ticker_data['event_type']
                            ])
                        
                        # Append data to CSV file
                        with open(self.csv_files[symbol], 'a', newline='', encoding='utf-8') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerows(csv_rows)
                        
                        total_saved += len(self.ticker_data_buffers[symbol])
                        self.ticker_data_buffers[symbol].clear()
            
            if total_saved > 0:
                print(f"💾 Saved {total_saved:,} book ticker updates to CSV files across {len(self.symbols)} symbols")
            self.last_save_time = datetime.now()
            
        except Exception as e:
            print(f"❌ Error saving to CSV: {e}")
    
    def should_save_data(self):
        """Check if it's time to save data (configurable interval)"""
        return datetime.now() - self.last_save_time >= self.save_interval
    
    def _ping_thread(self):
        """Send ping messages to keep connection alive"""
        while self.is_running and self.is_connected:
            time.sleep(PING_INTERVAL_SECONDS)
            if self.ws and self.is_connected:
                try:
                    ping_message = {"method": "PING"}
                    self.ws.send(json.dumps(ping_message))
                    if DEBUG_MODE:
                        print("📡 Sent PING")
                except Exception as e:
                    print(f"❌ Error sending PING: {e}")
    
    def _print_statistics(self):
        """Print connection statistics"""
        while self.is_running:
            time.sleep(STATS_INTERVAL_SECONDS)
            if self.start_time:
                elapsed = time.time() - self.start_time
                total_updates_all = sum(self.total_updates.values())
                updates_per_minute = (total_updates_all / elapsed) * 60 if elapsed > 0 else 0
                time_until_save = self.save_interval - (datetime.now() - self.last_save_time)
                
                print(f"\n📊 --- Book Ticker Statistics ---")
                print(f"Total updates (all symbols): {total_updates_all}")
                print(f"Elapsed time: {elapsed:.1f} seconds")
                print(f"Updates per minute: {updates_per_minute:.1f}")
                print(f"Output directory: {self.output_dir}")
                print(f"Next save in: {time_until_save}")
                print(f"Connection status: {'Connected' if self.is_connected else 'Disconnected'}")
                
                # Show per-symbol stats
                for symbol in self.symbols:
                    buffer_count = len(self.ticker_data_buffers[symbol])
                    update_count = self.total_updates[symbol]
                    print(f"  {symbol}: {update_count} updates, {buffer_count} buffered")
                print("------------------\n")
    
    def _auto_save_thread(self):
        """Background thread to save data at configured intervals"""
        while self.is_running:
            time.sleep(60)  # Check every minute
            
            # Check if it's time to save data
            if self.should_save_data():
                total_buffered = sum(len(self.ticker_data_buffers[symbol]) for symbol in self.symbols)
                if total_buffered > 0:
                    interval_text = f"{SAVE_INTERVAL_MINUTES} minute(s)" if SAVE_INTERVAL_MINUTES < 60 else f"{SAVE_INTERVAL_MINUTES//60} hour(s)"
                    print(f"\n⏰ {interval_text} elapsed - saving {total_buffered:,} book ticker updates to CSV...")
                    self.save_data_to_csv()
    
    def connect(self):
        """Connect to MEXC WebSocket"""
        print(f"🔗 Connecting to MEXC WebSocket: {self.ws_url}")
        print(f"📊 Streaming book ticker data for {len(self.symbols)} symbols: {', '.join(self.symbols)}")
        
        # Create WebSocket connection with SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        self.is_running = True
        self.start_time = time.time()
        
        # Start background threads
        ping_thread = threading.Thread(target=self._ping_thread, daemon=True)
        stats_thread = threading.Thread(target=self._print_statistics, daemon=True)
        save_thread = threading.Thread(target=self._auto_save_thread, daemon=True)
        
        ping_thread.start()
        stats_thread.start()
        save_thread.start()
        
        # Run WebSocket (blocking)
        try:
            self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
            self.disconnect()
    
    def disconnect(self):
        """Disconnect from WebSocket"""
        print(f"\n🔄 Disconnecting...")
        self.is_running = False
        self.is_connected = False
        
        if self.ws:
            self.ws.close()
        
        # Save any remaining data before disconnecting
        total_buffered = sum(len(self.ticker_data_buffers[symbol]) for symbol in self.symbols)
        if total_buffered > 0:
            print(f"💾 Saving {total_buffered:,} remaining book ticker updates to CSV...")
            self.save_data_to_csv()
        
        total_updates_all = sum(self.total_updates.values())
        print(f"🔌 Connection closed. Total book ticker updates collected: {total_updates_all:,}")
        print(f"📁 CSV files saved to: {self.output_dir}")


def main():
    """Main function to run the MEXC Book Ticker WebSocket client"""
    print(f"🚀 MEXC Book Ticker Data Streamer (CSV Version - Unlimited Rows)")
    print(f"📊 Symbols: {', '.join(SYMBOLS)}")
    print(f"💾 Save interval: {SAVE_INTERVAL_MINUTES} minutes")
    print(f"📁 Output directory: bookticker_data/")
    print(f"🔧 Debug mode: {'ON' if DEBUG_MODE else 'OFF'}")
    print("-" * 60)
    
    # Create and start the streamer
    streamer = MEXCBookTickerStreamer()
    streamer.connect()


if __name__ == "__main__":
    main()
