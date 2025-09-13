"""
MEXC Multi-Symbol WebSocket Client for streaming multiple trading pairs
Saves data to separate CSV files for each symbol (unlimited rows)
"""
import websocket
import json
import threading
import time
import csv
import os
from datetime import datetime, timedelta
import ssl
import socket
import struct
from config import SYMBOLS, WS_URL, SAVE_INTERVAL_MINUTES, PING_INTERVAL_SECONDS, STATS_INTERVAL_SECONDS, DEBUG_MODE, SHOW_RAW_MESSAGES
from PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper


class MEXCMultiStreamer:
    def __init__(self, symbols: list = None, output_dir: str = "trade_data"):
        """
        Initialize MEXC WebSocket client for multiple symbols
        
        Args:
            symbols: List of trading pair symbols (default: from config.py)
            output_dir: Directory to save CSV files (default: trade_data)
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
        self.trade_data_buffers = {symbol: [] for symbol in self.symbols}
        self.buffer_locks = {symbol: threading.Lock() for symbol in self.symbols}
        self.total_trades = {symbol: 0 for symbol in self.symbols}
        self.start_time = None
        self.last_save_time = datetime.now()
        self.save_interval = timedelta(minutes=SAVE_INTERVAL_MINUTES)
        
    def _initialize_csv_files(self):
        """Initialize CSV files for each symbol"""
        timestamp = datetime.now().strftime("%Y%m%d")
        
        for symbol in self.symbols:
            filename = f"{symbol}_trades_{timestamp}.csv"
            filepath = os.path.join(self.output_dir, filename)
            
            # Create CSV file with headers
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                headers = [
                    'timestamp', 'datetime', 'symbol', 'price', 'quantity', 
                    'trade_type', 'trade_type_str', 'channel', 'event_type'
                ]
                writer.writerow(headers)
            
            self.csv_files[symbol] = filepath
            print(f"📊 CSV file initialized: {filename}")
        
        print(f"📋 Created CSV files for {len(self.symbols)} symbols: {', '.join(self.symbols)}")
    
    def _parse_protobuf_message(self, data, symbol=None):
        """Parse protobuf message using proper protobuf deserialization"""
        try:
            trades = []
            
            if SHOW_RAW_MESSAGES:
                print(f"🔍 Parsing protobuf message, data length: {len(data)} bytes")
            
            # Parse the protobuf message
            wrapper = PushDataV3ApiWrapper()
            wrapper.ParseFromString(data)
            
            if DEBUG_MODE:
                print(f"🔍 Parsed wrapper - Channel: {wrapper.channel}, Symbol: {wrapper.symbol}")
                if wrapper.publicAggreDeals:
                    print(f"🔍 Found {len(wrapper.publicAggreDeals.deals)} deals")
            
            # Extract symbol from wrapper if available
            message_symbol = wrapper.symbol if wrapper.symbol else symbol
            
            # Only process if we have a valid symbol
            if not message_symbol:
                if DEBUG_MODE:
                    print("⚠️ No symbol found in message")
                return []
            
            # Process the deals if available
            if wrapper.publicAggreDeals and wrapper.publicAggreDeals.deals:
                for deal in wrapper.publicAggreDeals.deals:
                    try:
                        # Convert string values to float
                        price = float(deal.price)
                        quantity = float(deal.quantity)
                        
                        # Basic validation
                        if (price > 0 and quantity > 0 and 
                            price != quantity and 
                            abs(price - quantity) > 0.001):
                            
                            trade = {
                                'price': price,
                                'quantity': quantity,
                                'trade_type': deal.tradeType,
                                'symbol': message_symbol,
                                'channel': wrapper.channel,
                                'timestamp': deal.time if deal.time > 0 else int(time.time() * 1000)
                            }
                            trades.append(trade)
                            
                            if DEBUG_MODE:
                                print(f"✅ {message_symbol} - Parsed trade: {price} x {quantity} (type: {deal.tradeType})")
                    except (ValueError, TypeError) as e:
                        if DEBUG_MODE:
                            print(f"⚠️ {message_symbol} - Error parsing deal: {e}")
                        continue
            
            if DEBUG_MODE and not trades:
                print(f"⚠️ {message_symbol} - No valid trades found in message")
            
            return trades
            
        except Exception as e:
            print(f"❌ Error parsing protobuf message: {e}")
            if DEBUG_MODE:
                print(f"🔍 Raw data (first 100 bytes): {data[:100]}")
            return []
    
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
                    print(f"📨 Processing protobuf message ({len(message)} bytes)")
                
                # Parse the message once to extract the symbol
                trades = self._parse_protobuf_message(message, None)
                
                if trades:
                    # Group trades by symbol
                    trades_by_symbol = {}
                    for trade in trades:
                        symbol = trade.get('symbol')
                        if symbol and symbol in self.symbols:
                            if symbol not in trades_by_symbol:
                                trades_by_symbol[symbol] = []
                            trades_by_symbol[symbol].append(trade)
                    
                    # Process trades for each symbol
                    for symbol, symbol_trades in trades_by_symbol.items():
                        with self.buffer_locks[symbol]:
                            for trade in symbol_trades:
                                self._process_trade(trade, symbol)
                        
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def _process_trade(self, trade, symbol):
        """Process individual trade data"""
        try:
            # Convert timestamp to datetime
            timestamp = trade.get('timestamp', int(time.time() * 1000))
            trade_datetime = datetime.fromtimestamp(timestamp / 1000)
            
            # Prepare data row
            data_row = {
                'timestamp': timestamp,
                'datetime': trade_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                'symbol': trade.get('symbol', symbol),
                'price': trade.get('price', 0.0),
                'quantity': trade.get('quantity', 0.0),
                'trade_type': trade.get('trade_type', 0),
                'trade_type_str': 'BUY' if trade.get('trade_type') == 1 else 'SELL',
                'channel': trade.get('channel', ''),
                'event_type': 'spot@public.aggre.deals.v3.api.pb@100ms'
            }
            
            self.trade_data_buffers[symbol].append(data_row)
            self.total_trades[symbol] += 1
            
            # Print trade info
            if DEBUG_MODE:
                print(f"💰 [{trade_datetime.strftime('%H:%M:%S')}] {symbol}: "
                      f"{trade.get('price', 0):.2f} x {trade.get('quantity', 0):.8f} "
                      f"({'BUY' if trade.get('trade_type') == 1 else 'SELL'}) "
                      f"[Buffer: {len(self.trade_data_buffers[symbol])}]")
            
        except Exception as e:
            print(f"❌ Error processing trade for {symbol}: {e}")
    
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
        
        # Subscribe to trade data streams for all symbols
        for symbol in self.symbols:
            subscription_message = {
                "method": "SUBSCRIPTION",
                "params": [f"spot@public.aggre.deals.v3.api.pb@100ms@{symbol}"]
            }
            ws.send(json.dumps(subscription_message))
            print(f"📡 Subscribed to {symbol} trade data stream")
    
    def save_data_to_csv(self):
        """Save buffered data to CSV files"""
        try:
            total_saved = 0
            for symbol in self.symbols:
                with self.buffer_locks[symbol]:
                    if self.trade_data_buffers[symbol]:
                        # Convert trade data to CSV rows
                        csv_rows = []
                        for trade_data in self.trade_data_buffers[symbol]:
                            csv_rows.append([
                                trade_data['timestamp'],
                                trade_data['datetime'],
                                trade_data['symbol'],
                                trade_data['price'],
                                trade_data['quantity'],
                                trade_data['trade_type'],
                                trade_data['trade_type_str'],
                                trade_data['channel'],
                                trade_data['event_type']
                            ])
                        
                        # Append data to CSV file
                        with open(self.csv_files[symbol], 'a', newline='', encoding='utf-8') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerows(csv_rows)
                        
                        total_saved += len(self.trade_data_buffers[symbol])
                        self.trade_data_buffers[symbol].clear()
            
            if total_saved > 0:
                print(f"💾 Saved {total_saved:,} trades to CSV files across {len(self.symbols)} symbols")
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
                total_trades_all = sum(self.total_trades.values())
                trades_per_minute = (total_trades_all / elapsed) * 60 if elapsed > 0 else 0
                time_until_save = self.save_interval - (datetime.now() - self.last_save_time)
                
                print(f"\n📊 --- Statistics ---")
                print(f"Total trades (all symbols): {total_trades_all}")
                print(f"Elapsed time: {elapsed:.1f} seconds")
                print(f"Trades per minute: {trades_per_minute:.1f}")
                print(f"Output directory: {self.output_dir}")
                print(f"Next save in: {time_until_save}")
                print(f"Connection status: {'Connected' if self.is_connected else 'Disconnected'}")
                
                # Show per-symbol stats
                for symbol in self.symbols:
                    buffer_count = len(self.trade_data_buffers[symbol])
                    trade_count = self.total_trades[symbol]
                    print(f"  {symbol}: {trade_count} trades, {buffer_count} buffered")
                print("------------------\n")
    
    def _auto_save_thread(self):
        """Background thread to save data at configured intervals"""
        while self.is_running:
            time.sleep(60)  # Check every minute
            
            # Check if it's time to save data
            if self.should_save_data():
                total_buffered = sum(len(self.trade_data_buffers[symbol]) for symbol in self.symbols)
                if total_buffered > 0:
                    interval_text = f"{SAVE_INTERVAL_MINUTES} minute(s)" if SAVE_INTERVAL_MINUTES < 60 else f"{SAVE_INTERVAL_MINUTES//60} hour(s)"
                    print(f"\n⏰ {interval_text} elapsed - saving {total_buffered:,} trades to CSV...")
                    self.save_data_to_csv()
    
    def connect(self):
        """Connect to MEXC WebSocket"""
        print(f"🔗 Connecting to MEXC WebSocket: {self.ws_url}")
        print(f"📊 Streaming {len(self.symbols)} symbols: {', '.join(self.symbols)}")
        
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
        total_buffered = sum(len(self.trade_data_buffers[symbol]) for symbol in self.symbols)
        if total_buffered > 0:
            print(f"💾 Saving {total_buffered:,} remaining trades to CSV...")
            self.save_data_to_csv()
        
        total_trades_all = sum(self.total_trades.values())
        print(f"🔌 Connection closed. Total trades collected: {total_trades_all:,}")
        print(f"📁 CSV files saved to: {self.output_dir}")


def main():
    """Main function to run the MEXC Multi-Symbol WebSocket client"""
    print(f"🚀 MEXC Multi-Symbol Trade Data Streamer (CSV Version - Unlimited Rows)")
    print("=" * 70)
    print(f"📊 Symbols: {', '.join(SYMBOLS)}")
    if SAVE_INTERVAL_MINUTES < 60:
        print(f"⏰ Save interval: {SAVE_INTERVAL_MINUTES} minute(s)")
    else:
        print(f"⏰ Save interval: {SAVE_INTERVAL_MINUTES//60} hour(s)")
    print(f"📁 Output directory: trade_data/")
    print(f"🔧 Debug mode: {'ON' if DEBUG_MODE else 'OFF'}")
    print("-" * 70)
    
    # Create and run the client
    client = MEXCMultiStreamer()
    
    try:
        client.connect()
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
