import json
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
import time


class BinanceScrapeService:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
    def get_crypto_ohlcv(self, symbol: str, interval: str = '1h', limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch OHLCV data for a cryptocurrency
        
        Args:
            symbol: Cryptocurrency symbol (e.g., 'BTCUSDT', 'ETHUSDT')
            interval: Time interval ('1m', '5m', '15m', '30m', '1h', '4h', '1d')
            limit: Number of data points to fetch (max 1000)
            
        Returns:
            List of OHLCV data with timestamps
        """
        try:
            endpoint = f"{self.base_url}/klines"
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': min(limit, 1000)
            }
            
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            ohlcv_data = []
            for item in data:
                # Binance returns: [open_time, open, high, low, close, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore]
                structured_data = {
                    "id": f"binance_{symbol}_{item[0]}",
                    "platform": "binance",
                    "symbol": symbol,
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                    "timestamp": datetime.fromtimestamp(item[0] / 1000, tz=timezone.utc).isoformat(),
                    "interval": interval,
                    "meta": {
                        "close_time": datetime.fromtimestamp(item[6] / 1000, tz=timezone.utc).isoformat(),
                        "quote_volume": float(item[7]),
                        "trades_count": int(item[8]),
                        "taker_buy_volume": float(item[9]),
                        "taker_buy_quote_volume": float(item[10])
                    }
                }
                ohlcv_data.append(structured_data)
            
            return ohlcv_data
            
        except Exception as e:
            print(f"Error fetching OHLCV data for {symbol}: {str(e)}")
            return []
    
    def get_multiple_crypto_data(self, symbols: List[str], interval: str = '1h', limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch OHLCV data for multiple cryptocurrencies
        
        Args:
            symbols: List of cryptocurrency symbols
            interval: Time interval
            limit: Number of data points per symbol
            
        Returns:
            Combined list of OHLCV data for all symbols
        """
        all_data = []
        
        for symbol in symbols:
            print(f"Fetching data for {symbol}...")
            crypto_data = self.get_crypto_ohlcv(symbol, interval, limit)
            all_data.extend(crypto_data)
            time.sleep(0.1)  # Rate limiting
        
        return all_data
    
    def get_crypto_data_by_date_range(self, symbol: str, start_date: str, end_date: str, interval: str = '1h') -> List[Dict[str, Any]]:
        """
        Fetch OHLCV data for a cryptocurrency within a specific date range
        
        Args:
            symbol: Cryptocurrency symbol
            start_date: Start date in format "YYYY-MM-DD" or "dd/mm/yyyy"
            end_date: End date in format "YYYY-MM-DD" or "dd/mm/yyyy"
            interval: Time interval
            
        Returns:
            List of OHLCV data within the date range
        """
        try:
            # Parse dates
            start_timestamp = self._parse_date(start_date)
            end_timestamp = self._parse_date(end_date)
            
            # Calculate how many intervals we need
            interval_ms = self._get_interval_ms(interval)
            total_intervals = int((end_timestamp - start_timestamp) / interval_ms)
            
            # Binance API limit is 1000 per request
            all_data = []
            current_start = start_timestamp
            
            while current_start < end_timestamp:
                # Fetch in batches of 1000
                batch_limit = min(1000, int((end_timestamp - current_start) / interval_ms) + 1)
                
                endpoint = f"{self.base_url}/klines"
                params = {
                    'symbol': symbol,
                    'interval': interval,
                    'startTime': int(current_start),
                    'endTime': int(end_timestamp),
                    'limit': batch_limit
                }
                
                response = requests.get(endpoint, headers=self.headers, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                
                for item in data:
                    structured_data = {
                        "id": f"binance_{symbol}_{item[0]}",
                        "platform": "binance",
                        "symbol": symbol,
                        "open": float(item[1]),
                        "high": float(item[2]),
                        "low": float(item[3]),
                        "close": float(item[4]),
                        "volume": float(item[5]),
                        "timestamp": datetime.fromtimestamp(item[0] / 1000, tz=timezone.utc).isoformat(),
                        "interval": interval,
                        "meta": {
                            "close_time": datetime.fromtimestamp(item[6] / 1000, tz=timezone.utc).isoformat(),
                            "quote_volume": float(item[7]),
                            "trades_count": int(item[8]),
                            "taker_buy_volume": float(item[9]),
                            "taker_buy_quote_volume": float(item[10])
                        }
                    }
                    all_data.append(structured_data)
                
                if data:
                    current_start = data[-1][6] + 1  # Move to next period
                else:
                    break
                
                time.sleep(0.1)  # Rate limiting
            
            return all_data
            
        except Exception as e:
            print(f"Error fetching date range data for {symbol}: {str(e)}")
            return []
    
    def _parse_date(self, date_input: str) -> int:
        """Parse date string to Unix timestamp in milliseconds"""
        try:
            # Check if it's already a timestamp
            if date_input.replace('.', '').isdigit():
                return int(float(date_input) * 1000)
            
            # Try different date formats
            formats = [
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d',
                '%d/%m/%Y',
                '%m/%d/%Y'
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_input, fmt)
                    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
                except ValueError:
                    continue
            
            # Try adding time if only date provided
            for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%d/%m/%Y', '%m/%d/%Y']:
                try:
                    dt = datetime.strptime(date_input, fmt)
                    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
                except ValueError:
                    continue
                    
            raise ValueError(f"Unable to parse date: {date_input}")
            
        except Exception as e:
            print(f"Error parsing date '{date_input}': {str(e)}")
            return 0
    
    def _get_interval_ms(self, interval: str) -> int:
        """Convert interval string to milliseconds"""
        interval_map = {
            '1m': 60 * 1000,
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '30m': 30 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000,
        }
        return interval_map.get(interval, 60 * 60 * 1000)  # Default to 1 hour
    
    def get_current_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current price for a cryptocurrency"""
        try:
            endpoint = f"{self.base_url}/ticker/price"
            params = {'symbol': symbol}
            
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            return {
                "symbol": data['symbol'],
                "price": float(data['price']),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            print(f"Error fetching current price for {symbol}: {str(e)}")
            return None
    
    def save_to_json(self, data: List[Dict[str, Any]], filename: str) -> None:
        """Save data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)