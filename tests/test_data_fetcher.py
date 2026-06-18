# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch, MagicMock
import time
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import data_fetcher

class TestDataFetcher(unittest.TestCase):
    def setUp(self):
        # Reset the cache before each test
        data_fetcher._price_cache.clear()
        data_fetcher._fetch_history.clear()

    @patch('yfinance.Ticker')
    def test_cache_hit_within_ttl(self, mock_ticker):
        # Inject price in cache with current timestamp
        symbol = "MOCKSTOCK"
        data_fetcher._price_cache[symbol] = {
            'price': 150.0,
            'time': time.time(),
            'source': 'yahoo'
        }
        
        # Call get_stock_price - should hit cache and NOT call yfinance
        price, source, status = data_fetcher.get_stock_price(symbol)
        
        self.assertEqual(price, 150.0)
        self.assertEqual(source, 'yahoo')
        self.assertEqual(status, 'live')
        mock_ticker.assert_not_called()

    @patch('yfinance.Ticker')
    def test_cache_miss_triggers_fetch(self, mock_ticker):
        symbol = "MOCKSTOCK"
        
        # Setup mock ticker response
        mock_fast_info = MagicMock()
        mock_fast_info.get.side_effect = lambda k: 175.0 if k in ('lastPrice', 'last_price') else None
        mock_ticker.return_value.fast_info = mock_fast_info
        
        # First call - cache miss, triggers yfinance fetch
        price, source, status = data_fetcher.get_stock_price(symbol)
        
        self.assertEqual(price, 175.0)
        self.assertEqual(source, 'yahoo')
        self.assertEqual(status, 'live')
        mock_ticker.assert_called_once()
        
        # Verify it got saved to cache
        self.assertIn(symbol, data_fetcher._price_cache)
        self.assertEqual(data_fetcher._price_cache[symbol]['price'], 175.0)

    @patch('yfinance.Ticker')
    def test_dedupe_and_ttl(self, mock_ticker):
        symbol = "MOCKSTOCK"
        
        # Setup mock ticker response
        mock_fast_info = MagicMock()
        mock_fast_info.get.side_effect = lambda k: 200.0 if k in ('lastPrice', 'last_price') else None
        mock_ticker.return_value.fast_info = mock_fast_info
        
        # Call 1: Miss -> Fetch
        p1, src1, stat1 = data_fetcher.get_stock_price(symbol)
        self.assertEqual(p1, 200.0)
        self.assertEqual(stat1, 'live')
        
        # Call 2 (immediate): Hit -> Cache reuse
        p2, src2, stat2 = data_fetcher.get_stock_price(symbol)
        self.assertEqual(p2, 200.0)
        self.assertEqual(stat2, 'live')
        
        # Yahoo should only have been called once (first fetch)
        mock_ticker.assert_called_once()

    @patch('yfinance.Ticker')
    @patch('requests.Session')
    def test_fallback_to_stale_cache(self, mock_session, mock_ticker):
        symbol = "MOCKSTOCK"
        
        # 1. Put a stale price in cache (older than 30s)
        data_fetcher._price_cache[symbol] = {
            'price': 120.0,
            'time': time.time() - 45.0, # 45 seconds old (outside 30s TTL)
            'source': 'nse'
        }
        
        # 2. Mock network failure for both Yahoo and NSE
        mock_ticker.side_effect = Exception("Yahoo rate limit")
        mock_session.return_value.get.side_effect = Exception("NSE WAF blocked")
        
        # 3. Call get_stock_price - network fails, falls back to stale cache
        price, source, status = data_fetcher.get_stock_price(symbol)
        
        self.assertEqual(price, 120.0)
        self.assertTrue(source.startswith("cache"))
        self.assertEqual(status, 'cached')

    @patch('yfinance.Ticker')
    @patch('requests.Session')
    def test_unavailable_status(self, mock_session, mock_ticker):
        symbol = "MOCKSTOCK"
        
        # Mock total failures and no cache exists
        mock_ticker.side_effect = Exception("Yahoo down")
        mock_session.return_value.get.side_effect = Exception("NSE down")
        
        price, source, status = data_fetcher.get_stock_price(symbol)
        
        self.assertIsNone(price)
        self.assertEqual(source, 'unavailable')
        self.assertEqual(status, 'unavailable')

if __name__ == "__main__":
    unittest.main()
