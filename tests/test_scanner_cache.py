import unittest
import pandas as pd
import io
import json
import time
import warnings
import os

class TestScannerCache(unittest.TestCase):
    def setUp(self):
        # Create a mock dataframe similar to scanner.py cache
        dates = pd.date_range('2023-01-01', periods=5, freq='D')
        self.df = pd.DataFrame({
            'Open': [100.0] * 5,
            'High': [105.0] * 5,
            'Low': [95.0] * 5,
            'Close': [102.0] * 5,
            'Volume': [1000] * 5
        }, index=dates)

    def test_json_serialization_roundtrip_with_stringio(self):
        # 1. Serialize it exactly like scanner.py does
        cache_payload = {
            "timestamp": time.time(),
            "data": self.df.to_json(date_format='iso', orient='split')
        }
        json_dump = json.dumps(cache_payload)

        # 2. Parse it back
        parsed_payload = json.loads(json_dump)
        data_val = parsed_payload["data"]

        # 3. Call pd.read_json with io.StringIO
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            df_restored = pd.read_json(io.StringIO(data_val), orient='split')
            
            warning_msgs = [str(warning.message) for warning in w if "literal json" in str(warning.message).lower()]
            self.assertEqual(len(warning_msgs), 0, f"Expected no 'literal json' warning, but got: {warning_msgs}")

        self.assertIsInstance(df_restored.index, pd.DatetimeIndex)
        self.assertEqual(len(df_restored), 5)
        self.assertEqual(df_restored.iloc[0]['Open'], 100.0)

    def test_old_pattern_raises_warning_or_error(self):
        # 4. Assert old pattern raises warning or error
        cache_payload = {
            "timestamp": time.time(),
            "data": self.df.to_json(date_format='iso', orient='split')
        }
        data_val = json.loads(json.dumps(cache_payload))["data"]

        # In pandas 2.x, passing literal string to read_json emits FutureWarning
        # In pandas 3.x, it emits ValueError or FileNotFoundError
        raised = False
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                pd.read_json(data_val, orient='split')
                # If it doesn't throw an error, check if a warning was emitted
                if any("literal json" in str(warning.message).lower() for warning in w):
                    raised = True
            except Exception as e:
                # If it threw an error (like FileNotFoundError in pandas 3.0), we consider it successful
                raised = True
        
        self.assertTrue(raised, "Expected the old read_json(data_val) pattern to raise a warning or error, but it didn't.")

    def test_scanner_py_uses_stringio_for_read_json(self):
        # 5. Static analysis on scanner.py
        scanner_path = os.path.join(os.path.dirname(__file__), '..', 'scanner.py')
        with open(scanner_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Find all occurrences of pd.read_json
        import re
        matches = list(re.finditer(r'pd\.read_json\(', content))
        for match in matches:
            start_idx = match.end()
            # Look at the next 20 characters
            window = content[start_idx:start_idx+20]
            self.assertTrue("io.StringIO" in window, f"Found pd.read_json not followed by io.StringIO at index {match.start()} in scanner.py. Found: '{window}'")


    def test_cache_tail_refresh(self):
        import time
        from unittest.mock import patch, MagicMock
        from scanner import prefetch_batch
        
        # Create a historical dataframe of length 260
        dates = pd.date_range('2023-01-01', periods=260, freq='D')
        hist_df = pd.DataFrame({
            'open': [100.0] * 260, 'high': [105.0] * 260, 'low': [95.0] * 260, 'close': [102.0] * 260, 'volume': [1000] * 260
        }, index=dates)
        
        # Redis mock
        class MockRedis:
            def __init__(self):
                self.store = {}
            def get(self, key):
                return self.store.get(key)
            def setex(self, key, ttl, val):
                self.store[key] = val
                
        mock_redis = MockRedis()
        cache_payload = {
            "timestamp": time.time() - 300, # 5 mins old
            "last_full_refresh": time.time() - 3600, # 1 hour old (valid)
            "data": hist_df.to_json(date_format='iso', orient='split')
        }
        mock_redis.setex("stock:TEST.NS", 86400, json.dumps(cache_payload))
        
        # Tail dataframe
        tail_dates = pd.date_range('2023-09-15', periods=5, freq='D')
        tail_df = pd.DataFrame({
            'open': [110.0] * 5, 'high': [115.0] * 5, 'low': [105.0] * 5, 'close': [112.0] * 5, 'volume': [2000] * 5
        }, index=tail_dates)
        
        with patch('scanner.redis_client', mock_redis):
            with patch('scanner.yf.download') as mock_download:
                mock_download.return_value = tail_df
                # We also need to mock is_nse_market_open to return True so TTL is 120s
                with patch('scanner.is_nse_market_open', return_value=True):
                    with patch('scanner.is_post_market_invalidation_window', return_value=False):
                        res = prefetch_batch(["TEST.NS"])
                        
                        mock_download.assert_called_once()
                        # Assert period="5d" was used
                        self.assertEqual(mock_download.call_args[1].get('period'), '5d')
                        
                        # Assert merged correctly
                        cached_raw = mock_redis.get("stock:TEST.NS")
                        self.assertIsNotNone(cached_raw)
                        cached_parsed = json.loads(cached_raw)
                        self.assertGreater(cached_parsed["timestamp"], time.time() - 10)
                        self.assertEqual(cached_parsed["last_full_refresh"], cache_payload["last_full_refresh"])
                        
                        merged_df = pd.read_json(io.StringIO(cached_parsed["data"]), orient='split')
                        self.assertGreaterEqual(len(merged_df), 260)
                        # Check tail value
                        self.assertEqual(merged_df.iloc[-1]['close'], 112.0)

    def test_cache_full_refresh_on_missing(self):
        import time
        from unittest.mock import patch
        from scanner import prefetch_batch
        
        class MockRedis:
            def __init__(self):
                self.store = {}
            def get(self, key):
                return self.store.get(key)
            def setex(self, key, ttl, val):
                self.store[key] = val
                
        mock_redis = MockRedis()
        # Missing payload (None)
        
        # Mock full dataframe
        dates = pd.date_range('2022-01-01', periods=500, freq='D')
        full_df = pd.DataFrame({
            'open': [100.0] * 500, 'high': [105.0] * 500, 'low': [95.0] * 500, 'close': [102.0] * 500, 'volume': [1000] * 500
        }, index=dates)
        
        with patch('scanner.redis_client', mock_redis):
            with patch('scanner.yf.download') as mock_download:
                mock_download.return_value = full_df
                with patch('scanner.is_nse_market_open', return_value=True):
                    with patch('scanner.is_post_market_invalidation_window', return_value=False):
                        res = prefetch_batch(["MISSING.NS"])
                        mock_download.assert_called_once()
                        self.assertEqual(mock_download.call_args[1].get('period'), '2y')

    def test_cache_full_refresh_on_stale_last_full_refresh(self):
        import time
        from unittest.mock import patch
        from scanner import prefetch_batch
        
        dates = pd.date_range('2023-01-01', periods=260, freq='D')
        hist_df = pd.DataFrame({
            'open': [100.0] * 260, 'high': [105.0] * 260, 'low': [95.0] * 260, 'close': [102.0] * 260, 'volume': [1000] * 260
        }, index=dates)
        
        class MockRedis:
            def __init__(self):
                self.store = {}
            def get(self, key):
                return self.store.get(key)
            def setex(self, key, ttl, val):
                self.store[key] = val
                
        mock_redis = MockRedis()
        cache_payload = {
            "timestamp": time.time(), # Fresh timestamp!
            "last_full_refresh": time.time() - 90000, # > 24h old (stale)
            "data": hist_df.to_json(date_format='iso', orient='split')
        }
        mock_redis.setex("stock:STALE.NS", 86400, json.dumps(cache_payload))
        
        full_df = hist_df.copy()
        
        with patch('scanner.redis_client', mock_redis):
            with patch('scanner.yf.download') as mock_download:
                mock_download.return_value = full_df
                with patch('scanner.is_nse_market_open', return_value=True):
                    with patch('scanner.is_post_market_invalidation_window', return_value=False):
                        res = prefetch_batch(["STALE.NS"])
                        mock_download.assert_called_once()
                        self.assertEqual(mock_download.call_args[1].get('period'), '2y')

if __name__ == '__main__':
    unittest.main()
