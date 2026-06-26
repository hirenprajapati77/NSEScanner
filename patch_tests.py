import os

tests_code = """
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

"""

with open("C:/Users/hiren/.gemini/antigravity/scratch/NSE_Camarilla_Scanner/tests/test_scanner_cache.py", "r", encoding="utf-8") as f:
    content = f.read()

content = content.replace("if __name__ == '__main__':", tests_code + "\\nif __name__ == '__main__':")

with open("C:/Users/hiren/.gemini/antigravity/scratch/NSE_Camarilla_Scanner/tests/test_scanner_cache.py", "w", encoding="utf-8") as f:
    f.write(content)
print("Tests patched successfully")
