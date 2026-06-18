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

if __name__ == '__main__':
    unittest.main()
