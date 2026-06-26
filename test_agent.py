import scanner
import pandas as pd

# Mock market context
mc = {
    "NIFTY_LTP": 25000.0,
    "NIFTY_EMA50": 24000.0,
    "NIFTY_PREV_CLOSE": 25000.0,
    "SECTOR_MOMENTUM": {},
    "REGIME": "STRONG_BULL",
    "SCAN_TIME_IST": "14:30",
    "VOLUME_RATIO_MODE": "1.0x_standard",
    "SCAN_MODE": "bullish"
}

try:
    print("Running scanner test...")
    res = scanner.run_scan(tickers=["RELIANCE.NS"], bearish=False, market_context=mc)
    print("Success!", res)
except Exception as e:
    import traceback
    traceback.print_exc()
