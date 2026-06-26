import time
import logging
from scanner import run_scan

logging.basicConfig(level=logging.INFO)

tickers = ["RELIANCE.NS","TCS.NS","INFY.NS","SBIN.NS","NMDC.NS",
           "HDFCBANK.NS","WIPRO.NS","ICICIBANK.NS","AXISBANK.NS","LT.NS"]

start = time.time()
results = run_scan(tickers)
print("\n" + "="*80)
print(f"⚡ 10 stocks scan completed in {time.time()-start:.1f}s")
print(f"Signals found: {len(results.get('signals', [])) if isinstance(results, dict) else len(results)}")
print("="*80)
