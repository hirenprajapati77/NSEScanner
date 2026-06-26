import sys
sys.path.insert(0, '/home/ubuntu/NSEScanner')

from scanner import run_scan, get_nse_universe, CFG

# 1. Fetch NSE universe
tickers = get_nse_universe()

# 2. Setup config override
cfg = {
    **CFG,
    "VOL_MULT": 0.1,
    "MIN_SCORE": 0,
    "EMA_10": False,
    "EMA_20": False,
    "EMA_50": False,
    "EMA_200": False,
    "MAX_52W_AGE": 9999,
}

print(f"Starting production scan for {len(tickers)} tickers...")

# 3. Run Bull scan
bull_res = run_scan(tickers=tickers, bearish=False, cfg_override=cfg)
bull_signals = bull_res.get("signals", []) if isinstance(bull_res, dict) else bull_res

# 4. Run Bear scan
bear_res = run_scan(tickers=tickers, bearish=True, cfg_override=cfg)
bear_signals = bear_res.get("signals", []) if isinstance(bear_res, dict) else bear_res

print("\n--- RESULTS ---")
print(f"Bull signals found: {len(bull_signals)}")
print(f"Bear signals found: {len(bear_signals)}")
print(f"Total signals found: {len(bull_signals) + len(bear_signals)}")
