"""
Quick diagnostic: scan 3 stocks with full error output to find the exception.
"""
import json, traceback
from datetime import datetime
import pandas as pd
import yfinance as yf

# Patch scanner to expose skip reasons
import scanner
import agent_engine

# Build minimal market context
market_context = {
    "NIFTY_LTP": 24000.0,
    "NIFTY_EMA50": 23000.0,
    "NIFTY_PREV_CLOSE": 24000.0,
    "SECTOR_MOMENTUM": {},
    "REGIME": "SIDEWAYS",
    "SCAN_TIME_IST": datetime.now().strftime("%H:%M"),
    "VOLUME_RATIO_MODE": "1.0x_standard",
    "SCAN_MODE": "bullish"
}

TEST_TICKERS = ["RELIANCE.NS", "HDFCBANK.NS", "INFY.NS"]

for t in TEST_TICKERS:
    print(f"\n{'='*50}")
    print(f"Testing: {t}")
    try:
        # Fetch data directly
        df = yf.download(t, period="2y", interval="1d", progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0].lower() for c in df.columns]
        else:
            df.columns = [c.lower() for c in df.columns]
        df.dropna(inplace=True)
        print(f"  Data rows: {len(df)}, columns: {list(df.columns)}")
        
        result = agent_engine.analyse(
            ticker=t,
            bearish=False,
            cfg_override={},
            df=df,
            market_context=market_context,
            SKIP_TICKERS=scanner.SKIP_TICKERS,
            CFG=scanner.CFG,
            _download=None,
            get_nifty_returns=scanner.get_nifty_returns,
            is_nse_market_open=scanner.is_nse_market_open,
        )
        if result and result.get("skipped"):
            print(f"  SKIPPED gate={result.get('skip_gate')} reason={result.get('reason')}")
        elif result:
            print(f"  SIGNAL: score={result.get('quant_score')} grade={result.get('conf_grade')}")
        else:
            print(f"  None returned")
    except Exception as e:
        print(f"  EXCEPTION: {e}")
        traceback.print_exc()
