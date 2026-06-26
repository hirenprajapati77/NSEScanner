import time, json
from datetime import datetime
import pandas as pd
import yfinance as yf
from scanner import run_scan, NIFTY500_SAMPLE

def run_verification():
    print(f"Starting verification scan on sample of 102 stocks...")

    # 1. Build market context dynamically from single source of truth
    from agent_engine import get_market_context
    market_context = get_market_context(scan_mode="bullish")

    # Confirm regime is set BEFORE the stock loop
    print(f"[CHECK] Regime evaluated BEFORE scan loop: {market_context['REGIME']}")
    print(f"[CHECK] Scan time (live clock): {market_context['SCAN_TIME_IST']}")

    # Same 100-stock sample + the two specific stocks to check
    test_tickers = list(set(NIFTY500_SAMPLE[:100] + ["ASHOKLEY.NS", "SHRIRAMFIN.NS"]))

    result_dict = run_scan(tickers=test_tickers, bearish=False, market_context=market_context)
    signals   = result_dict.get("signals", [])
    scan_meta = result_dict.get("scan_meta", {})

    print("\n" + "=" * 60)
    print("SCAN META (full rejection breakdown)")
    print("=" * 60)
    print(json.dumps(scan_meta, indent=2))

    print("\n" + "=" * 60)
    print("SCORE DISTRIBUTION")
    print("=" * 60)
    scores = [r["quant_score"] for r in signals if isinstance(r, dict) and "quant_score" in r]
    if scores:
        print(f"  Surviving setups  : {len(scores)}")
        print(f"  Score range       : {min(scores)} – {max(scores)}")
        print(f"  Avg score         : {sum(scores)/len(scores):.1f}")
        for grade in ["A", "B", "C"]:
            count = len([r for r in signals if isinstance(r, dict) and r.get("conf_grade", "").startswith(grade)])
            print(f"  Grade {grade}           : {count}")
    else:
        print("  No surviving setups.")

    print("\n" + "=" * 60)
    print("EXTENDED STOCK CHECK (ASHOKLEY / SHRIRAMFIN)")
    print("=" * 60)
    found = False
    for r in signals:
        if isinstance(r, dict) and r.get("symbol") in ("ASHOKLEY", "SHRIRAMFIN"):
            found = True
            bd = r.get("score_breakdown", {})
            print(f"  {r['symbol']}: score={r.get('quant_score')} | "
                  f"flags={r.get('flags')} | ext_penalty={bd.get('extension')} | "
                  f"hard_cap={bd.get('hard_cap')}")
    if not found:
        print("  Neither stock survived pre-filters. Check rejection_reasons above.")

if __name__ == "__main__":
    run_verification()
