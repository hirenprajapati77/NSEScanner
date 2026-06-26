import sys
sys.path.insert(0, '/home/ubuntu/NSEScanner')

from scanner import analyse, CFG, is_nse_market_open

cfg = {
    **CFG,
    "VOL_MULT": 0.1,
    "EMA_10": False,
    "EMA_20": False,
    "EMA_50": False,
    "EMA_200": False,
    "MIN_SCORE": 0,
    "MAX_52W_AGE": 9999,
}

mkt = "OPEN" if is_nse_market_open() else "CLOSED"
print(f"--- 8-stock diagnostic | Market: {mkt} | VOL_MULT=0.1 | EMAs=OFF | MIN_SCORE=0 ---")
print(f"--- Risk cap: {'8%' if is_nse_market_open() else '10%'} | R/R floor: 1.0 (BEAR) or 1.5 ---")
print()

tickers = [
    "RELIANCE.NS", "TCS.NS", "SBIN.NS", "NMDC.NS", "INFY.NS",
    "HDFCBANK.NS", "ICICIBANK.NS", "WIPRO.NS"
]

passed = []
failed = []

for t in tickers:
    try:
        r = analyse(t, cfg_override=cfg, explain_skip=True)
        if r and not r.get("skipped"):
            passed.append(t)
            sym   = t.replace(".NS","")
            score = r.get("score","?")
            vol   = r.get("vol_ratio","?")
            rsi   = round(r.get("rsi",0), 1)
            rs50d = r.get("rs_50d","?")
            chg   = r.get("change","?")
            candle= r.get("candle","?")
            rr    = r.get("rr","?")
            risk  = r.get("risk_percentage","?")
            print(f"PASS {sym:12s} score={score:>4}  vol={vol}x  rsi={rsi}  rs50d={rs50d}%  chg={chg}%  candle={candle}  R/R={rr}  risk={risk}%")
        elif r and r.get("skipped"):
            failed.append((t, r["reason"]))
            sym = t.replace(".NS","")
            print(f"SKIP {sym:12s} {r['reason']}")
        else:
            failed.append((t, "None returned"))
            sym = t.replace(".NS","")
            print(f"FAIL {sym:12s} returned None")
    except Exception as e:
        failed.append((t, str(e)))
        sym = t.replace(".NS","")
        print(f"ERR  {sym:12s} {e}")

print()
print(f"=== RESULT: {len(passed)}/{len(tickers)} PASSED ===")
print(f"Passed: {', '.join(t.replace('.NS','') for t in passed) if passed else 'NONE'}")
print(f"Failed: {', '.join(t.replace('.NS','') for t,_ in failed) if failed else 'NONE'}")
