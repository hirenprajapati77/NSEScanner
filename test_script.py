import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
from scanner import analyse, CFG

test_stocks = ["RELIANCE.NS", "INFY.NS", "SBIN.NS", 
               "KOTAKBANK.NS", "MARUTI.NS"]

for ticker in test_stocks:
    r = analyse(ticker, cfg_override={
        **CFG,
        "VOL_MULT": 0.1,
        "MIN_SCORE": 0,
        "SCAN_MODE": "both",
        "EMA_10": False,
        "EMA_20": False,
        "EMA_50": False,
        "EMA_200": False,
        "MIN_TURNOVER_CR": 1.0,
        "MAX_52W_AGE": 9999,
        "MAX_52W_AGE": 9999,
    }, explain_skip=True)
    if r and not r.get('skipped'):
        print(f"✅ {ticker}: entry_type={r.get('entry_type')} "
              f"entry={r.get('entry')} sl={r.get('stop_loss')} "
              f"t1={r.get('target')} rr={r.get('rr')}x")
    else:
        reason = r.get('reason') if r else "Unknown (returned None)"
        print(f"❌ {ticker}: still rejected - {reason}")
