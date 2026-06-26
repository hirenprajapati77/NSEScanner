import re

with open("C:\\Users\\hiren\\.gemini\\antigravity\\scratch\\NSE_Camarilla_Scanner\\scanner.py", "r", encoding="utf-8") as f:
    code = f.read()

# Replace fetch_with_timeout signature
code = re.sub(
    r"def fetch_with_timeout\(\n\s*ticker: str,\n\s*bearish: bool = False,\n\s*cfg_override: Optional\[Dict\] = None,\n\s*explain_skip: bool = False,\n\s*timeout: int = 30,\n\) -> Optional\[Dict\]:",
    "def fetch_with_timeout(\n    ticker: str,\n    bearish: bool = False,\n    cfg_override: Optional[Dict] = None,\n    explain_skip: bool = False,\n    timeout: int = 30,\n    market_context: Optional[Dict] = None,\n) -> Optional[Dict]:",
    code
)
# Replace analyse call inside fetch_with_timeout
code = code.replace(
    "future = ex.submit(analyse, ticker, bearish=bearish, cfg_override=cfg_override, explain_skip=explain_skip)",
    "future = ex.submit(analyse, ticker, bearish=bearish, cfg_override=cfg_override, explain_skip=explain_skip, market_context=market_context)"
)

# Replace run_scan signature
code = re.sub(
    r"def run_scan\(\n\s*tickers: Optional\[List\[str\]\] = None,\n\s*bearish: bool = False,\n\s*progress_cb: Optional\[Callable\] = None,\n\s*cfg_override: Optional\[Dict\] = None,\n\s*stop_event: Optional\[threading\.Event\] = None,\n\) -> List\[Dict\]:",
    "def run_scan(\n    tickers: Optional[List[str]] = None,\n    bearish: bool = False,\n    progress_cb: Optional[Callable] = None,\n    cfg_override: Optional[Dict] = None,\n    stop_event: Optional[threading.Event] = None,\n    market_context: Optional[Dict] = None,\n) -> List[Dict]:",
    code
)
# Replace fetch_with_timeout call inside run_scan
code = code.replace(
    "result = fetch_with_timeout(ticker, bearish=bearish, cfg_override=cfg_override, timeout=stock_timeout)",
    "result = fetch_with_timeout(ticker, bearish=bearish, cfg_override=cfg_override, timeout=stock_timeout, market_context=market_context)"
)
# Replace analyse call inside run_scan
code = code.replace(
    "result = analyse(ticker, bearish=bearish, cfg_override=cfg_override, df=df)",
    "result = analyse(ticker, bearish=bearish, cfg_override=cfg_override, df=df, market_context=market_context)"
)

# Find def analyse and def run_scan indices to replace the whole chunk
start_idx = code.find("def analyse(\n    ticker: str,")
end_idx = code.find("# ─────────────────────────────────────────────────────────────\n# SCAN ALL STOCKS")

if start_idx != -1 and end_idx != -1:
    new_analyse = '''def analyse(
    ticker: str,
    bearish: bool = False,
    cfg_override: Optional[Dict] = None,
    explain_skip: bool = False,
    df: Optional[pd.DataFrame] = None,
    market_context: Optional[Dict] = None,
) -> Optional[Dict]:
    import agent_engine
    return agent_engine.analyse(
        ticker=ticker,
        bearish=bearish,
        cfg_override=cfg_override,
        explain_skip=explain_skip,
        df=df,
        market_context=market_context,
        SKIP_TICKERS=SKIP_TICKERS,
        CFG=CFG,
        _download=_download,
        get_nifty_returns=get_nifty_returns,
        is_nse_market_open=is_nse_market_open,
    )

'''
    code = code[:start_idx] + new_analyse + code[end_idx:]

with open("C:\\Users\\hiren\\.gemini\\antigravity\\scratch\\NSE_Camarilla_Scanner\\scanner.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Patch complete")
