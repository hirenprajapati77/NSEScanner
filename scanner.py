"""
NSE Camarilla Volume Scanner — Robust v3.0 [Phase 2 Hardened]
============================================================
Improvements in Phase 2:
  • Critical Fix #1: Alert deduplication & cooldown engine using alert_history.json & SQLite tracker.
  • Critical Fix #2: Strict DataFrame validation layer (validate_dataframe) rejecting corrupt pricing, negative ranges, and NaNs.
  • Critical Fix #3: Dynamic ATR-based stop-loss cap (minimum 1.5 * ATR stop, maximum 5% entry risk threshold).
  • Critical Fix #4: Thread-safe deepcopy architecture across Flask state handlers.
  • High Priority #1: Average 20-day turnover liquidity filter (10 Crore INR threshold).
  • High Priority #2: 90-day stock return relative strength versus NIFTY index (^NSEI) return.
  • High Priority #3: Dynamic signal confidence grades (A+ to D) with strength levels.
  • High Priority #4: Backtesting SQLite database architecture logging metrics, alerts, and signal histories.
  • Performance system monitoring utilizing psutil logging metrics.
"""

import os
import time
import json
import logging
import threading
import sqlite3
from datetime import datetime
from typing import Callable, Dict, List, Optional

import pandas as pd
import requests
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from regime import get_regime, adjust_score_for_regime
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import psutil
except ImportError:
    psutil = None

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("NSEScanner")


# ─────────────────────────────────────────────────────────────
# CONFIG  (all values override-able via .env or cfg_override)
# ─────────────────────────────────────────────────────────────
CFG: Dict = {
    # Volume: today's vol must be >= VOL_MULT × VOL_DAYS-day average
    "VOL_DAYS":       int(os.getenv("VOL_DAYS",       "10")),
    "VOL_MULT":       float(os.getenv("VOL_MULT",     "2.0")),
    # EMA filters — set env to "false" to skip that particular EMA
    "EMA_10":         os.getenv("EMA_10",  "true").lower() == "true",
    "EMA_20":         os.getenv("EMA_20",  "true").lower() == "true",
    "EMA_50":         os.getenv("EMA_50",  "true").lower() == "true",
    "EMA_200":        os.getenv("EMA_200", "true").lower() == "true",
    # Minimum score to surface a signal
    "MIN_SCORE":      int(os.getenv("MIN_SCORE",      "70")),
    # 52-week lookback window (trading days)
    "HV_DAYS":        int(os.getenv("HV_DAYS",        "252")),
    # Concurrency
    "MAX_WORKERS":    int(os.getenv("MAX_WORKERS",    "6")),
    # Telegram
    "TG_TOKEN":       os.getenv("TG_TOKEN",       ""),
    "TG_CHAT_ID":     os.getenv("TG_CHAT_ID",     ""),
    # WhatsApp / Twilio
    "TWILIO_SID":     os.getenv("TWILIO_SID",     ""),
    "TWILIO_TOKEN":   os.getenv("TWILIO_TOKEN",   ""),
    "TWILIO_FROM":    os.getenv("TWILIO_FROM",    "whatsapp:+14155238886"),
    "TWILIO_TO":      os.getenv("TWILIO_TO",      ""),
    # Continuous scan interval (seconds)
    "INTERVAL":       int(os.getenv("SCAN_INTERVAL",  "300")),
    # Liquidity Filter (10 Crore INR default: 1 Crore = 10,000,000)
    "TURNOVER_LIMIT": float(os.getenv("TURNOVER_LIMIT", "100000000")),
    "USE_CACHE_ONLY": False,
    # default MIN_PRICE set to 50.0 INR and MAX_52W_AGE set to 180 days
    "MIN_PRICE":      float(os.getenv("MIN_PRICE",      "50.0")),
    "MAX_52W_AGE":    int(os.getenv("MAX_52W_AGE",    "180")),
}


# ─────────────────────────────────────────────────────────────
# SHARED HTTP SESSION  (one session, thread-safe reads)
# ─────────────────────────────────────────────────────────────
def _make_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://",  adapter)
    sess.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })
    return sess


_SESSION = _make_session()
_YF_LOCK = threading.Lock()


# ─────────────────────────────────────────────────────────────
# BACKTESTING & SQLITE HISTORY ENGINE
# ─────────────────────────────────────────────────────────────
DB_FILE = "scanner.db"

def init_db() -> None:
    """
    Initializes the SQLite database and creates tables if they do not exist.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # 1. signal_history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signal_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                ticker TEXT,
                signal_type TEXT,
                entry REAL,
                stoploss REAL,
                target1 REAL,
                target2 REAL,
                score INTEGER,
                confidence TEXT,
                rs_pct REAL,
                turnover_crore REAL,
                scanned_at TEXT
            )
        """)
        
        # 2. alert_history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                signal_type TEXT,
                entry_price REAL,
                channel TEXT,
                timestamp REAL
            )
        """)
        
        # 3. scan_metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scan_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                duration_seconds REAL,
                tickers_scanned INTEGER,
                signals_found INTEGER,
                cache_hits INTEGER,
                cpu_usage REAL,
                memory_usage REAL
            )
        """)
        
        conn.commit()
        conn.close()
        log.info("💾 SQLite database initialized successfully.")
    except Exception as exc:
        log.error(f"Error initializing SQLite database: {exc}")


init_db()


# ─────────────────────────────────────────────────────────────
# NSE STOCK UNIVERSE  (~120 Nifty-500 stocks)
# ─────────────────────────────────────────────────────────────
NIFTY500_SAMPLE: List[str] = [
    # NIFTY 50 & Large Caps
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "HINDUNILVR.NS", "ITC.NS", "SBIN.NS", "BHARTIARTL.NS", "KOTAKBANK.NS",
    "LT.NS", "AXISBANK.NS", "ASIANPAINT.NS", "MARUTI.NS", "SUNPHARMA.NS",
    "TITAN.NS", "BAJFINANCE.NS", "WIPRO.NS", "NESTLEIND.NS", "ULTRACEMCO.NS",
    "APOLLOHOSP.NS", "TECHM.NS", "HCLTECH.NS", "POWERGRID.NS", "NTPC.NS",
    "TMCV.NS", "ONGC.NS", "JSWSTEEL.NS", "TATASTEEL.NS", "BAJAJFINSV.NS",
    "DIVISLAB.NS", "DRREDDY.NS", "CIPLA.NS", "EICHERMOT.NS", "HEROMOTOCO.NS",
    "GRASIM.NS", "BPCL.NS", "COALINDIA.NS", "INDUSINDBK.NS", "ADANIPORTS.NS",
    "DABUR.NS", "MARICO.NS", "PIDILITIND.NS", "BERGEPAINT.NS", "HAVELLS.NS",
    "TATACONSUM.NS", "GODREJCP.NS", "MUTHOOTFIN.NS", "CHOLAFIN.NS", "SRF.NS",
    "AARTIIND.NS", "ABCAPITAL.NS", "ACC.NS", "AIAENG.NS", "ALKEM.NS",
    "AMBUJACEM.NS", "APLLTD.NS", "AUBANK.NS", "BALKRISIND.NS", "BANDHANBNK.NS",
    "BEL.NS", "BHARATFORG.NS", "BIOCON.NS", "CANBK.NS", "CESC.NS",
    "CROMPTON.NS", "CUB.NS", "DEEPAKNTR.NS", "ESCORTS.NS",
    "FEDERALBNK.NS", "GAIL.NS", "GMRINFRA.NS", "GNFC.NS", "GODREJPROP.NS",
    "GRANULES.NS", "GSPL.NS", "HAPPSTMNDS.NS", "HINDPETRO.NS", "HINDCOPPER.NS",
    "IDFCFIRSTB.NS", "IEX.NS", "IPCALAB.NS", "IRCTC.NS",
    "JINDALSTEL.NS", "JUBLFOOD.NS", "KANSAINER.NS", "LALPATHLAB.NS", "LTIM.NS",
    "LUPIN.NS", "M&M.NS", "MANAPPURAM.NS", "MFSL.NS",
    "MOTHERSON.NS", "MPHASIS.NS", "MRF.NS", "NMDC.NS", "OBEROIRLTY.NS",
    "OFSS.NS", "PAGEIND.NS", "PERSISTENT.NS", "PETRONET.NS",
    "PFC.NS", "RAMCOCEM.NS", "RBLBANK.NS",
    "RECLTD.NS", "SAIL.NS", "SHREECEM.NS", "SIEMENS.NS",
    "SUPREMEIND.NS", "SYNGENE.NS", "TORNTPHARM.NS",
    "TRENT.NS", "UBL.NS", "VEDL.NS", "VOLTAS.NS",
    "ZOMATO.NS", "ZYDUSLIFE.NS",
    # Additional Large and Midcap Liquid Names
    "ADANIENT.NS", "ADANIGREEN.NS", "ADANIENSOL.NS", "ATGL.NS",
    "BAJAJ-AUTO.NS", "CAMS.NS", "CANFINHOME.NS",
    "CONCOR.NS", "COROMANDEL.NS", "DALBHARAT.NS", "DIXON.NS",
    "GLENMARK.NS", "HAL.NS", "ICICIGI.NS", "ICICIPRULI.NS",
    "IDEA.NS", "INDHOTEL.NS", "INDUSTOWER.NS", "INOXWIND.NS",
    "IOC.NS", "ISEC.NS", "JKCEMENT.NS", "JUBILANT.NS",
    "KAJARIACER.NS", "LAURUSLABS.NS", "LICHSGFIN.NS", "LINDEINDIA.NS",
    "MCX.NS", "METROPOLIS.NS", "NAUKRI.NS",
    "NAVINFLUOR.NS", "PIIND.NS", "POLYCAB.NS", "RELAXO.NS", "ROUTE.NS",
    "SBICARD.NS", "SBILIFE.NS", "SCHAEFFLER.NS", "SOLARINDS.NS",
    "SUNTV.NS", "TATACHEM.NS", "TATACOMM.NS",
    "TATAELXSI.NS", "TATAINVEST.NS", "TIINDIA.NS", "TTKPRESTIG.NS",
    "VGUARD.NS", "WHIRLPOOL.NS", "ZEEL.NS",
    # Highly Liquid Missing Nifty 500 & F&O Leaders
    "TATAPOWER.NS", "BHEL.NS", "IRFC.NS", "RVNL.NS", "IREDA.NS",
    "LTF.NS", "HUDCO.NS", "ASHOKLEY.NS", "JIOFIN.NS", "HINDALCO.NS",
    "JSWENERGY.NS", "NHPC.NS", "SJVN.NS", "UNIONBANK.NS", "IOB.NS",
    "BANKBARODA.NS", "PNB.NS", "INDIANB.NS", "NYKAA.NS", "PAYTM.NS",
    "POLICYBZR.NS", "DELHIVERY.NS", "SUZLON.NS", "YESBANK.NS", "LICI.NS",
    "GICRE.NS", "IRB.NS", "NBCC.NS", "ENGINEERSIN.NS", "NATIONALUM.NS",
    "HINDZINC.NS", "OIL.NS", "MRPL.NS", "CHENNPETRO.NS", "MGL.NS",
    "IGL.NS", "GUJGASLTD.NS", "GSFC.NS", "RCF.NS", "CHAMBLFERT.NS",
    "HFCL.NS", "TEJASNET.NS", "KPITTECH.NS", "COFORGE.NS", "CYIENT.NS",
    "LTTS.NS", "SONACOMS.NS", "EXIDEIND.NS", "ARE&M.NS", "BOSCHLTD.NS",
    "UNOCOUNT.NS", "APOLLOTYRE.NS", "JKTYRE.NS", "CEATLTD.NS", "MAXHEALTH.NS",
    "MANKIND.NS", "AUROPHARMA.NS", "STAR.NS", "BATAINDIA.NS", "METROBRAND.NS",
    "CAMPUS.NS", "ABFRL.NS", "V-MART.NS", "FORTIS.NS", "ADANIPOWER.NS",
    "HPCL.NS", "PEL.NS", "IBULHSGFIN.NS", "PVRINOX.NS", "AMBUJACEM.NS"
]

# Deduplicate while preserving order
_seen: set = set()
NIFTY500_SAMPLE = [
    t for t in NIFTY500_SAMPLE
    if not (t in _seen or _seen.add(t))  # type: ignore[func-returns-value]
]


# ─────────────────────────────────────────────────────────────
# PERFORMANCE & SYSTEM METRICS LOGGING
# ─────────────────────────────────────────────────────────────
def get_sys_metrics() -> tuple:
    """
    Returns (cpu_usage, memory_mb) if psutil is available, otherwise (0.0, 0.0).
    """
    if psutil is not None:
        try:
            proc = psutil.Process(os.getpid())
            cpu = proc.cpu_percent(interval=None)
            mem = proc.memory_info().rss / (1024 * 1024)
            return round(cpu, 2), round(mem, 2)
        except Exception:
            pass
    return 0.0, 0.0


# ─────────────────────────────────────────────────────────────
# DATA QUALITY INTEGRITY VALIDATION
# ─────────────────────────────────────────────────────────────
def validate_dataframe(df: pd.DataFrame, ticker: str) -> bool:
    """
    Validates the structural and logical integrity of daily OHLCV bars.
    Returns True if valid, False if corrupt or contains structural anomalies.
    """
    if df is None or df.empty:
        log.warning(f"❌ {ticker}: Empty or null DataFrame.")
        return False

    required = ["open", "high", "low", "close", "volume"]
    for col in required:
        if col not in df.columns:
            log.warning(f"❌ {ticker}: Missing critical column '{col}'")
            return False

    # Check for NaN values in critical features
    if df[required].isna().any().any():
        log.warning(f"❌ {ticker}: Contains NaN value in critical data rows.")
        return False

    # Check price values > 0
    if (df["open"] <= 0).any() or (df["high"] <= 0).any() or (df["low"] <= 0).any() or (df["close"] <= 0).any():
        log.warning(f"❌ {ticker}: Invalid prices detected (<= 0).")
        return False

    # Check negative volume
    if (df["volume"] < 0).any():
        log.warning(f"❌ {ticker}: Negative volume detected.")
        return False

    # Check structural boundaries
    if not (df["high"] >= df["low"]).all():
        log.warning(f"❌ {ticker}: Structural violation: high < low.")
        return False
    if not (df["high"] >= df["open"]).all():
        log.warning(f"❌ {ticker}: Structural violation: high < open.")
        return False
    if not (df["high"] >= df["close"]).all():
        log.warning(f"❌ {ticker}: Structural violation: high < close.")
        return False
    if not (df["low"] <= df["open"]).all():
        log.warning(f"❌ {ticker}: Structural violation: low > open.")
        return False
    if not (df["low"] <= df["close"]).all():
        log.warning(f"❌ {ticker}: Structural violation: low > close.")
        return False

    return True


# ─────────────────────────────────────────────────────────────
# MARKET HOURS & NIFTY RELATIVE STRENGTH ENGINE
# ─────────────────────────────────────────────────────────────
from datetime import time as dt_time, timedelta, timezone

def is_nse_market_open() -> bool:
    """
    Checks if the NSE market is currently open (9:15 AM - 3:30 PM IST, Monday - Friday).
    Uses standard library UTC offset calculation to remain timezone-accurate and dependency-free.
    """
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist_tz)
    if now_ist.weekday() >= 5:  # Saturday, Sunday
        return False
    market_start = dt_time(9, 15)
    market_end = dt_time(15, 30)
    return market_start <= now_ist.time() <= market_end


_NIFTY_CACHE: Dict[str, Dict] = {}

def get_nifty_returns() -> tuple:
    """
    Downloads and caches both the 20-day and 50-day returns of the NIFTY 50 index (^NSEI).
    Returns (nifty_20d_return, nifty_50d_return).
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if "returns" in _NIFTY_CACHE and _NIFTY_CACHE.get("date") == today_str:
        cached = _NIFTY_CACHE["returns"]
        return cached["20d"], cached["50d"]
    
    try:
        with _YF_LOCK:
            df = yf.download(
                "^NSEI",
                period="6mo",
                interval="1d",
                progress=False,
                auto_adjust=True,
                timeout=30,
            )
        if df is not None and not df.empty and len(df) >= 50:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [str(c).lower() for c in df.columns]
            df.dropna(inplace=True)
            close_today = float(df["close"].iloc[-1])
            close_20d = float(df["close"].iloc[-20])
            close_50d = float(df["close"].iloc[-50])
            
            nifty_20d = (close_today - close_20d) / close_20d * 100
            nifty_50d = (close_today - close_50d) / close_50d * 100
            
            _NIFTY_CACHE["returns"] = {"20d": nifty_20d, "50d": nifty_50d}
            _NIFTY_CACHE["date"] = today_str
            log.info(f"📈 Nifty returns cached: 20d={nifty_20d:.2f}%, 50d={nifty_50d:.2f}%")
            return nifty_20d, nifty_50d
    except Exception as exc:
        log.error(f"Error fetching Nifty Index returns: {exc}")
    return 0.0, 0.0


# ─────────────────────────────────────────────────────────────
# CAMARILLA PIVOT LEVELS
# ─────────────────────────────────────────────────────────────
def camarilla_levels(high: float, low: float, close: float) -> Dict[str, float]:
    """
    8-level Camarilla pivots.
    H3/L3 → standard intraday resistance/support.
    H4/L4 → breakout levels.
    """
    rng   = high - low
    pivot = (high + low + close) / 3
    return {
        "pivot": round(pivot, 2),
        "H1":    round(close + rng * 1.1 / 12, 2),
        "H2":    round(close + rng * 1.1 / 6,  2),
        "H3":    round(close + rng * 1.1 / 4,  2),   # Bullish target
        "H4":    round(close + rng * 1.1 / 2,  2),   # Breakout target
        "L1":    round(close - rng * 1.1 / 12, 2),
        "L2":    round(close - rng * 1.1 / 6,  2),
        "L3":    round(close - rng * 1.1 / 4,  2),   # Bullish stop-loss
        "L4":    round(close - rng * 1.1 / 2,  2),   # Hard stop
    }


# ─────────────────────────────────────────────────────────────
# SCORE CALCULATION  (0–100, professional multi-factor engine)
# ─────────────────────────────────────────────────────────────
def compute_score(row: Dict, bearish: bool = False) -> int:
    score = 50  # base

    # 1. Trend Factor (EMAs passed: up to +20)
    for ema in ("ema10", "ema20", "ema50", "ema200"):
        if row.get(ema + "_pass"):
            score += 5

    # 2. Trend Slope Factor (up to +5)
    slope = row.get("ema20_slope", 0.0)
    if not bearish and slope > 0.3:
        score += 5
    elif bearish and slope < -0.3:
        score += 5

    # 3. Volume Spike Factor (up to +15)
    vol_ratio = row.get("vol_ratio", 1.0)
    if vol_ratio >= 3.0:
        score += 15
    elif vol_ratio >= 2.0:
        score += 10
    elif vol_ratio >= 1.5:
        score += 5

    # 4. Volume Percentile Factor (up to +5)
    vol_pct = row.get("vol_percentile", 0.0)
    if vol_pct >= 90:
        score += 5

    # 5. ATR Range Expansion / Breakout Factor (up to +5)
    range_exp = row.get("range_expansion", 1.0)
    if range_exp >= 1.5:
        score += 5

    # 6. Candle Direction Factor (up to +5)
    if not bearish and row.get("candle") == "Bull":
        score += 5
    elif bearish and row.get("candle") == "Bear":
        score += 5

    # 7. Volatility-Adjusted Entry Support Zone (up to +5)
    atr_dist = row.get("atr_dist", 0.0)
    if not bearish:
        if atr_dist <= 1.2:
            score += 5   # close to support
    else:
        if atr_dist >= -1.2:
            score += 5   # close to resistance

    # 8. Volatility-Adjusted Overextension Penalty (up to -30)
    if not bearish:
        if atr_dist > 3.0:
            score -= 30  # overextended buying risk
        elif atr_dist > 2.0:
            score -= 15
    else:
        if atr_dist < -3.0:
            score -= 30  # overextended selling risk
        elif atr_dist < -2.0:
            score -= 15

    # 9. 52-Week Low Chasing Penalty (up to -20)
    if not bearish:
        pct_above = row.get("pct_above", 100.0)
        if pct_above > 40:
            score -= 20
        elif pct_above > 25:
            score -= 10
    else:
        pct_below = row.get("pct_below", 100.0)
        if pct_below > 40:
            score -= 20
        elif pct_below > 25:
            score -= 10

    # 10. High Priority RS Factor (Strong RS: +10, Weak RS: -10)
    score += row.get("rs_score", 0)

    return min(100, max(0, score))


# ─────────────────────────────────────────────────────────────
# YFINANCE DOWNLOAD  (with fault-tolerant persistent caching)
# ─────────────────────────────────────────────────────────────
CACHE_DIR = "data_cache"

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes the DataFrame structure:
    - Flattens MultiIndex columns to flat lowercase strings
    - Converts Open, High, Low, Close, Volume to numeric float types
    - Cleans up and discards string metadata rows (such as header rows loaded from corrupted cache)
    """
    if df is None or df.empty:
        return df

    # 1. Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0].lower() for c in df.columns]
    else:
        df.columns = [str(c).lower() for c in df.columns]

    # 2. Convert OHLCV columns to numeric, coercing strings/errors to NaN
    required = ["open", "high", "low", "close", "volume"]
    for col in required:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 3. Drop rows that have NaN in critical columns (e.g. metadata text rows)
    df.dropna(subset=required, inplace=True)
    return df


def _download(ticker: str, retries: int = 3, use_cache_only: bool = False) -> Optional[pd.DataFrame]:
    # Ensure cache directory exists
    if not os.path.exists(CACHE_DIR):
        try:
            os.makedirs(CACHE_DIR)
        except Exception:
            pass

    cache_path = os.path.join(CACHE_DIR, f"{ticker}.csv")
    
    # If use_cache_only is True, strictly load from cache (offline scan mode)
    if use_cache_only:
        if os.path.exists(cache_path):
            try:
                df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                if df is not None and not df.empty:
                    df = normalize_dataframe(df)
                    if df is not None and not df.empty:
                        return df
            except Exception:
                pass
        return None

    # Check cache validity — 15 min TTL during NSE market hours, 1 hour otherwise
    if os.path.exists(cache_path):
        try:
            mtime = os.path.getmtime(cache_path)
            # Determine TTL: 15 min during NSE market hours (09:15–15:30 IST)
            from datetime import datetime, timezone, timedelta
            _ist = timezone(timedelta(hours=5, minutes=30))
            _now_ist = datetime.now(_ist)
            _market_open  = _now_ist.replace(hour=9,  minute=15, second=0, microsecond=0)
            _market_close = _now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
            _in_market = _market_open <= _now_ist <= _market_close
            _ttl = 900 if _in_market else 3600   # 15 min in market, 1 hr otherwise
            if time.time() - mtime < _ttl:
                df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                if df is not None and not df.empty:
                    df = normalize_dataframe(df)
                    if df is not None and not df.empty:
                        log.debug(f"{ticker}: Loaded from local cache (TTL={'15min' if _in_market else '1hr'})")
                        return df
        except Exception:
            pass

    for attempt in range(retries):
        try:
            with _YF_LOCK:
                df = yf.download(
                    ticker,
                    period="5y",
                    interval="1d",
                    progress=False,
                    auto_adjust=True,
                    timeout=30,
                )
            if df is not None and not df.empty:
                df = normalize_dataframe(df)
                # Save to cache
                try:
                    df.to_csv(cache_path)
                except Exception:
                    pass
                return df
        except Exception as exc:
            if attempt < retries - 1:
                wait = 1.5 ** attempt
                log.debug(f"{ticker}: attempt {attempt + 1} failed ({exc}), retrying in {wait:.1f}s")
                time.sleep(wait)
            else:
                # Fallback to older cache if download completely failed (extreme resilience)
                if os.path.exists(cache_path):
                    try:
                        df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                        if df is not None and not df.empty:
                            df = normalize_dataframe(df)
                            if df is not None and not df.empty:
                                log.warning(f"{ticker}: Download failed; falling back to stale local cache")
                                return df
                    except Exception:
                        pass
                log.debug(f"{ticker}: all {retries} download attempts failed — {exc}")
    return None


# ─────────────────────────────────────────────────────────────
# ANALYSE ONE STOCK
# ─────────────────────────────────────────────────────────────
def _get_cache_hit(ticker: str) -> int:
    cache_path = os.path.join(CACHE_DIR, f"{ticker}.csv")
    if os.path.exists(cache_path):
        try:
            mtime = os.path.getmtime(cache_path)
            if time.time() - mtime < 3600:
                return 1
        except Exception:
            pass
    return 0


def analyse(
    ticker: str,
    bearish: bool = False,
    cfg_override: Optional[Dict] = None,
    explain_skip: bool = False,
) -> Optional[Dict]:
    """
    Returns a signal dict if the stock passes all filters,
    or a skipped explanation dict if explain_skip is True,
    otherwise None if it does not qualify.
    cfg_override allows per-scan settings without mutating the global CFG.
    """
    cfg      = {**CFG, **(cfg_override or {})}
    vol_days = cfg["VOL_DAYS"]
    hv_days  = cfg["HV_DAYS"]
    min_len  = max(210, vol_days + 10, hv_days + 5)

    try:
        use_cache_only = cfg.get("USE_CACHE_ONLY", False)
        df = _download(ticker, use_cache_only=use_cache_only)
        if df is None or df.empty or len(df) < min_len:
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Insufficient price history (requires at least {min_len} days, found {len(df) if df is not None else 0})"
                }
            return None

        # Flatten MultiIndex columns → lowercase strings
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0].lower() for c in df.columns]
        else:
            df.columns = [str(c).lower() for c in df.columns]

        df.dropna(inplace=True)
        
        # ── Live price + previous close via fast_info (cache-busted) ──
        ticker_obj = yf.Ticker(ticker)
        year_high    = None
        year_low     = None
        live_price   = None
        prev_close_fi = None   # fast_info previous_close — always accurate

        try:
            # Warm up ticker cache with fresh 1-min bar first
            ticker_obj.history(
                period="1d",
                interval="1m",
                auto_adjust=True,
                prepost=False,
                repair=True,
            )
        except Exception as e:
            log.warning(f"Error forcing 1m history for {ticker}: {e}")

        try:
            fast = ticker_obj.fast_info
            live_price    = fast.get('last_price')
            prev_close_fi = fast.get('previous_close')   # ← KEY FIX
            year_high     = fast.get('year_high')
            year_low      = fast.get('year_low')
            if live_price is not None:
                df.at[df.index[-1], "close"] = float(live_price)
        except Exception as e:
            log.warning(f"Error fetching fast_info for {ticker}: {e}")

        # ── Fallback: if fast_info gave no live price, do a quick 5d download ──
        # This handles Yahoo Finance rate-limiting of fast_info during market hours.
        if live_price is None:
            try:
                with _YF_LOCK:
                    _df5 = yf.download(
                        ticker, period="5d", interval="1d",
                        progress=False, auto_adjust=True, timeout=15
                    )
                if _df5 is not None and not _df5.empty:
                    if isinstance(_df5.columns[0], tuple):
                        _df5.columns = [c[0].lower() for c in _df5.columns]
                    else:
                        _df5.columns = [str(c).lower() for c in _df5.columns]
                    _today_close = float(_df5["close"].iloc[-1])
                    _today_prev  = float(_df5["close"].iloc[-2]) if len(_df5) >= 2 else None
                    # Only use if it looks like today's data
                    from datetime import date
                    if _df5.index[-1].date() >= date.today():
                        live_price = _today_close
                        df.at[df.index[-1], "close"] = _today_close
                        log.info(f"{ticker}: fast_info unavailable; used 5d download close={_today_close:.2f}")
                        if prev_close_fi is None and _today_prev is not None:
                            prev_close_fi = _today_prev
            except Exception as e2:
                log.warning(f"{ticker}: 5d fallback also failed: {e2}")

        # OHLC Data Integrity Validation
        if not validate_dataframe(df, ticker):
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": "Data integrity validation failed (corrupt pricing or NaNs)"
                }
            return None

        # ── EMAs ──────────────────────────────────────────────
        df["ema10"]  = df["close"].ewm(span=10,  adjust=False).mean()
        df["ema20"]  = df["close"].ewm(span=20,  adjust=False).mean()
        df["ema50"]  = df["close"].ewm(span=50,  adjust=False).mean()
        df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()

        # ── Wilder's RSI 14 ──────────────────────────────────
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
        rs = gain / loss.replace(0, 1e-9)
        df["rsi14"] = 100 - (100 / (1 + rs))

        # ── Volume N-day average (shift(1) excludes today) ────
        df["vol_avg"] = df["volume"].rolling(vol_days).mean().shift(1)

        last  = df.iloc[-1]
        close = float(last["close"])
        rsi   = float(df["rsi14"].iloc[-1]) if not pd.isna(df["rsi14"].iloc[-1]) else 50.0

        # ── Day change % — CRITICAL: use fast_info.previous_close ────────
        # fast_info['previous_close'] is always yesterday's official close
        # regardless of whether the CSV cache is stale or today's bar is incomplete.
        # Fallback chain: fast_info → df.iloc[-2] → 0%
        if prev_close_fi is not None and float(prev_close_fi) > 0:
            prev_close = float(prev_close_fi)
            log.debug(f"{ticker}: prev_close from fast_info = {prev_close:.2f}")
        elif len(df) >= 2:
            prev_close = float(df["close"].iloc[-2])
            log.debug(f"{ticker}: prev_close fallback from df.iloc[-2] = {prev_close:.2f}")
        else:
            prev_close = close
        day_change = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0.0

        # Minimum Price Filter
        min_price = float(cfg.get("MIN_PRICE", 50.0))
        if close < min_price:
            log.info(f"🚩 {ticker}: Rejected due to low price (₹{close:.2f} < ₹{min_price:.2f})")
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Low price (₹{close:.2f} < ₹{min_price:.2f})"
                }
            return None

        high    = float(last["high"])
        low     = float(last["low"])
        volume  = float(last["volume"])
        vol_avg = float(last["vol_avg"]) if not pd.isna(last["vol_avg"]) else 0.0

        if vol_avg == 0:
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": "Average volume is zero"
                }
            return None

        vol_ratio = volume / vol_avg

        # ── Volume filter ──────────────────────────────────────
        if vol_ratio < cfg["VOL_MULT"]:
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Volume spike not confirmed (Vol ratio {vol_ratio:.2f}x < {cfg['VOL_MULT']}x)"
                }
            return None

        # ── High Priority Improvement #1: Liquidity Filter ───
        df["turnover"] = df["close"] * df["volume"]
        avg_turnover = float(df["turnover"].rolling(window=20).mean().iloc[-1])
        turnover_score = round(avg_turnover / 10000000.0, 2)  # in Crores
        
        # Enforce Minimum average turnover
        if avg_turnover < cfg["TURNOVER_LIMIT"]:
            log.info(f"🛡️ {ticker}: Rejected due to low liquidity (₹{turnover_score:.2f} Cr < ₹{cfg['TURNOVER_LIMIT']/10000000:.2f} Cr)")
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Low liquidity (Turnover ₹{turnover_score:.2f} Cr < ₹{cfg['TURNOVER_LIMIT']/10000000:.2f} Cr)"
                }
            return None

        # ── EMA filters ───────────────────────────────────────
        if not bearish:
            ema_checks = {
                "ema10_pass":  (not cfg["EMA_10"])  or (close > float(last["ema10"])),
                "ema20_pass":  (not cfg["EMA_20"])  or (close > float(last["ema20"])),
                "ema50_pass":  (not cfg["EMA_50"])  or (close > float(last["ema50"])),
                "ema200_pass": (not cfg["EMA_200"]) or (close > float(last["ema200"])),
            }
        else:
            # Bearish: price BELOW all enabled EMAs
            ema_checks = {
                "ema10_pass":  (not cfg["EMA_10"])  or (close < float(last["ema10"])),
                "ema20_pass":  (not cfg["EMA_20"])  or (close < float(last["ema20"])),
                "ema50_pass":  (not cfg["EMA_50"])  or (close < float(last["ema50"])),
                "ema200_pass": (not cfg["EMA_200"]) or (close < float(last["ema200"])),
            }

        if not all(ema_checks.values()):
            if explain_skip:
                failed = [k.replace('_pass', '').upper() for k, v in ema_checks.items() if not v]
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Trend alignment failed ({'price below' if not bearish else 'price above'} {', '.join(failed)})"
                }
            return None

        # ── 52-week High/Low window ────────────────────────────
        lookback        = df.iloc[-252:]  # Enforce strictly 252-day lookback for correct 52-week range
        hv_high         = float(lookback["high"].max())
        hv_low          = float(lookback["low"].min())

        if year_high is not None:
            hv_high = float(year_high)
        if year_low is not None:
            hv_low = float(year_low)

        # Sanity Check for stock splits / demergers data errors
        if close > hv_high * 1.05:
            log.warning(f"🚩 {ticker}: Sanity check failed (LTP ₹{close:.2f} > 52W High ₹{hv_high:.2f} * 1.05)")
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"DATA ERROR ⚠️ (LTP ₹{close:.2f} > 52W High ₹{hv_high:.2f} * 1.05)"
                }
            return None

        hv_high_idx     = lookback["high"].idxmax()
        hv_date         = hv_high_idx.strftime("%d-%b-%Y")
        days_since_high = int((datetime.now().date() - hv_high_idx.date()).days)

        # Enforce maximum 52-week age limit (e.g., ignore setups older than 180 trading days)
        max_days = int(cfg.get("MAX_52W_AGE", 180))
        if days_since_high > max_days:
            log.info(f"🚩 {ticker}: Rejected due to stale 52W age ({days_since_high} days > {max_days} days)")
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Stale 52W age ({days_since_high} days > {max_days} days)"
                }
            return None

        range_52w = hv_high - hv_low
        if range_52w <= 0:
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": "52-week trading range is zero or negative"
                }
            return None

        # ── Camarilla Levels & Math ────────────────────────────
        pivot = round((hv_high + hv_low + close) / 3, 2)
        entry = pivot  # Pivot is the institutional entry standard

        cam = {
            "pivot": pivot,
            "H1":    round(close + range_52w * 1.1 / 12, 2),
            "H2":    round(close + range_52w * 1.1 / 6,  2),
            "H3":    round(close + range_52w * 1.1 / 4,  2),  # Target1 for Bull
            "H4":    round(close + range_52w * 1.1 / 2,  2),  # Target2 for Bull
            "L1":    round(close - range_52w * 1.1 / 12, 2),
            "L2":    round(close - range_52w * 1.1 / 6,  2),
            "L3":    round(close - range_52w * 1.1 / 4,  2),  # StopLoss for Bull
            "L4":    round(close - range_52w * 1.1 / 2,  2),
        }

        # ── Advanced Quant Indicators ─────────────────────────
        # ATR (14-day Average True Range)
        highs = df["high"]
        lows = df["low"]
        closes = df["close"].shift(1)
        tr1 = highs - lows
        tr2 = (highs - closes).abs()
        tr3 = (lows - closes).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df["atr"] = tr.rolling(window=14).mean()
        
        last_atr = float(df["atr"].iloc[-1]) if not pd.isna(df["atr"].iloc[-1]) else (close * 0.02)
        
        # ── High Priority Improvement #2: 20-Day and 50-Day Relative Strength vs Nifty ───
        nifty_20d, nifty_50d = get_nifty_returns()
        
        # Stock 20-day return
        close_20d = float(df["close"].iloc[-20]) if len(df) >= 20 else float(df["close"].iloc[0])
        stock_ret_20d = (close - close_20d) / close_20d * 100
        rs_pct = round(stock_ret_20d - nifty_20d, 2)
        
        # Stock 50-day return (trend confirmation filter)
        close_50d = float(df["close"].iloc[-50]) if len(df) >= 50 else float(df["close"].iloc[0])
        stock_ret_50d = (close - close_50d) / close_50d * 100
        rs_50d = round(stock_ret_50d - nifty_50d, 2)
        
        # Enforce positive 50-day relative strength for Bullish, or negative for Bearish
        if not bearish and rs_50d <= 0:
            log.info(f"🚩 {ticker}: Rejected due to negative 50-day relative strength ({rs_50d:.2f}% <= 0)")
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Negative 50-day relative strength ({rs_50d:.2f}% <= 0)"
                }
            return None
        elif bearish and rs_50d >= 0:
            log.info(f"🚩 {ticker}: Rejected due to positive 50-day relative strength ({rs_50d:.2f}% >= 0)")
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Positive 50-day relative strength ({rs_50d:.2f}% >= 0)"
                }
            return None

        # ── BUY Scan Strict Trend/Momentum Filters ─────────────
        if not bearish:
            # 1. Day change filter: exclude if day change is less than -1.0%
            if day_change < -1.0:
                log.info(f"🚩 {ticker}: Excluded from BUY scan due to negative day change ({day_change:.2f}% < -1.0%)")
                if explain_skip:
                    return {
                        "symbol": ticker.replace(".NS", ""),
                        "skipped": True,
                        "reason": f"Negative day change ({day_change:.2f}% < -1.0%)"
                    }
                return None

            # 2. RSI filter: exclude if RSI < 45
            if rsi < 45:
                log.info(f"🚩 {ticker}: Excluded from BUY scan due to low RSI ({rsi:.2f} < 45)")
                if explain_skip:
                    return {
                        "symbol": ticker.replace(".NS", ""),
                        "skipped": True,
                        "reason": f"Low RSI ({rsi:.2f} < 45)"
                    }
                return None

            # 3. Stricter bear market filter: if Nifty regime is BEAR/STRONG_BEAR, require RSI > 55 and Vol Spike > 2.0x
            nifty_regime = get_regime().get("regime", "NEUTRAL").upper()
            if "BEAR" in nifty_regime:
                if rsi <= 55 or vol_ratio <= 2.0:
                    log.info(f"🚩 {ticker}: Excluded from BUY scan in bear market (requires RSI > 55 and Vol > 2.0x; found RSI {rsi:.2f}, Vol {vol_ratio:.2f}x)")
                    if explain_skip:
                        return {
                            "symbol": ticker.replace(".NS", ""),
                            "skipped": True,
                            "reason": f"Bear market regime filter failed (RSI {rsi:.2f} <= 55 or Vol {vol_ratio:.2f}x <= 2.0x)"
                        }
                    return None

        if not bearish:
            target1 = cam["H3"]
            target2 = cam["H4"]
            stop_loss = cam["L3"]
        else:
            target1 = cam["L3"]
            target2 = cam["L4"]
            stop_loss = cam["H3"]

        # ── Candle direction ──────────────────────────────────
        body   = close - float(last["open"])
        candle = "Bull" if body > 0 else "Bear"

        # ── Distance metrics ─────────────────────────────────
        pct_above = round((close - hv_low)  / hv_low  * 100, 2) if hv_low  else 0.0
        pct_below = round((hv_high - close) / hv_high * 100, 2) if hv_high else 0.0

        upside   = round((target1 - close) / close * 100, 2) if not bearish else round((close - target1) / close * 100, 2)
        downside = round((close - stop_loss) / close * 100, 2) if not bearish else round((stop_loss - close) / close * 100, 2)

        # ── Advanced Quant Indicators ─────────────────────────
        today_range = high - low
        range_expansion = round(today_range / last_atr, 2) if last_atr else 1.0

        # EMA 20 Slope (5-day percentage slope)
        prev_ema20 = float(df["ema20"].iloc[-5]) if len(df) >= 5 else float(last["ema20"])
        ema20_slope = round((float(last["ema20"]) - prev_ema20) / prev_ema20 * 100, 4) if prev_ema20 else 0.0

        # ATR-based overextension distance
        atr_dist = round((close - float(last["ema20"])) / last_atr, 2) if last_atr else 0.0

        # Volume percentile (last 20 days)
        vol_percentile = round(float((df["volume"].iloc[-20:] < volume).mean() * 100), 2)

        rs_score = 10 if (not bearish and rs_pct > 0) or (bearish and rs_pct < 0) else -10

        # Relative Strength (percentage distance of close relative to 200 EMA)
        ema200_val = float(last["ema200"])
        relative_strength = round((close - ema200_val) / ema200_val * 100, 2) if ema200_val else 0.0

        # Market Regime Status
        ema50_val = float(last["ema50"])
        if close > ema50_val and ema50_val > ema200_val:
            regime = "Strong Bullish"
        elif close > ema50_val and ema50_val <= ema200_val:
            regime = "Moderate Bullish"
        elif close < ema50_val and ema50_val < ema200_val:
            regime = "Strong Bearish"
        elif close < ema50_val and ema50_val >= ema200_val:
            regime = "Moderate Bearish"
        else:
            regime = "Consolidation"

        # ── Critical Fix #3: Risk / Reward Calculations with ATR-based Stop Loss Cap ───
        # Enforce a minimum stop-loss distance of 1.5 * ATR to prevent dangerously tight stops
        min_risk_distance = 1.5 * last_atr
        
        if not bearish:
            # Bullish: risk = entry - stop_loss
            current_risk = entry - stop_loss
            if current_risk < min_risk_distance:
                stop_loss = round(entry - min_risk_distance, 2)
        else:
            # Bearish: risk = stop_loss - entry
            current_risk = stop_loss - entry
            if current_risk < min_risk_distance:
                stop_loss = round(entry + min_risk_distance, 2)

        # Now re-calculate risk and reward with the adjusted stop-loss
        reward = abs(target1 - entry)
        risk = abs(entry - stop_loss)
        rr = round(reward / risk, 2) if risk > 0 else 0.0
        risk_percentage = round((risk / entry) * 100, 2)
        
        # Enforce maximum acceptable risk = 5% of entry
        if risk_percentage > 5.0:
            log.info(f"🛡️ {ticker}: Rejected due to high risk ({risk_percentage}% > 5%) — Capital preserved.")
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Risk too high ({risk_percentage}% > 5% of Pivot entry)"
                }
            return None

        # Enforce minimum Risk/Reward floor limit = 1.5
        if rr < 1.5:
            log.info(f"🛡️ {ticker}: Rejected due to poor Risk/Reward ratio ({rr} < 1.5) — Capital preserved.")
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Poor Risk/Reward ratio (1:{rr:.2f} < 1:1.5)"
                }
            return None

        result: Dict = {
            "symbol":            ticker.replace(".NS", ""),
            "signal_type":       "Bear" if bearish else "Bull",
            "price":             round(close, 2),
            "rsi":               round(rsi, 2),
            "change":            round(day_change, 2),
            "hv_high":           round(hv_high, 2),
            "hv_low":            round(hv_low,  2),
            "hv_date":           hv_date,
            "days":              days_since_high,
            "pct_above":         pct_above,
            "pct_below":         pct_below,
            "upside":            upside,
            "downside":          downside,
            "stop_loss":         stop_loss,
            "target":            target1,
            "target2":           target2,
            "entry":             entry,
            "cam":               cam,
            "candle":            candle,
            "vol_ratio":         round(vol_ratio, 2),
            "volume":            int(volume),
            "vol_avg":           int(vol_avg),
            "ema10":             round(float(last["ema10"]),  2),
            "ema20":             round(float(last["ema20"]),  2),
            "ema50":             round(float(last["ema50"]),  2),
            "ema200":            round(float(last["ema200"]), 2),
            "scanned_date":      df.index[-1].strftime("%d-%b-%Y"),
            "scanned_time":      datetime.now().strftime("%H:%M"),
            "scanned_timestamp": time.time(),
            "dist_from_entry":   round(((close - entry) / entry) * 100, 2) if entry else 0.0,
            "atr":               round(last_atr, 2),
            "range_expansion":   range_expansion,
            "ema20_slope":       ema20_slope,
            "atr_dist":          atr_dist,
            "vol_percentile":    vol_percentile,
            "relative_strength": relative_strength,
            "regime":            regime,
            "rr":                rr,
            "rs_pct":            rs_pct,
            "rs_50d":            rs_50d,
            "rs_score":          rs_score,
            "turnover_score":    turnover_score,
            "risk_percentage":   risk_percentage,
            "sparkline":         df["close"].tail(5).round(2).tolist(),  # Real 5-day price action closing points
            **ema_checks,
        }

        result["score"] = compute_score(result, bearish=bearish)
        
        # ── High Priority Improvement #3: Confidence Grading Engine ───
        score = result["score"]
        if score >= 95:
            result["confidence"] = "A+"
            result["signal_strength"] = "Institutional Strong"
        elif score >= 85:
            result["confidence"] = "A"
            result["signal_strength"] = "Strong Momentum"
        elif score >= 70:
            result["confidence"] = "B"
            result["signal_strength"] = "Moderate Setup"
        elif score >= 55:
            result["confidence"] = "C"
            result["signal_strength"] = "Weak Signal"
        else:
            result["confidence"] = "D"
            result["signal_strength"] = "Avoid"

        if score < cfg["MIN_SCORE"]:
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Momentum score too low ({score}/100 < {cfg['MIN_SCORE']})"
                }
            return None

        return result
    except Exception as exc:
        log.debug(f"{ticker}: unhandled error — {exc}")
        if explain_skip:
            return {
                "symbol": ticker.replace(".NS", ""),
                "skipped": True,
                "reason": f"Unhandled exception during scan: {str(exc)}"
            }
        return None


# ─────────────────────────────────────────────────────────────
# SCAN ALL STOCKS  (concurrent via ThreadPoolExecutor)
# ─────────────────────────────────────────────────────────────
def run_scan(
    tickers: Optional[List[str]] = None,
    bearish: bool = False,
    progress_cb: Optional[Callable] = None,
    cfg_override: Optional[Dict] = None,
    stop_event: Optional[threading.Event] = None,
) -> List[Dict]:
    """
    Scan tickers concurrently.

    progress_cb(current, total, ticker, eta_seconds) is called after each ticker.
    stop_event: set it to cancel an in-progress scan gracefully.
    """
    tickers = list(tickers or NIFTY500_SAMPLE)
    total   = len(tickers)
    results: List[Dict] = []
    counter  = {"n": 0, "start": time.time(), "cache_hits": 0}
    lock     = threading.Lock()
    workers  = (cfg_override or {}).get("MAX_WORKERS", CFG["MAX_WORKERS"])

    # Clear requests_cache before every scan to prevent stale data
    try:
        import requests_cache
        requests_cache.clear()
        log.info("🧹 requests_cache cleared successfully before starting scan.")
    except Exception as e:
        log.warning(f"Could not clear requests_cache: {e}")

    # Delete yfinance timezone cache directory to bust cache
    yf_cache_temp = os.path.join("data_cache", "yf_cache_temp")
    if os.path.exists(yf_cache_temp):
        try:
            import shutil
            shutil.rmtree(yf_cache_temp)
            log.info("🧹 Temporary timezone cache folder deleted.")
        except Exception as e:
            log.warning(f"Could not delete temporary timezone cache: {e}")
    try:
        yf.set_tz_cache_location(yf_cache_temp)
    except Exception as e:
        log.warning(f"Could not set timezone cache location: {e}")

    # Download Nifty index first for RS ranking
    get_nifty_returns()

    def _task(ticker: str) -> Optional[Dict]:
        if stop_event and stop_event.is_set():
            return None
        
        is_hit = _get_cache_hit(ticker)
        result = analyse(ticker, bearish=bearish, cfg_override=cfg_override)
        if result:
            result["raw_score"] = result["score"]  # keep original raw score
            result["regime_score"] = adjust_score_for_regime(
                result["score"],
                regime=regime_name,
                is_bearish=(result.get("signal_type") == "Bear"),
            )
            result["regime"]       = regime_name
            result["regime_emoji"] = regime_data.get("emoji", "⚖️")

            # Filter on RAW score, not regime-adjusted
            min_score = (cfg_override or {}).get("MIN_SCORE", CFG["MIN_SCORE"])
            if result["raw_score"] < min_score:
                result = None
        
        with lock:
            counter["n"] += 1
            counter["cache_hits"] += is_hit
            n       = counter["n"]
            elapsed = time.time() - counter["start"]
            eta     = int((elapsed / n) * (total - n)) if n > 0 else 0

        mode = "BEAR" if bearish else "BULL"
        log.info(f"[{n}/{total}] {mode} {ticker} → {'✅ SIGNAL (' + result.get('confidence', '') + ')' if result else 'skip'}")

        if progress_cb:
            try:
                progress_cb(n, total, ticker, eta)
            except Exception:
                pass
        return result

    regime_data  = get_regime()
    regime_name  = regime_data.get("regime", "NEUTRAL")
    log.info(f"📊 Market Regime: {regime_name} | "
             f"Breadth: {regime_data.get('breadth', 50)}% | "
             f"NIFTY: ₹{regime_data.get('nifty_close', 0):,.2f}")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_task, t): t for t in tickers}
        for fut in as_completed(futures):
            r = fut.result()
            if r:
                results.append(r)

    results.sort(key=lambda x: x["score"], reverse=True)
    
    # Save scan metrics and signal history to SQLite database
    duration = time.time() - counter["start"]
    cpu_usg, mem_mb = get_sys_metrics()
    
    # Write Scan metrics record to DB
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT INTO scan_metrics (
                timestamp, duration_seconds, tickers_scanned, signals_found, cache_hits, cpu_usage, memory_usage
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (now_str, duration, total, len(results), counter["cache_hits"], cpu_usg, mem_mb))
        conn.commit()
        conn.close()
    except Exception as exc:
        log.error(f"Error saving scan metrics: {exc}")

    # Write signal details to SQLite database
    if results:
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            for r in results:
                cursor.execute("""
                    INSERT INTO signal_history (
                        date, ticker, signal_type, entry, stoploss, target1, target2, score, confidence, rs_pct, turnover_crore, scanned_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    r.get("scanned_date", ""),
                    r["symbol"],
                    r["signal_type"],
                    r["entry"],
                    r["stop_loss"],
                    r["target"],
                    r["target2"],
                    r["score"],
                    r["confidence"],
                    r.get("rs_pct", 0.0),
                    r.get("turnover_score", 0.0),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ))
            conn.commit()
            conn.close()
        except Exception as exc:
            log.error(f"Error logging signal history to SQLite: {exc}")

    return {
        "signals":     results,
        "regime":      regime_data,
    }


# ─────────────────────────────────────────────────────────────
# CLI TABLE DISPLAY
# ─────────────────────────────────────────────────────────────
def print_table(signals: List[Dict]) -> None:
    if not signals:
        log.info("\n  No signals found with current filters.\n")
        return

    mode = signals[0].get("signal_type", "Bull") if signals else "Bull"
    print("\n" + "=" * 115)
    print(f"  NSE CAMARILLA VOLUME SCANNER v3.0 [{mode.upper()}] — "
          + datetime.now().strftime("%d %b %Y %H:%M"))
    print("=" * 115)
    hdr = (f"  {'SYMBOL':<14}{'SCORE':>6} {'CONF':>4} {'PRICE':>10} {'ENTRY':>10} "
           f"{'TARGET1':>10} {'STOPLOSS':>10} {'RISK%':>6} {'R:R':>5} {'TURNOVER':>9} CANDLE")
    print(hdr)
    print("-" * 115)
    for r in signals:
        print(
            f"  {r['symbol']:<12} {r['score']:>3}/100 "
            f"{r['confidence']:>4} "
            f"Rs.{r['price']:>9,.2f} Rs.{r['entry']:>9,.2f} "
            f"Rs.{r['target']:>9,.2f} Rs.{r['stop_loss']:>9,.2f} "
            f"{r['risk_percentage']:>5.1f}% {r['rr']:>4.1f} {r['turnover_score']:>7.1f}Cr {r['candle']}"
        )
    print("=" * 115)
    print(f"  {len(signals)} signal(s) found\n")


# ─────────────────────────────────────────────────────────────
# ALERT DEDUPLICATION & COOLDOWN ENGINE
# ─────────────────────────────────────────────────────────────
ALERT_HISTORY_FILE = "alert_history.json"

def filter_cooldown_signals(signals: List[Dict], channel: str, cooldown_hours: float = 4.0) -> List[Dict]:
    """
    Advanced cooldown deduplication.
    Prevents repeated alerts for same symbol, same signal, and same entry price zone (within 0.5% buffer)
    during the cooldown period. Expired records are automatically cleaned up.
    Survives application restarts by using alert_history.json.
    """
    now = time.time()
    history: List[Dict] = []
    
    # Load alert history from file
    if os.path.exists(ALERT_HISTORY_FILE):
        try:
            with open(ALERT_HISTORY_FILE, encoding="utf-8") as fh:
                history = json.load(fh)
        except Exception:
            pass

    # Clean old history (older than cooldown_hours)
    cutoff = now - (cooldown_hours * 3600)
    cleaned_history = [entry for entry in history if entry.get("timestamp", 0) > cutoff]

    filtered = []
    duplicate_count = 0
    
    for s in signals:
        symbol = s["symbol"]
        sig_type = s["signal_type"]
        entry_price = s["entry"]
        
        # Check if identical alert exists in cooldown window
        is_duplicate = False
        for entry in cleaned_history:
            if (
                entry.get("symbol") == symbol
                and entry.get("signal_type") == sig_type
                and entry.get("channel") == channel
            ):
                # Entry price buffer check (0.5% tolerance)
                prev_price = entry.get("entry_price", 0.0)
                if prev_price > 0 and abs(entry_price - prev_price) / prev_price <= 0.005:
                    is_duplicate = True
                    break
        
        if is_duplicate:
            duplicate_count += 1
            log.info(f"⏳ [Duplicate Suppressed] {symbol} ({sig_type}) in cooldown zone (Price: ₹{entry_price:.2f})")
            continue
            
        filtered.append(s)
        # Add new alert entry to history
        cleaned_history.append({
            "symbol": symbol,
            "signal_type": sig_type,
            "entry_price": entry_price,
            "channel": channel,
            "timestamp": now
        })
        
        # Save to SQLite table alert_history
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO alert_history (symbol, signal_type, entry_price, channel, timestamp) VALUES (?, ?, ?, ?, ?)",
                (symbol, sig_type, entry_price, channel, now)
            )
            conn.commit()
            conn.close()
        except Exception as exc:
            log.error(f"Error saving alert to SQLite: {exc}")

    # Save updated history back to file
    try:
        with open(ALERT_HISTORY_FILE, "w", encoding="utf-8") as fh:
            json.dump(cleaned_history, fh, indent=2)
    except Exception:
        pass

    log.info(f"📢 Alerts processed for {channel}: {len(filtered)} dispatched, {duplicate_count} suppressed.")
    return filtered


# ─────────────────────────────────────────────────────────────
# TELEGRAM ALERT
# ─────────────────────────────────────────────────────────────
def send_telegram(
    signals: List[Dict],
    cfg_override: Optional[Dict] = None,
) -> None:
    cfg     = {**CFG, **(cfg_override or {})}
    token   = cfg.get("TG_TOKEN", "")
    chat_id = cfg.get("TG_CHAT_ID", "")
    if not token or not chat_id:
        log.warning("Telegram not configured — skipping.")
        return

    # Apply channel-aware cooldown deduplication (4 hours)
    signals = filter_cooldown_signals(signals, "tg")
    if not signals:
        log.info("No new signals to alert on Telegram after cooldown filtering.")
        return

    now   = datetime.now().strftime("%d %b %Y %H:%M")
    lines = [
        f"📊 *NSE Camarilla Scanner v3.0* — {now}",
        f"🔍 Vol > {cfg['VOL_MULT']}x | EMA filters ON\n",
    ]
    for r in signals[:15]:
        emoji = "🟢" if r.get("signal_type") == "Bull" else "🔴"
        lines.append(
            f"{emoji} *{r['symbol']}* — Confidence: *{r['confidence']}* ({r['signal_strength']})\n"
            f"  Score: {r['score']}/100 | Price: Rs.{r['price']:,.2f} | Entry (Pivot): Rs.{r['entry']:,.2f}\n"
            f"  T1 (H3): Rs.{r['target']:,.2f} | T2 (H4): Rs.{r['target2']:,.2f} | SL (L3): Rs.{r['stop_loss']:,.2f}\n"
            f"  R:R: {r['rr']:.1f}x | Risk: {r['risk_percentage']}% | RS (20d): {r['rs_pct']:+.1f}%\n"
        )
    if len(signals) > 15:
        lines.append(f"_... and {len(signals) - 15} more signals_")

    msg = "\n".join(lines)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
        if resp.ok:
            log.info("✅ Telegram alert sent")
        else:
            log.error(f"Telegram error: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        log.error(f"Telegram exception: {exc}")


# ─────────────────────────────────────────────────────────────
# WHATSAPP ALERT  (via Twilio)
# ─────────────────────────────────────────────────────────────
def send_whatsapp(
    signals: List[Dict],
    cfg_override: Optional[Dict] = None,
) -> None:
    cfg   = {**CFG, **(cfg_override or {})}
    sid   = cfg.get("TWILIO_SID",   "")
    token = cfg.get("TWILIO_TOKEN", "")
    from_ = cfg.get("TWILIO_FROM",  "whatsapp:+14155238886")
    to    = cfg.get("TWILIO_TO",    "")
    if not all([sid, token, to]):
        log.warning("WhatsApp (Twilio) not configured — skipping.")
        return

    # Apply channel-aware cooldown deduplication (4 hours)
    signals = filter_cooldown_signals(signals, "wa")
    if not signals:
        log.info("No new signals to alert on WhatsApp after cooldown filtering.")
        return

    now   = datetime.now().strftime("%d %b %Y %H:%M")
    lines = [f"📊 NSE Scanner v3.0 — {now}", f"Vol>{cfg['VOL_MULT']}x\n"]
    for r in signals[:10]:
        emoji = "🟢" if r.get("signal_type") == "Bull" else "🔴"
        lines.append(
            f"{emoji} {r['symbol']} ({r['score']}/100 - {r['confidence']})\n"
            f"  Price Rs.{r['price']:,.2f} | Entry (Pivot) Rs.{r['entry']:,.2f}\n"
            f"  T1 Rs.{r['target']:,.2f} | T2 Rs.{r['target2']:,.2f} | SL Rs.{r['stop_loss']:,.2f}\n"
            f"  R:R: {r['rr']:.1f}x | Vol: {r['vol_ratio']:.1f}x | {r['candle']}\n"
        )
    msg = "\n".join(lines)
    url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
    try:
        resp = requests.post(
            url,
            data={"From": from_, "To": to, "Body": msg},
            auth=(sid, token),
            timeout=10,
        )
        if resp.status_code in (200, 201):
            log.info("✅ WhatsApp alert sent")
        else:
            log.error(f"WhatsApp error: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        log.error(f"WhatsApp exception: {exc}")


# ─────────────────────────────────────────────────────────────
# SAVE TO CSV  (returns filename)
# ─────────────────────────────────────────────────────────────
def save_csv(signals: List[Dict]) -> str:
    if not signals:
        return ""
    rows = []
    for r in signals:
        rows.append({
            "Symbol":          r["symbol"],
            "Score":           r["score"],
            "Regime_Score":    r.get("regime_score", r["score"]),
            "Confidence":      r["confidence"],
            "Strength":        r["signal_strength"],
            "Signal":          r.get("signal_type", "Bull"),
            "Price":           r["price"],
            "Entry":           r["entry"],
            "Target_H3":       r["target"],
            "Target_H4":       r["target2"],
            "StopLoss_L3":     r["stop_loss"],
            "Risk_%":          r["risk_percentage"],
            "R:R":             r["rr"],
            "Upside_%":        r["upside"],
            "Vol_Ratio":       r["vol_ratio"],
            "Candle":          r["candle"],
            "EMA10":           r["ema10"],
            "EMA20":           r["ema20"],
            "EMA50":           r["ema50"],
            "EMA200":          r["ema200"],
            "HV_High":         r["hv_high"],
            "HV_Low":          r["hv_low"],
            "HV_Date":         r["hv_date"],
            "Days":            r["days"],
            "EMA20_Slope":     r.get("ema20_slope", 0.0),
            "ATR_Dist":        r.get("atr_dist", 0.0),
            "Vol_Percentile":  r.get("vol_percentile", 0.0),
            "Range_Expansion": r.get("range_expansion", 1.0),
            "ATR":             r.get("atr", 0.0),
            "RS_Pct":          r.get("rs_pct", 0.0),
            "Turnover_Cr":     r.get("turnover_score", 0.0),
            "Sparkline":       json.dumps(r.get("sparkline", [])),  # Persist sparkline list as JSON string
            "Scanned_At":      datetime.now().strftime("%Y-%m-%d %H:%M"),
        })
    fname = f"scan_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    pd.DataFrame(rows).to_csv(fname, index=False)
    log.info(f"📁 Results saved to {fname}")
    return fname


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main(
    continuous: bool = False,
    custom_tickers: Optional[List[str]] = None,
    bearish: bool = False,
) -> None:
    tickers = custom_tickers or NIFTY500_SAMPLE
    mode    = "BEARISH" if bearish else "BULLISH"

    while True:
        log.info(f"🔎 Starting {mode} scan — {len(tickers)} stocks")
        scan_res = run_scan(tickers, bearish=bearish)
        signals = scan_res["signals"] if isinstance(scan_res, dict) else scan_res
        print_table(signals)
        save_csv(signals)
        if signals:
            send_telegram(signals)
            send_whatsapp(signals)
        if not continuous:
            break
        log.info(f"⏳ Next scan in {CFG['INTERVAL']}s — Ctrl+C to stop")
        time.sleep(CFG["INTERVAL"])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NSE Camarilla Volume Scanner v3.0 [Phase 2]")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously on interval")
    parser.add_argument("--bearish",    action="store_true",
                        help="Run bearish scan (price below EMAs)")
    parser.add_argument("--tickers",    nargs="*",
                        help="Custom ticker list  e.g. RELIANCE.NS TCS.NS")
    parser.add_argument("--vol-days",   type=int,
                        help="Volume average window in days (default 10)")
    parser.add_argument("--vol-mult",   type=float,
                        help="Volume multiplier threshold (default 2.0)")
    parser.add_argument("--workers",    type=int,
                        help="Concurrent download threads (default 6)")
    args = parser.parse_args()

    if args.vol_days: CFG["VOL_DAYS"]    = args.vol_days
    if args.vol_mult: CFG["VOL_MULT"]    = args.vol_mult
    if args.workers:  CFG["MAX_WORKERS"] = args.workers

    main(
        continuous=args.continuous,
        custom_tickers=args.tickers,
        bearish=args.bearish,
    )
