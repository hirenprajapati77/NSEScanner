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
    "MIN_SCORE":      int(os.getenv("MIN_SCORE",      "55")),
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
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS",
    "HINDUNILVR.NS","ITC.NS","SBIN.NS","BHARTIARTL.NS","KOTAKBANK.NS",
    "LT.NS","AXISBANK.NS","ASIANPAINT.NS","MARUTI.NS","SUNPHARMA.NS",
    "TITAN.NS","BAJFINANCE.NS","WIPRO.NS","NESTLEIND.NS","ULTRACEMCO.NS",
    "APOLLOHOSP.NS","TECHM.NS","HCLTECH.NS","POWERGRID.NS","NTPC.NS",
    "TATAMOTORS.NS","ONGC.NS","JSWSTEEL.NS","TATASTEEL.NS","BAJAJFINSV.NS",
    "DIVISLAB.NS","DRREDDY.NS","CIPLA.NS","EICHERMOT.NS","HEROMOTOCO.NS",
    "GRASIM.NS","BPCL.NS","COALINDIA.NS","INDUSINDBK.NS","ADANIPORTS.NS",
    "DABUR.NS","MARICO.NS","PIDILITIND.NS","BERGEPAINT.NS","HAVELLS.NS",
    "TATACONSUM.NS","GODREJCP.NS","MUTHOOTFIN.NS","CHOLAFIN.NS","SRF.NS",
    "AARTIIND.NS","ABCAPITAL.NS","ACC.NS","AIAENG.NS","ALKEM.NS",
    "AMBUJACEM.NS","APLLTD.NS","AUBANK.NS","BALKRISIND.NS","BANDHANBNK.NS",
    "BEL.NS","BHARATFORG.NS","BIOCON.NS","CANBK.NS","CESC.NS",
    "CROMPTON.NS","CUB.NS","DEEPAKNTR.NS","ESCORTS.NS",
    "FEDERALBNK.NS","GAIL.NS","GMRINFRA.NS","GNFC.NS","GODREJPROP.NS",
    "GRANULES.NS","GSPL.NS","HAPPSTMNDS.NS","HINDPETRO.NS","HINDCOPPER.NS",
    "IDFCFIRSTB.NS","IEX.NS","IPCALAB.NS","IRCTC.NS",
    "JINDALSTEL.NS","JUBLFOOD.NS","KANSAINER.NS","LALPATHLAB.NS","LTIM.NS",
    "LUPIN.NS","M&M.NS","MANAPPURAM.NS","MFSL.NS",
    "MOTHERSON.NS","MPHASIS.NS","MRF.NS","NMDC.NS","OBEROIRLTY.NS",
    "OFSS.NS","PAGEIND.NS","PERSISTENT.NS","PETRONET.NS",
    "PFC.NS","RAMCOCEM.NS","RBLBANK.NS",
    "RECLTD.NS","SAIL.NS","SHREECEM.NS","SIEMENS.NS",
    "SUPREMEIND.NS","SYNGENE.NS","TORNTPHARM.NS",
    "TRENT.NS","UBL.NS","VEDL.NS","VOLTAS.NS",
    "ZOMATO.NS","ZYDUSLIFE.NS",
    # Additional F&O names
    "ADANIENT.NS","ADANIGREEN.NS","ADANITRANS.NS","ATGL.NS",
    "BAJAJ-AUTO.NS","BALKRISIND.NS","CAMS.NS","CANFINHOME.NS",
    "CONCOR.NS","COROMANDEL.NS","DALBHARAT.NS","DIXON.NS",
    "GLENMARK.NS","HAL.NS","ICICIGI.NS","ICICIPRULI.NS",
    "IDEA.NS","INDHOTEL.NS","INDUSTOWER.NS","INOXWIND.NS",
    "IOC.NS","ISEC.NS","JKCEMENT.NS","JUBILANT.NS",
    "KAJARIACER.NS","LAURUSLABS.NS","LICHSGFIN.NS","LINDEINDIA.NS",
    "MCX.NS","METROPOLIS.NS","MINDA.NS","NAUKRI.NS",
    "NAVINFLUOR.NS","PIIND.NS","POLYCAB.NS","RELAXO.NS","ROUTE.NS",
    "SBICARD.NS","SBILIFE.NS","SCHAEFFLER.NS","SOLARINDS.NS",
    "SUNPHARMA.NS","SUNTV.NS","TATACHEM.NS","TATACOMM.NS",
    "TATAELXSI.NS","TATAINVEST.NS","TIINDIA.NS","TTKPRESTIG.NS",
    "VGUARD.NS","WHIRLPOOL.NS","ZEEL.NS",
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
# DYNAMIC RELATIVE STRENGTH VS NIFTY (^NSEI)
# ─────────────────────────────────────────────────────────────
_NIFTY_CACHE: Dict[str, float] = {}

def get_nifty_90d_return() -> float:
    """
    Downloads and caches the 90-day return of the NIFTY index (^NSEI).
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if "return" in _NIFTY_CACHE and _NIFTY_CACHE.get("date") == today_str:
        return _NIFTY_CACHE["return"]
    
    try:
        df = yf.download(
            "^NSEI",
            period="6mo",
            interval="1d",
            progress=False,
            auto_adjust=True,
            timeout=30,
        )
        if df is not None and not df.empty and len(df) >= 90:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [c.lower() for c in df.columns]
            df.dropna(inplace=True)
            close_today = float(df["close"].iloc[-1])
            close_90d = float(df["close"].iloc[-90])
            nifty_ret = (close_today - close_90d) / close_90d * 100
            _NIFTY_CACHE["return"] = nifty_ret
            _NIFTY_CACHE["date"] = today_str
            log.info(f"📈 Nifty 90-day index return calculated: {nifty_ret:.2f}%")
            return nifty_ret
    except Exception as exc:
        log.error(f"Error fetching Nifty Index returns: {exc}")
    return 0.0


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


def _download(ticker: str, retries: int = 3) -> Optional[pd.DataFrame]:
    # Ensure cache directory exists
    if not os.path.exists(CACHE_DIR):
        try:
            os.makedirs(CACHE_DIR)
        except Exception:
            pass

    cache_path = os.path.join(CACHE_DIR, f"{ticker}.csv")
    
    # Check cache validity (reuse if modified within last 1 hour)
    if os.path.exists(cache_path):
        try:
            mtime = os.path.getmtime(cache_path)
            # 3600 seconds = 1 hour
            if time.time() - mtime < 3600:
                df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                if df is not None and not df.empty:
                    df = normalize_dataframe(df)
                    if df is not None and not df.empty:
                        log.debug(f"{ticker}: Loaded from local cache")
                        return df
        except Exception:
            pass

    for attempt in range(retries):
        try:
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
) -> Optional[Dict]:
    """
    Returns a signal dict if the stock passes all filters,
    or None if it does not qualify.
    cfg_override allows per-scan settings without mutating the global CFG.
    """
    cfg      = {**CFG, **(cfg_override or {})}
    vol_days = cfg["VOL_DAYS"]
    hv_days  = cfg["HV_DAYS"]
    min_len  = max(210, vol_days + 10, hv_days + 5)

    try:
        df = _download(ticker)
        if df is None or df.empty or len(df) < min_len:
            return None

        # Flatten MultiIndex columns → lowercase strings
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0].lower() for c in df.columns]
        else:
            df.columns = [c.lower() for c in df.columns]

        df.dropna(inplace=True)
        
        # 1. Critical Fix #2: OHLC Data Integrity Validation
        if not validate_dataframe(df, ticker):
            log.warning(f"❌ {ticker}: Validation failed — stock excluded from scoring.")
            return None

        # ── EMAs ──────────────────────────────────────────────
        df["ema10"]  = df["close"].ewm(span=10,  adjust=False).mean()
        df["ema20"]  = df["close"].ewm(span=20,  adjust=False).mean()
        df["ema50"]  = df["close"].ewm(span=50,  adjust=False).mean()
        df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()

        # ── Volume N-day average (shift(1) excludes today) ────
        df["vol_avg"] = df["volume"].rolling(vol_days).mean().shift(1)

        last    = df.iloc[-1]
        close   = float(last["close"])
        high    = float(last["high"])
        low     = float(last["low"])
        volume  = float(last["volume"])
        vol_avg = float(last["vol_avg"]) if not pd.isna(last["vol_avg"]) else 0.0

        if vol_avg == 0:
            return None

        vol_ratio = volume / vol_avg

        # ── Volume filter ──────────────────────────────────────
        if vol_ratio < cfg["VOL_MULT"]:
            return None

        # ── High Priority Improvement #1: Liquidity Filter ───
        df["turnover"] = df["close"] * df["volume"]
        avg_turnover = float(df["turnover"].rolling(window=20).mean().iloc[-1])
        turnover_score = round(avg_turnover / 10000000.0, 2)  # in Crores
        
        # Enforce Minimum average turnover
        if avg_turnover < cfg["TURNOVER_LIMIT"]:
            log.info(f"🛡️ {ticker}: Rejected due to low liquidity (₹{turnover_score:.2f} Cr < ₹{cfg['TURNOVER_LIMIT']/10000000:.2f} Cr)")
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
            return None

        # ── 52-week High/Low window ────────────────────────────
        lookback        = df.iloc[-hv_days:]
        hv_high         = float(lookback["high"].max())
        hv_low          = float(lookback["low"].min())
        hv_high_idx     = lookback["high"].idxmax()
        hv_date         = hv_high_idx.strftime("%d-%b-%Y")
        days_since_high = int((datetime.now().date() - hv_high_idx.date()).days)

        cam = camarilla_levels(hv_high, hv_low, close)

        # ── Candle direction ──────────────────────────────────
        body   = close - float(last["open"])
        candle = "Bull" if body > 0 else "Bear"

        # ── Distance metrics ─────────────────────────────────
        pct_above = round((close - hv_low)  / hv_low  * 100, 2) if hv_low  else 0.0
        pct_below = round((hv_high - close) / hv_high * 100, 2) if hv_high else 0.0

        # Bullish: upside to H3; Bearish: downside to L3
        upside   = round((cam["H3"] - close) / close * 100, 2)
        downside = round((close - cam["L3"]) / close * 100, 2)

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
        today_range = high - low
        range_expansion = round(today_range / last_atr, 2) if last_atr else 1.0
        
        # EMA 20 Slope (5-day percentage slope)
        prev_ema20 = float(df["ema20"].iloc[-5]) if len(df) >= 5 else float(last["ema20"])
        ema20_slope = round((float(last["ema20"]) - prev_ema20) / prev_ema20 * 100, 4) if prev_ema20 else 0.0
        
        # ATR-based overextension distance
        atr_dist = round((close - float(last["ema20"])) / last_atr, 2) if last_atr else 0.0
        
        # Volume percentile (last 20 days)
        vol_percentile = round(float((df["volume"].iloc[-20:] < volume).mean() * 100), 2)

        # ── High Priority Improvement #2: Relative Strength vs Nifty ───
        nifty_ret = get_nifty_90d_return()
        close_90d = float(df["close"].iloc[-90]) if len(df) >= 90 else float(df["close"].iloc[0])
        stock_ret = (close - close_90d) / close_90d * 100
        rs_pct = round(stock_ret - nifty_ret, 2)
        rs_score = 10 if rs_pct > 0 else -10

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

        # ── Critical Fix #3: Dynamic ATR Risk Engine ───
        entry = close
        target1 = cam["H3"] if not bearish else cam["L3"]
        target2 = cam["H4"] if not bearish else cam["L4"]

        # Volatility-adjusted 1.5 * ATR stop loss
        stop_distance = 1.5 * last_atr
        if not bearish:
            stop_loss = round(entry - stop_distance, 2)
        else:
            stop_loss = round(entry + stop_distance, 2)

        risk = abs(entry - stop_loss)
        risk_percentage = round((risk / entry) * 100, 2)
        
        # Enforce maximum acceptable risk = 5% of entry
        if risk_percentage > 5.0:
            log.info(f"🛡️ {ticker}: Rejected due to high risk ({risk_percentage}% > 5%) — Capital preserved.")
            return None

        # Risk / Reward calculations
        reward = abs(target1 - entry)
        rr = round(reward / risk, 2) if risk > 0 else 0.0
        
        # Reject setup if Risk/Reward is poor (e.g. less than 1 : 1.1) to preserve capital
        if rr < 1.1:
            log.info(f"🛡️ {ticker}: Rejected due to poor Risk/Reward ratio ({rr} < 1.1) — Capital preserved.")
            return None

        result: Dict = {
            "symbol":            ticker.replace(".NS", ""),
            "signal_type":       "Bear" if bearish else "Bull",
            "price":             round(close, 2),
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
            "atr":               round(last_atr, 2),
            "range_expansion":   range_expansion,
            "ema20_slope":       ema20_slope,
            "atr_dist":          atr_dist,
            "vol_percentile":    vol_percentile,
            "relative_strength": relative_strength,
            "regime":            regime,
            "rr":                rr,
            "rs_pct":            rs_pct,
            "rs_score":          rs_score,
            "turnover_score":    turnover_score,
            "risk_percentage":   risk_percentage,
            **ema_checks,
        }

        result["score"] = compute_score(result, bearish=bearish)
        
        # ── High Priority Improvement #3: Confidence Grading Engine ───
        score = result["score"]
        if score >= 90:
            result["confidence"] = "A+"
            result["signal_strength"] = "Institutional Strong"
        elif score >= 80:
            result["confidence"] = "A"
            result["signal_strength"] = "Strong"
        elif score >= 70:
            result["confidence"] = "B"
            result["signal_strength"] = "Moderate"
        elif score >= 60:
            result["confidence"] = "C"
            result["signal_strength"] = "Weak"
        else:
            result["confidence"] = "D"
            result["signal_strength"] = "Poor"

        if score < cfg["MIN_SCORE"]:
            return None

        return result
    except Exception as exc:
        log.debug(f"{ticker}: unhandled error — {exc}")
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

    # Download Nifty index first for RS ranking
    get_nifty_90d_return()

    def _task(ticker: str) -> Optional[Dict]:
        if stop_event and stop_event.is_set():
            return None
        
        is_hit = _get_cache_hit(ticker)
        result = analyse(ticker, bearish=bearish, cfg_override=cfg_override)
        
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

    return results


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
            f"  Score: {r['score']}/100 | Price: ₹{r['price']:,.2f}\n"
            f"  Entry: ₹{r['entry']:,.2f} | T1: ₹{r['target']:,.2f} | SL: ₹{r['stop_loss']:,.2f}\n"
            f"  R:R: {r['rr']:.1f} | Risk: {r['risk_percentage']}% | RS: {r['rs_pct']:+.1f}%\n"
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
            f"  Entry ₹{r['entry']:,.2f} | T1 ₹{r['target']:,.2f} | SL ₹{r['stop_loss']:,.2f}\n"
            f"  R:R: {r['rr']:.1f} | Vol: {r['vol_ratio']:.1f}x | {r['candle']}\n"
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
        signals = run_scan(tickers, bearish=bearish)
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
