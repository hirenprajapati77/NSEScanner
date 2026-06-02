"""
NSE Market Regime Engine — v1.0
================================
Classifies current NIFTY market regime and adjusts
signal scores accordingly.

Regimes:
  STRONG_BULL  → All EMAs aligned up, high breadth, expanding volume
  BULL         → Price above EMA50/200, moderate breadth
  NEUTRAL      → Mixed signals, sideways action
  BEAR         → Price below EMA50, weak breadth
  STRONG_BEAR  → Price below all EMAs, collapsing breadth

Usage:
  from regime import get_regime, adjust_score_for_regime

  r = get_regime()
  print(r["regime"])        # "STRONG_BULL"
  print(r["score"])         # 82
  adjusted = adjust_score_for_regime(signal_score=90, regime=r["regime"])
"""

import time
import logging
import threading
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
import yfinance as yf

log = logging.getLogger("RegimeEngine")

# ─────────────────────────────────────────────────────────────
# NIFTY BREADTH SAMPLE  (30 large-caps for breadth calc)
# ─────────────────────────────────────────────────────────────
BREADTH_STOCKS = [
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS",
    "HINDUNILVR.NS","ITC.NS","SBIN.NS","BHARTIARTL.NS","KOTAKBANK.NS",
    "LT.NS","AXISBANK.NS","ASIANPAINT.NS","MARUTI.NS","SUNPHARMA.NS",
    "TITAN.NS","BAJFINANCE.NS","WIPRO.NS","HCLTECH.NS","NTPC.NS",
    "TMCV.NS","ONGC.NS","JSWSTEEL.NS","TATASTEEL.NS","BAJAJFINSV.NS",
    "DIVISLAB.NS","DRREDDY.NS","CIPLA.NS","POWERGRID.NS","COALINDIA.NS",
]

# ─────────────────────────────────────────────────────────────
# REGIME DEFINITIONS
# ─────────────────────────────────────────────────────────────
REGIMES = {
    "STRONG_BULL": {
        "color":       "#4ade80",
        "bg":          "#052e16",
        "emoji":       "🚀",
        "label":       "Strong Bull",
        "description": "All systems go. Full allocation. Prioritise high-score bullish setups.",
        "bull_mult":   1.10,   # boost bullish scores 10%
        "bear_mult":   0.60,   # suppress bearish scores 40%
        "risk_mult":   1.00,   # normal position size
    },
    "BULL": {
        "color":       "#86efac",
        "bg":          "#052e16",
        "emoji":       "📈",
        "label":       "Bull",
        "description": "Trending up. Take quality bullish setups. Avoid weak setups.",
        "bull_mult":   1.05,
        "bear_mult":   0.70,
        "risk_mult":   1.00,
    },
    "NEUTRAL": {
        "color":       "#fbbf24",
        "bg":          "#1c1200",
        "emoji":       "⚖️",
        "label":       "Neutral",
        "description": "Sideways market. Trade only A+ setups. Reduce position size.",
        "bull_mult":   0.95,
        "bear_mult":   0.95,
        "risk_mult":   0.75,
    },
    "BEAR": {
        "color":       "#fca5a5",
        "bg":          "#2d0a0a",
        "emoji":       "📉",
        "label":       "Bear",
        "description": "Downtrend. Avoid new longs. Short setups take priority.",
        "bull_mult":   0.75,
        "bear_mult":   1.05,
        "risk_mult":   0.50,
    },
    "STRONG_BEAR": {
        "color":       "#f87171",
        "bg":          "#450a0a",
        "emoji":       "🔻",
        "label":       "Strong Bear",
        "description": "Market in freefall. Cash is a position. Hedges only.",
        "bull_mult":   0.50,
        "bear_mult":   1.10,
        "risk_mult":   0.25,
    },
}

# ─────────────────────────────────────────────────────────────
# CACHE  (refresh every 60 minutes)
# ─────────────────────────────────────────────────────────────
_cache: Dict = {}
_cache_lock  = threading.Lock()
CACHE_TTL    = 3600   # seconds


def _is_cache_valid() -> bool:
    with _cache_lock:
        if not _cache:
            return False
        age = time.time() - _cache.get("_ts", 0)
        return age < CACHE_TTL


# ─────────────────────────────────────────────────────────────
# NIFTY DATA
# ─────────────────────────────────────────────────────────────
def _fetch_nifty(period: str = "1y") -> Optional[pd.DataFrame]:
    try:
        df = yf.download(
            "^NSEI",
            period=period,
            interval="1d",
            progress=False,
            auto_adjust=True,
            timeout=20,
        )
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0].lower() for c in df.columns]
        else:
            df.columns = [c.lower() for c in df.columns]
        df.dropna(inplace=True)
        return df
    except Exception as exc:
        log.warning(f"NIFTY fetch failed: {exc}")
        return None


# ─────────────────────────────────────────────────────────────
# BREADTH CALCULATION
# ─────────────────────────────────────────────────────────────
def _calc_breadth() -> float:
    """Returns % of BREADTH_STOCKS whose close > EMA50 today."""
    above = 0
    total = 0
    try:
        raw = yf.download(
            BREADTH_STOCKS,
            period="3mo",
            interval="1d",
            progress=False,
            auto_adjust=True,
            timeout=30,
        )
        if raw is None or raw.empty:
            return 50.0

        close = raw["Close"] if "Close" in raw.columns else raw["close"]
        for ticker in BREADTH_STOCKS:
            try:
                s = close[ticker].dropna()
                if len(s) < 50:
                    continue
                ema50 = s.ewm(span=50, adjust=False).mean().iloc[-1]
                total += 1
                if s.iloc[-1] > ema50:
                    above += 1
            except Exception:
                continue
    except Exception as exc:
        log.warning(f"Breadth calc failed: {exc}")
        return 50.0

    return round((above / total * 100) if total else 50.0, 1)


# ─────────────────────────────────────────────────────────────
# CLASSIFY REGIME
# ─────────────────────────────────────────────────────────────
def _classify(
    close: float,
    ema20: float,
    ema50: float,
    ema200: float,
    atr_pct: float,
    vol_ratio: float,
    breadth: float,
    pct_from_high: float,
) -> str:
    """
    Scoring-based regime classifier.
    Each factor votes; total determines regime.
    """
    score = 0   # range roughly -4 to +4

    # EMA alignment (most important)
    if close > ema20 > ema50 > ema200:
        score += 2       # perfect bull alignment
    elif close > ema50 and close > ema200:
        score += 1
    elif close < ema20 < ema50 < ema200:
        score -= 2       # perfect bear alignment
    elif close < ema50 and close < ema200:
        score -= 1

    # Breadth
    if breadth >= 70:
        score += 1
    elif breadth >= 55:
        score += 0.5
    elif breadth <= 30:
        score -= 1
    elif breadth <= 45:
        score -= 0.5

    # Distance from 52W high
    if pct_from_high <= 5:
        score += 0.5     # near highs — bullish
    elif pct_from_high >= 20:
        score -= 0.5     # far from highs — bearish

    # ATR expansion (volatility)
    if atr_pct > 2.0 and score < 0:
        score -= 0.5     # high vol in downtrend = panic
    elif atr_pct > 2.0 and score > 0:
        score += 0.5     # high vol in uptrend = momentum

    # Volume
    if vol_ratio >= 1.5 and score > 0:
        score += 0.5
    elif vol_ratio >= 1.5 and score < 0:
        score -= 0.5

    # Map score → regime
    if score >= 2.5:
        return "STRONG_BULL"
    elif score >= 1.0:
        return "BULL"
    elif score >= -1.0:
        return "NEUTRAL"
    elif score >= -2.5:
        return "BEAR"
    else:
        return "STRONG_BEAR"


# ─────────────────────────────────────────────────────────────
# MAIN API
# ─────────────────────────────────────────────────────────────
def get_regime(force_refresh: bool = False) -> Dict:
    """
    Returns full regime dict. Cached for 60 minutes.

    Keys:
      regime        str   e.g. "STRONG_BULL"
      label         str   e.g. "Strong Bull"
      emoji         str   e.g. "🚀"
      color         str   CSS color
      bg            str   CSS background
      description   str   Trading guidance
      score         int   0-100 regime strength
      nifty_close   float
      nifty_ema20   float
      nifty_ema50   float
      nifty_ema200  float
      breadth       float % stocks above EMA50
      atr_pct       float ATR as % of price
      vol_ratio     float today vol / 20d avg
      pct_from_high float % below 52W high
      bull_mult     float score multiplier for bullish signals
      bear_mult     float score multiplier for bearish signals
      risk_mult     float position size multiplier
      updated_at    str   timestamp
      error         str|None
    """
    if not force_refresh and _is_cache_valid():
        with _cache_lock:
            return dict(_cache)

    log.info("🔄 Calculating market regime…")

    result: Dict = {
        "regime":       "NEUTRAL",
        "label":        "Neutral",
        "emoji":        "⚖️",
        "color":        "#fbbf24",
        "bg":           "#1c1200",
        "description":  "Calculating…",
        "score":        50,
        "nifty_close":  0.0,
        "nifty_ema20":  0.0,
        "nifty_ema50":  0.0,
        "nifty_ema200": 0.0,
        "breadth":      50.0,
        "atr_pct":      1.0,
        "vol_ratio":    1.0,
        "pct_from_high":0.0,
        "bull_mult":    1.0,
        "bear_mult":    1.0,
        "risk_mult":    1.0,
        "updated_at":   datetime.now().strftime("%d %b %Y %H:%M"),
        "error":        None,
    }

    try:
        df = _fetch_nifty("1y")
        if df is None or len(df) < 210:
            result["error"] = "Insufficient NIFTY data"
            return result

        # EMAs
        df["ema20"]  = df["close"].ewm(span=20,  adjust=False).mean()
        df["ema50"]  = df["close"].ewm(span=50,  adjust=False).mean()
        df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()

        # ATR (14-day)
        df["tr"]  = (
            (df["high"] - df["low"]).combine(
                (df["high"] - df["close"].shift(1)).abs(), max
            ).combine(
                (df["low"]  - df["close"].shift(1)).abs(), max
            )
        )
        df["atr14"] = df["tr"].rolling(14).mean()

        # Volume ratio (today / 20d avg, excluding today)
        df["vol_avg20"] = df["volume"].rolling(20).mean().shift(1)

        last = df.iloc[-1]
        close    = float(last["close"])
        ema20    = float(last["ema20"])
        ema50    = float(last["ema50"])
        ema200   = float(last["ema200"])
        atr14    = float(last["atr14"])
        atr_pct  = round(atr14 / close * 100, 2) if close else 1.0
        vol_avg  = float(last["vol_avg20"]) if not pd.isna(last["vol_avg20"]) else 1
        vol_ratio= round(float(last["volume"]) / vol_avg, 2) if vol_avg else 1.0

        # 52W high
        high_52w     = float(df["high"].iloc[-252:].max()) if len(df) >= 252 else float(df["high"].max())
        pct_from_high= round((high_52w - close) / high_52w * 100, 2) if high_52w else 0.0

        # Breadth (runs concurrent yf.download internally)
        breadth = _calc_breadth()

        # Classify
        regime = _classify(close, ema20, ema50, ema200,
                           atr_pct, vol_ratio, breadth, pct_from_high)

        # Regime strength score (0-100)
        regime_score_map = {
            "STRONG_BULL": 90, "BULL": 70, "NEUTRAL": 50,
            "BEAR": 30, "STRONG_BEAR": 10,
        }

        rd = REGIMES[regime]
        result.update({
            "regime":       regime,
            "label":        rd["label"],
            "emoji":        rd["emoji"],
            "color":        rd["color"],
            "bg":           rd["bg"],
            "description":  rd["description"],
            "score":        regime_score_map[regime],
            "nifty_close":  round(close, 2),
            "nifty_ema20":  round(ema20, 2),
            "nifty_ema50":  round(ema50, 2),
            "nifty_ema200": round(ema200, 2),
            "breadth":      breadth,
            "atr_pct":      atr_pct,
            "vol_ratio":    vol_ratio,
            "pct_from_high":pct_from_high,
            "bull_mult":    rd["bull_mult"],
            "bear_mult":    rd["bear_mult"],
            "risk_mult":    rd["risk_mult"],
            "updated_at":   datetime.now().strftime("%d %b %Y %H:%M"),
            "error":        None,
        })
        log.info(f"✅ Regime: {regime} | NIFTY: {close:,.2f} | Breadth: {breadth}%")

    except Exception as exc:
        log.error(f"Regime engine error: {exc}")
        result["error"] = str(exc)

    with _cache_lock:
        _cache.clear()
        _cache.update(result)
        _cache["_ts"] = time.time()

    return result


# ─────────────────────────────────────────────────────────────
# SCORE ADJUSTER
# ─────────────────────────────────────────────────────────────
def adjust_score_for_regime(
    signal_score: int,
    regime: str,
    is_bearish: bool = False,
) -> int:
    """
    Adjusts a signal score based on current market regime.

    Bullish signal in STRONG_BEAR → score reduced by 50%
    Bearish signal in STRONG_BULL → score reduced by 40%
    """
    rd = REGIMES.get(regime, REGIMES["NEUTRAL"])
    mult = rd["bear_mult"] if is_bearish else rd["bull_mult"]
    adjusted = int(signal_score * mult)
    return min(100, max(0, adjusted))


# ─────────────────────────────────────────────────────────────
# STANDALONE TEST
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    r = get_regime(force_refresh=True)
    print("\n" + "=" * 55)
    print(f"  {r['emoji']}  NIFTY MARKET REGIME: {r['regime']}")
    print("=" * 55)
    print(f"  NIFTY Close : ₹{r['nifty_close']:,.2f}")
    print(f"  EMA 20/50/200: {r['nifty_ema20']:,.0f} / {r['nifty_ema50']:,.0f} / {r['nifty_ema200']:,.0f}")
    print(f"  Breadth      : {r['breadth']}% stocks above EMA50")
    print(f"  ATR %        : {r['atr_pct']}%")
    print(f"  Vol Ratio    : {r['vol_ratio']}x")
    print(f"  From 52W High: -{r['pct_from_high']}%")
    print(f"  Guidance     : {r['description']}")
    print(f"  Bull mult    : {r['bull_mult']}x  |  Bear mult: {r['bear_mult']}x")
    print(f"  Risk mult    : {r['risk_mult']}x  (position size)")
    print("=" * 55)

    # Test score adjustment
    print("\n  Score Adjustment Examples:")
    for s in [100, 90, 75, 60]:
        adj = adjust_score_for_regime(s, r["regime"])
        print(f"    {s}/100 → {adj}/100 (regime-adjusted)")
    print()
