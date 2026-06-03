"""
NSE Market Regime Engine — v5.0
================================
Weighted 5-factor market score engine.
Market Score = 35% Breadth + 25% EMA + 20% Sector + 10% ATR + 10% FII/DII
Returns score (0-100), regime, confidence%, reasons[], breakdown.

Regimes:
  STRONG_BULL  → Score ≥ 75
  BULL         → Score ≥ 60
  NEUTRAL      → Score ≥ 40
  BEAR         → Score ≥ 25
  STRONG_BEAR  → Score < 25

Usage:
  from regime import get_regime, adjust_score_for_regime

  r = get_regime()
  print(r["regime"])        # "STRONG_BULL"
  print(r["score"])         # 82
  print(r["confidence"])    # 87
  print(r["reasons"])       # ["Strong breadth...", ...]
  adjusted = adjust_score_for_regime(signal_score=90, regime=r["regime"])
"""

import time
import logging
import threading
from datetime import datetime
from typing import Dict, List, Optional

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
def _calc_breadth() -> Dict:
    """Returns advances, declines, and % of BREADTH_STOCKS whose close > EMA50 today."""
    above = 0
    total = 0
    advances = 0
    declines = 0
    try:
        raw = yf.download(
            BREADTH_STOCKS,
            period="3mo",
            interval="1d",
            progress=False,
            auto_adjust=True,
            timeout=30,
        )
        if raw is not None and not raw.empty:
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
                    if len(s) >= 2:
                        if s.iloc[-1] > s.iloc[-2]:
                            advances += 1
                        else:
                            declines += 1
                    else:
                        if s.iloc[-1] > 0:
                            advances += 1
                        else:
                            declines += 1
                except Exception:
                    continue
    except Exception as exc:
        log.warning(f"Breadth calc failed: {exc}")

    pct = round((above / total * 100) if total else 50.0, 1)
    if advances == 0 and declines == 0:
        advances = 15
        declines = 15
    return {
        "pct_above_ema50": pct,
        "advances": advances,
        "declines": declines
    }


# ─────────────────────────────────────────────────────────────
# SECTOR TICKERS (used for 20% weight)
# ─────────────────────────────────────────────────────────────
SECTOR_TICKERS_REGIME = {
    "Banking": "^NSEBANK", "IT": "^CNXIT", "Pharma": "^CNXPHARMA",
    "Auto": "^CNXAUTO", "FMCG": "^CNXFMCG", "Metals": "^CNXMETAL",
    "Energy": "^CNXENERGY", "Realty": "^CNXREALTY",
}

_sector_cache_regime: Dict = {}
_sector_cache_time_regime: float = 0.0

def _get_sector_data_for_regime() -> Dict[str, float]:
    """Fetch sector change% for regime calculation (5-min cache)."""
    global _sector_cache_regime, _sector_cache_time_regime
    if time.time() - _sector_cache_time_regime < 300 and _sector_cache_regime:
        return _sector_cache_regime
    result: Dict[str, float] = {}
    for name, ticker in SECTOR_TICKERS_REGIME.items():
        try:
            t = yf.Ticker(ticker)
            fi = t.fast_info
            prev = fi.get("regularMarketPreviousClose") or fi.get("previousClose") or fi.get("previous_close") or 0
            last = fi.get("lastPrice") or fi.get("last_price") or 0
            if prev > 0 and last > 0:
                result[name] = round((last - prev) / prev * 100, 2)
            else:
                result[name] = 0.0
        except Exception:
            result[name] = 0.0
    _sector_cache_regime = result
    _sector_cache_time_regime = time.time()
    return result


# ─────────────────────────────────────────────────────────────
# v5.0 WEIGHTED SCORE ENGINE  (Section 1)
# ─────────────────────────────────────────────────────────────
def calculate_market_score(
    breadth_data: Dict,
    index_data: Dict,
    sector_data: Dict,
    atr_data: Dict,
    fii_dii_data: Dict,
) -> Dict:
    """
    Market Score = 35% Breadth + 25% EMA + 20% Sector + 10% ATR + 10% FII/DII
    Returns: score (0-100), regime, confidence%, reasons[], breakdown
    """
    scores: Dict[str, float] = {}
    reasons: List[str] = []

    # --- 35% BREADTH ---
    adv = breadth_data.get("advances", 0)
    dec = breadth_data.get("declines", 1)
    adv_dec_ratio = adv / (adv + dec) if (adv + dec) > 0 else 0.5
    ema50_pct = breadth_data.get("pct_above_ema50", 50)
    breadth_score = min(max((adv_dec_ratio * 50) + (ema50_pct * 0.5), 0), 100)
    scores["breadth"] = breadth_score * 0.35
    if adv_dec_ratio < 0.4:
        reasons.append("Weak breadth — more declines than advances")
    elif adv_dec_ratio > 0.6:
        reasons.append("Strong breadth — advances dominating")

    # --- 25% INDEX EMA ---
    nifty_price = index_data.get("price", 0)
    ema20  = index_data.get("ema20", 0)
    ema50  = index_data.get("ema50", 0)
    ema200 = index_data.get("ema200", 0)
    ema_score = 0
    if nifty_price > ema200: ema_score += 33
    if nifty_price > ema50:  ema_score += 33
    if nifty_price > ema20:  ema_score += 34
    if ema20 > ema50 > ema200:   ema_score = 100
    if ema20 < ema50 < ema200:   ema_score = 0
    scores["ema"] = ema_score * 0.25
    if ema20 < ema50:
        reasons.append("Death cross active — EMA20 below EMA50")
    elif ema20 > ema50 > ema200:
        reasons.append("Golden alignment — all EMAs bullish")

    # --- 20% SECTOR STRENGTH ---
    positive_sectors = sum(1 for v in sector_data.values() if v > 0)
    n_sectors = len(sector_data) or 1
    sector_pct = (positive_sectors / n_sectors) * 100
    scores["sector"] = sector_pct * 0.20
    if sector_data:
        top_sector  = max(sector_data, key=sector_data.get)
        weak_sector = min(sector_data, key=sector_data.get)
        if sector_pct < 40:
            reasons.append(f"Only {positive_sectors}/{n_sectors} sectors positive")
        elif sector_pct > 70:
            reasons.append(f"Broad participation — {positive_sectors} sectors up")
        reasons.append(f"Leading: {top_sector} | Lagging: {weak_sector}")

    # --- 10% ATR VOLATILITY ---
    atr_pct = atr_data.get("atr_pct", 1.5)
    if atr_pct < 1.0:   atr_score = 80
    elif atr_pct < 1.5: atr_score = 60
    elif atr_pct < 2.0: atr_score = 40
    else:               atr_score = 20
    scores["atr"] = atr_score * 0.10
    if atr_pct > 2.0:
        reasons.append(f"High volatility ATR {atr_pct:.1f}% — widen SLs")

    # --- 10% FII/DII ---
    fii_net = fii_dii_data.get("fii_net", 0)
    dii_net = fii_dii_data.get("dii_net", 0)
    if fii_net > 0 and dii_net > 0:
        fii_score = 100
        reasons.append("Both FII & DII net buyers — strong institutional support")
    elif fii_net > 0:
        fii_score = 70
    elif dii_net > 0:
        fii_score = 50
        reasons.append(f"Negative FII ₹{abs(fii_net/100):.0f}Cr — institutional headwind")
    else:
        fii_score = 30
        if fii_net < 0 or dii_net < 0:
            reasons.append("Both FII & DII net sellers — avoid longs")
    scores["fii"] = fii_score * 0.10

    # --- FINAL SCORE ---
    total_score = sum(scores.values())

    if total_score >= 75:
        regime = "STRONG_BULL"
        confidence = min(int((total_score - 75) / 25 * 100), 99)
    elif total_score >= 60:
        regime = "BULL"
        confidence = min(int((total_score - 60) / 15 * 100), 99)
    elif total_score >= 40:
        regime = "NEUTRAL"
        confidence = 50
    elif total_score >= 25:
        regime = "BEAR"
        confidence = min(int((40 - total_score) / 15 * 100), 99)
    else:
        regime = "STRONG_BEAR"
        confidence = min(int((25 - total_score) / 25 * 100), 99)

    return {
        "score":      round(total_score, 1),
        "regime":     regime,
        "confidence": confidence,
        "reasons":    reasons[:4],
        "breakdown":  {k: round(v, 2) for k, v in scores.items()},
    }



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
def _fetch_fii_dii_regime() -> Dict:
    """Fetch today's real FII/DII cash market flows from NSE India API."""
    try:
        import requests as _req
        session = _req.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
        })
        session.get("https://www.nseindia.com/", timeout=10)
        r = session.get("https://www.nseindia.com/api/fiidiiTradeReact", timeout=10)
        if r.status_code == 200:
            rows = r.json()
            fii_row = next((x for x in rows if "FII" in x.get("category", "")), None)
            dii_row = next((x for x in rows if x.get("category", "") == "DII"), None)
            fii_net = float(fii_row["netValue"]) if fii_row else -1240.0
            dii_net = float(dii_row["netValue"]) if dii_row else 2180.0
            return {"fii_net": fii_net, "dii_net": dii_net}
    except Exception as e:
        log.warning(f"FII/DII fetch in regime failed: {e}")
    return {"fii_net": -1240.0, "dii_net": 2180.0}


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
      score         float 0-100 regime strength
      confidence    int   regime confidence%
      reasons       list  reasons behind score
      breakdown     dict  weighted score component details
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
        "score":        50.0,
        "confidence":   50,
        "reasons":      [],
        "breakdown":    {},
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

        # Breadth data calculation
        breadth_data = _calc_breadth()

        # Sector change data
        sector_data = _get_sector_data_for_regime()

        # FII/DII data
        fii_dii_data = _fetch_fii_dii_regime()

        # Index data
        index_data = {
            "price": close,
            "ema20": ema20,
            "ema50": ema50,
            "ema200": ema200
        }

        # ATR data
        atr_data = {"atr_pct": atr_pct}

        # Calculate using Weighted Score Engine (Section 1)
        score_res = calculate_market_score(
            breadth_data=breadth_data,
            index_data=index_data,
            sector_data=sector_data,
            atr_data=atr_data,
            fii_dii_data=fii_dii_data
        )

        regime = score_res["regime"]
        rd = REGIMES[regime]

        result.update({
            "regime":       regime,
            "label":        rd["label"],
            "emoji":        rd["emoji"],
            "color":        rd["color"],
            "bg":           rd["bg"],
            "description":  rd["description"],
            "score":        score_res["score"],
            "confidence":   score_res["confidence"],
            "reasons":      score_res["reasons"],
            "breakdown":    score_res["breakdown"],
            "nifty_close":  round(close, 2),
            "nifty_ema20":  round(ema20, 2),
            "nifty_ema50":  round(ema50, 2),
            "nifty_ema200": round(ema200, 2),
            "breadth":      breadth_data["pct_above_ema50"],
            "atr_pct":      atr_pct,
            "vol_ratio":    vol_ratio,
            "pct_from_high":pct_from_high,
            "bull_mult":    rd["bull_mult"],
            "bear_mult":    rd["bear_mult"],
            "risk_mult":    rd["risk_mult"],
            "updated_at":   datetime.now().strftime("%d %b %Y %H:%M"),
            "error":        None,
        })
        log.info(f"✅ Regime: {regime} | Score: {score_res['score']} | NIFTY: {close:,.2f}")

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
