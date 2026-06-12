"""
NSE Scanner Web Dashboard — Robust v3.0 [Phase 2 Hardened]
==========================================================
Run:   python dashboard.py
Open:  http://localhost:5000

Improvements in Phase 2 & 3:
  • Structural Camarilla 52W range targets using Pivot entries and StopLoss caps.
  • Client-side dynamic timers and 5-day SVG sparklines.
  • Short-term 20d relative strength and सकारात्मक 50d RS positive confirmation filter.
  • Aligned tabs: Bullish, Bearish, Mixed Caution setups, and Entry Ready setups.
  • Dedicated single-stock lookup `/scan-single` with skipped filters descriptions feedback.
  • Real-time NSE Market Hours guard and timezone-aware banner alerts.
"""

import io
import json
import os
import threading
import uuid
from regime import get_regime
import copy
from datetime import datetime
from journal import init_db, add_trade, close_trade, \
                    get_trades, delete_trade, get_scorecard
import csv

import pandas as pd
from flask import Flask, jsonify, render_template_string, request, send_file

from scanner import (
    CFG,
    NIFTY500_SAMPLE,
    run_scan,
    save_csv,
    send_telegram,
    send_whatsapp,
)

# ─────────────────────────────────────────────────────────────
# APP SETUP
# ─────────────────────────────────────────────────────────────
app = Flask(__name__)

from admin_routes import admin_bp
app.register_blueprint(admin_bp)

STATUS_FILE    = "scan_status.json"
CONFIG_FILE    = "config.json"
WATCHLIST_FILE = "watchlist.json"

# ─────────────────────────────────────────────────────────────
# THREAD-SAFE STATE
# ─────────────────────────────────────────────────────────────
_state_lock = threading.Lock()
_state = {
    "regime":    {},
    "signals":   [],
    "last_scan": "Never",
    "scanning":  False,
    "progress":  {"current": 0, "total": 0, "ticker": "Idle", "eta": 0},
    "scan_id":   None,
    "error":     None,
}
_stop_event:  threading.Event = threading.Event()
_scan_thread: threading.Thread = None   # type: ignore[assignment]


def _read_state() -> dict:
    """Thread-safe state reader using deepcopy to prevent concurrency race conditions."""
    with _state_lock:
        return copy.deepcopy(_state)


def _update_state(**kwargs) -> None:
    """Thread-safe state updater."""
    with _state_lock:
        _state.update(kwargs)
    _persist_state()


def _persist_state() -> None:
    """Write state snapshot to disk for persistence."""
    with _state_lock:
        snapshot = copy.deepcopy(_state)
    try:
        with open(STATUS_FILE, "w", encoding="utf-8") as fh:
            json.dump(snapshot, fh, indent=2)
    except Exception as exc:
        print(f"[warn] Could not write {STATUS_FILE}: {exc}")


def _load_state_from_disk() -> None:
    """Restore state snapshot from disk on startup."""
    if not os.path.exists(STATUS_FILE):
        return
    try:
        with open(STATUS_FILE, encoding="utf-8") as fh:
            data = json.load(fh)
        with _state_lock:
            _state["signals"]   = data.get("signals",   [])
            _state["last_scan"] = data.get("last_scan", "Never")
            _state["scanning"]  = False          # never inherit a stale scanning=True
            _state["error"]     = None
    except Exception as exc:
        print(f"[warn] Could not restore state from {STATUS_FILE}: {exc}")


# ─────────────────────────────────────────────────────────────
# CONFIG PERSISTENCE
# ─────────────────────────────────────────────────────────────
def _load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            pass
    return {}


def _save_config(data: dict) -> None:
    existing = _load_config()
    existing.update(data)
    with open(CONFIG_FILE, "w", encoding="utf-8") as fh:
        json.dump(existing, fh, indent=2)


def _mask(value: str) -> str:
    if not value:
        return ""
    return "•" * max(0, len(value) - 4) + value[-4:]


# ─────────────────────────────────────────────────────────────
# WATCHLIST PERSISTENCE
# ─────────────────────────────────────────────────────────────
def _load_watchlist() -> list:
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            pass
    return []


def _save_watchlist(wl: list) -> None:
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as fh:
        json.dump(wl, fh)


# ─────────────────────────────────────────────────────────────
# LOAD LATEST CSV CACHE ON STARTUP
# ─────────────────────────────────────────────────────────────
def _bootstrap_cache() -> None:
    """If no in-memory signals, try to load the most recent scan CSV."""
    if _state.get("signals"):
        return
    try:
        csvs = sorted(f for f in os.listdir(".") if f.startswith("scan_") and f.endswith(".csv"))
        if not csvs:
            return
        df = pd.read_csv(csvs[-1])

        def _safe(row, col, cast=str, default=""):
            try:
                return cast(row[col])
            except Exception:
                return default

        signals = []
        for _, r in df.iterrows():
            price = _safe(r, "Price", float, 0.0)
            signals.append({
                "symbol":          _safe(r, "Symbol"),
                "score":           _safe(r, "Score",          int,   0),
                "regime_score":    _safe(r, "Regime_Score",   int,   _safe(r, "Score", int, 0)),
                "confidence":      _safe(r, "Confidence",     str,   "B"),
                "signal_strength": _safe(r, "Strength",       str,   "Moderate"),
                "signal_type":     _safe(r, "Signal",         str,   "Bull"),
                "price":           price,
                "entry":           _safe(r, "Entry",          float, 0.0),
                "dist_from_entry": _safe(r, "Dist_From_Entry_%", float, 0.0),
                "scanned_time":    _safe(r, "Scanned_Time",   str,   ""),
                "target":          _safe(r, "Target_H3",      float, 0.0),
                "target2":         _safe(r, "Target_H4",      float, 0.0),
                "stop_loss":       _safe(r, "StopLoss_L3",    float, 0.0),
                "risk_percentage": _safe(r, "Risk_%",         float, 0.0),
                "rr":              _safe(r, "R:R",            float, 0.0),
                "upside":          _safe(r, "Upside_%",       float, 0.0),
                "vol_ratio":       _safe(r, "Vol_Ratio",      float, 0.0),
                "candle":          _safe(r, "Candle"),
                "ema10":           _safe(r, "EMA10",          float, 0.0),
                "ema20":           _safe(r, "EMA20",          float, 0.0),
                "ema50":           _safe(r, "EMA50",          float, 0.0),
                "ema200":          _safe(r, "EMA200",         float, 0.0),
                "hv_high":         _safe(r, "HV_High",        float, 0.0),
                "hv_low":          _safe(r, "HV_Low",         float, 0.0),
                "hv_date":         _safe(r, "HV_Date"),
                "days":            _safe(r, "Days",           int,   0),
                "ema20_slope":     _safe(r, "EMA20_Slope",    float, 0.0),
                "atr_dist":        _safe(r, "ATR_Dist",       float, 0.0),
                "vol_percentile":  _safe(r, "Vol_Percentile", float, 0.0),
                "range_expansion": _safe(r, "Range_Expansion",float, 1.0),
                "atr":             _safe(r, "ATR",            float, 0.0),
                "rs_pct":          _safe(r, "RS_Pct",         float, 0.0),
                "turnover_score":  _safe(r, "Turnover_Cr",    float, 0.0),
                "sparkline":       json.loads(_safe(r, "Sparkline", str, "[]")),  # Deserialize sparkline list
                "ema10_pass":      price > _safe(r, "EMA10",  float, 0.0),
                "ema20_pass":      price > _safe(r, "EMA20",  float, 0.0),
                "ema50_pass":      price > _safe(r, "EMA50",  float, 0.0),
                "ema200_pass":     price > _safe(r, "EMA200", float, 0.0),
                "scanned_date":    _safe(r, "Scanned_At"),
            })

        t_str = csvs[-1].replace("scan_", "").replace(".csv", "")
        dt = datetime.strptime(t_str, "%Y%m%d_%H%M")
        with _state_lock:
            _state["signals"]   = signals
            _state["last_scan"] = dt.strftime("%d %b %Y %H:%M")
    except Exception as exc:
        print(f"[bootstrap] No CSV cache loaded: {exc}")


# ── Startup initialisation ───────────────────────────────────
_load_state_from_disk()
_bootstrap_cache()
init_db()


# ─────────────────────────────────────────────────────────────
# BACKGROUND SCAN WORKER
# ─────────────────────────────────────────────────────────────
def _do_scan(params: dict, scan_id: str) -> None:
    """Runs in a background thread. Populates thread-safe _state as it progresses."""
    global _scan_thread

    cfg_override = {
        "VOL_DAYS":       int(params.get("vol_days", CFG["VOL_DAYS"])),
        "VOL_MULT":       float(params.get("vol_mult", CFG["VOL_MULT"])),
        "EMA_10":         bool(params.get("ema10", CFG["EMA_10"])),
        "EMA_20":         bool(params.get("ema20", CFG["EMA_20"])),
        "EMA_50":         bool(params.get("ema50", CFG["EMA_50"])),
        "EMA_200":        bool(params.get("ema200", CFG["EMA_200"])),
        "TG_TOKEN":       params.get("tg_token",     _load_config().get("tg_token",    "")),
        "TG_CHAT_ID":     params.get("tg_chat_id",   _load_config().get("tg_chat_id",  "")),
        "TWILIO_SID":     params.get("twilio_sid",   _load_config().get("twilio_sid",  "")),
        "TWILIO_TOKEN":   params.get("twilio_token", _load_config().get("twilio_token","")),
        "TWILIO_TO":      params.get("twilio_to",    _load_config().get("twilio_to",   "")),
        "TURNOVER_LIMIT": float(params.get("turnover_limit", CFG["TURNOVER_LIMIT"])),
        "USE_CACHE_ONLY": bool(params.get("use_cache", False)),
        "MIN_PRICE":      float(params.get("min_price", 50.0)),
        "MIN_SCORE":      int(params.get("min_score", CFG["MIN_SCORE"])),
        "MAX_52W_AGE":    int(params.get("max_52w_age", CFG["MAX_52W_AGE"])),
    }

    scan_mode = params.get("scan_mode", "bullish")   # bullish | bearish | both
    tickers   = params.get("tickers") or NIFTY500_SAMPLE

    def _progress(current: int, total: int, ticker: str, eta: int = 0) -> None:
        if _state.get("scan_id") != scan_id:
            _stop_event.set()
        _update_state(
            progress={"current": current, "total": total, "ticker": ticker, "eta": eta}
        )

    try:
        all_signals = []

        if scan_mode in ("bullish", "both"):
            _update_state(progress={"current": 0, "total": len(tickers),
                                     "ticker": "Starting bullish scan📈", "eta": 0})
            bull_result = run_scan(
                tickers,
                bearish=False,
                progress_cb=_progress,
                cfg_override=cfg_override,
                stop_event=_stop_event,
            )
            if isinstance(bull_result, dict):
                all_signals.extend(bull_result.get("signals", []))
                regime_data = bull_result.get("regime", {})
            else:
                all_signals.extend(bull_result)
                regime_data = get_regime()

        if scan_mode in ("bearish", "both") and not _stop_event.is_set():
            _update_state(progress={"current": 0, "total": len(tickers),
                                     "ticker": "Starting bearish scan📉", "eta": 0})
            bear_result = run_scan(
                tickers,
                bearish=True,
                progress_cb=_progress,
                cfg_override=cfg_override,
                stop_event=_stop_event,
            )
            if isinstance(bear_result, dict):
                all_signals.extend(bear_result.get("signals", []))
                regime_data = bear_result.get("regime", {})
            else:
                all_signals.extend(bear_result)
                regime_data = get_regime()

        all_signals.sort(key=lambda x: x["score"], reverse=True)

        if not _stop_event.is_set():
            save_csv(all_signals)
            now   = datetime.now().strftime("%d %b %Y %H:%M")
            if len(all_signals) > 0:
                _update_state(
                    signals=all_signals,
                    last_scan=now,
                    scanning=False,
                    scan_id=None,
                    regime=regime_data,
                    error=None,
                    progress={"current": len(tickers), "total": len(tickers),
                               "ticker": "Done", "eta": 0},
                )
            else:
                import logging
                logging.getLogger("NSEScanner").warning("Scan returned 0 signals — keeping previous results")
                _update_state(
                    last_scan=now + " (0 new)",
                    scanning=False,
                    scan_id=None,
                    regime=regime_data,
                    error=None,
                    progress={"current": len(tickers), "total": len(tickers),
                               "ticker": "Done", "eta": 0},
                )
            if all_signals:
                send_telegram(all_signals, cfg_override=cfg_override)
                send_whatsapp(all_signals, cfg_override=cfg_override)
        else:
            _update_state(
                scanning=False,
                scan_id=None,
                error="Scan cancelled",
                progress={"current": 0, "total": 0, "ticker": "Cancelled", "eta": 0},
            )

    except Exception as exc:
        _update_state(
            scanning=False,
            scan_id=None,
            error=str(exc),
            progress={"current": 0, "total": 0, "ticker": "Error", "eta": 0},
        )
    finally:
        _scan_thread = None


# ─────────────────────────────────────────────────────────────
# FLASK ROUTES
# ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    from flask import Response
    html_str = render_template_string(HTML)
    # Encode explicitly as UTF-8 to handle emoji/surrogate characters correctly
    # on Python 3.14+ Windows where Werkzeug's default encoder is strict
    html_bytes = html_str.encode("utf-8", errors="replace")
    resp = Response(html_bytes, mimetype="text/html; charset=utf-8")
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/health")
def health():
    return jsonify({"status": "ok", "ts": datetime.now().isoformat()})


@app.route("/market_status")
def market_status():
    from scanner import is_nse_market_open
    from datetime import datetime, timezone, timedelta
    
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist_tz)
    is_open = is_nse_market_open()
    
    # Calculate next open datetime in IST (9:15 AM)
    next_open = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    
    # If today is a weekday and already after 9:15 AM, push to tomorrow
    if now_ist >= next_open:
        next_open += timedelta(days=1)
        
    # If the next open falls on a weekend, push to Monday
    while next_open.weekday() >= 5:
        next_open += timedelta(days=1)
        
    diff = next_open - now_ist
    seconds_remaining = int(diff.total_seconds())
    
    h = seconds_remaining // 3600
    m = (seconds_remaining % 3600) // 60
    s_sec = seconds_remaining % 60
    countdown_str = f"{h}h {m}m {s_sec}s"
    
    return jsonify({
        "is_open": is_open,
        "time": now_ist.strftime("%d-%b-%Y %H:%M:%S IST"),
        "day": now_ist.strftime("%A"),
        "next_open_in": countdown_str if not is_open else "",
        "next_open_seconds": seconds_remaining if not is_open else 0
    })


@app.route("/scan-single", methods=["POST"])
def scan_single():
    from scanner import analyse, CFG
    data = request.get_json(force=True, silent=True) or {}
    ticker = data.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify({"error": "Ticker is required."}), 400
        
    # Standardize NSE tickers (add .NS if missing and not index)
    if not ticker.endswith(".NS") and not ticker.startswith("^"):
        ticker += ".NS"

    cfg_override = {
        "VOL_DAYS":       int(data.get("vol_days", CFG["VOL_DAYS"])),
        "VOL_MULT":       float(data.get("vol_mult", CFG["VOL_MULT"])),
        "EMA_10":         bool(data.get("ema10", True)),
        "EMA_20":         bool(data.get("ema20", True)),
        "EMA_50":         bool(data.get("ema50", True)),
        "EMA_200":        bool(data.get("ema200", True)),
        "TURNOVER_LIMIT": float(data.get("turnover_limit", CFG["TURNOVER_LIMIT"])),
        "USE_CACHE_ONLY": bool(data.get("use_cache", False)),
    }

    # Attempt to scan for Bullish first
    res = analyse(ticker, bearish=False, cfg_override=cfg_override, explain_skip=True)
    
    # If not a signal (skipped) or returned None, check Bearish
    if res is None or res.get("skipped"):
        bull_skip_reason = res.get("reason") if res else "Unknown skip"
        res_bear = analyse(ticker, bearish=True, cfg_override=cfg_override, explain_skip=True)
        
        if res_bear is not None and not res_bear.get("skipped"):
            res = res_bear
        else:
            bear_skip_reason = res_bear.get("reason") if res_bear else "Unknown skip"
            # Both failed to produce a valid signal. Return the skip reasons.
            return jsonify({
                "ok": False,
                "symbol": ticker.replace(".NS", ""),
                "message": f"{ticker.replace('.NS', '')}: skipped — {bull_skip_reason} (Bullish) | {bear_skip_reason} (Bearish)"
            })

    # Valid signal found (Bull or Bear)!
    # Update active list thread-safely
    symbol = res["symbol"]
    with _state_lock:
        # Remove any existing signal for this ticker
        _state["signals"] = [s for s in _state["signals"] if s["symbol"] != symbol]
        _state["signals"].append(res)
        _state["signals"].sort(key=lambda x: x["score"], reverse=True)
    _persist_state()

    return jsonify({
        "ok": True,
        "signal": res
    })


# ── FII/DII real EOD data from NSE India ──────────────────
_fii_dii_cache = {"data": None, "fetched_at": None}
_fii_dii_lock = threading.Lock()

def _fetch_fii_dii():
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
        # Warm up session cookies first
        session.get("https://www.nseindia.com/", timeout=10)
        r = session.get("https://www.nseindia.com/api/fiidiiTradeReact", timeout=10)
        if r.status_code == 200:
            rows = r.json()
            fii_row = next((x for x in rows if "FII" in x.get("category", "")), None)
            dii_row = next((x for x in rows if x.get("category", "") == "DII"), None)
            fii_net = float(fii_row["netValue"]) if fii_row else -1240.0
            dii_net = float(dii_row["netValue"]) if dii_row else 2180.0
            fii_buy = float(fii_row["buyValue"]) if fii_row else 0.0
            fii_sell = float(fii_row["sellValue"]) if fii_row else 0.0
            dii_buy = float(dii_row["buyValue"]) if dii_row else 0.0
            dii_sell = float(dii_row["sellValue"]) if dii_row else 0.0
            date_str = fii_row.get("date", "") if fii_row else ""
            bias = "DII Dominant — Bullish Bias" if dii_net > 0 and dii_net > abs(fii_net) else \
                   "FII Dominant — Bearish Bias" if fii_net < -500 else \
                   "Balanced Flows — Neutral"
            return {
                "fii_net": round(fii_net, 2),
                "dii_net": round(dii_net, 2),
                "fii_buy": round(fii_buy, 2),
                "fii_sell": round(fii_sell, 2),
                "dii_buy": round(dii_buy, 2),
                "dii_sell": round(dii_sell, 2),
                "date": date_str,
                "bias": bias,
                "source": "NSE India (Real EOD)",
            }
    except Exception as e:
        import logging
        logging.warning(f"FII/DII fetch failed: {e}")
    # Fallback defaults when market closed / API unavailable
    return {
        "fii_net": -1240.0, "dii_net": 2180.0,
        "fii_buy": 0.0, "fii_sell": 0.0,
        "dii_buy": 0.0, "dii_sell": 0.0,
        "date": "", "bias": "DII Dominant — Bullish Bias",
        "source": "Simulated (NSE Unavailable)",
    }


@app.route("/fii_dii")
def fii_dii_route():
    """Return real FII/DII EOD flows from NSE, cached for 30 min."""
    with _fii_dii_lock:
        now = datetime.now()
        cache = _fii_dii_cache
        if cache["data"] is None or cache["fetched_at"] is None or \
           (now - cache["fetched_at"]).total_seconds() > 1800:  # 30-min cache
            cache["data"] = _fetch_fii_dii()
            cache["fetched_at"] = now
        return jsonify(cache["data"])


# ── Real NSE Sector Index data via Yahoo Finance fast_info ─────
_sector_cache = {"data": None, "fetched_at": None, "fetched_time": None}
_sector_lock = threading.Lock()

NSE_SECTOR_TICKERS = {
    "Banking":  "^NSEBANK",
    "IT":       "^CNXIT",
    "Pharma":   "^CNXPHARMA",
    "Auto":     "^CNXAUTO",
    "FMCG":     "^CNXFMCG",
    "Metals":   "^CNXMETAL",
    "Energy":   "^CNXENERGY",
    "Realty":   "^CNXREALTY",
    "Infra":    "^CNXINFRA",
    "Media":    "^CNXMEDIA",
}

def _fetch_sector_rotation():
    """Fetch today's % change for each NSE sector index.
    Uses yf.download (5d EOD) — works both intraday and after-hours."""
    import yfinance as yf
    import pandas as pd
    from datetime import timezone, timedelta
    result = []
    for name, ticker in NSE_SECTOR_TICKERS.items():
        change = 0.0
        try:
            data = yf.download(ticker, period="5d", interval="1d",
                               progress=False, auto_adjust=True)
            if not data.empty:
                # yfinance may return multi-level columns: ("Close", ticker)
                close_col = data["Close"]
                if isinstance(close_col, pd.DataFrame):
                    close_col = close_col.squeeze()  # flatten to Series
                closes = close_col.dropna()
                if len(closes) >= 2:
                    p = float(closes.iloc[-2])
                    l = float(closes.iloc[-1])
                    if p > 0:
                        change = round((l - p) / p * 100, 2)
        except Exception:
            change = 0.0
        trend    = "up" if change >= 2.0 else "down" if change <= -2.0 else "neutral"
        strength = min(100, max(0, int(50 + change * 10)))
        result.append({"name": name, "change": change,
                        "trend": trend, "strength": strength})
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    time_label = datetime.now(ist_tz).strftime("%H:%M IST")
    return {"sectors": result, "fetched_time": time_label}


@app.route("/sector_rotation")
def sector_rotation_route():
    """Return real NSE sector index daily % change, cached for 5 min."""
    with _sector_lock:
        now = datetime.now()
        cache = _sector_cache
        if cache["data"] is None or cache["fetched_at"] is None or \
           (now - cache["fetched_at"]).total_seconds() > 300:   # 5-min cache
            cache["data"] = _fetch_sector_rotation()
            cache["fetched_at"] = now
        return jsonify(cache["data"])


@app.route("/results")
def results():
    s = _read_state()
    return jsonify({
        "signals":   s["signals"],
        "last_scan": s["last_scan"],
        "scanning":  s["scanning"],
        "error":     s.get("error"),
    })


@app.route("/status")
def status():
    s = _read_state()
    return jsonify({
        "scanning":  s["scanning"],
        "progress":  s["progress"],
        "signals":   s["signals"],
        "last_scan": s["last_scan"],
        "error":     s.get("error"),
    })


@app.route("/scan", methods=["POST"])
def start_scan():
    global _scan_thread, _stop_event

    s = _read_state()
    if s["scanning"]:
        return jsonify({"error": "A scan is already running. Stop it first."}), 409

    params  = request.get_json(force=True, silent=True) or {}
    scan_id = str(uuid.uuid4())

    _stop_event = threading.Event()   # fresh event for new scan

    _update_state(
        scanning=True,
        scan_id=scan_id,
        error=None,
        progress={"current": 0, "total": len(NIFTY500_SAMPLE),
                  "ticker": "Initialising…", "eta": 0},
    )

    _scan_thread = threading.Thread(
        target=_do_scan,
        args=(params, scan_id),
        daemon=True,
        name=f"scan-{scan_id[:8]}",
    )
    _scan_thread.start()
    return jsonify({"ok": True, "scan_id": scan_id})


@app.route("/stop", methods=["POST"])
def stop_scan():
    global _stop_event
    _stop_event.set()
    _update_state(scanning=False, scan_id=None,
                  error="Scan stopped by user",
                  progress={"current": 0, "total": 0, "ticker": "Stopped", "eta": 0})
    return jsonify({"ok": True, "message": "Scan stop signal sent."})


@app.route("/export")
def export_csv():
    s = _read_state()
    if not s["signals"]:
        return jsonify({"error": "No results to export"}), 404

    # Read filter query parameters to align CSV export with dashboard view
    try:
        vol_mult = float(request.args.get("vol_mult", 0.0))
    except Exception:
        vol_mult = 0.0

    try:
        turnover_limit = float(request.args.get("turnover_limit", 0.0))
    except Exception:
        turnover_limit = 0.0

    try:
        min_price = float(request.args.get("min_price", 0.0))
    except Exception:
        min_price = 0.0

    active_tab = request.args.get("active_tab", "camarilla")
    active_sub_tab = request.args.get("active_sub_tab", "all")

    # EMA filter flags — 1 means "require price above this EMA", 0 means don't filter
    ema_req = {
        10:  request.args.get("ema10",  "0") == "1",
        20:  request.args.get("ema20",  "0") == "1",
        50:  request.args.get("ema50",  "0") == "1",
        200: request.args.get("ema200", "0") == "1",
    }

    rows = []
    for r in s["signals"]:
        price = r.get("price", 0.0)
        vol_ratio = r.get("vol_ratio", 0.0)
        # turnover_score in signals is in Crores (1 Crore = 10,000,000 INR)
        turnover_inr = r.get("turnover_score", 0.0) * 10000000.0

        # Apply basic control panel filters
        if vol_ratio < vol_mult:
            continue
        if turnover_inr < turnover_limit:
            continue
        if price < min_price:
            continue

        # Apply EMA filters — skip stock if price is NOT above a required EMA
        ema_vals = {
            10:  r.get("ema10",  0.0) or 0.0,
            20:  r.get("ema20",  0.0) or 0.0,
            50:  r.get("ema50",  0.0) or 0.0,
            200: r.get("ema200", 0.0) or 0.0,
        }
        ema_fail = False
        for period, required in ema_req.items():
            if required and ema_vals[period] > 0 and price <= ema_vals[period]:
                ema_fail = True
                break
        if ema_fail:
            continue

        # Apply Tab Filters matching the frontend render() logic
        signal_type = r.get("signal_type", "Bull")
        candle = r.get("candle", "Bull")
        symbol = r.get("symbol", "")

        # Watchlist filter
        if active_tab == 'watchlist':
            watchlist = _load_watchlist()
            if symbol not in watchlist:
                continue

        # ProTrader filter
        if active_tab == 'protrader':
            if vol_ratio < 1.2:
                continue

        # Camarilla sub-tab filters
        if active_tab == 'camarilla':
            if active_sub_tab == 'bullish':
                if not (signal_type == 'Bull' and candle == 'Bull'):
                    continue
            elif active_sub_tab == 'bearish':
                if not (signal_type == 'Bear' and candle == 'Bear'):
                    continue
            elif active_sub_tab == 'mixed':
                is_mixed = (signal_type == 'Bull' and candle == 'Bear') or (signal_type == 'Bear' and candle == 'Bull')
                if not is_mixed:
                    continue
            elif active_sub_tab == 'entry':
                entry_val = r.get("entry", 0.0)
                if entry_val <= 0 or abs(price - entry_val) / entry_val > 0.02:
                    continue
            elif active_sub_tab == 'fresh':
                if r.get("entry_status") != "FRESH":
                    continue
            elif active_sub_tab == 'hv':
                if r.get("days", 999) > 10:
                    continue

        rows.append({
            "Symbol":          r.get("symbol"),
            "Score":           r.get("score"),
            "Regime_Score":    r.get("regime_score", r.get("score")),
            "Confidence":      r.get("confidence", "B"),
            "Strength":        r.get("signal_strength", "Moderate"),
            "Signal":          r.get("signal_type", "Bull"),
            "Price":           r.get("price"),
            "Entry":           r.get("entry"),
            "Dist_From_Entry_%": r.get("dist_from_entry"),
            "Target_H3":       r.get("target"),
            "Target_H4":       r.get("target2"),
            "StopLoss_L3":     r.get("stop_loss"),
            "Risk_%":          r.get("risk_percentage"),
            "R:R":             r.get("rr"),
            "Upside_%":        r.get("upside"),
            "Vol_Ratio":       r.get("vol_ratio"),
            "Candle":          r.get("candle"),
            "EMA10":           r.get("ema10"),
            "EMA20":           r.get("ema20"),
            "EMA50":           r.get("ema50"),
            "EMA200":          r.get("ema200"),
            "HV_High":         r.get("hv_high"),
            "HV_Low":          r.get("hv_low"),
            "HV_Date":         r.get("hv_date"),
            "Days":            r.get("days"),
            "RS_Pct":          r.get("rs_pct", 0.0),
            "Turnover_Cr":     r.get("turnover_score", 0.0),
            "Sparkline":       json.dumps(r.get("sparkline", [])),
            "Scanned_Time":    r.get("scanned_time", ""),
            "Scanned_At":      s["last_scan"],
        })

    if not rows:
        df = pd.DataFrame(columns=[
            "Symbol", "Score", "Regime_Score", "Confidence", "Strength", "Signal", "Price",
            "Entry", "Dist_From_Entry_%", "Target_H3", "Target_H4", "StopLoss_L3", "Risk_%",
            "R:R", "Upside_%", "Vol_Ratio", "Candle", "EMA10", "EMA20", "EMA50", "EMA200",
            "HV_High", "HV_Low", "HV_Date", "Days", "RS_Pct", "Turnover_Cr", "Sparkline",
            "Scanned_Time", "Scanned_At"
        ])
    else:
        df = pd.DataFrame(rows)

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    fname = f"nse_scan_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=fname,
    )


@app.route("/alert", methods=["POST"])
def trigger_alerts():
    s      = _read_state()
    params = request.get_json(force=True, silent=True) or {}
    cfg_ov = {
        "TG_TOKEN":    params.get("tg_token",    _load_config().get("tg_token",    "")),
        "TG_CHAT_ID":  params.get("tg_chat_id",  _load_config().get("tg_chat_id",  "")),
        "TWILIO_SID":  params.get("twilio_sid",  _load_config().get("twilio_sid",  "")),
        "TWILIO_TOKEN":params.get("twilio_token",_load_config().get("twilio_token","")),
        "TWILIO_TO":   params.get("twilio_to",   _load_config().get("twilio_to",   "")),
    }
    if not s["signals"]:
        return jsonify({"message": "No signals to alert."})
    send_telegram(s["signals"], cfg_override=cfg_ov)
    send_whatsapp(s["signals"], cfg_override=cfg_ov)
    return jsonify({"message": f"Alerts processed for {len(s['signals'])} signal(s)."})


@app.route("/get_config")
def get_config():
    cfg = _load_config()
    return jsonify({
        "tg_token":    _mask(cfg.get("tg_token",    "")),
        "tg_chat_id":  _mask(cfg.get("tg_chat_id",  "")),
        "twilio_sid":  _mask(cfg.get("twilio_sid",  "")),
        "twilio_token":_mask(cfg.get("twilio_token","")),
        "twilio_to":   _mask(cfg.get("twilio_to",   "")),
    })


@app.route("/save_config", methods=["POST"])
def save_config_route():
    data  = request.get_json(force=True, silent=True) or {}
    mtype = data.get("type")
    saved = {}
    if mtype == "tg":
        saved = {"tg_token": data.get("f1", ""), "tg_chat_id": data.get("f2", "")}
    elif mtype == "wa":
        saved = {
            "twilio_sid":   data.get("f1", ""),
            "twilio_to":    data.get("f2", ""),
            "twilio_token": data.get("f3", ""),
        }
    if saved:
        _save_config(saved)
    return jsonify({"message": "Configuration saved successfully."})


@app.route("/watchlist", methods=["GET"])
def get_watchlist():
    import sqlite3
    import journal
    try:
        conn = sqlite3.connect(journal.DB_PATH)
        rows = conn.execute("SELECT symbol FROM watchlist").fetchall()
        conn.close()
        symbols = [r[0] for r in rows]
        if symbols:
            return jsonify({"watchlist": symbols})
    except Exception:
        pass
    return jsonify({"watchlist": _load_watchlist()})


@app.route("/watchlist", methods=["POST"])
def update_watchlist():
    data = request.get_json(force=True, silent=True) or {}
    wl   = data.get("watchlist", [])
    _save_watchlist(wl)
    
    # Sync with SQLite watchlist table
    import sqlite3
    import journal
    try:
        conn = sqlite3.connect(journal.DB_PATH)
        if wl:
            placeholders = ",".join("?" for _ in wl)
            conn.execute(f"DELETE FROM watchlist WHERE symbol NOT IN ({placeholders})", wl)
        else:
            conn.execute("DELETE FROM watchlist")
            
        state = _read_state()
        active_signals = {s["symbol"]: s for s in state.get("signals", [])}
        
        for sym in wl:
            row = conn.execute("SELECT id FROM watchlist WHERE symbol=?", (sym,)).fetchone()
            if not row:
                sig = active_signals.get(sym, {})
                entry = sig.get("entry", sig.get("price", 0.0))
                sl = sig.get("stop_loss", entry * 0.985)
                target = sig.get("target", entry * 1.03)
                sector = sig.get("sector", "")
                conn.execute("""
                    INSERT OR IGNORE INTO watchlist (symbol, entry_price, sl, target, sector)
                    VALUES (?, ?, ?, ?, ?)
                """, (sym, entry, sl, target, sector))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[warn] Watchlist SQLite sync failed: {e}")
        
    return jsonify({"ok": True, "count": len(wl)})


@app.route("/watchlist/status", methods=["GET"])
def get_watchlist_status_route():
    import sqlite3
    from watchlist import get_watchlist_status
    from data_fetcher import get_stock_price
    import journal
    
    results = []
    try:
        conn = sqlite3.connect(journal.DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM watchlist").fetchall()
        conn.close()
        
        for row in rows:
            stock = dict(row)
            symbol = stock["symbol"]
            ltp, _ = get_stock_price(symbol)
            if not ltp:
                ltp = stock["entry_price"] or 0.0
                
            status_data = get_watchlist_status(stock, stock["entry_price"], stock["sl"], stock["target"], ltp)
            results.append({
                "symbol": symbol,
                "sector": stock["sector"],
                "notes": stock["notes"],
                "status": status_data["status"],
                "color": status_data["color"],
                "entry": status_data["entry"],
                "sl": status_data["sl"],
                "target": status_data["target"],
                "ltp": ltp,
                "rr": status_data["current_rr"],
                "dist": status_data["dist_to_entry"]
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
    return jsonify({"watchlist_status": results})


@app.route("/regime")
def regime_route():
    """Return Nifty Market Regime, refreshing if force parameter is passed."""
    force = request.args.get("refresh") == "1"
    return jsonify(get_regime(force_refresh=force))


@app.route("/journal", methods=["GET"])
def journal_get():
    outcome = request.args.get("outcome")   # OPEN/WIN/LOSS/all
    trades  = get_trades(outcome=outcome if outcome != "all" else None)

    from data_fetcher import get_stock_price
    from trade_ledger import get_trade_metrics

    for t in trades:
        # sqlite3 Row must be converted to a dict first if it isn't already,
        # but get_trades already returns a list of dictionaries! So it is mutable.
        sym = str(t.get("symbol", "")).upper()
        
        # Get live ltp
        ltp, source = get_stock_price(sym)
        if not ltp:
            # Fallback to the scan cache price
            state = _read_state()
            signals_list = state.get("signals", [])
            price_map = {str(s.get("symbol", "")).upper(): s.get("price") for s in signals_list if s.get("price")}
            ltp = price_map.get(sym, t.get("entry_price", 0.0))
            
        t["current_ltp"] = ltp
        
        # Calculate live metrics
        try:
            metrics = get_trade_metrics(t, ltp)
            t["mtm"] = metrics["mtm"]
            t["days_held"] = metrics["days"]
            t["status"] = metrics["status"]
            t["status_color"] = metrics["status_color"]
            t["current_rr"] = metrics["current_rr"]
        except Exception:
            pass

    return jsonify({"trades": trades, "count": len(trades)})


@app.route("/api/backtest", methods=["GET", "POST"])
def backtest_route():
    from backtest import run_backtest
    symbol = request.args.get("symbol") or (request.get_json(force=True, silent=True) or {}).get("symbol")
    years = int(request.args.get("years", "1"))
    if not symbol:
        return jsonify({"error": "Symbol is required"}), 400
    res = run_backtest(symbol, years)
    if not res:
        return jsonify({"error": "No trade data or history found for backtesting"}), 404
    return jsonify(res)


@app.route("/ltp_live")
def ltp_live():
    """Fetch live LTP for all open trades using yfinance fast_info — independent of scan."""
    import yfinance as yf
    from journal import get_trades
    open_trades = [t for t in get_trades(outcome="OPEN") if t.get("symbol")]
    result = {}
    for t in open_trades:
        sym = t["symbol"].strip().upper()
        try:
            ticker_obj = yf.Ticker(sym + ".NS")
            ltp = ticker_obj.fast_info.get("lastPrice") or \
                  ticker_obj.fast_info.get("regularMarketPreviousClose") or \
                  ticker_obj.fast_info.get("previousClose") or None
            if ltp and float(ltp) > 0:
                result[sym] = round(float(ltp), 2)
        except Exception:
            result[sym] = None
    return jsonify(result)


@app.route("/journal", methods=["POST"])
def journal_add():
    data = request.get_json(force=True, silent=True) or {}
    if not data.get("symbol") or not data.get("entry_price"):
        return jsonify({"error": "symbol and entry_price required"}), 400
    trade_id = add_trade(data)
    return jsonify({"ok": True, "id": trade_id})


@app.route("/journal/<int:trade_id>", methods=["PUT"])
def journal_close(trade_id):
    data   = request.get_json(force=True, silent=True) or {}
    result = close_trade(trade_id, data)
    if "error" in result:
        return jsonify(result), 404
    return jsonify({"ok": True, **result})


@app.route("/journal/<int:trade_id>", methods=["DELETE"])
def journal_delete(trade_id):
    delete_trade(trade_id)
    return jsonify({"ok": True})


@app.route("/journal/scorecard")
def journal_scorecard():
    return jsonify(get_scorecard())


@app.route("/journal/export")
def journal_export():
    trades = get_trades()
    if not trades:
        return jsonify({"error": "No trades to export"}), 404
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=trades[0].keys())
    writer.writeheader()
    writer.writerows(trades)
    buf.seek(0)
    fname = f"trade_journal_{datetime.now().strftime('%Y%m%d')}.csv"
    return send_file(
        io.BytesIO(buf.getvalue().encode()),
        mimetype="text/csv",
        as_attachment=True,
        download_name=fname,
    )


# ─────────────────────────────────────────────────────────────
# HTML DASHBOARD (inline template with Dynamic Upgrades)
# ─────────────────────────────────────────────────────────────
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title id="pageTitle">ProTrader Terminal | Indian NSE Intraday F&O Signals Dashboard</title>
<!-- Google Fonts -->
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=JetBrains+Mono:ital,wght@0,100..800;1,100..800&display=swap" rel="stylesheet">
<!-- Tabler Icons -->
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@latest/dist/tabler-icons.min.css">
<!-- ApexCharts CDN -->
<script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>

<style>
:root {
  --font-sans: 'Inter', sans-serif;
  --font-mono: 'JetBrains Mono', monospace;
  --bg-dark: #0A0E1A;
  --bg-card: #111827;
  --bg-inner: #0D1117;
  --bg-hover: #1F2937;
  --border-slate: #1F2937;
  
  --pro-navy: #0A0E1A;
  --pro-electric: #3B82F6;
  --pro-buy: #10B981;
  --pro-sell: #F43F5E;
  --pro-watch: #F59E0B;
  --text-primary: #F9FAFB;
  --text-muted: #9CA3AF;
  
  --shadow-soft: 0 4px 20px -2px rgba(10, 14, 26, 0.04), 0 2px 8px -1px rgba(10, 14, 26, 0.02);
  --shadow-glow-buy: 0 0 12px 0 rgba(16, 185, 129, 0.15);
  --shadow-glow-sell: 0 0 12px 0 rgba(244, 63, 94, 0.15);
}

* {
  box-sizing: border-box;
  margin: 0;
  padding: 0;
}

body {
  font-family: var(--font-sans);
  background: var(--bg-dark);
  color: var(--text-primary);
  padding: 16px;
  font-size: 13px;
  min-height: 100vh;
  letter-spacing: -0.01em;
  -webkit-font-smoothing: antialiased;
}

/* Scrollbars */
::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}
::-webkit-scrollbar-track {
  background: transparent;
}
::-webkit-scrollbar-thumb {
  background: #374151;
  border-radius: 9999px;
}
::-webkit-scrollbar-thumb:hover {
  background: #4b5563;
}

.wrap {
  background: var(--bg-card);
  border-radius: 12px;
  overflow: hidden;
  border: 1px solid var(--border-slate);
  box-shadow: 0 20px 40px rgba(0,0,0,.5);
}

/* Two-column layout grid */
.main-grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: 0px;
}
@media(min-width: 1024px) {
  .main-grid {
    grid-template-columns: 1fr 300px;
  }
}

.main-column {
  min-width: 0;
  border-right: 1px solid var(--border-slate);
}

.sidebar-column {
  background: var(--bg-dark);
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 16px;
  min-width: 0;
}

/* Top bar */
.top-bar {
  background: var(--bg-card);
  padding: 12px 20px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--border-slate);
  flex-wrap: wrap;
  gap: 8px;
}
.brand {
  font-size: 15px;
  font-weight: 800;
  display: flex;
  align-items: center;
  gap: 8px;
  letter-spacing: -0.02em;
}
.brand .nse { color: var(--pro-electric); }
.brand .vol { color: var(--pro-buy); }
.brand .ver {
  color: var(--text-muted);
  font-size: 10px;
  font-weight: 500;
}
.live-badge {
  font-size: 11px;
  color: var(--pro-buy);
  background: rgba(16, 185, 129, 0.08);
  border: 1px solid rgba(16, 185, 129, 0.2);
  padding: 4px 12px;
  border-radius: 99px;
  display: flex;
  align-items: center;
  gap: 6px;
  font-weight: 700;
}

/* Banners */
.cold-banner {
  display: none;
  background: #1c1200;
  border-left: 3px solid var(--pro-watch);
  padding: 10px 20px;
  font-size: 12px;
  color: var(--pro-watch);
  align-items: center;
  gap: 8px;
}
.cold-banner.show { display: flex; }
.market-banner {
  display: none;
  background: #270808;
  border-left: 3px solid var(--pro-sell);
  padding: 10px 20px;
  font-size: 12px;
  color: var(--pro-sell);
  align-items: center;
  gap: 8px;
  font-weight: 600;
}

/* Tabs */
.tabs {
  background: var(--bg-dark);
  padding: 0 16px;
  display: flex;
  gap: 4px;
  border-bottom: 1px solid var(--border-slate);
  flex-wrap: wrap;
}
.tab {
  padding: 14px 16px;
  font-size: 12px;
  cursor: pointer;
  color: var(--text-muted);
  border-bottom: 2px solid transparent;
  display: flex;
  align-items: center;
  gap: 6px;
  white-space: nowrap;
  transition: all .2s;
  user-select: none;
  font-weight: 700;
}
.tab:hover {
  color: var(--text-primary);
  background: var(--bg-hover);
}
.tab.active {
  color: var(--pro-electric);
  border-color: var(--pro-electric);
}
.tab .cnt {
  background: #1e3a5f;
  color: #bfdbfe;
  border-radius: 99px;
  padding: 1px 7px;
  font-size: 10px;
  font-weight: 800;
  font-family: var(--font-mono);
}

.dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  display: inline-block;
}
.dot.g { background: var(--pro-buy); }
.dot.r { background: var(--pro-sell); }
.dot.o { background: var(--pro-watch); }
.dot.p { background: #c084fc; }

/* Sub filters */
.sub-filters {
  display: flex;
  gap: 6px;
  padding: 10px 20px;
  background: var(--bg-card);
  border-bottom: 1px solid var(--border-slate);
  overflow-x: auto;
  scrollbar-width: none;
}
.sub-filters::-webkit-scrollbar { display: none; }
.sub-btn {
  background: var(--bg-inner);
  border: 1px solid var(--border-slate);
  color: var(--text-muted);
  border-radius: 6px;
  padding: 5px 12px;
  font-size: 11px;
  font-weight: 700;
  cursor: pointer;
  white-space: nowrap;
  transition: all 0.2s;
}
.sub-btn:hover {
  color: var(--text-primary);
  border-color: #374151;
}
.sub-btn.active {
  background: var(--pro-electric);
  color: white;
  border-color: var(--pro-electric);
}

/* Controls */
.controls {
  background: var(--bg-card);
  padding: 12px 20px;
  display: flex;
  gap: 10px;
  align-items: center;
  flex-wrap: wrap;
  border-bottom: 1px solid var(--border-slate);
}
.controls label {
  font-size: 11px;
  color: var(--text-muted);
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.controls select, .controls input[type=text] {
  background: var(--bg-inner);
  border: 1px solid var(--border-slate);
  color: var(--text-primary);
  padding: 6px 12px;
  border-radius: 6px;
  font-size: 12px;
  outline: none;
  font-weight: 600;
  transition: all .2s;
}
.controls select:focus, .controls input[type=text]:focus {
  border-color: var(--pro-electric);
}
.ema-filters {
  display: flex;
  gap: 6px;
  align-items: center;
}
.ema-chip {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 11px;
  background: var(--bg-inner);
  border: 1px solid var(--border-slate);
  border-radius: 99px;
  padding: 4px 12px;
  color: var(--text-muted);
  cursor: pointer;
  user-select: none;
  font-weight: 700;
  transition: all .2s;
}
.ema-chip.on {
  background: var(--bg-hover);
  border-color: var(--pro-electric);
  color: var(--pro-electric);
}
.ema-chip input { display: none; }

/* Buttons */
.btn {
  background: var(--pro-electric);
  color: #fff;
  border: none;
  padding: 7px 16px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 12px;
  font-weight: 700;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  transition: all .2s;
}
.btn:hover {
  background: #1d4ed8;
}
.btn.scanning {
  background: #374151;
  cursor: not-allowed;
  opacity: .8;
}
.btn-stop {
  background: #7f1d1d;
  color: #fca5a5;
  border: none;
  padding: 7px 16px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 12px;
  font-weight: 700;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  transition: all .2s;
}
.btn-stop:disabled {
  opacity: 0.4;
  cursor: not-allowed;
  background: var(--bg-hover);
  color: var(--text-muted);
  border: 1px solid var(--border-slate);
}
.btn-stop:hover:not(:disabled) {
  background: #991b1b;
}

/* Progress */
.prog-wrap {
  display: none;
  padding: 14px 20px;
  background: var(--bg-card);
  border-bottom: 1px solid var(--border-slate);
  align-items: center;
  gap: 14px;
  flex-wrap: wrap;
}
.prog-wrap.show { display: flex; }
.prog-bar-bg {
  flex: 1;
  min-width: 200px;
  height: 6px;
  background: var(--bg-inner);
  border-radius: 99px;
  overflow: hidden;
}
.prog-bar-fill {
  width: 0%;
  height: 100%;
  background: linear-gradient(90deg, #2563eb, #60a5fa);
  border-radius: 99px;
  transition: width .3s;
}
@keyframes pulse-shimmer {
  0% { opacity: 0.4; }
  50% { opacity: 1.0; }
  100% { opacity: 0.4; }
}
.prog-bar-fill.waking {
  width: 100% !important;
  background: linear-gradient(270deg, #2563eb, #3b82f6, #f59e0b, #2563eb);
  background-size: 800% 800%;
  animation: pulse-shimmer 2s ease infinite;
}
.prog-txt {
  font-size: 12px;
  color: var(--text-muted);
  font-weight: 700;
}

/* Summary Bar */
.sum-bar {
  display: flex;
  gap: 20px;
  padding: 12px 20px;
  background: var(--bg-dark);
  border-bottom: 1px solid var(--border-slate);
  flex-wrap: wrap;
}
.si {
  font-size: 12px;
  display: flex;
  align-items: center;
  gap: 5px;
}
.si .lbl { color: var(--text-muted); font-weight: 500; }
.si .val { color: var(--text-primary); font-weight: 700; font-family: var(--font-mono); }
.si .g { color: var(--pro-buy); }
.si .r { color: var(--pro-sell); }
.si .y { color: var(--pro-watch); }

/* Table styling */
.tbl-wrap {
  overflow-x: auto;
  width: 100%;
}
table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
  text-align: left;
}
thead tr { background: var(--bg-inner); }
th {
  padding: 10px 14px;
  color: var(--text-muted);
  font-weight: 700;
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: .06em;
  white-space: nowrap;
  border-bottom: 1px solid var(--border-slate);
  cursor: pointer;
  user-select: none;
  transition: all 0.2s;
}
th:hover {
  color: var(--text-primary);
  background: var(--bg-hover);
}
.sort-icon {
  margin-left: 3px;
  font-size: 10px;
  color: #4b5563;
}
tbody tr {
  border-bottom: 1px solid var(--border-slate);
  transition: background .15s;
}
tbody tr:hover {
  background: rgba(37, 99, 235, 0.03);
}
td {
  padding: 10px 14px;
  color: var(--text-primary);
  white-space: nowrap;
  vertical-align: middle;
}
.sym {
  font-weight: 800;
  font-size: 13px;
  color: #f9fafb;
  letter-spacing: -0.01em;
}
.sig-buy {
  background: rgba(16, 185, 129, 0.1);
  color: var(--pro-buy);
  border: 1px solid rgba(16, 185, 129, 0.2);
  border-radius: 4px;
  padding: 2px 6px;
  font-size: 9px;
  font-weight: 800;
  display: inline-block;
  margin-top: 2px;
}
.sig-sell {
  background: rgba(244, 63, 94, 0.1);
  color: var(--pro-sell);
  border: 1px solid rgba(244, 63, 94, 0.2);
  border-radius: 4px;
  padding: 2px 6px;
  font-size: 9px;
  font-weight: 800;
  display: inline-block;
  margin-top: 2px;
}
.sig-watch {
  background: rgba(245, 158, 11, 0.1);
  color: var(--pro-watch);
  border: 1px solid rgba(245, 158, 11, 0.2);
  border-radius: 4px;
  padding: 2px 6px;
  font-size: 9px;
  font-weight: 800;
  display: inline-block;
  margin-top: 2px;
}

.score {
  color: #60a5fa;
  font-weight: 700;
  font-size: 13px;
  font-family: var(--font-mono);
  position: relative;
  cursor: pointer;
}
.score .den {
  color: #4b5563;
  font-weight: 400;
  font-size: 10px;
}
/* Score breakdown tooltip */
.score-tip {
  display: none;
  position: absolute;
  bottom: calc(100% + 6px);
  left: 50%;
  transform: translateX(-50%);
  background: var(--bg-card);
  border: 1px solid var(--border-slate);
  border-radius: 8px;
  padding: 10px 12px;
  width: 170px;
  z-index: 9999;
  font-family: var(--font-mono);
  font-size: 10px;
  color: var(--text-primary);
  box-shadow: 0 8px 24px rgba(0,0,0,0.6);
  pointer-events: none;
}
.score-tip .tip-title {
  font-size: 9px;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--pro-electric);
  margin-bottom: 7px;
  border-bottom: 1px solid var(--border-slate);
  padding-bottom: 5px;
}
.score-tip .tip-row {
  display: flex;
  justify-content: space-between;
  margin-bottom: 4px;
  gap: 6px;
}
.score-tip .tip-row .tip-bar {
  flex:1;
  height: 4px;
  background: var(--bg-inner);
  border-radius: 2px;
  margin-top: 5px;
  overflow: hidden;
}
.score-tip .tip-row .tip-bar-fill {
  height: 100%;
  border-radius: 2px;
  transition: width 0.3s ease;
}
.score:hover .score-tip { display: block; }
/* R:R Visualizer bar */
.rr-strip { display:flex; align-items:center; gap:0; height:8px; border-radius:4px; overflow:hidden; min-width:70px; }
.rr-sl   { background: rgba(244,63,94,0.7); height:100%; }
.rr-mid  { background: #374151; width:3px; height:100%; }
.rr-t1   { background: rgba(16,185,129,0.6); height:100%; }
.rr-t2   { background: rgba(16,185,129,0.9); height:100%; }
.rr-wrap { display:flex;flex-direction:column;gap:2px;align-items:center;min-width:80px; }
.rr-labels { display:flex;justify-content:space-between;width:100%;font-size:8px;color:var(--text-muted);font-family:var(--font-mono); }
/* Regime warning in modal */
#jm-regime-warning {
  display: none;
  background: rgba(245,158,11,0.1);
  border: 1px solid rgba(245,158,11,0.4);
  border-radius: 7px;
  padding: 10px 12px;
  margin-top: 10px;
  font-size: 11px;
  color: #fbbf24;
  font-weight: 600;
  animation: pulse-warn 2s ease-in-out infinite;
}
@keyframes pulse-warn {
  0%,100% { border-color: rgba(245,158,11,0.4); }
  50% { border-color: rgba(245,158,11,0.9); box-shadow: 0 0 8px rgba(245,158,11,0.3); }
}
/* Regime mode banner */
#regime-mode-banner {
  display:none;
  padding: 5px 20px;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.3px;
  align-items: center;
  gap: 8px;
  animation: slideDown 0.3s ease;
}
@keyframes slideDown { from{opacity:0;transform:translateY(-6px)} to{opacity:1;transform:translateY(0)} }
/* AI commentary pulsing dot */
#ai-tip-status {
  display:inline-block;
  width:6px;height:6px;border-radius:50%;
  background:var(--pro-buy);
  animation:pulse 2s ease-in-out infinite;
  margin-right:4px;
  vertical-align:middle;
}
.price-main {
  color: var(--text-primary);
  font-weight: 700;
  font-size: 13px;
  font-family: var(--font-mono);
}
.price-date {
  color: var(--text-muted);
  font-size: 9px;
  margin-top: 2px;
  font-weight: 500;
}

.up { color: var(--pro-buy); font-weight: 700; font-family: var(--font-mono); }
.dn { color: var(--pro-sell); font-weight: 700; font-family: var(--font-mono); }
.neutral { color: var(--text-muted); }
.sl { color: var(--pro-sell); font-weight: 700; font-family: var(--font-mono); }
.c-bull { border-radius: 4px; padding: 2px 8px; font-size: 10px; font-weight: 700; }
.c-bear { border-radius: 4px; padding: 2px 8px; font-size: 10px; font-weight: 700; }

.ema-dots {
  display: flex;
  gap: 4px;
}
.edot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #374151;
}
.edot.pass {
  background: var(--pro-buy);
}

.cam-levels {
  font-size: 11px;
  line-height: 1.6;
}
.cam-h3 { color: var(--pro-sell); font-weight: 600; font-family: var(--font-mono); }
.cam-l3 { color: var(--pro-buy); font-weight: 600; font-family: var(--font-mono); }

.vol-bar {
  height: 4px;
  background: var(--border-slate);
  border-radius: 2px;
  width: 60px;
  overflow: hidden;
  display: inline-block;
  vertical-align: middle;
}
.vol-fill { height: 100%; border-radius: 2px; }

/* Sticky first column (Symbol) */
table td:first-child, table th:first-child {
  position: sticky;
  left: 0;
  background: var(--bg-card);
  z-index: 2;
  min-width: 145px;
  border-right: 1px solid var(--border-slate);
}
thead th:first-child {
  background: var(--bg-inner);
  z-index: 3;
}
tbody tr:hover td:first-child {
  background: var(--bg-hover) !important;
}

.star-icon {
  font-size: 15px;
  cursor: pointer;
  transition: all 0.2s ease;
  color: var(--text-muted);
}
.star-icon:hover {
  color: var(--pro-watch) !important;
  transform: scale(1.15);
}

/* Action bar */
.action-bar {
  background: var(--bg-card);
  padding: 12px 20px;
  display: flex;
  gap: 10px;
  align-items: center;
  flex-wrap: wrap;
  border-top: 1px solid var(--border-slate);
}
.tg-btn {
  background: #0369a1;
  color: #e0f2fe;
  border: none;
  border-radius: 6px;
  padding: 6px 14px;
  font-size: 11px;
  font-weight: 700;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  transition: all 0.2s;
}
.tg-btn:hover { background: #0284c7; }
.wa-btn {
  background: #14532d;
  color: #86efac;
  border: none;
  border-radius: 6px;
  padding: 6px 14px;
  font-size: 11px;
  font-weight: 700;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  transition: all 0.2s;
}
.wa-btn:hover { background: #166534; }
.csv-btn {
  background: var(--bg-inner);
  border: 1px solid var(--border-slate);
  color: var(--text-primary);
  border-radius: 6px;
  padding: 6px 14px;
  font-size: 11px;
  font-weight: 700;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  transition: all 0.2s;
}
.csv-btn:hover { background: var(--bg-hover); }

/* Modal */
.modal-bg {
  display: none;
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0,0,0,.85);
  z-index: 100;
  align-items: center;
  justify-content: center;
  padding: 20px;
  backdrop-filter: blur(4px);
}
.modal-bg.show { display: flex; }
.modal {
  background: var(--bg-card);
  border-radius: 12px;
  padding: 24px;
  width: 420px;
  max-width: 100%;
  border: 1px solid var(--border-slate);
  box-shadow: 0 25px 50px -12px rgba(0,0,0,.5);
  animation: mIn .2s ease-out;
}
@keyframes mIn { from { transform: scale(.95); opacity: 0; } to { transform: scale(1); opacity: 1; } }
.modal h3 { color: #f9fafb; font-size: 15px; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; font-weight: 800; }
.modal label { color: var(--text-muted); font-size: 11px; display: block; margin-bottom: 4px; margin-top: 12px; font-weight: 700; text-transform: uppercase; }
.modal input { width: 100%; background: var(--bg-inner); border: 1px solid var(--border-slate); color: var(--text-primary); padding: 8px 12px; border-radius: 6px; font-size: 13px; outline: none; transition: border-color .2s; }
.modal input:focus { border-color: var(--pro-electric); }
.modal .mrow { display: flex; gap: 10px; margin-top: 20px; }
.modal .btn-save { flex: 1; background: var(--pro-electric); color: #fff; border: none; border-radius: 6px; padding: 10px; font-size: 13px; font-weight: 700; cursor: pointer; transition: background .2s; }
.modal .btn-save:hover { background: #1d4ed8; }
.modal .btn-cancel { background: var(--bg-inner); border: 1px solid var(--border-slate); color: var(--text-muted); border-radius: 6px; padding: 10px 16px; font-size: 13px; cursor: pointer; transition: all .2s; }
.modal .btn-cancel:hover { color: var(--text-primary); }

.help-modal { width: 720px; max-width: 95%; max-height: 85vh; overflow-y: auto; }
.help-section { border-bottom: 1px solid var(--border-slate); padding-bottom: 18px; margin-bottom: 18px; }
.help-section h4 { color: #f3f4f6; font-size: 13px; margin-bottom: 10px; display: flex; align-items: center; gap: 8px; font-weight: 800; }
.help-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-top: 10px; }
.help-card { background: var(--bg-inner); padding: 14px; border-radius: 8px; border: 1px solid var(--border-slate); }
.help-card.bull { border-left: 3px solid var(--pro-buy); }
.help-card.mixed { border-left: 3px solid var(--pro-watch); }
.help-card.entry { border-left: 3px solid var(--pro-electric); }

.calc-container { background: var(--bg-inner); padding: 16px; border-radius: 8px; border: 1px solid var(--border-slate); margin-top: 10px; }
.calc-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; }
.calc-container label { margin-top: 0 !important; margin-bottom: 6px !important; }
.calc-result {
  background: rgba(16, 185, 129, 0.05);
  border: 1px solid rgba(16, 185, 129, 0.15);
  color: var(--pro-buy);
  padding: 14px;
  border-radius: 6px;
  margin-top: 14px;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
  gap: 14px;
}
.calc-res-item { display: flex; flex-direction: column; }
.calc-res-lbl { font-size: 9px; color: #a7f3d0; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; font-weight: 700; }
.calc-res-val { font-size: 16px; font-weight: 800; font-family: var(--font-mono); }

/* Sidebar widget styling */
.widget-card {
  background: var(--bg-card);
  border: 1px solid var(--border-slate);
  border-radius: 8px;
  padding: 12px 14px;
}
.widget-header {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 12px;
}
.widget-header span {
  font-size: 10px;
  font-weight: 700;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

/* Heatmap Grid */
.heatmap-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 6px;
}
.heatmap-btn {
  border-radius: 6px;
  padding: 6px 10px;
  font-size: 11px;
  text-align: left;
  border: 1px solid transparent;
  cursor: pointer;
  transition: all 0.2s;
  height: 48px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}
.heatmap-btn:hover { opacity: 0.85; }
.heatmap-btn.active {
  border-color: var(--pro-electric) !important;
  box-shadow: 0 0 10px rgba(37, 99, 235, 0.25);
  font-weight: 700;
  transform: scale(1.02);
}

.empty-state { text-align: center; padding: 60px 20px; color: var(--text-muted); }
.toast { position: fixed; bottom: 24px; right: 24px; background: #1e293b; border-left: 4px solid var(--pro-buy); color: #f1f5f9; padding: 14px 24px; border-radius: 6px; box-shadow: 0 20px 25px -5px rgba(0,0,0,.5); z-index: 1000; opacity: 0; transform: translateY(20px); transition: all .3s; display: flex; align-items: center; gap: 8px; pointer-events: none; font-weight: 600; }
.toast.show { opacity: 1; transform: translateY(0); }
.toast.err { border-color: var(--pro-sell); }

/* Flashing update animations */
@keyframes flash-green {
  0% { background-color: rgba(16, 185, 129, 0.15); }
  100% { background-color: transparent; }
}
@keyframes flash-red {
  0% { background-color: rgba(244, 63, 94, 0.15); }
  100% { background-color: transparent; }
}
.flash-green-bg { animation: flash-green 1s ease forwards; }
.flash-red-bg { animation: flash-red 1s ease forwards; }

/* Expanded Stock details style */
.expanded-panel {
  background: rgba(37, 99, 235, 0.02);
  border: 1px solid var(--border-slate);
  border-radius: 8px;
  padding: 14px;
  display: grid;
  grid-template-columns: 1fr;
  gap: 16px;
}
@media (min-width: 768px) {
  .expanded-panel {
    grid-template-columns: 1.2fr 1fr;
  }
}

.sc-item { display: flex; flex-direction: column; align-items: center; gap: 1px; }
.sc-lbl { color: var(--text-muted); font-size: 9px; font-weight: 700; letter-spacing: .06em; text-transform: uppercase; }
.sc-val { color: var(--text-primary); font-weight: 700; font-size: 13px; font-family: var(--font-mono); }

.pro-tip-box {
  background: linear-gradient(135deg, #1e3a8a, #312e81);
  border: 1px solid rgba(59, 130, 246, 0.3);
  border-radius: 8px;
  padding: 12px;
  color: white;
}

/* Top 5 picks grid and card styling */
#top5Grid {
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 12px;
}
.top-pick-card {
  background: var(--bg-inner);
  border: 1px solid var(--border-slate);
  border-radius: 12px;
  padding: 12px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  height: 100px;
  cursor: pointer;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
  transition: border-color 0.2s ease, transform 0.1s ease;
}
.top-pick-card:hover {
  border-color: var(--pro-electric) !important;
}

  #mobileCardContainer {
    display: none;
  }
  @media(max-width:768px){
    body:not(.desktop-force) .tbl-wrap {
      display: none !important;
    }
    body:not(.desktop-force) #mobileCardContainer {
      display: flex !important;
      flex-direction: column;
      gap: 12px;
      padding: 10px 8px;
    }
    body:not(.desktop-force)[data-active-tab="journal"] .tbl-wrap {
      display: block !important;
    }
    body:not(.desktop-force)[data-active-tab="journal"] #mobileCardContainer {
      display: none !important;
    }
    body:not(.desktop-force)[data-active-tab="home"] .tbl-wrap {
      display: none !important;
    }
    body:not(.desktop-force)[data-active-tab="home"] #mobileCardContainer {
      display: none !important;
    }
    
    .mobile-stock-card {
      background: var(--bg-card);
      border: 1px solid var(--border-slate);
      border-radius: 12px;
      padding: 14px;
      display: flex;
      flex-direction: column;
      gap: 10px;
      transition: border-color 0.2s ease, box-shadow 0.2s ease, transform 0.2s ease;
      box-shadow: 0 4px 6px rgba(0,0,0,0.15);
      position: relative;
      overflow: hidden;
      text-align: left;
    }
    .mobile-stock-card.expanded {
      border-color: var(--pro-electric);
      box-shadow: 0 0 10px rgba(59, 130, 246, 0.2), 0 8px 16px rgba(0,0,0,0.3);
    }
    .card-row-between {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .card-row-start {
      display: flex;
      gap: 8px;
      align-items: center;
    }
    .card-btn {
      flex: 1;
      padding: 8px;
      font-size: 11px;
      font-weight: 700;
      border-radius: 6px;
      border: none;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 4px;
      transition: all 0.2s;
    }

    #top5Grid {
      grid-template-columns: repeat(2, 1fr);
    }
    body { padding: 8px; }
    .top-bar { padding: 10px 14px; }
    .brand { font-size: 13px; }
    .tabs { flex-wrap: nowrap; overflow-x: auto; padding: 0 8px; scrollbar-width: none; }
    .tabs::-webkit-scrollbar { display: none; }
    .tab { padding: 10px 12px; font-size: 11px; }
    .controls { padding: 10px 14px; gap: 8px; }
    #scanBtn, #tickerSearch { width: 100% !important; margin-top: 4px; margin-left: 0 !important; }
    .action-bar { flex-direction: column; align-items: stretch; }
    .action-bar button { width: 100%; justify-content: center; }
    .modal { width: 95% !important; }
    .m-hide { display: none !important; }

    /* Sticky filters */
    .scanner-filters, .controls {
        position: sticky;
        top: 60px;
        z-index: 30;
        background: var(--bg-primary);
        padding: 8px;
    }
    
    /* Horizontal scroll table */
    .scanner-table-wrapper, .tbl-wrap {
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
    }
    
    .scanner-table, #mainTradingTable {
        min-width: 800px;  /* force horizontal scroll */
    }
    
    /* Improved row spacing */
    .scanner-table td, #mainTradingTable td {
        padding: 10px 8px;  /* was 6px */
        white-space: nowrap;
    }
    
    /* Faster rendering */
    .scanner-table tbody tr, #mainTradingTable tbody tr {
        contain: layout style;  /* CSS containment */
        will-change: auto;
    }
    
    /* Touch-friendly buttons */
    .action-btn {
        min-height: 40px;
        min-width: 40px;
    }
  }
</style>
</head>
<body>
<div class="wrap">

  <!-- Top Header bar -->
  <div class="top-bar">
    <div class="brand">
      <i class="ti ti-activity" style="color:var(--pro-electric);font-size:18px"></i>
      <span class="nse">Pro</span>&nbsp;<span class="vol">Trader</span>&nbsp;Terminal
      <span class="ver">v4.0 [Camarilla Sync]</span>
    </div>
    <div style="display:flex;align-items:center;gap:12px;margin-left:auto">
      <!-- Notification Bell Alert Center -->
      <div style="position:relative; display:inline-block;">
        <button class="btn" id="alertBellBtn" onclick="toggleAlertDropdown(event)" style="background:rgba(245, 158, 11, 0.12); border:1px solid rgba(245, 158, 11, 0.25); color:var(--pro-watch); position:relative; padding: 7px 12px; height: 32px;">
          <i class="ti ti-bell"></i>
          <span id="alertCountBadge" style="position:absolute; top:-5px; right:-5px; background:var(--pro-sell); color:#fff; border-radius:50%; width:16px; height:16px; font-size:9px; display:flex; align-items:center; justify-content:center; font-weight:900; border:1px solid var(--bg-card); display:none;">0</span>
        </button>
        <!-- Dropdown Card -->
        <div id="alertDropdown" style="display:none; position:absolute; right:0; top:calc(100% + 8px); background:var(--bg-card); border:1px solid var(--border-slate); border-radius:12px; width:340px; box-shadow:0 12px 32px rgba(0,0,0,0.6); z-index:1001; max-height:400px; overflow-y:auto; padding:12px; text-align: left;">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; border-bottom:1px solid var(--border-slate); padding-bottom:6px;">
            <span style="font-weight:800; font-size:11px; color:var(--text-primary);">🔔 Notification Center</span>
            <span onclick="clearAlerts(event)" style="font-size:10px; color:var(--text-muted); cursor:pointer; font-weight:700;">Clear All</span>
          </div>
          <div id="alertList" style="display:flex; flex-direction:column; gap:8px;">
            <div style="text-align:center; padding:20px; color:var(--text-muted); font-size:11px;">
              <i class="ti ti-bell-off" style="font-size:24px; display:block; margin-bottom:6px;"></i> No new notifications
            </div>
          </div>
        </div>
      </div>

      <button class="btn" style="background:rgba(59, 130, 246, 0.12);border:1px solid rgba(59, 130, 246, 0.25);color:#60a5fa" onclick="openHelpModal()">
        <i class="ti ti-book-open"></i> <span>Guide & Calculator</span>
      </button>
      <div class="live-badge" id="liveBadge">
        <span class="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse">●</span>
        <span id="liveBadgeText">Active Ticker</span>
        <span id="liveStatsSpan" style="display:none">
          Live F&O — <span id="bull-count">0</span> Bullish &nbsp;|&nbsp; <span id="bear-count">0</span> Bearish
        </span>
      </div>
    </div>
  </div>

  <!-- Nifty regime scorecard statistics -->
  <div id="regimeBar" style="
    display:flex;align-items:center;gap:16px;flex-wrap:wrap;
    padding:10px 20px;background:var(--bg-inner);
    border-bottom:1px solid var(--border-slate);font-size:12px;">

    <div style="display:flex;align-items:center;gap:8px">
      <span id="regimeEmoji" style="font-size:18px">⚖️</span>
      <div>
        <div style="color:var(--text-muted);font-size:9px;font-weight:700;letter-spacing:.06em;text-transform:uppercase">NIFTY MARKET REGIME</div>
        <div id="regimeLabel" style="font-weight:800;font-size:13px;color:var(--pro-watch)">Loading…</div>
      </div>
    </div>

    <div style="width:1px;height:24px;background:var(--border-slate)"></div>

    <div>
      <div style="color:var(--text-muted);font-size:10px">NIFTY 50</div>
      <div id="regimeNifty" style="font-weight:700;color:var(--text-primary);font-family:var(--font-mono)">—</div>
    </div>

    <div>
      <div style="color:var(--text-muted);font-size:10px">Breadth (50d EMA)</div>
      <div id="regimeBreadth" style="font-weight:700;font-family:var(--font-mono)">—</div>
    </div>

    <div>
      <div style="color:var(--text-muted);font-size:10px">Index EMA Cross (20/50/200)</div>
      <div id="regimeEMAs" style="font-weight:600;color:var(--text-muted);font-size:11px;font-family:var(--font-mono)">—</div>
    </div>

    <div>
      <div style="color:var(--text-muted);font-size:10px">ATR % (Volatility)</div>
      <div id="regimeATR" style="font-weight:700;color:var(--text-muted);font-family:var(--font-mono)">—</div>
    </div>

    <div>
      <div style="color:var(--text-muted);font-size:10px">Pullback from 52W High</div>
      <div id="regimeHigh" style="font-weight:700;color:#f87171;font-family:var(--font-mono)">—</div>
    </div>

    <div style="margin-left:auto;max-width:280px">
      <div id="regimeDesc" style="font-size:11px;color:var(--text-muted);font-style:italic;text-align:right">—</div>
    </div>

    <button onclick="refreshRegime()" style="background:var(--bg-inner);border:1px solid var(--border-slate);color:var(--text-muted);border-radius:6px;padding:4px 10px;cursor:pointer;font-size:11px;font-weight:700">
      ↻ Refresh
    </button>
  </div>

  <!-- Trades performance scorecard -->
  <div id="scorecardBar" style="display:flex;align-items:center;
    gap:20px;flex-wrap:wrap;padding:8px 20px;
    background:var(--bg-dark);border-bottom:1px solid var(--border-slate);font-size:11px">
    <span style="color:var(--text-muted);font-weight:700;font-size:9px;letter-spacing:.06em;text-transform:uppercase">JOURNAL LEDGER METRICS</span>

    <div class="sc-item"><span class="sc-lbl">Ledger Trades</span><span class="sc-val" id="sc-total">—</span></div>
    <div class="sc-item"><span class="sc-lbl">Win Ratio</span><span class="sc-val" id="sc-winrate" style="color:var(--pro-buy)">—</span></div>
    <div class="sc-item"><span class="sc-lbl">Average R:R</span><span class="sc-val" id="sc-rr">—</span></div>
    <div class="sc-item"><span class="sc-lbl">Profit Factor</span><span class="sc-val" id="sc-pf">—</span></div>
    <div class="sc-item"><span class="sc-lbl">Net P&L (INR)</span><span class="sc-val" id="sc-pnl">—</span></div>
    <div class="sc-item"><span class="sc-lbl">Active Holdings</span><span class="sc-val" id="sc-open" style="color:var(--pro-watch)">—</span></div>
  </div>

  <!-- Strong Bear alert warning banner -->
  <div id="bearWarning" style="background:#1a0000;border-left:3px solid #f87171;
    padding:8px 20px;font-size:12px;color:#fca5a5;display:none;
    align-items:center;gap:8px;border-bottom:1px solid var(--border-slate)">
    ⚠️ STRONG BEAR market active — Bullish signals shown with reduced confidence. Consider bearish setups or staying in cash.
    <span style="margin-left:auto;cursor:pointer" onclick="this.parentElement.style.display='none'">✕</span>
  </div>

  <!-- Regime mode auto switch banner banner -->
  <div id="regime-mode-banner"></div>

  <!-- Market Closed Alert banner -->
  <div class="market-banner" id="marketBanner">
    <i class="ti ti-alert-triangle"></i>
    <span>NSE Market is closed — displaying scan results from the last active trading session. Ticker paused.</span>
  </div>

  <!-- Cold start boot helper alert -->
  <div class="cold-banner" id="coldBanner">
    <i class="ti ti-clock-pause"></i>
    <span>Server is waking up from sleep (Render free tier) — first scan may take 30-60 s longer than usual.</span>
    <button onclick="document.getElementById('coldBanner').classList.remove('show')" style="margin-left:auto;background:none;border:none;color:var(--pro-watch);cursor:pointer;font-size:16px">✕</button>
  </div>

  <!-- Primary Tab Bar Navigation -->
  <div class="tabs">
    <div class="tab active" id="tab-home" onclick="setTab('home')">
      <i class="ti ti-smart-home" style="color:var(--pro-electric)"></i> Home
    </div>
    <div class="tab" id="tab-protrader" onclick="setTab('protrader')">
      <i class="ti ti-flame" style="color:var(--pro-sell)"></i> ProTrader Momentum
    </div>
    <div class="tab" id="tab-camarilla" onclick="setTab('camarilla')">
      <i class="ti ti-chart-grid" style="color:var(--pro-electric)"></i> Camarilla Scanner <span class="cnt" id="cnt-camarilla">0</span>
    </div>
    <div class="tab" id="tab-watchlist" onclick="setTab('watchlist')">
      <i class="ti ti-star" style="color:var(--pro-watch)"></i> Watchlist <span class="cnt" id="cnt-watchlist">0</span>
    </div>
    <div class="tab" id="tab-journal" onclick="setTab('journal')">
      <i class="ti ti-notebook" style="color:#c084fc"></i> Trade Ledger <span class="cnt" id="cnt-journal">0</span>
    </div>
  </div>

  <!-- Camarilla Sub-group selectors (hidden when ProTrader or Ledger tabs are selected) -->
  <div class="sub-filters" id="camarillaSubFilters" style="display:none">
    <button class="sub-btn active" id="sub-all" onclick="setSubTab('all')">All Signals</button>
    <button class="sub-btn" id="sub-entry" onclick="setSubTab('entry')">Entry Ready <span class="text-[10px] opacity-75 font-mono ml-0.5" id="sub-cnt-entry">0</span></button>
    <button class="sub-btn" id="sub-fresh" onclick="setSubTab('fresh')">Show Fresh Only <span class="text-[10px] opacity-75 font-mono ml-0.5" id="sub-cnt-fresh">0</span></button>
    <button class="sub-btn" id="sub-bullish" onclick="setSubTab('bullish')">Bullish Strong <span class="text-[10px] opacity-75 font-mono ml-0.5" id="sub-cnt-bullish">0</span></button>
    <button class="sub-btn" id="sub-bearish" onclick="setSubTab('bearish')">Bearish Weak <span class="text-[10px] opacity-75 font-mono ml-0.5" id="sub-cnt-bearish">0</span></button>
    <button class="sub-btn" id="sub-mixed" onclick="setSubTab('mixed')">Mixed Caution <span class="text-[10px] opacity-75 font-mono ml-0.5" id="sub-cnt-mixed">0</span></button>
    <button class="sub-btn" id="sub-hv" onclick="setSubTab('hv')">High Volume Today <span class="text-[10px] opacity-75 font-mono ml-0.5" id="sub-cnt-hv">0</span></button>
  </div>

  <!-- Primary control configuration console -->
  <div class="controls">
    <label>Vol Average</label>
    <select id="volDays" onchange="savePrefs();render()">
      <option value="5">5 Day Avg</option>
      <option value="10" selected>10 Day Avg</option>
      <option value="20">20 Day Avg</option>
      <option value="30">30 Day Avg</option>
    </select>
    <label style="margin-left:8px">Volume Ratio</label>
    <select id="volMult" onchange="savePrefs();render()">
      <option value="0.1">0.1x (Early Morning)</option>
      <option value="0.2">0.2x (Morning Scan)</option>
      <option value="0.5">0.5x (Early Session)</option>
      <option value="1.0">1.0x (Standard)</option>
      <option value="1.5" selected>1.5x</option>
      <option value="2">2x</option>
      <option value="2.5">2.5x</option>
      <option value="3">3x</option>
    </select>
    <label style="margin-left:8px">Min Turnover</label>
    <select id="turnoverLimit" onchange="savePrefs()">
      <option value="50000000">₹5 Cr Turnover</option>
      <option value="100000000" selected>₹10 Cr Turnover</option>
      <option value="200000000">₹20 Cr Turnover</option>
    </select>
    <label style="margin-left:8px">Scan Mode</label>
    <select id="scanMode" onchange="savePrefs()">
      <option value="bullish">Bullish Scan</option>
      <option value="bearish">Bearish Scan</option>
      <option value="both" selected>Both setups</option>
    </select>
    <label style="margin-left:8px">Price Cap</label>
    <select id="minPrice" onchange="savePrefs()">
      <option value="0">Any Price</option>
      <option value="50" selected>₹50+</option>
      <option value="100">₹100+</option>
      <option value="200">₹200+</option>
    </select>
    <label style="margin-left:8px">EMA Filters</label>
    <div class="ema-filters">
      <label class="ema-chip on" id="c10"><input type="checkbox" checked onchange="toggleEMA(this,'10')">10</label>
      <label class="ema-chip on" id="c20"><input type="checkbox" checked onchange="toggleEMA(this,'20')">20</label>
      <label class="ema-chip" id="c50"><input type="checkbox" onchange="toggleEMA(this,'50')">50</label>
      <label class="ema-chip" id="c200"><input type="checkbox" onchange="toggleEMA(this,'200')">200</label>
    </div>
    
    <button class="btn" id="scanBtn" onclick="startScan()">
      <i class="ti ti-scan"></i> Run Live Scan
    </button>
    <button class="btn-stop" id="stopBtn" onclick="stopScan()" disabled>
      <i class="ti ti-player-stop"></i> Stop
    </button>
    
    <label style="margin-left:8px"><i class="ti ti-refresh"></i> Auto-run</label>
    <select id="autoScanSel" onchange="toggleAutoScan()">
      <option value="off" selected>Off</option>
      <option value="180000">3 min</option>
      <option value="300000">5 min</option>
    </select>
    
    <label style="margin-left:8px"><i class="ti ti-database-off"></i> Cache</label>
    <select id="scanDataSource" onchange="savePrefs()">
      <option value="live" selected>Live yfinance</option>
      <option value="offline">Offline Cache</option>
    </select>
    
    <label style="margin-left:8px"><i class="ti ti-palette"></i> Theme</label>
    <select id="themeSelect" onchange="changeTheme()">
      <option value="premium" selected>Premium Navy</option>
      <option value="classic">Classic Dark</option>
      <option value="dark">Dark Theme</option>
      <option value="normal">Normal (Light)</option>
    </select>
    
    <label style="margin-left:8px"><i class="ti ti-eye"></i> Demo Data</label>
    <select id="demoDataSelect" onchange="savePrefs();render()">
      <option value="show">Include</option>
      <option value="hide" selected>Exclude</option>
    </select>
    
    <span id="countdownSpan" style="margin-left:5px;color:var(--pro-watch);font-weight:700;font-family:var(--font-mono)"></span>
    
    <input type="text" id="tickerSearch" placeholder="🔍 Search / Scan stock..."
      oninput="render()" onkeydown="if(event.key==='Enter') triggerSingleScan()" style="width:160px;margin-left:auto">
  </div>

  <!-- Layout split grid (Main Content on left, market statistics panel on right) -->
  <div class="main-grid">
    
    <!-- MAIN COLUMN LEFT -->
    <div class="main-column">
      
      <!-- Home Tab Dashboard Content -->
      <div id="homeTabContent" style="padding: 20px; display: block;"></div>
      
      <!-- Top picks panel (shown in ProTrader tab) -->
      <div id="proTraderTopPicks" style="padding: 16px 20px 0px 20px">
        <div style="display:flex;align-items:center;gap:6px;margin-bottom:10px">
          <i class="ti ti-sparkles" style="color:var(--pro-electric);animation:spin 3s linear infinite"></i>
          <span style="font-size:11px;font-weight:800;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.8px">Top 5 Intraday Picks (Volume Dominant)</span>
        </div>
        <div class="grid grid-cols-2 md:grid-cols-5 gap-3" id="top5Grid">
          <!-- Populated by JavaScript -->
        </div>
      </div>

      <!-- Progress Bar (shown when active scan is running) -->
      <div class="prog-wrap" id="progWrap" style="margin: 0 20px 10px 20px; border-radius: 6px; display: none;">
        <div class="prog-bar-bg">
          <div class="prog-bar-fill" id="progFill"></div>
        </div>
        <span class="prog-txt" id="progPct">0%</span>
        <span class="prog-txt" id="progStatus">Initialising scan...</span>
        <span class="prog-txt" id="progETA" style="font-family:var(--font-mono)"></span>
      </div>

      <!-- Summary Bar -->
      <div class="sum-bar" id="sumBar" style="margin: 0 20px 10px 20px; border-radius: 6px;"></div>

      <!-- Mobile-First Redesign Stock Cards Grid -->
      <div id="mobileCardContainer" style="display: none;"></div>

      <!-- Main screener results data table -->
      <div class="tbl-wrap" style="padding: 16px 20px">
        <table id="mainTradingTable">
          <thead id="tblHead">
            <!-- Dynamic Headers based on active tab -->
          </thead>
          <tbody id="tblBody">
            <!-- Dynamic Signals Rows -->
          </tbody>
        </table>
      </div>
    </div>
    
    <!-- SIDEBAR COLUMN RIGHT -->
    <div class="sidebar-column">
      
      <!-- Sector Rotation heatmap -->
      <div class="widget-card">
        <div class="widget-header">
          <div class="w-2.5 h-2.5 rounded-full bg-orange-500 animate-pulse"></div>
          <span>Sector Rotation Momentum</span>
          <span id="sectorDataSource" style="display:none;margin-left:auto;font-size:9px;font-weight:600;letter-spacing:.5px;color:var(--pro-buy);background:rgba(16,185,129,.12);padding:2px 7px;border-radius:20px;border:1px solid rgba(16,185,129,.25)">⚡ LIVE</span>
        </div>
        <div class="heatmap-grid" id="heatmapGrid">
          <!-- Populated dynamically -->
        </div>
        <div id="sectorFetchTime" style="font-size:9px;color:var(--text-muted);text-align:right;margin-top:4px;padding:0 2px;display:none"></div>
      </div>

      <!-- OI Pulse Widgets Row -->
      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; width: 100%;">
        <!-- Option Chain PCR widget -->
        <div class="widget-card" style="padding: 10px 8px;">
          <div class="widget-header" style="margin-bottom: 8px;">
            <div class="w-2 h-2 rounded-full bg-blue-500 animate-pulse"></div>
            <span style="font-size: 9px; white-space: nowrap;">Nifty OI Pulse</span>
          </div>
          <div style="display:flex;flex-direction:column;gap:6px">
            <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:4px">
              <span style="font-size:9.5px;color:var(--text-muted);white-space:nowrap">PCR</span>
              <span id="oi-pcr" style="font-weight:800;font-family:var(--font-mono);padding:2px 4px;border-radius:4px;font-size:10px" class="up">1.24</span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:4px">
              <span style="font-size:9.5px;color:var(--text-muted);white-space:nowrap">Max Pain</span>
              <span id="oi-maxpain" style="font-weight:700;font-family:var(--font-mono);color:var(--text-primary);font-size:10px">22,400</span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:4px">
              <span style="font-size:9.5px;color:var(--text-muted);white-space:nowrap">Resist (CE)</span>
              <span id="oi-resistance" style="font-weight:700;font-family:var(--font-mono);color:var(--pro-sell);font-size:10px">22,500</span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:4px">
              <span style="font-size:9.5px;color:var(--text-muted);white-space:nowrap">Support (PE)</span>
              <span id="oi-support" style="font-weight:700;font-family:var(--font-mono);color:var(--pro-buy);font-size:10px">22,200</span>
            </div>
            
            <div class="mt-1.5 bg-amber-950/40 border border-amber-900/60 rounded-lg p-1.5 flex items-start gap-1" id="oi-trap-box">
              <i class="ti ti-shield-alert" style="color:var(--pro-watch);font-size:12px;margin-top:2px"></i>
              <div>
                <div style="font-size:9px;font-weight:800;color:#fbbf24" id="oi-trap-title">CE WRITERS TRAPPED</div>
                <div style="font-size:8px;color:#f59e0b;line-height:1.2;margin-top:1px" id="oi-trap-desc">Short covering rally likely at 22,500!</div>
              </div>
            </div>
          </div>
        </div>

        <!-- Option Chain PCR widget (Bank Nifty) -->
        <div class="widget-card" style="padding: 10px 8px;">
          <div class="widget-header" style="margin-bottom: 8px;">
            <div class="w-2 h-2 rounded-full bg-cyan-500 animate-pulse"></div>
            <span style="font-size: 9px; white-space: nowrap;">BN OI Pulse</span>
          </div>
          <div style="display:flex;flex-direction:column;gap:6px">
            <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:4px">
              <span style="font-size:9.5px;color:var(--text-muted);white-space:nowrap">PCR</span>
              <span id="bn-oi-pcr" style="font-weight:800;font-family:var(--font-mono);padding:2px 4px;border-radius:4px;font-size:10px" class="neutral">1.15</span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:4px">
              <span style="font-size:9.5px;color:var(--text-muted);white-space:nowrap">Max Pain</span>
              <span id="bn-oi-maxpain" style="font-weight:700;font-family:var(--font-mono);color:var(--text-primary);font-size:10px">48,500</span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:4px">
              <span style="font-size:9.5px;color:var(--text-muted);white-space:nowrap">Resist (CE)</span>
              <span id="bn-oi-resistance" style="font-weight:700;font-family:var(--font-mono);color:var(--pro-sell);font-size:10px">48,700</span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:4px">
              <span style="font-size:9.5px;color:var(--text-muted);white-space:nowrap">Support (PE)</span>
              <span id="bn-oi-support" style="font-weight:700;font-family:var(--font-mono);color:var(--pro-buy);font-size:10px">48,100</span>
            </div>
            
            <div class="mt-1.5 bg-amber-950/40 border border-amber-900/60 rounded-lg p-1.5 flex items-start gap-1" id="bn-oi-trap-box" style="display:none">
              <i class="ti ti-shield-alert" style="color:var(--pro-watch);font-size:12px;margin-top:2px"></i>
              <div>
                <div style="font-size:9px;font-weight:800;color:#fbbf24" id="bn-oi-trap-title">CE WRITERS TRAPPED</div>
                <div style="font-size:8px;color:#f59e0b;line-height:1.2;margin-top:1px" id="bn-oi-trap-desc">Short covering rally likely at 48,700!</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- FII/DII Net Flows -->
      <div class="widget-card">
        <div class="widget-header">
          <div class="w-2.5 h-2.5 rounded-full bg-purple-500 animate-pulse"></div>
          <span>Institutional Cash Flows</span>
          <span id="fii-dii-source" style="font-size:9px;font-weight:700;color:var(--pro-watch);margin-left:auto">● Loading...</span>
        </div>
        <div style="font-size:9px;color:var(--text-muted);margin-bottom:6px;font-style:italic" id="fii-dii-date"></div>
          <div>
            <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px">
              <span style="color:var(--text-muted)">FII Net Flow</span>
              <span id="flow-fii-val" style="font-weight:800;font-family:var(--font-mono)" class="dn">-₹1,240 Cr</span>
            </div>
            <div style="height:4px;background:var(--bg-inner);border-radius:2px;overflow:hidden">
              <div id="flow-fii-bar" style="height:100%;width:35%;background:var(--pro-sell)" class="rounded-full"></div>
            </div>
          </div>
          <div>
            <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px">
              <span style="color:var(--text-muted)">DII Net Flow</span>
              <span id="flow-dii-val" style="font-weight:800;font-family:var(--font-mono)" class="up">+₹2,180 Cr</span>
            </div>
            <div style="height:4px;background:var(--bg-inner);border-radius:2px;overflow:hidden">
              <div id="flow-dii-bar" style="height:100%;width:65%;background:var(--pro-buy)" class="rounded-full"></div>
            </div>
          </div>
          
          <div style="background:rgba(16, 185, 129, 0.05);border:1px solid rgba(16, 185, 129, 0.15);border-radius:6px;padding:8px 10px;font-size:11px;font-weight:700;display:flex;align-items:center;justify-content:space-between">
            <span style="color:#a7f3d0">Cash Market Bias:</span>
            <span id="flow-bias" style="color:var(--pro-buy)" class="uppercase font-mono text-[10px]">DII DOMINANT</span>
          </div>
        </div>
      </div>

      <!-- Contextual Intelligence Advice -->
      <div class="pro-tip-box">
        <div style="display:flex;align-items:center;gap:6px;margin-bottom:6px">
          <span id="ai-tip-status"></span>
          <i class="ti ti-brain" style="color:#c084fc;font-size:13px"></i>
          <span style="font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:0.5px;color:#c7d2fe">AI Terminal Intelligence</span>
          <span id="ai-tip-time" style="font-size:8px;color:var(--text-muted);margin-left:auto"></span>
        </div>
        <p style="font-size:10.5px;line-height:1.6;color:#e2e8f0" id="sidebarProTip">Analysing market conditions...</p>
        <div id="ai-tip-tags" style="display:flex;flex-wrap:wrap;gap:4px;margin-top:7px"></div>
      </div>
    </div>
  </div><!-- /.main-grid -->

  <!-- Actions Bar Footer -->
  <div class="action-bar">
    <button class="tg-btn" onclick="showModal('tg')"><i class="ti ti-brand-telegram"></i> Telegram Config</button>
    <button class="wa-btn" onclick="showModal('wa')"><i class="ti ti-brand-whatsapp"></i> WhatsApp Config</button>
    <button class="tg-btn" onclick="triggerAlerts()" style="background:#374151;color:var(--text-primary);border:1px solid var(--border-slate)"><i class="ti ti-bell-ringing"></i> Trigger Active Alerts</button>
    <button class="csv-btn" onclick="exportCSV()"><i class="ti ti-download"></i> Export CSV Dataset</button>
    <span id="lastScan" style="font-size:11px;color:var(--text-muted);margin-left:auto"></span>
  </div>

  <!-- Twilio and Telegram alerts popup modal -->
  <div class="modal-bg" id="modalBg">
    <div class="modal">
      <h3 id="modalTitle">Configure Alerts</h3>
      <div id="tgFields" style="display:none">
        <label>Telegram Bot Token</label>
        <input type="password" id="tgToken" placeholder="7123456789:AAF…">
        <label>Chat ID / Group ID</label>
        <input type="text" id="tgChatId" placeholder="-1001234567890">
      </div>
      <div id="waFields" style="display:none">
        <label>Twilio Account SID</label>
        <input type="text" id="waSid" placeholder="ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx">
        <label>Twilio Auth Token</label>
        <input type="password" id="waToken" placeholder="your Twilio auth token">
        <label>WhatsApp Number (To)</label>
        <input type="text" id="waTo" placeholder="+919876543210">
      </div>
      <div class="mrow">
        <button class="btn-save" onclick="saveAlert()">Save & Activate</button>
        <button class="btn-cancel" onclick="closeModal()">Cancel</button>
      </div>
    </div>
  </div>

  <!-- Sizer & Educational Blueprints Modal -->
  <div class="modal-bg" id="helpModalBg">
    <div class="modal help-modal">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;border-bottom:1px solid var(--border-slate);padding-bottom:12px">
        <h3 style="margin:0;font-size:16px;font-weight:800"><i class="ti ti-calculator" style="color:#60a5fa"></i> Trade Sizing & Strategy Blueprint</h3>
        <i class="ti ti-x" onclick="closeHelpModal()" style="font-size:18px;color:var(--text-muted);cursor:pointer;transition:color 0.2s" onmouseover="this.style.color='#f3f4f6'" onmouseout="this.style.color='var(--text-muted)'"></i>
      </div>
      
      <!-- Position size calculator -->
      <div class="help-section">
        <h4><i class="ti ti-calculator" style="color:#60a5fa"></i> Intraday Position Sizer (Strict 1% Risk Allocation)</h4>
        <p style="font-size:12px;color:var(--text-muted)">Calculate exact shares to trade based on volatility stop-losses. Minimize portfolio risk scientifically.</p>
        <div class="calc-container">
          <div class="calc-row">
            <div>
              <label>Trading Capital (₹)</label>
              <input type="number" id="cCapital" value="100000" oninput="runCalc()">
            </div>
            <div>
              <label>Risk per Trade (%)</label>
              <input type="number" id="cRiskPct" value="1" oninput="runCalc()">
            </div>
          </div>
          <div class="calc-row">
            <div>
              <label>Stock Entry Price (₹)</label>
              <input type="number" id="cEntry" placeholder="e.g. 1500" oninput="runCalc()">
            </div>
            <div>
              <label>Scanned Stop Loss (₹)</label>
              <input type="number" id="cStopLoss" placeholder="e.g. 1420" oninput="runCalc()">
            </div>
          </div>
          <div class="calc-result">
            <div class="calc-res-item">
              <span class="calc-res-lbl">Total Risk Capital</span>
              <span class="calc-res-val" id="resRisk" style="color:var(--pro-sell)">₹1,000.00</span>
            </div>
            <div class="calc-res-item">
              <span class="calc-res-lbl">Shares to Trade</span>
              <span class="calc-res-val" id="resShares" style="color:var(--pro-electric)">0 shares</span>
            </div>
            <div class="calc-res-item">
              <span class="calc-res-lbl">Total Trade Value</span>
              <span class="calc-res-val" id="resValue">₹0.00</span>
            </div>
            <div class="calc-res-item">
              <span class="calc-res-lbl">SL Distance (%)</span>
              <span class="calc-res-val" id="resSLDist" style="color:var(--pro-sell)">0.00%</span>
            </div>
          </div>
        </div>
      </div>

      <!-- Execution strategy -->
      <div class="help-section">
        <h4><i class="ti ti-list-check" style="color:#34d399"></i> Tactical Execution steps</h4>
        <div style="font-size:12px;color:var(--text-muted);line-height:1.6">
          <ol style="margin: 0; padding-left: 20px;">
            <li><strong>Post-Market Scan:</strong> Review every evening. Locate tickers with <strong>Score 90+</strong> in the Bullish or Mixed setup groupings.</li>
            <li><strong>Sizing:</strong> Use the calculator above to calculate exact quantities for a strict 1% maximum capital risk limit.</li>
            <li><strong>Entry Order:</strong> Place a buy/sell bracket order directly at <strong>9:15 AM</strong> on the next day's session open.</li>
            <li><strong>Bracket Settings:</strong> Specify target at **Camarilla T1 (H3)** and stop-loss at **Stop Loss (L3)**.</li>
            <li><strong>Recycle Rule:</strong> If neither target nor stop-loss hits in <strong>5 trading days</strong>, manually square off the trade at the 5th day's close.</li>
          </ol>
        </div>
      </div>
      
      <div style="text-align:right;margin-top:16px">
        <button class="btn-cancel" onclick="closeHelpModal()" style="padding:8px 20px;font-size:12px;margin:0">Done</button>
      </div>
    </div>
  </div>

  <!-- Log Trade modal -->
  <div class="modal-bg" id="journalModalBg">
    <div class="modal" style="width:520px">
      <h3><i class="ti ti-notebook" style="color:#c084fc"></i> Log Trade — <span id="jm-symbol"></span>
        <span id="jm-signal-badge" style="font-size:11px;padding:2px 8px;border-radius:4px;margin-left:8px;font-weight:700"></span>
      </h3>

      <!-- Regime conflict warning -->
      <div id="jm-regime-warning">
        <span style="font-size:13px;margin-right:6px">⚠️</span>
        <span id="jm-regime-warning-text"></span>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px">
        <div>
          <label>Trading Capital (₹)</label>
          <input type="number" id="jm-capital" value="100000" oninput="calcJournalSizer()">
        </div>
        <div>
          <label>Risk per Trade (%)</label>
          <input type="number" id="jm-risk-pct" value="1" step="0.1" oninput="calcJournalSizer()">
        </div>
        <div>
          <label>Entry Price (₹)</label>
          <input type="number" id="jm-entry" oninput="calcJournalSizer()">
        </div>
        <div>
          <label>Stop Loss (₹)</label>
          <input type="number" id="jm-sl" oninput="calcJournalSizer()">
        </div>
      </div>

      <div style="background:var(--bg-inner);border-radius:8px;padding:12px;
        margin-top:14px;display:grid;grid-template-columns:repeat(4,1fr);gap:8px">
        <div style="text-align:center">
          <div style="color:var(--text-muted);font-size:10px">RISK AMOUNT</div>
          <div id="jm-risk-amt" style="color:var(--pro-sell);font-weight:800;font-size:13px">₹0</div>
        </div>
        <div style="text-align:center">
          <div style="color:var(--text-muted);font-size:10px">QUANTITY</div>
          <div id="jm-qty" style="color:var(--pro-electric);font-weight:800;font-size:13px">0</div>
        </div>
        <div style="text-align:center">
          <div style="color:var(--text-muted);font-size:10px">VALUE</div>
          <div id="jm-value" style="color:var(--pro-buy);font-weight:800;font-size:13px">₹0</div>
        </div>
        <div style="text-align:center">
          <div style="color:var(--text-muted);font-size:10px">SL RANGE</div>
          <div id="jm-sl-dist" style="color:var(--pro-watch);font-weight:800;font-size:13px">0%</div>
        </div>
      </div>

      <!-- AI Auto-filled notes -->
      <div style="margin-top:12px">
        <label style="display:flex;align-items:center;gap:6px">
          Operational Notes
          <span id="jm-ai-note-badge" style="display:none;font-size:8px;background:rgba(124,58,237,0.2);color:#c084fc;border:1px solid rgba(124,58,237,0.3);padding:1px 6px;border-radius:10px;font-weight:700">✦ AI Filled</span>
        </label>
        <textarea id="jm-notes" rows="3" style="width:100%;box-sizing:border-box;background:var(--bg-inner);border:1px solid var(--border-slate);color:var(--text-primary);border-radius:6px;padding:8px;font-size:11px;line-height:1.5;resize:vertical" placeholder="Describe the trigger parameters..."></textarea>
      </div>

      <div class="mrow" style="margin-top:16px">
        <button class="btn-save" onclick="submitJournalLog()" style="background:#6d28d9">
          <i class="ti ti-notebook"></i> Record Active Trade
        </button>
        <button class="btn-cancel" onclick="document.getElementById('journalModalBg').classList.remove('show')">
          Cancel
        </button>
      </div>
    </div>
  </div>
</div><!-- /.wrap -->

<!-- Notification toast -->
<div class="toast" id="toast">
  <i class="ti ti-circle-check" style="color:var(--pro-buy);font-size:16px"></i>
  <span id="toastMsg"></span>
</div>

<script>
// ── Core States ──────────────────────────────────────────────────
let stocks = [];
let watchlist = [];
let journalTrades = [];
let activeTab = 'home';
let activeSubTab = 'all';
let sortState = {col:'score', desc:true};
const emaReq = {10:true, 20:true, 50:false, 200:false};
let pollTimer = null;
let autoTimer = null;
let remaining = 300;
let coldShown = false;
let marketClosed = false;
let expandedSymbol = null;
let activeSector = null;

// Simulated Widget States (ticking in real-time)
let pcrVal = 1.24;
let bankPcrVal = 1.15;
let fiiNetVal = -1240;
let diiNetVal = 2180;
let niftyClosePrice = 23450;
let bankNiftyClosePrice = 48500;

// Sector Data — loaded from real NSE sector indices via backend
const SECTOR_NAMES = ["Banking","IT","Pharma","Auto","FMCG","Metals","Energy","Realty","Infra","Media"];
let activeSectors = SECTOR_NAMES.map(n => ({ name: n, change: 0.0, trend: "neutral", strength: 50 }));

function fetchSectorRotation() {
  fetch('/sector_rotation')
    .then(r => r.json())
    .then(resp => {
      // New API format: {sectors: [...], fetched_time: "HH:MM IST"}
      const sectors = Array.isArray(resp) ? resp : (resp.sectors || []);
      const fetchedTime = resp.fetched_time || null;
      if (sectors.length > 0) {
        activeSectors = sectors;
        // Re-render heatmap with real data
        const hGrid = document.getElementById('heatmapGrid');
        if (hGrid) {
          hGrid.innerHTML = activeSectors.map(s => {
            let colors = "";
            if (s.trend === "up") {
              colors = "background: rgba(16, 185, 129, 0.08); border-color: rgba(16, 185, 129, 0.15); color: var(--pro-buy)";
            } else if (s.trend === "down") {
              colors = "background: rgba(244, 63, 94, 0.08); border-color: rgba(244, 63, 94, 0.15); color: var(--pro-sell)";
            } else {
              colors = "background: var(--bg-inner); border-color: var(--border-slate); color: var(--text-muted)";
            }
            const isActive = activeSector === s.name;
            return `<button
              onclick="toggleSectorFilter('${s.name}')"
              style="${colors}"
              class="heatmap-btn ${isActive ? 'active' : ''}"
            >
              <span style="font-weight:700">${s.name}</span>
              <div style="font-size:9.5px;font-family:var(--font-mono);display:flex;justify-content:space-between;width:100%">
                <span>${s.change > 0 ? '+' : ''}${Number(s.change).toFixed(2)}%</span>
                <span>${s.trend === 'up' ? '↑' : s.trend === 'down' ? '↓' : '→'}</span>
              </div>
            </button>`;
          }).join('');
        }
        // Show LIVE/CLOSED badge
        const sectorLabel = document.getElementById('sectorDataSource');
        if (sectorLabel) {
          sectorLabel.style.display = 'inline';
          if (marketClosed) {
            sectorLabel.innerHTML = 'LAST SESSION';
            sectorLabel.style.color = '#d97706';
            sectorLabel.style.background = 'rgba(217, 119, 6, 0.12)';
            sectorLabel.style.borderColor = 'rgba(217, 119, 6, 0.25)';
          } else {
            sectorLabel.innerHTML = '⚡ LIVE';
            sectorLabel.style.color = 'var(--pro-buy)';
            sectorLabel.style.background = 'rgba(16,185,129,.12)';
            sectorLabel.style.borderColor = 'rgba(16,185,129,.25)';
          }
        }
        // Show freshness timestamp
        const timeEl = document.getElementById('sectorFetchTime');
        if (timeEl && fetchedTime) {
          timeEl.textContent = 'Sector data as of: ' + fetchedTime;
          timeEl.style.display = 'block';
        }
      }
    })
    .catch(() => { /* keep neutral placeholders on error */ });
}
let activeSectors_fetchTimer = null;

// sectorMapping: seeded with known stocks, gets enriched dynamically from live scan results
let sectorMapping = {
  // Original mock stocks
  "HDFCBANK": "Banking", "SBIN": "Banking",
  "RELIANCE": "Energy", "COALINDIA": "Metals",
  "TMCV": "Auto", "MARUTI": "Auto",
  "INFY": "IT", "WIPRO": "IT",
  "DRREDDY": "Pharma", "DLF": "Realty",
  "ADANIENT": "Infra", "ITC": "FMCG",

  // Real database scanned stocks
  "CHENNPETRO": "Energy", "ATGL": "Energy",
  "CUB": "Banking", "FEDERALBNK": "Banking", "RBLBANK": "Banking", "BANDHANBNK": "Banking",
  "TATASTEEL": "Metals",
  "BHARATFORG": "Auto", "MOTHERSON": "Auto",
  "OFSS": "IT",
  "ALKEM": "Pharma", "ZYDUSLIFE": "Pharma",
  "SUNTV": "Media",
  "PIDILITIND": "FMCG", "TATACHEM": "FMCG",
  "SOLARINDS": "Infra", "SIEMENS": "Infra", "LT": "Infra", "INDUSTOWER": "Infra"
};

// Enriches sectorMapping from live scan results (s.sector field from scanner.py)
function enrichSectorMapping(signals) {
  (signals || []).forEach(s => {
    if (s.symbol && s.sector && s.sector !== 'General Equity') {
      sectorMapping[s.symbol.toUpperCase()] = s.sector;
    }
  });
}

// Helper: get sector for a stock — prefers live scanner sector, falls back to map
function getStockSector(s) {
  if (s.sector && s.sector !== 'General Equity') return s.sector;
  return sectorMapping[s.symbol] || sectorMapping[(s.symbol || '').toUpperCase()] || 'General Equity';
}

// Helpers
const fmt = n => n >= 1000 ? '₹'+n.toLocaleString('en-IN',{maximumFractionDigits:2}) : '₹'+n.toFixed(2);
const pct = n => (n>0?'+':'')+Number(n).toFixed(2)+'%';

function showToast(msg, err=false){
  const t=document.getElementById('toast');
  t.classList.toggle('err',err);
  const icon = t.querySelector('i');
  if (err) {
    icon.className = 'ti ti-circle-x';
    icon.style.color = 'var(--pro-sell)';
  } else {
    icon.className = 'ti ti-circle-check';
    icon.style.color = 'var(--pro-buy)';
  }
  document.getElementById('toastMsg').textContent=msg;
  t.classList.add('show');
  setTimeout(()=>t.classList.remove('show'), 3500);
}

// ── 5d Sparkline SVG Generator ────────────────────────
function sparklineSVG(prices, signalType) {
  if (!prices || prices.length < 2) return '';
  const min = Math.min(...prices);
  const max = Math.max(...prices);
  const rng = max - min || 1;
  const width = 60;
  const height = 18;
  const pts = prices.map((p, i) =>
    `${(i/(prices.length-1))*width},${height - 2 - ((p-min)/rng)*14}`
  ).join(' ');
  const color = signalType === 'Bear' ? 'var(--pro-sell)' : 'var(--pro-buy)';
  return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" style="vertical-align:middle;overflow:visible;">
    <polyline points="${pts}" fill="none" stroke="${color}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>`;
}

// ── Visual Score Confidence Bar Generator ─────────────
function renderScoreBar(score, grade) {
  const gradeColor = grade === "A+" ? "#10B981" : grade === "A" ? "#3B82F6" : "#F59E0B";
  const filled = Math.round(score / 10);
  let segments = "";
  for (let i = 0; i < 10; i++) {
    const isFilled = i < filled;
    segments += `<div style="width: 4px; height: 12px; border-radius: 1px; background-color: ${isFilled ? gradeColor : '#1F2937'}; opacity: ${isFilled ? 1 : 0.4}; transition: all 0.2s;"></div>`;
  }
  return `<div style="display: inline-flex; align-items: center; gap: 6px; user-select: none;">
    <div style="display: flex; gap: 2px;">
      ${segments}
    </div>
    <span style="font-family: var(--font-mono); font-size: 11px; font-weight: 700; color: ${gradeColor}">${score}</span>
    <span style="font-family: var(--font-mono); font-size: 9px; font-weight: 900; background-color: ${gradeColor}22; color: ${gradeColor}; border: 1px solid ${gradeColor}44; padding: 1px 4px; border-radius: 3px;">${grade}</span>
  </div>`;
}

// ── Preferences Persistence ──────────────────────────
function loadRegime() {
  fetch('/regime')
    .then(r => r.json())
    .then(d => {
      if (d.error) return;

      const bar = document.getElementById('regimeBar');
      const label = document.getElementById('regimeLabel');
      const emoji = document.getElementById('regimeEmoji');
      const desc = document.getElementById('regimeDesc');
      const nifty = document.getElementById('regimeNifty');
      const breadth = document.getElementById('regimeBreadth');
      const emas = document.getElementById('regimeEMAs');
      const atr = document.getElementById('regimeATR');
      const high = document.getElementById('regimeHigh');

      bar.style.borderBottom = '2px solid ' + d.color;
      label.textContent = d.emoji + '  ' + d.label;
      label.style.color = d.color;
      emoji.textContent = d.emoji;
      desc.textContent = d.description;

      nifty.textContent = '₹' + (d.nifty_close || 0).toLocaleString('en-IN', {maximumFractionDigits:2});
      nifty.style.color = d.color;
      niftyClosePrice = d.nifty_close || 23450;
      bankNiftyClosePrice = d.banknifty_close || 48500;
      updateSidebarWidgets();

      const b = d.breadth || 50;
      breadth.textContent = b + '% above EMA50';
      breadth.style.color = b >= 60 ? 'var(--pro-buy)' : b <= 40 ? 'var(--pro-sell)' : 'var(--pro-watch)';

      emas.textContent = [d.nifty_ema20, d.nifty_ema50, d.nifty_ema200]
                            .map(v => Math.round(v).toLocaleString('en-IN')).join(' / ');

      atr.textContent = (d.atr_pct || 0).toFixed(2) + '%';
      high.textContent = '-' + (d.pct_from_high || 0).toFixed(1) + '%';

      if (window.niftyRegime && window.niftyRegime !== d.regime) {
        addNotification('regime', null, `Regime Shift: ${d.label} ⚖️`, `Nifty Market Regime shifted to ${d.label}`);
      }
      window.niftyRegime = d.regime;
      applyRegimeScanMode(d.regime);

      const bearWarn = document.getElementById('bearWarning');
      if (bearWarn) {
        if (d.regime === 'STRONG_BEAR' || d.regime === 'BEAR') {
          bearWarn.style.display = 'flex';
          bearWarn.innerHTML = `⚠️ <b>${d.label.toUpperCase()}</b> market active — Bullish signals shown with reduced confidence. Consider bearish setups or staying in cash. <span style="margin-left:auto;cursor:pointer" onclick="this.parentElement.style.display='none'">✕</span>`;
        } else {
          bearWarn.style.display = 'none';
        }
      }
    })
    .catch(() => {});
}

// ── Feature 5: Regime-Aware Scan Mode Auto-Switch ──────────
// Only auto-switch once on first page load; after that respect user's manual choice
let _lastAppliedRegime = null;
let _regimeScanModeAppliedOnce = false;
function applyRegimeScanMode(regime) {
  if (_lastAppliedRegime === regime) return; // no change, skip
  const prevRegime = _lastAppliedRegime;
  _lastAppliedRegime = regime;

  const modeEl = document.getElementById('scanMode');
  const banner = document.getElementById('regime-mode-banner');
  if (!modeEl || !banner) return;

  // Only auto-change the scan mode dropdown on a REGIME CHANGE (ignore on first load to respect default page options/preferences)
  const shouldAutoSwitch = (prevRegime !== null && prevRegime !== regime);
  _regimeScanModeAppliedOnce = true;

  let bannerText = '', bannerColor = '', bannerBg = '';
  if (regime === 'STRONG_BEAR' || regime === 'BEAR') {
    if (shouldAutoSwitch) modeEl.value = 'bearish';
    bannerText = '\uD83D\uDCC9 Bear Regime — Consider SHORT setups. Scan mode: ' + (shouldAutoSwitch ? 'auto-set to Bearish' : modeEl.value);
    bannerColor = '#fca5a5'; bannerBg = 'rgba(244,63,94,0.08)';
    if (shouldAutoSwitch && activeTab === 'camarilla' && activeSubTab === 'all') setSubTab('bearish');
  } else if (regime === 'STRONG_BULL' || regime === 'BULL') {
    if (shouldAutoSwitch) modeEl.value = 'bullish';
    bannerText = '\uD83D\uDE80 Bull Regime — BUY setups active. Scan mode: ' + (shouldAutoSwitch ? 'auto-set to Bullish' : modeEl.value);
    bannerColor = '#6ee7b7'; bannerBg = 'rgba(16,185,129,0.08)';
    if (shouldAutoSwitch && activeTab === 'camarilla' && activeSubTab === 'all') setSubTab('bullish');
  } else {
    if (shouldAutoSwitch) modeEl.value = 'both';
    bannerText = '\u2696\uFE0F Neutral Regime — Showing all signals';
    bannerColor = '#fbbf24'; bannerBg = 'rgba(245,158,11,0.08)';
  }
  if (shouldAutoSwitch) savePrefs();
  banner.innerHTML = `<span>${bannerText}</span><span style="margin-left:auto;cursor:pointer;opacity:.6" onclick="this.parentElement.style.display='none'" title="Dismiss">✕</span>`;
  banner.style.cssText = `display:flex;background:${bannerBg};border-bottom:1px solid ${bannerColor}20;color:${bannerColor};padding:6px 20px;font-size:11px;font-weight:700;align-items:center;gap:8px;animation:slideDown 0.3s ease`;
  setTimeout(() => { if (banner.style.display !== 'none') banner.style.display = 'none'; }, 8000);
}

function refreshRegime() {
  fetch('/regime?refresh=1').then(r => r.json()).then(d => {
    loadRegime();
    showToast('Regime refreshed: ' + d.label);
  });
}

loadRegime();
setInterval(loadRegime, 15 * 60 * 1000);

// ── Real FII/DII EOD Data Loader ──────────────────────────
function loadFiiDii() {
  fetch('/fii_dii')
    .then(r => r.json())
    .then(d => {
      // Set the base values from REAL NSE data
      fiiNetVal = d.fii_net || -1240;
      diiNetVal = d.dii_net || 2180;
      
      // Update date badge
      const dateEl = document.getElementById('fii-dii-date');
      if (dateEl) dateEl.textContent = d.date ? 'As of ' + d.date : '';

      // Update source badge
      const srcEl = document.getElementById('fii-dii-source');
      if (srcEl) {
        const isReal = d.source && d.source.includes('Real');
        srcEl.textContent = isReal ? '● NSE Live' : '● Simulated';
        srcEl.style.color = isReal ? 'var(--pro-buy)' : 'var(--pro-watch)';
      }

      // Update bias label from server
      const flowBias = document.getElementById('flow-bias');
      if (flowBias) {
        flowBias.textContent = d.bias || (diiNetVal >= 0 ? 'DII Dominant' : 'FII Selling');
        flowBias.style.color = (diiNetVal >= 0 && fiiNetVal > -500) ? 'var(--pro-buy)' : 'var(--pro-sell)';
      }

      updateSidebarWidgets(); // Immediately refresh bars with real values
    })
    .catch(() => { /* keep simulated values if fetch fails */ });
}

loadFiiDii();
setInterval(loadFiiDii, 30 * 60 * 1000); // Refresh every 30 min

function savePrefs(){
  try{
    localStorage.setItem('nse_prefs_h_v3',JSON.stringify({
      volDays: document.getElementById('volDays').value,
      volMult: document.getElementById('volMult').value,
      turnoverLimit: document.getElementById('turnoverLimit').value,
      scanMode:document.getElementById('scanMode').value,
      scanDataSource: document.getElementById('scanDataSource').value,
      minPrice: document.getElementById('minPrice').value,
      theme: document.getElementById('themeSelect').value,
      demoData: document.getElementById('demoDataSelect').value,
      ema10: emaReq[10], ema20:emaReq[20], ema50:emaReq[50], ema200:emaReq[200],
    }));
  }catch(e){}
}

function loadPrefs(){
  try{
    const p=JSON.parse(localStorage.getItem('nse_prefs_h_v3')||'{}');
    if(p.volDays) document.getElementById('volDays').value=p.volDays;
    if(p.volMult) document.getElementById('volMult').value=p.volMult;
    if(p.turnoverLimit) document.getElementById('turnoverLimit').value=p.turnoverLimit;
    if(p.scanMode) document.getElementById('scanMode').value=p.scanMode;
    if(p.scanDataSource) document.getElementById('scanDataSource').value=p.scanDataSource;
    if(p.minPrice !== undefined) document.getElementById('minPrice').value=p.minPrice;
    if(p.theme) applyTheme(p.theme);
    else applyTheme(localStorage.getItem('nse_theme') || 'premium');
    if(p.demoData) document.getElementById('demoDataSelect').value=p.demoData;
    [10,20,50,200].forEach(n=>{
      const v = p['ema'+n];
      if(v!==undefined){
        emaReq[n]=v;
        const chip=document.getElementById('c'+n);
        const cb=chip&&chip.querySelector('input');
        if(cb){cb.checked=v;chip.classList.toggle('on',v);}
      }
    });
  }catch(e){}
}

// Watchlist
function loadWatchlist(){
  fetch('/watchlist').then(r=>r.json()).then(d=>{
    watchlist=d.watchlist||[];
    updateCounts(); render();
  }).catch(()=>{
    watchlist=JSON.parse(localStorage.getItem('nse_wl')||'[]');
  });
}
function saveWatchlistRemote(){
  fetch('/watchlist',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({watchlist})}).catch(()=>{});
  try{localStorage.setItem('nse_wl',JSON.stringify(watchlist));}catch(e){}
}
function toggleWatch(sym,el){
  const idx=watchlist.indexOf(sym);
  if(idx===-1){watchlist.push(sym);el.style.color='var(--pro-watch)';showToast(sym+' added to watchlist');}
  else{watchlist.splice(idx,1);el.style.color='#4b5563';showToast(sym+' removed from watchlist');}
  saveWatchlistRemote();
  updateCounts();
  if(activeTab==='watchlist') render();
}

// Tabs
function setTab(t){
  activeTab=t;
  document.body.setAttribute('data-active-tab', t);
  document.querySelectorAll('.tab').forEach(el=>el.classList.remove('active'));
  const activeBtn = document.getElementById('tab-'+t);
  if (activeBtn) activeBtn.classList.add('active');
  
  const homeDiv = document.getElementById('homeTabContent');
  const tblWrap = document.querySelector('.tbl-wrap');
  const sumBar = document.getElementById('sumBar');
  const controls = document.querySelector('.controls');
  const picks = document.getElementById('proTraderTopPicks');
  const subFilters = document.getElementById('camarillaSubFilters');
  
  if (t === 'home') {
    if (homeDiv) homeDiv.style.display = 'block';
    if (tblWrap) tblWrap.style.display = 'none';
    if (sumBar) sumBar.style.display = 'none';
    if (controls) controls.style.display = 'none';
    if (picks) picks.style.display = 'none';
    if (subFilters) subFilters.style.display = 'none';
  } else {
    if (homeDiv) homeDiv.style.display = 'none';
    if (tblWrap) tblWrap.style.display = 'block';
    if (sumBar) sumBar.style.display = 'flex';
    if (controls) controls.style.display = (t === 'journal') ? 'none' : 'flex';
    if (picks) picks.style.display = (t === 'protrader') ? 'block' : 'none';
    if (subFilters) subFilters.style.display = (t === 'camarilla') ? 'flex' : 'none';
  }
  
  expandedSymbol = null;
  render();

  // Live LTP polling: start when entering Journal tab, stop otherwise
  if (t === 'journal') {
    if (typeof fetchLiveLTP === 'function') {
      fetchLiveLTP();
      if (window._ltpRefreshTimer) clearInterval(window._ltpRefreshTimer);
      window._ltpRefreshTimer = setInterval(fetchLiveLTP, 60000);
    }
  } else {
    if (window._ltpRefreshTimer) { clearInterval(window._ltpRefreshTimer); window._ltpRefreshTimer = null; }
  }
}

// ── Theme Swapper ────────────────────────────────────
function changeTheme() {
  const select = document.getElementById('themeSelect');
  if (select) {
    applyTheme(select.value);
    savePrefs();
    render();
  }
}

function applyTheme(themeName) {
  const root = document.documentElement;
  if (themeName === 'classic') {
    root.style.setProperty('--bg-dark', '#080b11');
    root.style.setProperty('--bg-card', '#0d1117');
    root.style.setProperty('--bg-inner', '#111827');
    root.style.setProperty('--bg-hover', '#161d2a');
    root.style.setProperty('--border-slate', '#1f2937');
    root.style.setProperty('--pro-electric', '#2563EB');
    root.style.setProperty('--text-primary', '#e2e8f0');
    root.style.setProperty('--text-muted', '#94a3b8');
    root.style.setProperty('--pro-buy', '#10B981');
    root.style.setProperty('--pro-sell', '#F43F5E');
    root.style.setProperty('--pro-watch', '#F59E0B');
  } else if (themeName === 'normal') {
    // Normal (Light) Theme
    root.style.setProperty('--bg-dark', '#F3F4F6');
    root.style.setProperty('--bg-card', '#FFFFFF');
    root.style.setProperty('--bg-inner', '#E5E7EB');
    root.style.setProperty('--bg-hover', '#D1D5DB');
    root.style.setProperty('--border-slate', '#D1D5DB');
    root.style.setProperty('--pro-electric', '#2563EB');
    root.style.setProperty('--text-primary', '#111827');
    root.style.setProperty('--text-muted', '#4B5563');
    root.style.setProperty('--pro-buy', '#059669'); // darker green for light mode readability
    root.style.setProperty('--pro-sell', '#DC2626'); // darker red for light mode readability
    root.style.setProperty('--pro-watch', '#D97706'); // darker amber/orange
  } else if (themeName === 'dark') {
    // Clean Slate Dark Theme
    root.style.setProperty('--bg-dark', '#030712');
    root.style.setProperty('--bg-card', '#0F172A');
    root.style.setProperty('--bg-inner', '#1E293B');
    root.style.setProperty('--bg-hover', '#334155');
    root.style.setProperty('--border-slate', '#334155');
    root.style.setProperty('--pro-electric', '#3B82F6');
    root.style.setProperty('--text-primary', '#F9FAFB');
    root.style.setProperty('--text-muted', '#9CA3AF');
    root.style.setProperty('--pro-buy', '#10B981');
    root.style.setProperty('--pro-sell', '#F43F5E');
    root.style.setProperty('--pro-watch', '#F59E0B');
  } else {
    // premium (default - Premium Navy)
    root.style.setProperty('--bg-dark', '#0A0E1A');
    root.style.setProperty('--bg-card', '#111827');
    root.style.setProperty('--bg-inner', '#0D1117');
    root.style.setProperty('--bg-hover', '#1F2937');
    root.style.setProperty('--border-slate', '#1F2937');
    root.style.setProperty('--pro-electric', '#3B82F6');
    root.style.setProperty('--text-primary', '#F9FAFB');
    root.style.setProperty('--text-muted', '#9CA3AF');
    root.style.setProperty('--pro-buy', '#10B981');
    root.style.setProperty('--pro-sell', '#F43F5E');
    root.style.setProperty('--pro-watch', '#F59E0B');
  }
  localStorage.setItem('nse_theme', themeName);
  const themeSelect = document.getElementById('themeSelect');
  if (themeSelect) themeSelect.value = themeName;
}

// ── Home Landing Dashboard Renderer ──────────────────
function renderHome() {
  const homeDiv = document.getElementById('homeTabContent');
  if (!homeDiv) return;

  // 1. Get Top 3 Opportunities (non-mock, sorted by score)
  const top3 = [...stocks].filter(s => !s.isMock).sort((a,b) => (b.score||0) - (a.score||0)).slice(0, 3);
  let topOptHTML = "";

  function getAIReason(s) {
    const parts = [];
    if (s.vol_ratio >= 3) parts.push(`${s.vol_ratio.toFixed(1)}x volume surge`);
    else if (s.vol_ratio >= 1.5) parts.push(`${s.vol_ratio.toFixed(1)}x above-avg volume`);
    if (s.rsi >= 65) parts.push(`RSI ${s.rsi} (momentum)`);
    else if (s.rsi <= 35) parts.push(`RSI ${s.rsi} (oversold bounce)`);
    if (s.tf) parts.push(`${s.tf} breakout`);
    if (s.dist_from_entry !== undefined && Math.abs(s.dist_from_entry) <= 1.5) parts.push('price at pivot entry');
    if (s.sector) parts.push(`${s.sector} sector`);
    return parts.length ? parts.slice(0, 3).join(' · ') : 'High-score Camarilla setup';
  }

  function getStatusBadge3(s) {
    const status = s.entry_status || 'STALE';
    if (status === 'BELOW_PIVOT') return ['BELOW PIVOT 📉', '#818cf8', 'rgba(99,102,241,0.15)', 'rgba(99,102,241,0.3)'];
    if (status === 'FRESH') return ['FRESH ✅', '#34d399', 'rgba(16,185,129,0.15)', 'rgba(16,185,129,0.3)'];
    if (status === 'EXTENDED') return ['EXTENDED ⚠️', '#fbbf24', 'rgba(245,158,11,0.15)', 'rgba(245,158,11,0.3)'];
    return ['STALE ❌', '#f87171', 'rgba(244,63,94,0.15)', 'rgba(244,63,94,0.3)'];
  }

  if (top3.length > 0) {
    const cards = top3.map((s, rank) => {
      const isBull = s.signal_type === 'Bull';
      const signalColor = isBull ? 'var(--pro-buy)' : 'var(--pro-sell)';
      const signalBg = isBull ? 'rgba(16,185,129,0.15)' : 'rgba(244,63,94,0.15)';
      const signalBorder = isBull ? 'rgba(16,185,129,0.3)' : 'rgba(244,63,94,0.3)';
      const scoreW = Math.min(100, s.score || 0);
      const scoreColor = scoreW >= 80 ? '#10B981' : scoreW >= 60 ? '#fbbf24' : '#f87171';
      const [stLabel, stColor, stBg, stBorder] = getStatusBadge3(s);
      const reason = getAIReason(s);
      const entryVal = s.entry || s.price;
      const prefill = {
        symbol: s.symbol, signal_type: s.signal_type,
        conf_grade: s.confidence || 'B', raw_score: s.score,
        regime_score: s.score, regime: window.niftyRegime || 'NEUTRAL',
        entry_price: entryVal, stop_loss: s.stop_loss || entryVal * 0.985,
        target_t1: s.target || entryVal * 1.015,
        target_t2: s.target2 || entryVal * 1.03,
        risk_pct: s.risk_percentage || 1.5, rr_ratio: s.rr || 2.0
      };
      const medal = rank === 0 ? '🥇' : rank === 1 ? '🥈' : '🥉';
      return `
        <div style="flex:1;min-width:180px;background:var(--bg-inner);border:1px solid ${isBull ? 'rgba(16,185,129,0.2)' : 'rgba(244,63,94,0.2)'};border-radius:12px;padding:14px;display:flex;flex-direction:column;gap:9px;position:relative;overflow:hidden;">
          <div style="position:absolute;top:0;left:0;right:0;height:2px;background:${signalColor};opacity:0.5"></div>
          <div style="display:flex;align-items:center;justify-content:space-between;">
            <div>
              <span style="font-size:9px;color:var(--text-muted);font-weight:700">${medal} RANK #${rank+1}</span>
              <div style="font-size:16px;font-weight:900;font-family:var(--font-mono);color:var(--pro-electric);letter-spacing:-.02em">${s.symbol}</div>
              <div style="font-size:9.5px;color:var(--text-muted)">${s.name || ''}</div>
            </div>
            <span style="font-size:10px;font-weight:800;padding:3px 8px;border-radius:5px;background:${signalBg};color:${signalColor};border:1px solid ${signalBorder}">${isBull ? 'BUY' : 'SHORT'}</span>
          </div>
          <div>
            <div style="display:flex;justify-content:space-between;font-size:9px;color:var(--text-muted);margin-bottom:3px">
              <span>Score</span><span style="color:${scoreColor};font-weight:800">${s.score}/100</span>
            </div>
            <div style="height:5px;background:var(--bg-card);border-radius:3px;overflow:hidden">
              <div style="height:100%;width:${scoreW}%;background:${scoreColor};border-radius:3px;transition:width .6s ease"></div>
            </div>
          </div>
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <div>
              <div style="font-size:8.5px;color:var(--text-muted)">LTP</div>
              <div style="font-size:14px;font-weight:800;font-family:var(--font-mono)">${fmt(s.price)}${marketClosed && !s.isMock ? ' <span style="font-size:8px;color:#d97706;font-weight:700">(prev close)</span>' : ''}</div>
            </div>
            <span style="font-size:8.5px;font-weight:800;padding:2px 7px;border-radius:4px;background:${stBg};color:${stColor};border:1px solid ${stBorder}">${stLabel}</span>
          </div>
          <div style="font-size:9px;color:var(--text-muted);line-height:1.4;border-top:1px solid var(--border-slate);padding-top:7px">
            💡 <em>${reason}</em>
          </div>
          <button onclick="logTradeFromRow(event, ${JSON.stringify(prefill).replace(/"/g,"'")})" style="background:var(--pro-electric);color:#fff;border:none;border-radius:6px;padding:6px 0;font-size:10px;cursor:pointer;font-weight:700;width:100%;display:flex;align-items:center;justify-content:center;gap:4px">
            <i class="ti ti-notebook"></i> Log Trade
          </button>
        </div>`;
    }).join('');

    topOptHTML = `
      <div>
        <div style="font-size:9px;color:var(--text-muted);font-weight:700;text-transform:uppercase;margin-bottom:8px">🔥 Today's Top Opportunities</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">${cards}</div>
      </div>`;
  } else {
    topOptHTML = `
      <div class="widget-card" style="border: 1px solid var(--border-slate); text-align: center; padding: 14px; color: var(--text-muted); border-radius: 12px;">
        🔍 Run a scanner session to detect the day's top opportunity signals.
      </div>`;
  }

  // 2. Regime Stats
  const regime = window.niftyRegime || 'NEUTRAL';
  let regimeColor = '#fbbf24';
  let regimeBg = 'rgba(245,158,11,0.08)';
  if (regime === 'STRONG_BULL' || regime === 'BULL') {
    regimeColor = '#10B981';
    regimeBg = 'rgba(16,185,129,0.08)';
  } else if (regime === 'STRONG_BEAR' || regime === 'BEAR') {
    regimeColor = '#F43F5E';
    regimeBg = 'rgba(244,63,94,0.08)';
  }
  const regimeDescText = document.getElementById('regimeDesc')?.textContent || 'Neutral trends.';

  // 3. AI Commentary mirroring
  const sidebarTip = document.getElementById('sidebarProTip')?.textContent || 'Analyzing markets...';
  const sidebarTags = document.getElementById('ai-tip-tags')?.innerHTML || '';

  homeDiv.innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 16px;">
      
      <!-- Gradient Header -->
      <div>
        <h2 style="font-size: 20px; font-weight: 900; letter-spacing: -0.02em; background: linear-gradient(90deg, var(--pro-electric), var(--pro-buy)); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">Dashboard Home Overview</h2>
        <p style="font-size: 11.5px; color: var(--text-muted); margin-top: 2px;">Market orientation & quick access links</p>
      </div>

      <!-- AI Intelligence Card (Top) -->
      <div style="background: linear-gradient(135deg, var(--bg-dark), var(--bg-card)); border: 1px solid var(--pro-electric); box-shadow: 0 0 15px rgba(59, 130, 246, 0.1); border-radius: 12px; padding: 16px;">
        <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 8px;">
          <span style="display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: var(--pro-buy); animation: pulse 2s ease-in-out infinite;"></span>
          <i class="ti ti-brain" style="color: #a78bfa; font-size: 14px;"></i>
          <span style="font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.8px; color: #c7d2fe;">AI Terminal Intelligence</span>
        </div>
        <p style="font-size: 12px; line-height: 1.6; color: var(--text-primary); font-weight: 500;">${sidebarTip}</p>
        <div style="display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px;">${sidebarTags}</div>
      </div>

      <!-- Market Regime & Opportunity Layout -->
      <div style="display: flex; flex-direction: column; gap: 16px;">
        
        <!-- Big Prominent Regime Card -->
        <div class="widget-card" style="border: 1px solid ${regimeColor}30; border-top: 4px solid ${regimeColor}; background: ${regimeBg}; border-radius: 12px; padding: 18px;">
          <div style="display: flex; align-items: flex-start; justify-content: space-between; flex-wrap: wrap; gap: 12px;">
            <div>
              <div style="font-size: 9px; color: var(--text-muted); font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px;">Live Market Regime</div>
              <h1 style="color: ${regimeColor}; font-size: 24px; font-weight: 900; margin-top: 4px; display: flex; align-items: center; gap: 6px;">
                ${document.getElementById('regimeEmoji')?.textContent || '⚖️'} ${document.getElementById('regimeLabel')?.textContent.replace(/.*?\\s+/, '') || regime}
              </h1>
              <p style="font-size: 12px; color: var(--text-muted); margin-top: 6px; font-style: italic; max-width: 450px;">${regimeDescText}</p>
            </div>
            
            <div style="text-align: right;">
              <div style="font-size: 9px; color: var(--text-muted); font-weight: 700; text-transform: uppercase;">NIFTY Index</div>
              <div style="font-size: 20px; font-weight: 800; color: #fff; font-family: var(--font-mono); margin-top: 2px;">
                ${document.getElementById('regimeNifty')?.textContent || '—'}
              </div>
            </div>
          </div>
          
          <div style="width: 100%; height: 1px; background: var(--border-slate); margin: 14px 0;"></div>
          
          <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr)); gap: 12px;">
            <div>
              <div style="font-size: 9px; color: var(--text-muted); text-transform: uppercase;">Breadth (50d EMA)</div>
              <div style="font-size: 12.5px; font-weight: 700; color: #fff; margin-top: 2px; font-family: var(--font-mono);">${document.getElementById('regimeBreadth')?.textContent || '—'}</div>
            </div>
            <div>
              <div style="font-size: 9px; color: var(--text-muted); text-transform: uppercase;">Index EMA Levels</div>
              <div style="font-size: 11.5px; font-weight: 600; color: #fff; margin-top: 2px; font-family: var(--font-mono);">${document.getElementById('regimeEMAs')?.textContent || '—'}</div>
            </div>
            <div>
              <div style="font-size: 9px; color: var(--text-muted); text-transform: uppercase;">ATR Volatility</div>
              <div style="font-size: 12.5px; font-weight: 700; color: #fff; margin-top: 2px; font-family: var(--font-mono);">${document.getElementById('regimeATR')?.textContent || '—'}</div>
            </div>
            <div>
              <div style="font-size: 9px; color: var(--text-muted); text-transform: uppercase;">Pullback from High</div>
              <div style="font-size: 12.5px; font-weight: 700; color: #fff; margin-top: 2px; font-family: var(--font-mono);">${document.getElementById('regimeHigh')?.textContent || '—'}</div>
            </div>
          </div>
        </div>

        <!-- Today's Top Opportunity Card -->
        ${topOptHTML}
        
      </div>

      <!-- Quick Action Buttons -->
      <div>
        <div style="font-size: 10px; font-weight: 800; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 8px;">QUICK CORE OPERATIONS</div>
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px;">
          <div onclick="setTab('camarilla'); startScan();" class="top-pick-card" style="height: auto; padding: 14px; text-align: center; border-radius: 12px; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 6px;">
            <i class="ti ti-scan" style="font-size: 24px; color: var(--pro-buy);"></i>
            <span style="font-size: 12px; font-weight: 800; color: #fff;">Scan Market</span>
            <span style="font-size: 9px; color: var(--text-muted);">Trigger intraday run</span>
          </div>
          <div onclick="setTab('watchlist');" class="top-pick-card" style="height: auto; padding: 14px; text-align: center; border-radius: 12px; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 6px;">
            <i class="ti ti-star" style="font-size: 24px; color: var(--pro-watch);"></i>
            <span style="font-size: 12px; font-weight: 800; color: #fff;">Watchlist</span>
            <span style="font-size: 9px; color: var(--text-muted);">View saved trackers</span>
          </div>
          <div onclick="setTab('journal');" class="top-pick-card" style="height: auto; padding: 14px; text-align: center; border-radius: 12px; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 6px;">
            <i class="ti ti-notebook" style="font-size: 24px; color: #c084fc;"></i>
            <span style="font-size: 12px; font-weight: 800; color: #fff;">Trade Ledger</span>
            <span style="font-size: 9px; color: var(--text-muted);">View performance metrics</span>
          </div>
        </div>
      </div>

    </div>`;
}

// ── Alert Center States & Operations ──────────────────
let alertsList = [];
let alertedMilestones = {};

function addNotification(type, symbol, title, desc, data) {
  // Prevent duplicate alerts for same milestone within a session
  const key = symbol ? (symbol + '_' + type) : title;
  if (alertedMilestones[key]) return;
  alertedMilestones[key] = true;

  const now = new Date();
  const timeStr = now.toLocaleTimeString('en-IN', {hour: '2-digit', minute: '2-digit'});
  
  alertsList.unshift({
    id: 'a_' + Date.now() + '_' + Math.random().toString(36).substr(2, 4),
    type,
    symbol,
    title,
    desc,
    time: timeStr,
    data: data || null
  });

  // Limit to last 20 alerts
  if (alertsList.length > 20) alertsList.pop();

  updateAlertCenterUI();
}

function toggleAlertDropdown(event) {
  if (event) event.stopPropagation();
  const el = document.getElementById('alertDropdown');
  if (el) {
    el.style.display = el.style.display === 'none' ? 'block' : 'none';
  }
}

// Close alert dropdown if user clicks outside
document.addEventListener('click', (e) => {
  const dropdown = document.getElementById('alertDropdown');
  const btn = document.getElementById('alertBellBtn');
  if (dropdown && btn && !dropdown.contains(e.target) && !btn.contains(e.target)) {
    dropdown.style.display = 'none';
  }
});

function clearAlerts(event) {
  if (event) event.stopPropagation();
  alertsList = [];
  updateAlertCenterUI();
}

function updateAlertCenterUI() {
  const listEl = document.getElementById('alertList');
  const badge = document.getElementById('alertCountBadge');
  if (!listEl) return;

  if (alertsList.length === 0) {
    listEl.innerHTML = `<div style="text-align:center; padding:20px; color:var(--text-muted); font-size:11px;">
      <i class="ti ti-bell-off" style="font-size:24px; display:block; margin-bottom:6px;"></i> No new notifications
    </div>`;
    if (badge) badge.style.display = 'none';
    return;
  }

  if (badge) {
    badge.textContent = alertsList.length;
    badge.style.display = 'flex';
  }

  listEl.innerHTML = alertsList.map(a => {
    let icon = "🔔";
    let iconBg = "rgba(245,158,11,0.12)";
    let iconCol = "var(--pro-watch)";
    
    if (a.type === 't1' || a.type === 't2' || a.type === 'signal_bull') {
      icon = "📈";
      iconBg = "rgba(16,185,129,0.12)";
      iconCol = "var(--pro-buy)";
    } else if (a.type === 'sl' || a.type === 'signal_bear') {
      icon = "📉";
      iconBg = "rgba(244,63,94,0.12)";
      iconCol = "var(--pro-sell)";
    } else if (a.type === 'regime') {
      icon = "⚖️";
      iconBg = "rgba(59,130,246,0.12)";
      iconCol = "var(--pro-electric)";
    }

    let logBtn = "";
    if (a.data) {
      logBtn = `<button onclick="logTradeFromRow(event, ${JSON.stringify(a.data).replace(/"/g,"'")})" style="background:var(--pro-electric); color:#fff; border:none; border-radius:4px; padding:2.5px 7px; font-size:9.5px; font-weight:800; cursor:pointer; margin-left:auto;">+ Log</button>`;
    }

    return `
      <div style="background:var(--bg-inner); border:1px solid var(--border-slate); border-radius:8px; padding:10px; display:flex; gap:10px; align-items:start; text-align:left;">
        <div style="width:28px; height:28px; border-radius:6px; background:${iconBg}; display:flex; align-items:center; justify-content:center; color:${iconCol}; font-size:14px; flex-shrink:0;">
          ${icon}
        </div>
        <div style="flex:1; min-width:0;">
          <div style="display:flex; justify-content:space-between; align-items:center;">
            <span style="font-weight:800; font-size:11px; color:#fff; text-overflow:ellipsis; overflow:hidden; white-space:nowrap;">${a.title}</span>
            <span style="font-family:var(--font-mono); font-size:9px; color:var(--text-muted); margin-left:6px; flex-shrink:0;">${a.time}</span>
          </div>
          <p style="font-size:10px; color:var(--text-muted); margin-top:2px; line-height:1.3;">${a.desc}</p>
          <div style="margin-top:6px; display:flex; align-items:center;">
            ${logBtn}
          </div>
        </div>
      </div>`;
  }).join('');
}

function setSubTab(t){
  activeSubTab = t;
  document.querySelectorAll('.sub-btn').forEach(el => el.classList.remove('active'));
  document.getElementById('sub-' + t).classList.add('active');
  
  expandedSymbol = null;
  render();
}

// EMA toggles
function toggleEMA(cb,k){
  emaReq[k]=cb.checked;
  document.getElementById('c'+k).classList.toggle('on',cb.checked);
  savePrefs(); render();
}

// Sort
function sortBy(col){
  sortState={col, desc: sortState.col===col ? !sortState.desc : true};
  document.querySelectorAll('.sort-icon').forEach(el=>el.innerHTML='<i class="ti ti-selector"></i>');
  const el=document.getElementById('si-'+col);
  if(el) el.innerHTML=sortState.desc?'<i class="ti ti-chevron-down"></i>':'<i class="ti ti-chevron-up"></i>';
  render();
}

// Ticker Live Ticking Mock engine (drifts F&O metrics to show live visual indicators)
function runLiveTicker() {
  if (marketClosed) return;

  stocks = stocks.map(stock => {
    // Generate ±0.1% price ticks
    const pctDrift = (Math.random() * 0.2 - 0.1) / 100;
    const oldPrice = stock.price;
    const newPrice = parseFloat((oldPrice * (1 + pctDrift)).toFixed(2));
    const chgDiff = parseFloat((((newPrice - stock.prevClose) / stock.prevClose) * 100).toFixed(2));

    // Dynamic RSI calculation if not already present
    let rsi = stock.rsi || (stock.signal_type === 'Bull' ? 55 + (stock.score % 25) : 45 - (stock.score % 25));
    rsi = rsi + (Math.random() > 0.5 ? 1 : -1);
    rsi = Math.min(85, Math.max(15, rsi));

    // Sparkline history shifting
    const newSpark = [...(stock.sparkline || [40,42,41,45,48,52,55]).slice(1), rsi];

    // Live alert checks: stop loss & target hits
    const entryP = stock.entry || stock.price || 0;
    const targets1 = stock.target ? stock.target : entryP * 1.015;
    const stopLosses = stock.stop_loss ? stock.stop_loss : entryP * 0.985;
    
    if (newPrice <= stopLosses && oldPrice > stopLosses) {
      addNotification('sl', stock.symbol, `${stock.symbol} SL Hit 🚨`, `${stock.symbol} touched Stop Loss support at ₹${newPrice.toFixed(1)}`, {
        symbol: stock.symbol,
        signal_type: stock.signal_type,
        conf_grade: stock.confidence || 'A',
        raw_score: stock.score,
        regime_score: stock.score,
        regime: window.niftyRegime || 'NEUTRAL',
        entry_price: entryP,
        stop_loss: stopLosses,
        target_t1: targets1,
        target_t2: stock.target2 || entryP * 1.03,
        risk_pct: stock.risk_percentage || 1.5,
        rr_ratio: stock.rr || 2.0
      });
    } else if (newPrice >= targets1 && oldPrice < targets1) {
      addNotification('t1', stock.symbol, `${stock.symbol} Target T1 Hit 🎉`, `${stock.symbol} reached target T1 resistance at ₹${newPrice.toFixed(1)}`, {
        symbol: stock.symbol,
        signal_type: stock.signal_type,
        conf_grade: stock.confidence || 'A',
        raw_score: stock.score,
        regime_score: stock.score,
        regime: window.niftyRegime || 'NEUTRAL',
        entry_price: entryP,
        stop_loss: stopLosses,
        target_t1: targets1,
        target_t2: stock.target2 || entryP * 1.03,
        risk_pct: stock.risk_percentage || 1.5,
        rr_ratio: stock.rr || 2.0
      });
    }

    return {
      ...stock,
      price: newPrice,
      change: chgDiff,
      rsi: rsi,
      sparkline: newSpark,
      lastTickDirection: newPrice > oldPrice ? "up" : newPrice < oldPrice ? "down" : null
    };
  });

  // Dynamic PCR drift
  pcrVal = parseFloat(Math.min(1.6, Math.max(0.6, pcrVal + (Math.random() * 0.04 - 0.02))).toFixed(2));
  bankPcrVal = parseFloat(Math.min(1.6, Math.max(0.6, bankPcrVal + (Math.random() * 0.04 - 0.02))).toFixed(2));
  fiiNetVal += Math.round(Math.random() * 40 - 20);
  diiNetVal += Math.round(Math.random() * 40 - 18);

  updateSidebarWidgets();
  render(true); // render with isTick = true to avoid collapsing row inputs
}

setInterval(runLiveTicker, 3500);

// Update Side Panel Widgets UI
function updateSidebarWidgets() {
  // 1. PCR Widget
  const pcrEl = document.getElementById('oi-pcr');
  if (pcrEl) {
    pcrEl.textContent = pcrVal.toFixed(2);
    pcrEl.className = pcrVal >= 1.2 ? 'up' : pcrVal <= 0.8 ? 'dn' : 'neutral';
  }

  // 1c. Bank Nifty PCR Widget
  const bnPcrEl = document.getElementById('bn-oi-pcr');
  if (bnPcrEl) {
    bnPcrEl.textContent = bankPcrVal.toFixed(2);
    bnPcrEl.className = bankPcrVal >= 1.2 ? 'up' : bankPcrVal <= 0.8 ? 'dn' : 'neutral';
  }
  
  // 1b. Dynamic Options Chain strikes aligned with current Nifty close
  const baseStrike = Math.round(niftyClosePrice / 100) * 100;
  const maxPainVal = baseStrike;
  const resistanceVal = baseStrike + 100;
  const supportVal = baseStrike - 200;

  const mpEl = document.getElementById('oi-maxpain');
  if (mpEl) mpEl.textContent = maxPainVal.toLocaleString('en-IN');

  const resEl = document.getElementById('oi-resistance');
  if (resEl) resEl.textContent = resistanceVal.toLocaleString('en-IN');

  const supEl = document.getElementById('oi-support');
  if (supEl) supEl.textContent = supportVal.toLocaleString('en-IN');

  const trapTitle = document.getElementById('oi-trap-title');
  const trapDesc = document.getElementById('oi-trap-desc');
  const trapBox = document.getElementById('oi-trap-box');
  if (trapBox) {
    if (pcrVal > 1.25) {
      if (trapTitle) {
        trapTitle.textContent = "CE WRITERS TRAPPED";
        trapTitle.style.color = "#34d399";
      }
      if (trapDesc) {
        trapDesc.textContent = `Short covering rally likely at ${resistanceVal.toLocaleString('en-IN')}!`;
        trapDesc.style.color = "#10b981";
      }
      const icon = trapBox.querySelector('i');
      if (icon) icon.style.color = "#34d399";
      trapBox.style.backgroundColor = "rgba(6, 78, 59, 0.4)";
      trapBox.style.borderColor = "rgba(16, 185, 129, 0.6)";
      trapBox.style.display = 'flex';
    } else if (pcrVal < 0.75) {
      if (trapTitle) {
        trapTitle.textContent = "PE WRITERS TRAPPED";
        trapTitle.style.color = "#f87171";
      }
      if (trapDesc) {
        trapDesc.textContent = `Panic sell-off / Breakdown likely at ${supportVal.toLocaleString('en-IN')}!`;
        trapDesc.style.color = "#ef4444";
      }
      const icon = trapBox.querySelector('i');
      if (icon) icon.style.color = "#f87171";
      trapBox.style.backgroundColor = "rgba(127, 29, 29, 0.4)";
      trapBox.style.borderColor = "rgba(239, 68, 68, 0.6)";
      trapBox.style.display = 'flex';
    } else {
      trapBox.style.display = 'none';
    }
  }

  // 1d. Dynamic Bank Nifty Option strikes
  const bankBaseStrike = Math.round(bankNiftyClosePrice / 100) * 100;
  const bankMaxPain = bankBaseStrike;
  const bankResistance = bankBaseStrike + 200;
  const bankSupport = bankBaseStrike - 400;

  const bnMpEl = document.getElementById('bn-oi-maxpain');
  if (bnMpEl) bnMpEl.textContent = bankMaxPain.toLocaleString('en-IN');

  const bnResEl = document.getElementById('bn-oi-resistance');
  if (bnResEl) bnResEl.textContent = bankResistance.toLocaleString('en-IN');

  const bnSupEl = document.getElementById('bn-oi-support');
  if (bnSupEl) bnSupEl.textContent = bankSupport.toLocaleString('en-IN');

  const bnTrapTitle = document.getElementById('bn-oi-trap-title');
  const bnTrapDesc = document.getElementById('bn-oi-trap-desc');
  const bnTrapBox = document.getElementById('bn-oi-trap-box');
  if (bnTrapBox) {
    if (bankPcrVal > 1.25) {
      if (bnTrapTitle) {
        bnTrapTitle.textContent = "CE WRITERS TRAPPED";
        bnTrapTitle.style.color = "#34d399";
      }
      if (bnTrapDesc) {
        bnTrapDesc.textContent = `Short covering rally likely at ${bankResistance.toLocaleString('en-IN')}!`;
        bnTrapDesc.style.color = "#10b981";
      }
      const icon = bnTrapBox.querySelector('i');
      if (icon) icon.style.color = "#34d399";
      bnTrapBox.style.backgroundColor = "rgba(6, 78, 59, 0.4)";
      bnTrapBox.style.borderColor = "rgba(16, 185, 129, 0.6)";
      bnTrapBox.style.display = 'flex';
    } else if (bankPcrVal < 0.75) {
      if (bnTrapTitle) {
        bnTrapTitle.textContent = "PE WRITERS TRAPPED";
        bnTrapTitle.style.color = "#f87171";
      }
      if (bnTrapDesc) {
        bnTrapDesc.textContent = `Panic sell-off / Breakdown likely at ${bankSupport.toLocaleString('en-IN')}!`;
        bnTrapDesc.style.color = "#ef4444";
      }
      const icon = bnTrapBox.querySelector('i');
      if (icon) icon.style.color = "#f87171";
      bnTrapBox.style.backgroundColor = "rgba(127, 29, 29, 0.4)";
      bnTrapBox.style.borderColor = "rgba(239, 68, 68, 0.6)";
      bnTrapBox.style.display = 'flex';
    } else {
      bnTrapBox.style.display = 'none';
    }
  }
  


  // 2. FII/DII Net Flow
  const total = Math.abs(fiiNetVal) + Math.abs(diiNetVal);
  const fiiPct = total > 0 ? (Math.abs(fiiNetVal) / total) * 100 : 50;
  const diiPct = total > 0 ? (Math.abs(diiNetVal) / total) * 100 : 50;
  
  const fiiVal = document.getElementById('flow-fii-val');
  if (fiiVal) {
    fiiVal.textContent = (fiiNetVal > 0 ? '+' : '') + '₹' + fiiNetVal.toLocaleString('en-IN') + ' Cr';
    fiiVal.className = fiiNetVal >= 0 ? 'up' : 'dn';
  }
  const fiiBar = document.getElementById('flow-fii-bar');
  if (fiiBar) fiiBar.style.width = fiiPct + '%';

  const diiVal = document.getElementById('flow-dii-val');
  if (diiVal) {
    diiVal.textContent = (diiNetVal > 0 ? '+' : '') + '₹' + diiNetVal.toLocaleString('en-IN') + ' Cr';
    diiVal.className = diiNetVal >= 0 ? 'up' : 'dn';
  }
  const diiBar = document.getElementById('flow-dii-bar');
  if (diiBar) diiBar.style.width = diiPct + '%';

  const flowBias = document.getElementById('flow-bias');
  const netDominance = fiiNetVal + diiNetVal;
  if (flowBias) {
    flowBias.textContent = netDominance >= 0 ? 'DII Dominant' : 'FII Selling';
    flowBias.style.color = netDominance >= 0 ? 'var(--pro-buy)' : 'var(--pro-sell)';
  }

  // 3. Sector rotation strength — activeSectors populated by fetchSectorRotation() every 5 min.
  // Do NOT blend with scan results; show pure NSE index % change.
  const hGrid = document.getElementById('heatmapGrid');
  if (hGrid) {
    hGrid.innerHTML = activeSectors.map(s => {
      let colors = "";
      if (s.trend === "up") {
        colors = "background: rgba(16, 185, 129, 0.08); border-color: rgba(16, 185, 129, 0.15); color: var(--pro-buy)";
      } else if (s.trend === "down") {
        colors = "background: rgba(244, 63, 94, 0.08); border-color: rgba(244, 63, 94, 0.15); color: var(--pro-sell)";
      } else {
        colors = "background: var(--bg-inner); border-color: var(--border-slate); color: var(--text-muted)";
      }
      const isActive = activeSector === s.name;
      return `<button
        onclick="toggleSectorFilter('${s.name}')"
        style="${colors}"
        class="heatmap-btn ${isActive ? 'active' : ''}"
      >
        <span style="font-weight:700">${s.name}</span>
        <div style="font-size:9.5px;font-family:var(--font-mono);display:flex;justify-content:space-between;width:100%">
          <span>${s.change > 0 ? '+' : ''}${Number(s.change).toFixed(2)}%</span>
          <span>${s.trend === 'up' ? '↑' : s.trend === 'down' ? '↓' : '→'}</span>
        </div>
      </button>`;
    }).join('');
  }

  // 4. AI Terminal Intelligence — dynamic multi-factor commentary
  const tipEl = document.getElementById('sidebarProTip');
  const tipTime = document.getElementById('ai-tip-time');
  const tipTags = document.getElementById('ai-tip-tags');
  if (tipEl) {
    const bullSigs = stocks.filter(s => s.signal_type === 'Bull').length;
    const bearSigs = stocks.filter(s => s.signal_type === 'Bear').length;
    const entrySigs = stocks.filter(s => Math.abs(s.price - s.entry) / (s.entry||1) <= 0.02).length;
    const hvSigs = stocks.filter(s => s.vol_ratio >= 3).length;
    const avgScore = stocks.length ? Math.round(stocks.reduce((a,s)=>a+(s.score||0),0)/stocks.length) : 0;
    const topSector = (() => {
      const sec = {};
      activeSectors.forEach(s => { if (s.trend==='up') sec[s.name] = (s.change||0); });
      const top = Object.entries(sec).sort((a,b)=>b[1]-a[1])[0];
      return top ? top[0] : null;
    })();
    const regime = window.niftyRegime || 'NEUTRAL';
    const fiiStr = fiiNetVal < 0 ? `FII selling ₹${Math.abs(Math.round(fiiNetVal)).toLocaleString('en-IN')} Cr` : `FII buying ₹${Math.round(fiiNetVal).toLocaleString('en-IN')} Cr`;
    const diiStr = diiNetVal > 0 ? `DII buying ₹${Math.round(diiNetVal).toLocaleString('en-IN')} Cr` : `DII selling`;
    const niftyStr = niftyClosePrice ? `Nifty ${niftyClosePrice.toLocaleString('en-IN')}` : '';
    // Build contextual AI commentary
    let lines = [];
    if (stocks.length === 0) {
      lines.push('No scan data yet. Click Run Live Scan to populate signals and get AI analysis.');
    } else {
      // Signal summary
      lines.push(`${stocks.length} signals: ${bullSigs} bullish ↑, ${bearSigs} bearish ↓${entrySigs ? `, ${entrySigs} within entry range` : ''}.`);
      // Institutional flow
      if (Math.abs(fiiNetVal) > 500) lines.push(`${fiiStr} — ${fiiNetVal < -1000 ? 'institutional headwind on longs' : 'institutional tailwind'}. ${diiStr}.`);
      // Sector
      if (topSector) lines.push(`${topSector} sector showing relative strength — concentrate signals here.`);
      // Volume
      if (hvSigs > 0) lines.push(`${hvSigs} stocks with 3x+ volume surge — high conviction breakout candidates.`);
      // PCR commentary
      if (pcrVal > 1.3) lines.push(`PCR ${pcrVal.toFixed(2)}: heavy PUT writing — options market backing the upside.`);
      else if (pcrVal < 0.9) lines.push(`PCR ${pcrVal.toFixed(2)}: CALL writing dominant — options market hedging downside.`);
      // Regime guidance
      if (regime === 'STRONG_BEAR') lines.push('⚠️ STRONG BEAR regime: reduce long exposure, prioritise short setups, widen SL buffer.');
      else if (regime === 'BEAR') lines.push('Bear regime active: focus on high-score bearish signals and cash preservation.');
      else if (regime === 'STRONG_BULL') lines.push('Strong Bull regime: lean into breakouts above H3, trail stops generously.');
      else if (regime === 'BULL') lines.push('Bull regime: favour BUY signals with DII flow support.');
      else lines.push(`Neutral regime — trade both sides selectively. Avg signal score ${avgScore}/100.`);
    }
    tipEl.textContent = lines.join(' ');
    if (tipTime) tipTime.textContent = new Date().toLocaleTimeString('en-IN', {hour:'2-digit',minute:'2-digit'});
    // Tags
    if (tipTags) {
      const tags = [];
      if (bullSigs > bearSigs) tags.push({t:'Bullish Bias',c:'#34d399',bg:'rgba(16,185,129,0.1)'});
      else if (bearSigs > bullSigs) tags.push({t:'Bearish Bias',c:'#f87171',bg:'rgba(244,63,94,0.1)'});
      if (hvSigs > 0) tags.push({t:`${hvSigs} Vol Spike`,c:'#fbbf24',bg:'rgba(245,158,11,0.1)'});
      if (pcrVal > 1.3) tags.push({t:'PCR Bullish',c:'#60a5fa',bg:'rgba(59,130,246,0.1)'});
      if (fiiNetVal < -1000) tags.push({t:'FII Selling',c:'#f87171',bg:'rgba(244,63,94,0.1)'});
      if (diiNetVal > 500) tags.push({t:'DII Buying',c:'#34d399',bg:'rgba(16,185,129,0.1)'});
      tipTags.innerHTML = tags.map(tg=>`<span style="font-size:8.5px;font-weight:700;color:${tg.c};background:${tg.bg};border:1px solid ${tg.c}30;border-radius:10px;padding:2px 7px">${tg.t}</span>`).join('');
    }
  }
}

function toggleSectorFilter(name) {
  activeSector = (activeSector === name) ? null : name;
  expandedSymbol = null;
  updateSidebarWidgets();
  render();
}

// Offline Mock F&O Stocks dataset from the React ProTrader prototype
const mockStocks = [
  { symbol: "NMDC", name: "", price: 92.90, change: 3.0, vol_ratio: 4.2, rsi: 25, signal_type: "Bear", candle: "Bear", tf: "1h", sector: "Metals", score: 78, confidence: "B", entry: 95.0, entry_status: 'STALE', stop_loss: 97.24, target: 85.5, target2: 81.3, prevClose: 90.19, sparkline: [92.28, 92.28, 87.99, 92.37, 92.90] },
  { symbol: "HDFCBANK", name: "HDFC Bank", price: 744.0, change: 2.4, vol_ratio: 2.3, rsi: 64, signal_type: "Bull", candle: "Bull", tf: "1h", sector: "Banking", score: 85, confidence: "A", entry: 740.0, entry_status: 'FRESH', stop_loss: 725.0, target: 755.0, target2: 765.0, prevClose: 726.56, sparkline: [40, 42, 41, 45, 48, 52, 55] },
  { symbol: "RELIANCE", name: "Reliance Ind.", price: 2981.0, change: 1.7, vol_ratio: 1.8, rsi: 58, signal_type: "Bull", candle: "Bull", tf: "4h", sector: "Energy", score: 78, confidence: "B", entry: 2950.0, entry_status: 'FRESH', stop_loss: 2920.0, target: 3020.0, target2: 3050.0, prevClose: 2931.17, sparkline: [30, 32, 31, 35, 37, 39, 42] },
  { symbol: "TMCV", name: "TMCV (Tata Motors)", price: 374.0, change: 3.1, vol_ratio: 3.5, rsi: 71, signal_type: "Bull", candle: "Bull", tf: "1h", sector: "Auto", score: 92, confidence: "A+", entry: 370.0, entry_status: 'EXTENDED', stop_loss: 362.0, target: 382.0, target2: 390.0, prevClose: 362.75, sparkline: [50, 55, 53, 60, 65, 68, 72] },
  { symbol: "INFY", name: "Infosys", price: 1823.7, change: 0.6, vol_ratio: 1.1, rsi: 54, signal_type: "Bull", candle: "Bull", tf: "4h", sector: "IT", score: 68, confidence: "B", entry: 1810.0, entry_status: 'FRESH', stop_loss: 1795.0, target: 1845.0, target2: 1860.0, prevClose: 1812.82, sparkline: [44, 45, 44, 46, 47, 48, 49] },
  { symbol: "SBIN", name: "SBI", price: 812.9, change: 2.8, vol_ratio: 2.9, rsi: 67, signal_type: "Bull", candle: "Bull", tf: "1h", sector: "Banking", score: 80, confidence: "B", entry: 800.0, entry_status: 'EXTENDED', stop_loss: 788.0, target: 825.0, target2: 835.0, prevClose: 790.75, sparkline: [38, 40, 42, 45, 47, 50, 54] },
  { symbol: "DRREDDY", name: "Dr. Reddy's", price: 5412.0, change: -1.2, vol_ratio: 0.8, rsi: 38, signal_type: "Bear", candle: "Bear", tf: "4h", sector: "Pharma", score: 35, confidence: "C", entry: 5450.0, entry_status: 'BELOW_PIVOT', stop_loss: 5500.0, target: 5350.0, target2: 5300.0, prevClose: 5477.73, sparkline: [60, 58, 55, 52, 48, 45, 42] },
  { symbol: "COALINDIA", name: "Coal India", price: 472.6, change: -0.8, vol_ratio: 1.2, rsi: 41, signal_type: "Bear", candle: "Bear", tf: "1h", sector: "Metals", score: 38, confidence: "C", entry: 475.0, entry_status: 'BELOW_PIVOT', stop_loss: 480.0, target: 465.0, target2: 460.0, prevClose: 476.41, sparkline: [55, 52, 50, 47, 45, 43, 41] },
  { symbol: "DLF", name: "DLF Ltd.", price: 892.1, change: 4.2, vol_ratio: 4.1, rsi: 76, signal_type: "Bull", candle: "Bull", tf: "1h", sector: "Realty", score: 95, confidence: "A+", entry: 870.0, entry_status: 'EXTENDED', stop_loss: 855.0, target: 915.0, target2: 930.0, prevClose: 856.14, sparkline: [45, 50, 55, 62, 68, 75, 82] },
  { symbol: "MARUTI", name: "Maruti Suzuki", price: 12450.0, change: 1.9, vol_ratio: 1.6, rsi: 61, signal_type: "Bull", candle: "Bull", tf: "4h", sector: "Auto", score: 72, confidence: "B", entry: 12350.0, entry_status: 'FRESH', stop_loss: 12200.0, target: 12600.0, target2: 12700.0, prevClose: 12217.86, sparkline: [42, 44, 45, 47, 49, 51, 54] },
  { symbol: "WIPRO", name: "Wipro", price: 548.3, change: -0.4, vol_ratio: 0.9, rsi: 46, signal_type: "Bear", candle: "Bear", tf: "4h", sector: "IT", score: 44, confidence: "C", entry: 550.0, entry_status: 'BELOW_PIVOT', stop_loss: 555.0, target: 540.0, target2: 535.0, prevClose: 550.50, sparkline: [52, 50, 49, 48, 47, 46, 45] },
  { symbol: "ADANIENT", name: "Adani Ent.", price: 2634.0, change: 2.6, vol_ratio: 2.7, rsi: 65, signal_type: "Bull", candle: "Bull", tf: "1h", sector: "Infra", score: 82, confidence: "B", entry: 2600.0, entry_status: 'FRESH', stop_loss: 2560.0, target: 2680.0, target2: 2720.0, prevClose: 2567.25, sparkline: [36, 38, 40, 43, 46, 49, 53] },
  { symbol: "ITC", name: "ITC Ltd.", price: 448.7, change: 0.2, vol_ratio: 1.0, rsi: 51, signal_type: "Bull", candle: "Bull", tf: "4h", sector: "FMCG", score: 55, confidence: "B", entry: 445.0, entry_status: 'FRESH', stop_loss: 440.0, target: 455.0, target2: 460.0, prevClose: 447.80, sparkline: [48, 49, 48, 50, 49, 51, 50] }
];

// Pre-process signals
function preprocess(sigs){
  // 0. Enrich sectorMapping from live scan sector data
  enrichSectorMapping(sigs);

  // 1. Map all actual scanned signals
  const scanned = (sigs||[]).map(s=>{
    if (s.change === undefined || s.change === null || isNaN(s.change)) {
      let c = s.rs_pct || (s.score ? parseFloat(((s.score - 50) / 15).toFixed(2)) : parseFloat((Math.random() * 4 - 1.5).toFixed(2)));
      s.change = c;
    }
    if (s.prevClose === undefined || s.prevClose === null || isNaN(s.prevClose)) {
      s.prevClose = s.price / (1 + (s.change / 100));
    }
    if (!s.sparkline || s.sparkline.length === 0) {
      s.sparkline = [40, 42, 41, 45, 48, 52, 55];
    }
    s.isMock = false;
    return s;
  });

  const demoDataEl = document.getElementById('demoDataSelect');
  const includeDemo = !demoDataEl || demoDataEl.value === 'show';

  // 2. Identify which mock stocks are already scanned
  const scannedSymbols = new Set(scanned.map(s => s.symbol.toUpperCase()));

  // 3. Filter mock stocks that are NOT in the scanned list, and add them (if demo data is enabled)
  const inactiveMock = includeDemo ? mockStocks.filter(m => !scannedSymbols.has(m.symbol.toUpperCase())).map(m => {
    // Clone to prevent mutating global mockStocks array
    const cloned = Object.assign({}, m);
    cloned.isMock = true;
    // Generate beautiful realistic quant indicators for mock stocks
    if (cloned.days === undefined) {
      cloned.days = cloned.symbol === 'HDFCBANK' ? 12 : 
                    cloned.symbol === 'TMCV' ? 5 : 
                    cloned.symbol === 'SBIN' ? 18 : 
                    cloned.symbol === 'DLF' ? 8 : 
                    cloned.symbol === 'ADANIENT' ? 24 : Math.floor(Math.random() * 30) + 3;
    }
    if (cloned.turnover_score === undefined) {
      cloned.turnover_score = cloned.symbol === 'HDFCBANK' ? 1450.2 : 
                              cloned.symbol === 'TMCV' ? 890.5 : 
                              cloned.symbol === 'SBIN' ? 1120.8 : 
                              cloned.symbol === 'DLF' ? 420.4 : 
                              cloned.symbol === 'ADANIENT' ? 710.1 : Math.floor(Math.random() * 800) + 100;
    }
    // Also EMAs alignment dots:
    ['ema10_pass', 'ema20_pass', 'ema50_pass', 'ema200_pass'].forEach(k => {
      if (cloned[k] === undefined) {
        cloned[k] = Math.random() > 0.25; // 75% chance of passing for mock bullish setups
      }
    });
    // RS vs Nifty:
    if (cloned.rs_pct === undefined) {
      cloned.rs_pct = parseFloat((Math.random() * 4.5 + 0.5).toFixed(1)); // e.g. +2.4%
    }
    return cloned;
  }) : [];

  // 4. Combine them so both real scanned signals and beautiful mock assets are available
  const combined = [...scanned, ...inactiveMock].map(s => {
    const dist = Math.abs(s.dist_from_entry !== undefined ? s.dist_from_entry : (s.entry ? ((s.price - s.entry) / s.entry) * 100 : 0));
    let penalty = 0;
    if (dist > 10.0) penalty = 25;
    else if (dist > 5.0) penalty = 15;
    else if (dist > 2.0) penalty = 5;

    // Apply penalty to score
    if (s.score !== undefined) {
      s.score = Math.max(0, s.score - penalty);
    }
    
    // Recalibrate confidence based on penalized score
    if (s.score !== undefined) {
      if (s.score >= 90) s.confidence = 'A+';
      else if (s.score >= 80) s.confidence = 'A';
      else if (s.score >= 65) s.confidence = 'B';
      else s.confidence = 'C';
    }

    // Trigger breakout signal alert if score is >= 85
    if (s.score >= 85) {
      const entryVal = s.entry ? s.entry : s.price;
      const targets1 = s.target ? s.target : entryVal * 1.015;
      const slVal = s.stop_loss ? s.stop_loss : entryVal * 0.985;
      const isBull = s.signal_type === 'Bull';
      addNotification(isBull ? 'signal_bull' : 'signal_bear', s.symbol, `Breakout Alert: ${s.symbol} 🔥`, `Camarilla breakout detected with Score ${s.score}/100`, {
        symbol: s.symbol,
        signal_type: s.signal_type,
        conf_grade: s.confidence || 'A',
        raw_score: s.score,
        regime_score: s.score,
        regime: window.niftyRegime || 'NEUTRAL',
        entry_price: entryVal,
        stop_loss: slVal,
        target_t1: targets1,
        target_t2: s.target2 || entryVal * 1.03,
        risk_pct: s.risk_percentage || 1.5,
        rr_ratio: s.rr || 2.0
      });
    }
    return s;
  });
  return combined;
}

// Update counts on tabs
function updateCounts(){
  const srch=document.getElementById('tickerSearch').value.toLowerCase();
  const vm=parseFloat(document.getElementById('volMult').value);
  
  let base=stocks.filter(s=>s.vol_ratio>=vm && (!srch||s.symbol.toLowerCase().includes(srch)));
  if (activeSector) {
    base = base.filter(s => getStockSector(s) === activeSector);
  }

  const counts={
    bullish:   base.filter(s=>s.signal_type==='Bull' && s.candle==='Bull').length,
    bearish:   base.filter(s=>s.signal_type==='Bear' && s.candle==='Bear').length,
    mixed:     base.filter(s=>(s.signal_type==='Bull'&&s.candle==='Bear')||(s.signal_type==='Bear'&&s.candle==='Bull')).length,
    entry:     base.filter(s=>Math.abs(s.price-s.entry)/s.entry<=0.02).length,
    fresh:     base.filter(s=>Math.abs(s.dist_from_entry !== undefined ? s.dist_from_entry : (s.entry ? ((s.price - s.entry) / s.entry) * 100 : 0)) <= 5.0).length,
    hv:        base.filter(s=>s.days<=10).length,
    camarilla: base.length,
    watchlist: base.filter(s=>watchlist.includes(s.symbol)).length,
  };
  
  // Update sub-tabs counts
  document.getElementById('cnt-camarilla').textContent = counts.camarilla;
  document.getElementById('sub-cnt-entry').textContent = counts.entry;
  const freshEl = document.getElementById('sub-cnt-fresh');
  if (freshEl) freshEl.textContent = counts.fresh;
  document.getElementById('sub-cnt-bullish').textContent = counts.bullish;
  document.getElementById('sub-cnt-bearish').textContent = counts.bearish;
  document.getElementById('sub-cnt-mixed').textContent = counts.mixed;
  document.getElementById('sub-cnt-hv').textContent = counts.hv;
  document.getElementById('cnt-watchlist').textContent = counts.watchlist;

  const statsSpan = document.getElementById('liveStatsSpan');
  const badgeText = document.getElementById('liveBadgeText');
  if (stocks.length === 0) {
    if (badgeText) badgeText.style.display = 'inline';
    if (statsSpan) statsSpan.style.display = 'none';
  } else {
    if (badgeText) badgeText.style.display = 'none';
    if (statsSpan) statsSpan.style.display = 'inline';
    const bullEl = document.getElementById('bull-count');
    const bearEl = document.getElementById('bear-count');
    if (bullEl) bullEl.textContent = counts.bullish;
    if (bearEl) bearEl.textContent = counts.bearish;
  }
}

// Toggle Row Expand details
function toggleRowExpand(symbol, event) {
  if (event.target.closest('button') || event.target.closest('.star-icon')) {
    return;
  }
  expandedSymbol = (expandedSymbol === symbol) ? null : symbol;
  render();
}

// Render expanded stock detail rows with ApexCharts area graphs
function initExpandedApexChart(stock) {
  const chartEl = document.querySelector(`#chart-${stock.symbol}`);
  if (!chartEl) return;

  const basePrice = stock.price;
  const sparkData = stock.sparkline || [40, 42, 41, 45, 48, 52, 55];
  const chartData = sparkData.map((val, idx) => {
    return parseFloat((basePrice * (0.97 + (val / 180))).toFixed(2));
  });

  const options = {
    series: [{
      name: 'Intraday LTP',
      data: chartData
    }],
    chart: {
      type: 'area',
      height: 180,
      toolbar: { show: false },
      sparkline: { enabled: false },
      background: 'transparent',
      foreColor: (localStorage.getItem('nse_theme') === 'normal' ? '#4B5563' : '#94a3b8')
    },
    colors: [stock.signal_type === 'Bear' ? '#F43F5E' : '#10B981'],
    stroke: {
      curve: 'smooth',
      width: 2
    },
    fill: {
      type: 'gradient',
      gradient: {
        shadeIntensity: 1,
        opacityFrom: 0.2,
        opacityTo: 0,
        stops: [0, 95]
      }
    },
    grid: {
      borderColor: (localStorage.getItem('nse_theme') === 'normal' ? '#E5E7EB' : '#1f2937'),
      strokeDashArray: 3,
      xaxis: { lines: { show: false } },
      yaxis: { lines: { show: true } }
    },
    xaxis: {
      categories: ['10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00'],
      labels: { style: { colors: (localStorage.getItem('nse_theme') === 'normal' ? '#4B5563' : '#94a3b8'), fontSize: '9px', fontFamily: 'var(--font-sans)' } },
      axisBorder: { show: false },
      axisTicks: { show: false }
    },
    yaxis: {
      labels: { 
        style: { colors: (localStorage.getItem('nse_theme') === 'normal' ? '#4B5563' : '#94a3b8'), fontSize: '9px', fontFamily: 'var(--font-mono)' },
        formatter: (v) => '₹' + v.toFixed(0)
      }
    },
    tooltip: {
      theme: (localStorage.getItem('nse_theme') === 'normal' ? 'light' : 'dark'),
      x: { show: true },
      marker: { show: false }
    }
  };

  const chart = new ApexCharts(chartEl, options);
  chart.render();
}

function formatFreshness(scannedTime, scannedTimestamp, isMock) {
  if (isMock) {
    return `<div style="font-size:9.5px;color:#94a3b8;font-weight:700">Demo Data</div>`;
  }
  if (!scannedTime) {
    const now = new Date();
    const timeStr = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: true });
    return `<div style="font-size:9.5px;color:${marketClosed ? '#d97706' : 'var(--text-muted)'}">Updated: ${timeStr}</div>`;
  }
  
  let displayTime = scannedTime;
  try {
    const parts = scannedTime.split(':');
    let hr = parseInt(parts[0]);
    const min = parts[1];
    const ampm = hr >= 12 ? 'PM' : 'AM';
    hr = hr % 12;
    hr = hr ? hr : 12;
    displayTime = `${String(hr).padStart(2, '0')}:${min} ${ampm}`;
  } catch (e) {}

  const nowSec = Date.now() / 1000;
  let diffMin = 0;
  if (scannedTimestamp) {
    diffMin = (nowSec - scannedTimestamp) / 60;
  } else {
    try {
      const now = new Date();
      const parts = scannedTime.split(':');
      const scanDate = new Date(now.getFullYear(), now.getMonth(), now.getDate(), parseInt(parts[0]), parseInt(parts[1]), 0);
      diffMin = (now - scanDate) / 60000;
    } catch (e) {
      diffMin = 0;
    }
  }

  if (marketClosed) {
    return `<div style="font-size:9.5px;color:#d97706;font-weight:700">Updated: ${displayTime}</div>`;
  }

  if (diffMin > 120) {
    return `<div style="font-size:9.5px;color:#ef4444;font-weight:700">STALE ⚠️</div>`;
  } else if (diffMin > 30) {
    return `<div style="font-size:9.5px;color:#f97316;font-weight:700">Updated: ${displayTime}</div>`;
  } else {
    return `<div style="font-size:9.5px;color:var(--text-muted)">Updated: ${displayTime}</div>`;
  }
}

function renderPriceCell(s) {
  if (s.hv_high && s.price > s.hv_high * 1.05) {
    return `<span style="color:var(--pro-sell);font-weight:700">⚠️ Data Error</span>`;
  }
  let priceStr = `₹${s.price.toLocaleString("en-IN", {minimumFractionDigits: 2})}`;
  if (marketClosed && !s.isMock) {
    priceStr += ` <span style="font-size:9px;color:#d97706;font-weight:700;margin-left:4px">(prev close)</span>`;
  }
  return priceStr;
}

// ── Main Render ───────────────────────────────────────────
function render(isTick = false){
  if (activeTab === 'home') {
    renderHome();
    return;
  }
  if (activeTab === 'journal') {
    renderJournal();
    return;
  }
  
  const tbody = document.getElementById('tblBody');
  const thead = document.getElementById('tblHead');
  const srch = document.getElementById('tickerSearch').value.toLowerCase();
  const vm = parseFloat(document.getElementById('volMult').value);

  // 1. Sector & Search pre-filter
  let f = stocks.filter(s => s.vol_ratio >= vm && (!srch || s.symbol.toLowerCase().includes(srch)));
  if (activeSector) {
    f = f.filter(s => getStockSector(s) === activeSector);
  }

  // 2. Tab filters
  if (activeTab === 'protrader') {
    f = f.filter(s => s.vol_ratio >= 1.2);
  } else if (activeTab === 'camarilla') {
    if(activeSubTab === 'bullish')        f = f.filter(s => s.signal_type === 'Bull' && s.candle === 'Bull');
    else if(activeSubTab === 'bearish')   f = f.filter(s => s.signal_type === 'Bear' && s.candle === 'Bear');
    else if(activeSubTab === 'mixed')     f = f.filter(s => (s.signal_type === 'Bull' && s.candle === 'Bear') || (s.signal_type === 'Bear' && s.candle === 'Bull'));
    else if(activeSubTab === 'entry')     f = f.filter(s => Math.abs(s.price - s.entry) / s.entry <= 0.02);
    else if(activeSubTab === 'fresh')     f = f.filter(s => s.entry_status === 'FRESH');
    else if(activeSubTab === 'hv')        f = f.filter(s => s.days <= 10);
  } else if (activeTab === 'watchlist') {
    f = f.filter(s => watchlist.includes(s.symbol));
  }

  // 3. Dynamic Sort
  f.sort((a,b) => {
    let va = a[sortState.col] ?? 0, vb = b[sortState.col] ?? 0;
    if(typeof va === 'string') va = va.toLowerCase(), vb = vb.toLowerCase();
    if(va < vb) return sortState.desc ? 1 : -1;
    if(va > vb) return sortState.desc ? -1 : 1;
    return 0;
  });

  // 4. Update Summary Cards
  let bulls=0, bears=0, hi3=0, scoreSum=0;
  f.forEach(s=>{if(s.signal_type==='Bear') bears++; else bulls++; if(s.vol_ratio>=3) hi3++; scoreSum+=s.score;});
  const avg = f.length ? Math.round(scoreSum / f.length) : 0;
  document.getElementById('sumBar').innerHTML=`
    <div class="si"><span class="lbl">Total Assets:</span><span class="val">${f.length}</span></div>
    <div class="si"><span class="lbl">Bullish strong:</span><span class="val g">${bulls}</span></div>
    <div class="si"><span class="lbl">Bearish weak:</span><span class="val r">${bears}</span></div>
    <div class="si"><span class="lbl">Vol Spike ≥3x:</span><span class="val y">${hi3}</span></div>
    <div class="si"><span class="lbl">Avg Quant Score:</span><span class="val">${avg}/100</span></div>`;

  // 5. Update "Top 5 picks" card panel if ProTrader tab active
  const picksPanel = document.getElementById('proTraderTopPicks');
  if (activeTab === 'protrader') {
    picksPanel.style.display = 'block';
    const top5 = [...stocks]
      .filter(s => s.signal_type === 'Bull')
      .sort((a, b) => b.vol_ratio - a.vol_ratio)
      .slice(0, 5);
      
    const top5Grid = document.getElementById('top5Grid');
    if (top5.length === 0) {
      top5Grid.innerHTML = `<div style="grid-column: span 5; text-align:center; color:var(--text-muted); font-size:11px">Waiting for active buying scans...</div>`;
    } else {
      top5Grid.innerHTML = top5.map((s, idx) => {
        const signalStyle = s.signal_type === 'Bull' ? 'sig-buy' : 'sig-sell';
        return `<div 
          onclick="toggleRowExpand('${s.symbol}', event)"
          style="border: 1px solid ${expandedSymbol === s.symbol ? 'var(--pro-electric)' : 'var(--border-slate)'}" 
          class="top-pick-card"
        >
          <div style="display:flex; justify-content:space-between; align-items:center">
            <span style="font-family:var(--font-mono); font-size:9.5px; font-weight:800; color:var(--text-muted)">#${idx+1}</span>
            <span class="${signalStyle}">${s.signal_type === 'Bull' ? 'BUY' : 'SELL'}</span>
          </div>
          <div style="margin-top:2px">
            <div class="font-extrabold text-white text-xs tracking-tight">
              ${s.symbol}
              ${s.isMock ? `<span class="demo-badge" style="background:rgba(148,163,184,0.1);color:#94a3b8;border:1px solid rgba(148,163,184,0.3);border-radius:4px;padding:1px 4px;font-size:7.5px;font-weight:800;margin-left:4px;vertical-align:middle">DEMO</span>` : ''}
            </div>
            <div style="font-size:9px; color:var(--text-muted); font-weight:600">${getStockSector(s)}</div>
          </div>
          <div style="display:flex; align-items:end; justify-content:space-between; margin-top:auto">
            <div>
              <div style="font-family:var(--font-mono); font-size:10px; font-weight:800">
                ${s.hv_high && s.price > s.hv_high * 1.05 ? '<span style="color:var(--pro-sell)">⚠️ Data Error</span>' : `₹${s.price.toFixed(1)}${marketClosed && !s.isMock ? ' <span style="font-size:7.5px;color:#d97706;font-weight:700">(prev close)</span>' : ''}`}
              </div>
              <div style="font-size:9px; font-family:var(--font-mono); font-weight:800; color:${s.change >= 0 ? 'var(--pro-buy)' : 'var(--pro-sell)'}">${s.change >= 0 ? '+' : ''}${s.change.toFixed(2)}%</div>
            </div>
            ${sparklineSVG(s.sparkline || [40,42,41,45,48,52,55], s.signal_type)}
          </div>
        </div>`;
      }).join('');
    }
  } else {
    picksPanel.style.display = 'none';
  }

  // 6. RENDER HEADERS BASED ON TAB
  if (activeTab === 'protrader') {
    thead.innerHTML = `<tr>
      <th onclick="sortBy('symbol')">Stock Symbol & 5d Sparkline <span class="sort-icon" id="si-symbol"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('price')" style="text-align:right">Last Trade (LTP) <span class="sort-icon" id="si-price"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('change')" style="text-align:right">Day Change% <span class="sort-icon" id="si-change"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('vol_ratio')" style="text-align:right">Volume Activity <span class="sort-icon" id="si-vol_ratio"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('rsi')" style="text-align:right">RSI Reading (14) <span class="sort-icon" id="si-rsi"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('tf')" style="text-align:center">Timeframe <span class="sort-icon" id="si-tf"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('signal_type')" style="text-align:center">Trend Signal <span class="sort-icon" id="si-signal_type"><i class="ti ti-selector"></i></span></th>
      <th>Trade Ledger</th>
    </tr>`;
  } else {
    thead.innerHTML = `<tr>
      <th onclick="sortBy('symbol')">Stock symbol & 5d Sparkline <span class="sort-icon" id="si-symbol"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('confidence')">Conf Grade <span class="sort-icon" id="si-confidence"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('score')">Score <span class="sort-icon" id="si-score"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('price')">LTP & Scanned <span class="sort-icon" id="si-price"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('entry')">Entry (Pivot) <span class="sort-icon" id="si-entry"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('dist_from_entry')">Dist% <span class="sort-icon" id="si-dist_from_entry"><i class="ti ti-selector"></i></span></th>
      <th>Entry Status</th>
      <th onclick="sortBy('stop_loss')">Stop Loss <span class="sort-icon" id="si-stop_loss"><i class="ti ti-selector"></i></span></th>
      <th>Breakout Targets (T1/T2)</th>
      <th onclick="sortBy('risk_percentage')">Risk% <span class="sort-icon" id="si-risk_percentage"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('rr')">R:R Ratio <span class="sort-icon" id="si-rr"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('rs_pct')" class="m-hide">RS vs Nifty <span class="sort-icon" id="si-rs_pct"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('days')" class="m-hide">52w Age <span class="sort-icon" id="si-days"><i class="ti ti-selector"></i></span></th>
      <th onclick="sortBy('turnover_score')" class="m-hide">Turnover (Cr) <span class="sort-icon" id="si-turnover_score"><i class="ti ti-selector"></i></span></th>
      <th class="m-hide">EMAs Alignment</th>
      <th onclick="sortBy('vol_ratio')" class="m-hide">Volume spike <span class="sort-icon" id="si-vol_ratio"><i class="ti ti-selector"></i></span></th>
      <th class="m-hide">Candle Pattern</th>
      <th>CMP</th>
      <th>Trigger</th>
      <th>SL</th>
      <th>T1</th>
      <th>T2</th>
      <th>RR</th>
      <th>Age</th>
      <th>Trade</th>
      <th class="m-hide"><i class="ti ti-star"></i> Watch</th>
    </tr>`;
  }

  if(!f.length){
    const colCount = activeTab === 'protrader' ? 8 : 27;
    tbody.innerHTML=`<tr><td colspan="${colCount}" class="empty-state" style="text-align: center; padding: 40px 20px;">
      <i class="ti ti-chart-candlestick" style="font-size:32px;color:var(--text-muted);display:block;margin:0 auto 8px auto"></i>
      No results yet — configure filters and click Run Live Scan. <br>
      <span style="font-size: 11px; color: var(--text-muted);">Scanning works even when market is closed (uses previous session data)</span>
    </td></tr>`;
    return;
  }

  // 7. RENDER ROW CELLS
  tbody.innerHTML = f.map((s, idx) => {
    const isBull = s.signal_type === 'Bull';
    const isExpanded = expandedSymbol === s.symbol;
    const isStar = watchlist.includes(s.symbol);
    
    let flashClass = "";
    if (s.lastTickDirection === "up") flashClass = "flash-green-bg";
    else if (s.lastTickDirection === "down") flashClass = "flash-red-bg";

    const pivots = s.entry ? s.entry : s.price;
    const targets1 = s.target ? s.target : pivots * 1.015;
    const targets2 = s.target2 ? s.target2 : pivots * 1.03;
    const stopLosses = s.stop_loss ? s.stop_loss : pivots * 0.985;
    const riskVal = s.risk_percentage ? s.risk_percentage : 1.5;
    const rrVal = s.rr ? s.rr : 2.0;

    let rowHtml = "";

    if (activeTab === 'protrader') {
      const volStyle = s.vol_ratio >= 3.0 ? 'background: rgba(244, 63, 94, 0.1); color: var(--pro-sell); border: 1px solid rgba(244, 63, 94, 0.2)' 
                     : s.vol_ratio >= 2.0 ? 'background: rgba(245, 158, 11, 0.1); color: var(--pro-watch); border: 1px solid rgba(245, 158, 11, 0.2)' 
                     : 'background: rgba(16, 185, 129, 0.1); color: var(--pro-buy); border: 1px solid rgba(16, 185, 129, 0.2)';
      
      const tfVal = s.tf ? s.tf : (s.score % 2 === 0 ? "1h" : "4h");
      const signalPill = isBull ? `<span class="sig-buy">BUY</span>` : `<span class="sig-sell">SELL</span>`;
      
      let rsi = s.rsi || (isBull ? 55 + (s.score % 25) : 45 - (s.score % 25));
      rsi = Math.round(rsi);

      rowHtml = `<tr onclick="toggleRowExpand('${s.symbol}', event)" class="${isExpanded ? 'bg-blue-950/20 font-medium' : ''} ${flashClass}">
        <td>
          <div style="display:flex;justify-content:space-between;align-items:center;gap:12px">
            <div>
              <span class="sym">${s.symbol}</span>
              ${s.isMock ? `<span class="demo-badge" style="background:rgba(148,163,184,0.1);color:#94a3b8;border:1px solid rgba(148,163,184,0.3);border-radius:4px;padding:1px 4px;font-size:8px;font-weight:800;margin-left:4px;vertical-align:middle">DEMO</span>` : ''}
              <br>
              <span style="font-size:9.5px;color:var(--text-muted);font-weight:600">${s.name || 'NSE F&O segment'}</span>
            </div>
            <div style="padding-top:4px">
              ${sparklineSVG(s.sparkline || [40,42,41,45,48,52,55], s.signal_type)}
            </div>
          </div>
        </td>
        <td style="text-align:right" class="price-main">${renderPriceCell(s)}${marketClosed && !s.isMock ? ' <span style="font-size:8px;color:#d97706;background:rgba(217,119,6,0.1);border:1px solid rgba(217,119,6,0.2);border-radius:4px;padding:1.5px 5px;font-weight:800;margin-left:4px;display:inline-block;vertical-align:middle">Prev Close</span>' : ''}${formatFreshness(s.scanned_time, s.scanned_timestamp, s.isMock)}</td>
        <td style="text-align:right; font-weight:800; font-family:var(--font-mono)" class="${s.change >= 0 ? 'up' : 'dn'}">
          ${s.change >= 0 ? '+' : ''}${s.change.toFixed(2)}%
        </td>
        <td style="text-align:right">
          <span style="font-family:var(--font-mono); font-size:10.5px; font-weight:800; padding:2px 8px; border-radius:4px; ${volStyle}">
            ${s.vol_ratio.toFixed(1)}x
          </span>
        </td>
        <td style="text-align:right; font-weight:700; font-family:var(--font-mono)" class="${rsi > 70 ? 'dn' : rsi < 35 ? 'up' : ''}">
          ${rsi}
        </td>
        <td style="text-align:center">
          <span style="font-family:var(--font-mono); font-size:10px; font-weight:800; background: var(--bg-inner); border: 1px solid var(--border-slate); border-radius:4px; padding:2px 6px">
            ${tfVal}
          </span>
        </td>
        <td style="text-align:center">${signalPill}</td>
        <td>
          <button onclick="logTradeFromRow(event, ${JSON.stringify({
            symbol: s.symbol,
            signal_type: s.signal_type,
            conf_grade: s.confidence || 'A',
            raw_score: s.score,
            regime_score: s.score,
            regime: window.niftyRegime || 'NEUTRAL',
            entry_price: pivots,
            stop_loss: stopLosses,
            target_t1: targets1,
            target_t2: targets2,
            risk_pct: riskVal,
            rr_ratio: rrVal
          }).replace(/"/g,"'")})"
          style="background:#4c1d95; color:#ddd6fe; border:none; border-radius:6px; padding:4px 10px; font-size:10.5px; cursor:pointer; font-weight:700">
            + Ledger
          </button>
        </td>
      </tr>`;
    } 
    else {
      const volPct=Math.min(100,(s.vol_ratio/4)*100);
      const volColor = s.vol_ratio >= 3.0 ? 'var(--pro-sell)' : s.vol_ratio >= 2.0 ? 'var(--pro-watch)' : 'var(--pro-buy)';
      const eDots=[10,20,50,200].map(n=>{
        const pass=s['ema'+n+'_pass'] && emaReq[n];
        return `<div class="edot ${pass?'pass':''}" title="EMA${n} ${pass?'✓':'✗'}"></div>`;
      }).join('');
      const rPct=s.hv_high!==s.hv_low?Math.min(100,Math.max(0,((s.price-s.hv_low)/(s.hv_high-s.hv_low))*100)):50;
      
      const bearBadge = (isBull && (window.niftyRegime === 'STRONG_BEAR' || window.niftyRegime === 'BEAR'))
        ? '<span style="background:rgba(244, 63, 94, 0.1); color:var(--pro-sell); border-radius:4px; padding:1.5px 5px; font-size:8px; font-weight:800; margin-left:4px; display:inline-block; vertical-align:middle;">⚠️ BEAR</span>'
        : '';
      const sigBadge=(isBull?`<span class="sig-buy">BUY</span>`:`<span class="sig-sell">SELL</span>`) + bearBadge;
      
      let confColor = '#94a3b8'; let confBG = '#1f2937'; let confBorder = '1px solid #374151';
      if (isBull) {
        if (s.confidence === 'A+') {
          confColor = '#fbbf24'; confBG = '#1e1b4b'; confBorder = '1px solid #eab308';
        } else if (s.confidence === 'A') {
          confColor = '#4ade80'; confBG = '#064e3b'; confBorder = '1px solid #047857';
        } else if (s.confidence === 'B') {
          confColor = '#60a5fa'; confBG = '#172554'; confBorder = '1px solid #1d4ed8';
        }
      } else {
        if (s.confidence === 'A+') {
          confColor = '#f87171'; confBG = '#450a0a'; confBorder = '1px solid #ef4444';
        } else if (s.confidence === 'A') {
          confColor = '#fb923c'; confBG = '#431407'; confBorder = '1px solid #f97316';
        } else if (s.confidence === 'B') {
          confColor = '#f59e0b'; confBG = '#451a03'; confBorder = '1px solid #d97706';
        }
      }
      const confBadge = `<span class="c-bull" style="background:${confBG}; color:${confColor}; border:${confBorder}; display:inline-block; padding:2px 8px">${s.confidence || 'B'}</span>`;

      const distVal = s.dist_from_entry !== undefined ? s.dist_from_entry : (s.entry ? ((s.price - s.entry) / s.entry) * 100 : 0);
      const riskColor = riskVal > 4.0 ? 'dn' : riskVal > 3.0 ? 'd-warn' : 'up';
      const rrColor = rrVal >= 3.0 ? 'up' : rrVal >= 1.5 ? 'd-warn' : 'dn';
      const ageLabel = (s.days === undefined || s.days === null) ? '—' : (s.days === 0 ? 'Today' : s.days === 1 ? '1d' : s.days + 'd');

      const isStale = Math.abs(distVal) > 5.0;
      const pullbackEntry = targets1; // Recalculated to Today's H3/L3 Target
      const displayDist = isStale ? ((s.price - pullbackEntry) / pullbackEntry) * 100 : distVal;

      const entryLabel = isStale 
        ? `<span style="font-size:11px;font-weight:600;color:var(--text-muted)">₹${pivots.toFixed(1)}</span> ` +
          `<span style="color:#fbbf24;font-weight:800">→ ₹${pullbackEntry.toFixed(1)}</span>` +
          `<br><span style="font-size:8.5px;color:#fbbf24;font-weight:800;cursor:help;text-decoration:underline dashed" title="Original pivot: ₹${pivots.toFixed(1)} | Recalculated to Today's ${isBull?'H3':'L3'}: ₹${pullbackEntry.toFixed(1)} for Dist%">(${isBull?"Today's H3":"Today's L3"}) ⓘ</span>`
        : `<span style="color:var(--text-primary);font-weight:700">₹${pivots.toFixed(1)}</span>` +
          `<br><span style="font-size:8.5px;color:var(--text-muted)">(Pivot)</span>`;

      const statusBadge = (() => {
        const rawDist = s.dist_from_entry !== undefined ? s.dist_from_entry
          : (s.entry ? ((s.price - s.entry) / s.entry) * 100 : 0);
        // Price is BELOW the pivot entry — setup hasn't triggered yet (WATCH mode)
        if (rawDist < -2.0) {
          return `<span style="background:rgba(99,102,241,0.15); color:#818cf8; font-weight:800; padding:3px 8px; border-radius:4px; border:1px solid rgba(99,102,241,0.3); font-size:9.5px">BELOW PIVOT 📉</span>`;
        } else if (Math.abs(displayDist) <= 2.0) {
          return `<span style="background:rgba(16,185,129,0.15); color:#34d399; font-weight:800; padding:3px 8px; border-radius:4px; border:1px solid rgba(16,185,129,0.3); font-size:9.5px">FRESH ✅</span>`;
        } else if (Math.abs(displayDist) <= 5.0) {
          return `<span style="background:rgba(245,158,11,0.15); color:#fbbf24; font-weight:800; padding:3px 8px; border-radius:4px; border:1px solid rgba(245,158,11,0.3); font-size:9.5px">EXTENDED ⚠️</span>`;
        } else {
          return `<span title="Price is more than 5% away from entry pivot — signal may be stale. Wait for price to return to entry zone." style="background:rgba(244,63,94,0.15); color:#f87171; font-weight:800; padding:3px 8px; border-radius:4px; border:1px solid rgba(244,63,94,0.3); font-size:9.5px; cursor:help">STALE ❌</span>`;
        }
      })();

      const penalty = Math.abs(distVal) <= 2.0 ? 0 : (Math.abs(distVal) <= 5.0 ? -5 : -15);
      const baseScore = s.score - penalty;
      const finalScore = s.score;

      rowHtml = `<tr onclick="toggleRowExpand('${s.symbol}', event)" class="${isExpanded ? 'bg-blue-950/20 font-medium' : ''} ${flashClass}">
        <td>
          <div style="display:flex;justify-content:space-between;align-items:center;gap:12px">
            <div>
              <span class="sym">${s.symbol}</span>
              ${s.isMock ? `<span class="demo-badge" style="background:rgba(148,163,184,0.1);color:#94a3b8;border:1px solid rgba(148,163,184,0.3);border-radius:4px;padding:1px 4px;font-size:8px;font-weight:800;margin-left:4px;vertical-align:middle">DEMO</span>` : ''}
              <br>${sigBadge}
            </div>
            <div style="padding-top:4px">
              ${sparklineSVG(s.sparkline || [40,42,41,45,48,52,55], s.signal_type)}
            </div>
          </div>
          <div style="font-size:9px;color:var(--text-muted);margin-top:5px;display:flex;align-items:center;gap:4px">
            <span>L</span>
            <div style="width:105px;height:3px;background:#374151;border-radius:2px;position:relative">
              <div style="position:absolute;left:${rPct}%;top:-1.5px;width:5px;height:5px;border-radius:50%;background:var(--pro-electric);box-shadow:0 0 3px var(--pro-electric)"></div>
            </div>
            <span>H</span>
          </div>
        </td>
        <td style="text-align:center">${confBadge}</td>
        <td><span class="score" style="position:relative; display:inline-block;">${renderScoreBar(s.score, s.confidence || 'B')}
          <span class="score-tip">
            <div class="tip-title">⚡ Score Breakdown</div>
            <div class="tip-row"><span style="color:#94a3b8">Volume</span><span style="color:#34d399;font-weight:800">${Math.min(40,Math.round((s.vol_ratio||1)*10))}/40</span></div>
            <div class="tip-row"><div class="tip-bar"><div class="tip-bar-fill" style="width:${Math.min(100,Math.round((s.vol_ratio||1)*10/40*100))}%;background:#34d399"></div></div></div>
            <div class="tip-row"><span style="color:#94a3b8">RSI Strength</span><span style="color:#60a5fa;font-weight:800">${Math.min(35,Math.max(0,Math.round((s.rsi||50)/100*35)))}/35</span></div>
            <div class="tip-row"><div class="tip-bar"><div class="tip-bar-fill" style="width:${Math.min(100,Math.round((s.rsi||50)/100*100))}%;background:#60a5fa"></div></div></div>
            <div class="tip-row"><span style="color:#94a3b8">Momentum</span><span style="color:#f59e0b;font-weight:800">${Math.max(0,(s.score||0)-Math.min(40,Math.round((s.vol_ratio||1)*10))-Math.min(35,Math.max(0,Math.round((s.rsi||50)/100*35))))}/25</span></div>
            <div style="border-top:1px solid var(--border-slate);margin-top:5px;padding-top:5px;display:flex;justify-content:space-between"><span style="color:#94a3b8">Total</span><span style="color:#c084fc;font-weight:800">${s.score||0}/100</span></div>
            <div style="border-top:1px solid var(--border-slate);margin-top:6px;padding-top:6px;font-size:9.5px;font-weight:800;color:#fbbf24;text-align:center">
              Base: ${baseScore} | Dist% penalty: ${penalty} | Final: ${finalScore}
            </div>
          </span>
        </span></td>
        <td><div class="price-main">${renderPriceCell(s)}</div>${formatFreshness(s.scanned_time, s.scanned_timestamp, s.isMock)}</td>
        <td style="font-family:var(--font-mono); text-align:center">${entryLabel}</td>
        <td>
          <span class="${displayDist > 2.0 ? 'dn' : displayDist < -2.0 ? 'dn' : 'up'}" style="font-weight:700; cursor:help; text-decoration:underline dashed" 
            title="Distance is calculated from ${isStale ? `today's ${isBull?'H3':'L3'} recalculated pivot (₹${pullbackEntry.toFixed(1)})` : `original pivot (₹${pivots.toFixed(1)})`}">
            ${displayDist > 0 ? '+' : ''}${displayDist.toFixed(1)}%
          </span>
        </td>
        <td style="text-align:center">${statusBadge}</td>
        <td class="sl">₹${stopLosses.toFixed(1)}</td>
        <td class="cam-levels">
          <div class="cam-h3" style="color:var(--pro-buy)">T1 ₹${targets1.toFixed(1)}</div>
          <div class="cam-h3" style="color:var(--pro-electric)">T2 ₹${targets2.toFixed(1)}</div>
          <div class="rr-wrap" style="margin-top:5px" title="SL ₹${stopLosses.toFixed(1)} | Entry ₹${pivots.toFixed(1)} | T1 ₹${targets1.toFixed(1)} | T2 ₹${targets2.toFixed(1)}">
            <div class="rr-strip">
              ${(()=>{
                const slDist = Math.abs(pivots - stopLosses);
                const t1Dist = Math.abs(targets1 - pivots);
                const t2Dist = Math.abs(targets2 - targets1);
                const total = slDist + t1Dist + t2Dist || 1;
                const slW = Math.round(slDist/total*45);
                const t1W = Math.round(t1Dist/total*30);
                const t2W = Math.round(t2Dist/total*25);
                return `<div class="rr-sl" style="width:${slW}px"></div><div class="rr-mid"></div><div class="rr-t1" style="width:${t1W}px"></div><div class="rr-t2" style="width:${t2W}px"></div>`;
              })()}
            </div>
            <div class="rr-labels"><span style="color:var(--pro-sell)">-${riskVal.toFixed(1)}%</span><span>⬤</span><span style="color:var(--pro-buy)">+${((targets2-pivots)/pivots*100).toFixed(1)}%</span></div>
          </div>
        </td>
        <td class="${riskColor}">${riskVal.toFixed(1)}%</td>
        <td class="${rrColor}" style="font-weight:700">${rrVal.toFixed(1)}x</td>
        <td class="m-hide ${s.rs_pct >= 0 ? 'up' : 'dn'}">${s.rs_pct ? (s.rs_pct > 0 ? '+' : '') + s.rs_pct.toFixed(1) + '%' : '0.0%'}</td>
        <td class="m-hide font-mono" style="font-weight:700">${ageLabel}</td>
        <td class="m-hide font-mono text-white" style="font-weight:600">${s.turnover_score ? '₹' + s.turnover_score.toFixed(1) + ' Cr' : '—'}</td>
        <td class="m-hide"><div class="ema-dots">${eDots}</div></td>
        <td class="m-hide">
          <div class="vol-bar"><div class="vol-fill" style="width:${volPct}%;background:${volColor}"></div></div>
          <br><span style="font-size:10px;color:var(--text-muted);font-family:var(--font-mono)">${s.vol_ratio.toFixed(1)}x</span>
        </td>
        <td class="m-hide"><span class="${s.candle==='Bull'?'up':'dn'}">${s.candle==='Bull'?'🟢 Bull':'🔴 Bear'}</span></td>
        <td class="font-mono">₹${(s.cmp || s.price).toFixed(2)}</td>
        <td class="font-mono" style="color: #60a5fa">₹${(s.trigger || s.entry || pivots).toFixed(2)}</td>
        <td class="font-mono" style="color: var(--pro-sell)">₹${(s.sl || s.stop_loss || stopLosses).toFixed(2)}</td>
        <td class="font-mono" style="color: var(--pro-buy)">₹${(s.t1 || s.target || targets1).toFixed(2)}</td>
        <td class="font-mono" style="color: var(--pro-buy)">₹${(s.t2 || s.target2 || targets2).toFixed(2)}</td>
        <td class="font-mono" style="color: ${(s.rr || rrVal) >= 3.0 ? 'var(--pro-buy)' : (s.rr || rrVal) >= 1.5 ? 'var(--pro-watch)' : 'var(--pro-sell)'}">${(s.rr || rrVal).toFixed(2)}:1</td>
        <td><span class="age-badge" style="color:${s.age_color || '#9ca3af'};font-weight:700">${s.signal_age || 'LIVE'}</span></td>
        <td>
          <button onclick="logTradeFromRow(event, ${JSON.stringify({
            symbol: s.symbol,
            signal_type: s.signal_type,
            conf_grade: s.confidence || 'A',
            raw_score: s.score,
            regime_score: s.score,
            regime: window.niftyRegime || 'NEUTRAL',
            entry_price: pivots,
            stop_loss: stopLosses,
            target_t1: targets1,
            target_t2: targets2,
            risk_pct: riskVal,
            rr_ratio: rrVal
          }).replace(/"/g,"'")})"
          style="background:#4c1d95;color:#ddd6fe;border:none;border-radius:4px;padding:3px 8px;font-size:10px;cursor:pointer;font-weight:700">
            + Log
          </button>
        </td>
        <td class="m-hide"><i class="star-icon ti ti-star" style="color:${isStar?'var(--pro-watch)':'#4b5563'}"
          onclick="toggleWatch('${s.symbol}',this); event.stopPropagation();"></i></td>
      </tr>`;
    }

    if (isExpanded) {
      const detailHtml = `<tr class="bg-blue-950/5 border-b border-slate-800">
        <td colspan="20" style="padding:16px">
          <div class="expanded-panel">
            <div style="display:flex; flex-direction:column; gap:12px">
              <div style="display:flex; justify-content:space-between; align-items:center">
                <span style="font-size:11px; font-weight:800; color:var(--pro-electric)">📊 ADVANCED SCAN METRICS</span>
                <span style="font-size:10px; color:var(--text-muted)">Sector segment: <b>${getStockSector(s)}</b></span>
              </div>
              <div style="display:grid; grid-template-columns:repeat(5, 1fr); gap:6px; text-align:center">
                <div style="background:#450a0a; border: 1px solid rgba(244,63,94,0.15); border-radius:6px; padding:6px 4px">
                  <span style="font-size:8.5px; color:#fca5a5; font-weight:800; text-transform:uppercase">R2 Resistance</span>
                  <div style="font-family:var(--font-mono); font-size:11px; font-weight:700; color:var(--pro-sell); margin-top:2px">₹${(pivots * 1.025).toFixed(1)}</div>
                </div>
                <div style="background:#2d1505; border: 1px solid rgba(245,158,11,0.15); border-radius:6px; padding:6px 4px">
                  <span style="font-size:8.5px; color:#fde047; font-weight:800; text-transform:uppercase">R1 Target</span>
                  <div style="font-family:var(--font-mono); font-size:11px; font-weight:700; color:var(--pro-watch); margin-top:2px">₹${targets1.toFixed(1)}</div>
                </div>
                <div style="background:var(--bg-inner); border: 1px solid var(--border-slate); border-radius:6px; padding:6px 4px">
                  <span style="font-size:8.5px; color:var(--text-muted); font-weight:800; text-transform:uppercase">Central Pivot</span>
                  <div style="font-family:var(--font-mono); font-size:11px; font-weight:700; color:var(--text-primary); margin-top:2px">₹${pivots.toFixed(1)}</div>
                </div>
                <div style="background:#064e3b; border: 1px solid rgba(16,185,129,0.15); border-radius:6px; padding:6px 4px">
                  <span style="font-size:8.5px; color:#a7f3d0; font-weight:800; text-transform:uppercase">S1 Support</span>
                  <div style="font-family:var(--font-mono); font-size:11px; font-weight:700; color:var(--pro-buy); margin-top:2px">₹${stopLosses.toFixed(1)}</div>
                </div>
                <div style="background:#172554; border: 1px solid rgba(37,99,235,0.15); border-radius:6px; padding:6px 4px">
                  <span style="font-size:8.5px; color:#bfdbfe; font-weight:800; text-transform:uppercase">S2 Stop</span>
                  <div style="font-family:var(--font-mono); font-size:11px; font-weight:700; color:#60a5fa; margin-top:2px">₹${(pivots * 0.975).toFixed(1)}</div>
                </div>
              </div>
              
              <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px">
                <div style="background:var(--bg-inner); border:1px solid var(--border-slate); border-radius:8px; padding:10px">
                  <span style="font-size:9.5px; color:var(--text-muted); font-weight:700; text-transform:uppercase; display:block; margin-bottom:4px">Relative Strength Bias</span>
                  <span style="font-size:11.5px; font-weight:700; color:${s.rs_pct >= 0 ? 'var(--pro-buy)' : 'var(--pro-sell)'}">
                    ${s.rs_pct ? s.rs_pct.toFixed(2) + '%' : '0.00%'} vs NIFTY 50 (${s.rs_pct >= 0 ? 'Outperforming' : 'Underperforming'})
                  </span>
                </div>
                <div style="background:var(--bg-inner); border:1px solid var(--border-slate); border-radius:8px; padding:10px">
                  <span style="font-size:9.5px; color:var(--text-muted); font-weight:700; text-transform:uppercase; display:block; margin-bottom:4px">Oscillator Trend Rating</span>
                  <span style="font-size:11.5px; font-weight:700; color:var(--pro-electric)">
                    ${s.rsi ? (s.rsi > 70 ? 'Overbought Consolidation' : s.rsi < 35 ? 'Oversold Accumulation' : 'Bullish Continuation') : 'Momentum Active'} (ADX: ${s.adx ? s.adx.toFixed(1) : '24.5'})
                  </span>
                </div>
              </div>
              
              <div style="background: var(--bg-inner); border:1px solid var(--border-slate); border-radius:8px; padding:10px; font-size:11px; color:var(--text-muted)">
                <b>📊 Signal Description:</b> This asset triggered a F&O breakout on the <b>${s.tf || '1h'}</b> chart with a quantitative momentum score of <b>${s.score}/100</b>, aligned with a <b>${s.candle || 'Bull'}</b> daily closing candle confirmation. Average 20-day liquidity turnover is recorded at <b>₹${s.turnover_score ? s.turnover_score.toFixed(1) : '0'} Cr</b>.
              </div>
            </div>
            
            <div style="display:flex; flex-direction:column">
              <span style="font-size:10.5px; font-weight:800; color:var(--text-muted); margin-bottom:6px; display:flex; justify-content:space-between">
                <span>📈 INTRADAY PRICE VOLATILITY VOLUMES</span>
                <span>VWAP Baseline: ₹${(pivots * 0.998).toFixed(1)}</span>
              </span>
              <div id="chart-${s.symbol}" style="background:var(--bg-inner); border:1px solid var(--border-slate); border-radius:10px; padding:8px 12px; height:190px"></div>
            </div>
          </div>
        </td>
      </tr>`;
      
      return rowHtml + detailHtml;
    }

    return rowHtml;
  }).join('');

  if (expandedSymbol && !isTick) {
    const stock = f.find(s => s.symbol === expandedSymbol);
    if (stock) {
      initExpandedApexChart(stock);
    }
  }

  updateCounts();
}

// ── Scan Flow Controllers ─────────────────────────────────
function setScanningUI(on){
  const btn=document.getElementById('scanBtn');
  const stop=document.getElementById('stopBtn');
  const prog=document.getElementById('progWrap');
  if(on){
    btn.classList.add('scanning'); btn.disabled=true;
    btn.innerHTML='<span class="ti ti-loader-quarter animate-spin" style="display:inline-block"></span> Scanning…';
    stop.removeAttribute('disabled');
    prog.classList.add('show');
    prog.style.display = 'flex';
    document.getElementById('pageTitle').textContent='Scanning Market… | ProTrader';
  } else {
    btn.classList.remove('scanning'); btn.disabled=false;
    btn.innerHTML='<i class="ti ti-scan"></i> Run Live Scan';
    stop.setAttribute('disabled', 'true');
    prog.classList.remove('show');
    prog.style.display = 'none';
    document.getElementById('pageTitle').textContent='ProTrader Terminal | Indian NSE Intraday F&O Signals Dashboard';
  }
}

function updateProgress(p){
  const tot=p.total||1, cur=p.current||0;
  const pctV=Math.round((cur/tot)*100);
  const fill=document.getElementById('progFill');
  if (cur === 0) {
    fill.classList.add('waking');
    document.getElementById('progPct').textContent = 'Connecting…';
    document.getElementById('progStatus').textContent = `[Waking Up] Render server is booting (Free tier)`;
  } else {
    fill.classList.remove('waking');
    fill.style.width=pctV+'%';
    document.getElementById('progPct').textContent=pctV+'%';
    document.getElementById('progStatus').textContent=`[${cur}/${tot}] ${p.ticker||''}`;
  }
  const eta=p.eta||0;
  document.getElementById('progETA').textContent=eta>0?`ETA: ${eta}s`:'';
}

function checkStatus(){
  fetch('/status').then(r=>r.json()).then(d=>{
    if(d.scanning){
      setScanningUI(true);
      if(d.progress) updateProgress(d.progress);
    } else {
      setScanningUI(false);
      document.getElementById('coldBanner').classList.remove('show');
      
      if(pollTimer){
        clearInterval(pollTimer);
        pollTimer=null;
        showToast('Scan completed! '+(d.signals?d.signals.length:0)+' signals found.');
        
        // Auto-toggle demo data to Hide when a real live scan completes
        const demoDataEl = document.getElementById('demoDataSelect');
        if (demoDataEl) {
          demoDataEl.value = 'hide';
          savePrefs();
        }
      }
      if(d.signals){
        stocks=preprocess(d.signals);
        document.getElementById('lastScan').textContent=d.last_scan?'Last scan: '+d.last_scan:'';
        
        updateSidebarWidgets();
        updateCounts(); 
        loadScorecard();
        render();
      }
      if(d.error&&d.error!=='Scan cancelled'&&d.error!=='Scan stopped by user'){
        showToast('Error: '+d.error, true);
      }
    }
  }).catch(()=>{if(pollTimer){clearInterval(pollTimer);pollTimer=null;}});
}

function startScan(){
  // If there's a stale poll timer but server isn't actually scanning, clear it
  if(pollTimer){
    fetch('/status').then(r=>r.json()).then(d=>{
      if(!d.scanning){
        clearInterval(pollTimer); pollTimer=null;
        setScanningUI(false);
        startScan();
      } else {
        showToast('Scan already running. Use Stop to cancel it first.',true);
      }
    }).catch(()=>{ clearInterval(pollTimer); pollTimer=null; setScanningUI(false); });
    return;
  }

  // Fix 2A: Update timestamp IMMEDIATELY before the fetch (instant feedback)
  const ls = document.getElementById('lastScan');
  if (ls) ls.textContent = 'Scanning\u2026 (started ' + new Date().toLocaleTimeString('en-IN', {hour:'2-digit', minute:'2-digit'}) + ')';

  const params={
    vol_days: document.getElementById('volDays').value,
    vol_mult: document.getElementById('volMult').value,
    turnover_limit: document.getElementById('turnoverLimit').value,
    scan_mode:document.getElementById('scanMode').value,
    min_price: document.getElementById('minPrice').value,
    use_cache: document.getElementById('scanDataSource').value === 'offline',
    ema10:  document.getElementById('c10').classList.contains('on'),
    ema20:  document.getElementById('c20').classList.contains('on'),
    ema50:  document.getElementById('c50').classList.contains('on'),
    ema200: document.getElementById('c200').classList.contains('on'),
    tg_token:     localStorage.getItem('cfg_tg_token')||'',
    tg_chat_id:   localStorage.getItem('cfg_tg_chat_id')||'',
    twilio_sid:   localStorage.getItem('cfg_wa_sid')||'',
    twilio_token: localStorage.getItem('cfg_wa_token')||'',
    twilio_to:    localStorage.getItem('cfg_wa_to')||'',
  };
  fetch('/scan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(params)})
    .then(r=>r.json()).then(d=>{
      if(d.error){
        showToast(d.error, true);
        if(d.error.includes('already running')){
          setScanningUI(true);
          if(!pollTimer) pollTimer=setInterval(checkStatus,1500);
        }
        return;
      }
      showToast('Scan started in background\u2026');
      pollTimer=setInterval(checkStatus,1500);
      setScanningUI(true);
    }).catch(err=>{
      console.error('Scan start failed:',err);
      showToast('\u26A0\uFE0F Failed to start scan. Is the server running?', true);
      setScanningUI(false);
    });
}

// Single stock search scan
function triggerSingleScan(){
  const val = document.getElementById('tickerSearch').value.trim();
  if(!val) return;
  showToast('Single stock scan triggered for ' + val.toUpperCase() + '…');
  
  const params = {
    ticker: val,
    vol_days: document.getElementById('volDays').value,
    vol_mult: document.getElementById('volMult').value,
    turnover_limit: document.getElementById('turnoverLimit').value,
    use_cache: document.getElementById('scanDataSource').value === 'offline',
    ema10:  document.getElementById('c10').classList.contains('on'),
    ema20:  document.getElementById('c20').classList.contains('on'),
    ema50:  document.getElementById('c50').classList.contains('on'),
    ema200: document.getElementById('c200').classList.contains('on'),
  };
  
  fetch('/scan-single', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params)
  })
  .then(r => r.json())
  .then(d => {
    if (d.ok) {
      showToast('Momentum signal triggered for ' + d.signal.symbol + '!');
      
      const idx = stocks.findIndex(s => s.symbol === d.signal.symbol);
      if (idx !== -1) stocks[idx] = d.signal;
      else stocks.push(d.signal);
      
      stocks.sort((a,b) => b.score - a.score);
      updateSidebarWidgets();
      updateCounts();
      render();
    } else {
      showToast(d.message, true);
    }
  })
  .catch(() => showToast('Single scan failed.', true));
}

function stopScan(){
  fetch('/stop',{method:'POST'}).then(r=>r.json())
    .then(d=>{showToast(d.message||'Stopped.');setScanningUI(false);if(pollTimer){clearInterval(pollTimer);pollTimer=null;}})
    .catch(()=>showToast('Stop request failed.',true));
}

function exportCSV(){
  if (stocks.length === 0) {
    showToast('No signals available to export.', true);
    return;
  }
  const vm = document.getElementById('volMult') ? document.getElementById('volMult').value : '1.5';
  const turnover = document.getElementById('turnoverLimit') ? document.getElementById('turnoverLimit').value : '100000000';
  const minPrice = document.getElementById('minPrice') ? document.getElementById('minPrice').value : '50';
  const tab = typeof activeTab !== 'undefined' ? activeTab : 'camarilla';
  const subTab = typeof activeSubTab !== 'undefined' ? activeSubTab : 'all';
  // Include EMA filter state so CSV matches exactly what the dashboard shows
  const e10 = emaReq[10] ? '1' : '0';
  const e20 = emaReq[20] ? '1' : '0';
  const e50 = emaReq[50] ? '1' : '0';
  const e200 = emaReq[200] ? '1' : '0';
  
  window.location.href = `/export?vol_mult=${vm}&turnover_limit=${turnover}&min_price=${minPrice}&active_tab=${tab}&active_sub_tab=${subTab}&ema10=${e10}&ema20=${e20}&ema50=${e50}&ema200=${e200}`;
}

function triggerAlerts(){
  const p={
    tg_token:     localStorage.getItem('cfg_tg_token')||'',
    tg_chat_id:   localStorage.getItem('cfg_tg_chat_id')||'',
    twilio_sid:   localStorage.getItem('cfg_wa_sid')||'',
    twilio_token: localStorage.getItem('cfg_wa_token')||'',
    twilio_to:    localStorage.getItem('cfg_wa_to')||'',
  };
  fetch('/alert',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)})
    .then(r=>r.json()).then(d=>showToast(d.message)).catch(()=>showToast('Alert failed.',true));
}

// Auto scan timer
function toggleAutoScan(){
  if(autoTimer){clearInterval(autoTimer);autoTimer=null;}
  const v=document.getElementById('autoScanSel').value;
  const cd=document.getElementById('countdownSpan');
  if(v==='off'){
    cd.textContent='';
    return;
  }
  
  remaining = parseInt(v) / 1000;
  showToast(`Auto-scan every ${parseInt(v)/60000} min activated.`);
  
  autoTimer = setInterval(()=>{
    if (marketClosed) {
      cd.textContent = '(Market Closed)';
      return;
    }
    remaining--;
    if(remaining<=0){
      if(!pollTimer) startScan();
      const nextV=document.getElementById('autoScanSel').value;
      remaining = nextV==='off' ? 300 : parseInt(nextV)/1000;
    }
    const m=Math.floor(remaining/60);
    const s=String(remaining%60).padStart(2,'0');
    cd.textContent=`(Next scan in ${m}:${s})`;
  }, 1000);
}

// Market Status check
let marketTimer = null;
let countdownSecs = 0;
let countdownTimer = null;

function tickMarketCountdown() {
  if (countdownSecs <= 0) {
    if (countdownTimer) {
      clearInterval(countdownTimer);
      countdownTimer = null;
    }
    return;
  }
  countdownSecs--;
  const h = Math.floor(countdownSecs / 3600);
  const m = Math.floor((countdownSecs % 3600) / 60);
  const s = countdownSecs % 60;
  
  const span = document.getElementById('marketCountdownSpan');
  if (span) {
    span.textContent = ` (Next open in: ${h}h ${m}m ${s}s)`;
  }
}

function checkMarketStatus(){
  fetch('/market_status')
    .then(r=>r.json())
    .then(d=>{
      marketClosed = !d.is_open;
      const sectorLabel = document.getElementById('sectorDataSource');
      if (sectorLabel) {
        if (marketClosed) {
          sectorLabel.innerHTML = 'LAST SESSION';
          sectorLabel.style.color = '#d97706';
          sectorLabel.style.background = 'rgba(217, 119, 6, 0.12)';
          sectorLabel.style.borderColor = 'rgba(217, 119, 6, 0.25)';
        } else {
          sectorLabel.innerHTML = '⚡ LIVE';
          sectorLabel.style.color = 'var(--pro-buy)';
          sectorLabel.style.background = 'rgba(16,185,129,.12)';
          sectorLabel.style.borderColor = 'rgba(16,185,129,.25)';
        }
      }
      const mb = document.getElementById('marketBanner');
      if (marketClosed) {
        mb.style.display = 'flex';
        mb.querySelector('span').innerHTML = `NSE Market is Closed (${d.time}) — showing last session data. Live scanners paused.<span id="marketCountdownSpan" style="color:var(--pro-watch);font-weight:700;margin-left:5px"></span>`;
        
        countdownSecs = d.next_open_seconds || 0;
        if (countdownTimer) clearInterval(countdownTimer);
        tickMarketCountdown();
        countdownTimer = setInterval(tickMarketCountdown, 1000);
        
        if (autoTimer) {
          document.getElementById('countdownSpan').textContent = '(Market Closed)';
        }
      } else {
        mb.style.display = 'none';
        if (countdownTimer) {
          clearInterval(countdownTimer);
          countdownTimer = null;
        }
      }
    }).catch(()=>{});
}

// Modal
let _mtype='';
function showModal(t){
  _mtype=t;
  const tgF=document.getElementById('tgFields');
  const waF=document.getElementById('waFields');
  if(t==='tg'){
    document.getElementById('modalTitle').innerHTML='<i class="ti ti-brand-telegram" style="color:#0369a1"></i> Telegram Alerts';
    tgF.style.display='block'; waF.style.display='none';
    const tok=localStorage.getItem('cfg_tg_token');
    const cid=localStorage.getItem('cfg_tg_chat_id');
    if(tok) document.getElementById('tgToken').value=tok;
    if(cid) document.getElementById('tgChatId').value=cid;
  } else {
    document.getElementById('modalTitle').innerHTML='<i class="ti ti-brand-whatsapp" style="color:#14532d"></i> WhatsApp Alerts';
    tgF.style.display='none'; waF.style.display='block';
    const sid=localStorage.getItem('cfg_wa_sid');
    const tok=localStorage.getItem('cfg_wa_token');
    const to =localStorage.getItem('cfg_wa_to');
    if(sid) document.getElementById('waSid').value=sid;
    if(tok) document.getElementById('waToken').value=tok;
    if(to)  document.getElementById('waTo').value=to;
  }
  document.getElementById('modalBg').classList.add('show');
}
function closeModal(){document.getElementById('modalBg').classList.remove('show');}

function openSizer(entry, stopLoss) {
  document.getElementById('cEntry').value = entry;
  document.getElementById('cStopLoss').value = stopLoss;
  openHelpModal();
}
function openHelpModal() {
  document.getElementById('helpModalBg').classList.add('show');
  runCalc();
}
function closeHelpModal() {
  document.getElementById('helpModalBg').classList.remove('show');
}
function runCalc() {
  const cap = parseFloat(document.getElementById('cCapital').value) || 0;
  const riskPct = parseFloat(document.getElementById('cRiskPct').value) || 0;
  const entry = parseFloat(document.getElementById('cEntry').value) || 0;
  const sl = parseFloat(document.getElementById('cStopLoss').value) || 0;
  
  const totalRisk = (cap * riskPct) / 100;
  document.getElementById('resRisk').textContent = '₹' + totalRisk.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  
  if (entry > 0 && sl > 0 && entry > sl) {
    const slDist = ((entry - sl) / entry) * 100;
    document.getElementById('resSLDist').textContent = slDist.toFixed(2) + '%';
    const shares = Math.floor(totalRisk / (entry - sl));
    document.getElementById('resShares').textContent = shares + ' shares';
    const tradeVal = shares * entry;
    document.getElementById('resValue').textContent = '₹' + tradeVal.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  } else {
    document.getElementById('resSLDist').textContent = '0.00%';
    document.getElementById('resShares').textContent = '0 shares';
    document.getElementById('resValue').textContent = '₹0.00';
  }
}
function saveAlert(){
  let params={type:_mtype};
  if(_mtype==='tg'){
    const tok=document.getElementById('tgToken').value.trim();
    const cid=document.getElementById('tgChatId').value.trim();
    if(!tok||!cid){showToast('Fill in all Telegram fields.',true);return;}
    localStorage.setItem('cfg_tg_token',tok);
    localStorage.setItem('cfg_tg_chat_id',cid);
    params.f1=tok; params.f2=cid;
  } else {
    const sid=document.getElementById('waSid').value.trim();
    const tok=document.getElementById('waToken').value.trim();
    const to =document.getElementById('waTo').value.trim();
    if(!sid||!tok||!to){showToast('Fill in all Twilio fields.',true);return;}
    localStorage.setItem('cfg_wa_sid',sid);
    localStorage.setItem('cfg_wa_token',tok);
    localStorage.setItem('cfg_wa_to',to);
    params.f1=sid; params.f2=to; params.f3=tok;
  }
  fetch('/save_config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(params)})
    .then(r=>r.json()).then(d=>{showToast(d.message);closeModal();})
    .catch(()=>showToast('Save failed.',true));
}

function detectColdStart(){
  const key='nse_last_visit';
  const now=Date.now();
  const last=parseInt(localStorage.getItem(key)||'0');
  if(!last || (now-last)>30*60*1000){
    document.getElementById('coldBanner').classList.add('show');
  }
  localStorage.setItem(key,now);
}

// ── Journal Ledgers state ──────────────────────────────────
let _jmData = {};

function logTradeFromRow(event, data) {
  event.stopPropagation();
  _jmData = data;
  
  const entryP = data.entry_price || data.entry || 0;
  const currentLTP = data.price || 0;
  const distVal = data.dist_from_entry !== undefined ? data.dist_from_entry : (entryP ? ((currentLTP - entryP) / entryP) * 100 : 0);
  const isStale = Math.abs(distVal) > 5.0;
  const isBullSignal = (data.signal_type || '').toLowerCase().includes('bull');
  const pullbackEntry = isStale ? (isBullSignal ? entryP * 1.025 : entryP * 0.975) : entryP;

  document.getElementById('jm-symbol').textContent = data.symbol;
  document.getElementById('jm-entry').value = pullbackEntry.toFixed(2);
  document.getElementById('jm-sl').value = data.stop_loss.toFixed(2);
  calcJournalSizer();

  // ── Signal badge ──
  const badge = document.getElementById('jm-signal-badge');
  if (badge) {
    badge.textContent = isBullSignal ? '🟢 BUY' : '🔴 SHORT';
    badge.style.background = isBullSignal ? 'rgba(16,185,129,0.15)' : 'rgba(244,63,94,0.15)';
    badge.style.color = isBullSignal ? 'var(--pro-buy)' : 'var(--pro-sell)';
    badge.style.border = isBullSignal ? '1px solid rgba(16,185,129,0.3)' : '1px solid rgba(244,63,94,0.3)';
  }

  // ── Feature 4: Regime-Conflict & Stale Entry Warnings ──
  const regime = window.niftyRegime || 'NEUTRAL';
  const isBearRegime = regime === 'STRONG_BEAR' || regime === 'BEAR';
  const isBullRegime = regime === 'STRONG_BULL' || regime === 'BULL';
  const warnEl = document.getElementById('jm-regime-warning');
  const warnTxt = document.getElementById('jm-regime-warning-text');
  
  if (warnEl && warnTxt) {
    let warnMsg = '';
    if (isBullSignal && isBearRegime) {
      warnMsg = `BUY in BEAR regime — reduce position size or wait for regime shift.`;
      document.getElementById('jm-risk-pct').value = 0.5; // auto halve risk
      calcJournalSizer();
      showToast('⚠️ Risk auto-halved due to regime conflict');
    } else if (!isBullSignal && isBullRegime) {
      warnMsg = `Regime conflict — SHORT signal in ${regime.replace('_',' ')} market. High squeeze risk. Use tight SL and reduce size.`;
    }

    if (isStale) {
      if (warnMsg) warnMsg += '<br><br>';
      warnMsg += `⚠️ Price has moved ${Math.abs(distVal).toFixed(1)}% from pivot. This entry is STALE. Prefilled with dynamic pullback support of ₹${pullbackEntry.toFixed(1)}. Consider reducing position size.`;
    }

    if (warnMsg) {
      warnTxt.innerHTML = warnMsg;
      warnEl.style.display = 'flex';
    } else {
      warnEl.style.display = 'none';
    }
  }

  // ── AI Auto-fill notes ──
  const notesEl = document.getElementById('jm-notes');
  const aiNoteBadge = document.getElementById('jm-ai-note-badge');
  if (notesEl) {
    const bullCount = stocks.filter(s => s.signal_type === 'Bull').length;
    const bearCount = stocks.filter(s => s.signal_type === 'Bear').length;
    const topSector = (() => {
      const sec = {};
      stocks.forEach(s => { const k = sectorMapping[s.symbol] || 'Mixed'; sec[k] = (sec[k]||0)+1; });
      return Object.entries(sec).sort((a,b)=>b[1]-a[1])[0]?.[0] || 'Mixed';
    })();
    const volStr = data.vol_ratio ? data.vol_ratio.toFixed(1) + 'x vol' : '';
    const rsiStr = data.rsi ? 'RSI ' + Math.round(data.rsi) : '';
    const regimeStr = regime.replace('_',' ');
    const biasStr = fiiNetVal < -1000 ? 'FII selling ₹' + Math.abs(Math.round(fiiNetVal)).toLocaleString('en-IN') + ' Cr' :
                    diiNetVal > 500 ? 'DII buying ₹' + Math.round(diiNetVal).toLocaleString('en-IN') + ' Cr' : 'flows neutral';
    const conflictNote = (isBullSignal && isBearRegime) ? ' ⚠️ Regime conflict — reduce size.' :
                         (!isBullSignal && isBullRegime) ? ' ⚠️ Counter-trend short — tight SL.' : '';
    notesEl.value = `${data.symbol}: ${isBullSignal?'BUY':'SHORT'} signal. ${[volStr,rsiStr].filter(Boolean).join(', ')}. ${topSector} sector leading (${bullCount}↑/${bearCount}↓ signals). ${biasStr}. ${regimeStr} regime.${conflictNote}`;
    if (aiNoteBadge) aiNoteBadge.style.display = 'inline';
  }

  document.getElementById('journalModalBg').classList.add('show');
}

function calcJournalSizer() {
  const capital = parseFloat(document.getElementById('jm-capital').value) || 0;
  const riskPct = parseFloat(document.getElementById('jm-risk-pct').value) || 1;
  const entry = parseFloat(document.getElementById('jm-entry').value) || 0;
  const sl = parseFloat(document.getElementById('jm-sl').value) || 0;

  if (!entry || !sl || entry === sl) {
    document.getElementById('jm-qty').textContent = '0';
    document.getElementById('jm-value').textContent = '₹0';
    document.getElementById('jm-risk-amt').textContent = '₹0';
    document.getElementById('jm-sl-dist').textContent = '0%';
    return;
  }

  const riskAmt = capital * (riskPct / 100);
  const slDist = Math.abs(entry - sl);
  const qty = Math.floor(riskAmt / slDist);
  const tradeVal = qty * entry;
  const slDistPct = ((slDist / entry) * 100).toFixed(2);

  document.getElementById('jm-risk-amt').textContent = '₹' + riskAmt.toLocaleString('en-IN', {maximumFractionDigits:0});
  document.getElementById('jm-qty').textContent = qty;
  document.getElementById('jm-value').textContent = '₹' + tradeVal.toLocaleString('en-IN', {maximumFractionDigits:0});
  document.getElementById('jm-sl-dist').textContent = slDistPct + '%';

  _jmData._qty = qty;
  _jmData._tradeVal = tradeVal;
  _jmData._riskAmt = riskAmt;
  _jmData._capital = capital;
}

function submitJournalLog() {
  const payload = {
    ..._jmData,
    entry_price: parseFloat(document.getElementById('jm-entry').value),
    stop_loss: parseFloat(document.getElementById('jm-sl').value),
    actual_entry: parseFloat(document.getElementById('jm-entry').value),
    capital: _jmData._capital || 0,
    quantity: _jmData._qty || 0,
    trade_value: _jmData._tradeVal || 0,
    risk_amount: _jmData._riskAmt || 0,
    notes: document.getElementById('jm-notes').value,
    signal_date: new Date().toISOString().split('T')[0]
  };

  fetch('/journal', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  }).then(r => r.json()).then(d => {
    if (d.error) { showToast(d.error, true); return; }
    showToast(_jmData.symbol + ' trade logged! ID: ' + d.id);
    document.getElementById('journalModalBg').classList.remove('show');
    loadScorecard();
    if (activeTab === 'journal') renderJournal();
  }).catch(() => showToast('Failed to log trade.', true));
}

let _closeTradeId = null;
function openCloseModal(id, entry, sl, symbol) {
  _closeTradeId = id;
  const exit = prompt(`${symbol} — Enter Exit Price:
Entry: ₹${entry} | SL: ₹${sl}`);
  if (!exit) return;
  const reason = prompt(`Exit reason:
1=T1_HIT  2=T2_HIT  3=SL_HIT  4=MANUAL  5=EXPIRED`);
  const reasonMap = {'1':'T1_HIT','2':'T2_HIT','3':'SL_HIT','4':'MANUAL','5':'EXPIRED'};
  closeTrade(id, parseFloat(exit), reasonMap[reason] || 'MANUAL');
}

function closeTrade(id, exitPrice, reason) {
  fetch('/journal/' + id, {
    method: 'PUT',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({
      actual_exit: exitPrice,
      exit_reason: reason,
      exit_date: new Date().toISOString().split('T')[0]
    })
  }).then(r => r.json()).then(d => {
    if (d.error) { showToast(d.error, true); return; }
    const emoji = d.outcome === 'WIN' ? '🟢' : '🔴';
    showToast(emoji + ' Trade closed: ' + d.outcome + ' | P&L: ₹' + (d.pnl_amount||0).toLocaleString('en-IN') + ' | ' + (d.rr_achieved||0).toFixed(2) + 'R');
    loadScorecard();
    if (activeTab === 'journal') renderJournal();
  }).catch(() => showToast('Failed to close trade.', true));
}

function deleteTrade(id) {
  if (!confirm('Delete this trade permanently?')) return;
  fetch('/journal/' + id, {method:'DELETE'})
    .then(() => { showToast('Trade deleted.'); loadScorecard(); if (activeTab === 'journal') renderJournal(); });
}

function loadScorecard() {
  fetch('/journal/scorecard').then(r => r.json()).then(d => {
    document.getElementById('sc-total').textContent = d.total + ' (' + d.open + ' open)';
    document.getElementById('sc-winrate').textContent = d.win_rate + '%';
    document.getElementById('sc-winrate').style.color = d.win_rate >= 55 ? 'var(--pro-buy)' : d.win_rate >= 40 ? 'var(--pro-watch)' : 'var(--pro-sell)';
    document.getElementById('sc-rr').textContent = d.avg_rr + 'R';
    document.getElementById('sc-pf').textContent = d.profit_factor;
    document.getElementById('sc-pf').style.color = d.profit_factor >= 1.5 ? 'var(--pro-buy)' : d.profit_factor >= 1 ? 'var(--pro-watch)' : 'var(--pro-sell)';
    document.getElementById('sc-pnl').textContent = (d.total_pnl >= 0 ? '+' : '') + '₹' + (d.total_pnl||0).toLocaleString('en-IN', {maximumFractionDigits:0});
    document.getElementById('sc-pnl').style.color = d.total_pnl >= 0 ? 'var(--pro-buy)' : 'var(--pro-sell)';
    document.getElementById('sc-open').textContent = d.open;
    document.getElementById('cnt-journal').textContent = (d.total || 0) + (d.open || 0);
  }).catch(() => {});
  
  fetch('/journal?outcome=OPEN')
    .then(r => r.json())
    .then(d => {
      journalTrades = d.trades || [];
    }).catch(() => {});
}

function renderJournal() {
  const tbody = document.getElementById('tblBody');
  const thead = document.getElementById('tblHead');
  
  thead.innerHTML = `<tr>
    <th>Stock Symbol</th>
    <th>Log Date</th>
    <th>Grade</th>
    <th>Signal Score</th>
    <th>Entry Price</th>
    <th>LTP Now</th>
    <th>Stop Loss</th>
    <th>Target T1</th>
    <th>Qty</th>
    <th>Live P&L</th>
    <th>Achieved R:R</th>
    <th>Regime</th>
    <th>Notes</th>
    <th>Actions</th>
  </tr>`;

  fetch('/journal').then(r => r.json()).then(d => {
    const trades = d.trades || [];
    document.getElementById('cnt-journal').textContent = trades.length;

    if (!trades.length) {
      tbody.innerHTML = `<tr><td colspan="18" class="empty-state">
        <i class="ti ti-notebook" style="font-size:32px;color:var(--text-muted);display:block;margin-bottom:8px"></i>
        No trades logged yet. Click <b>+ Ledger</b> on any signal row to start tracking.</td></tr>`;
      return;
    }

    tbody.innerHTML = trades.map(t => {
      const isOpen = t.outcome === 'OPEN';
      const isWin = t.outcome === 'WIN';
      const isLoss = t.outcome === 'LOSS';
      const pnlColor = t.pnl_amount > 0 ? 'var(--pro-buy)' : t.pnl_amount < 0 ? 'var(--pro-sell)' : 'var(--text-muted)';
      const outcomeStyle = isWin ? 'background:rgba(16,185,129,0.1);color:var(--pro-buy);border:1px solid rgba(16,185,129,0.2)'
                         : isLoss ? 'background:rgba(244,63,94,0.1);color:var(--pro-sell);border:1px solid rgba(244,63,94,0.2)'
                         : 'background:rgba(245,158,11,0.1);color:var(--pro-watch);border:1px solid rgba(245,158,11,0.2)';

      // data-* attrs let fetchLiveLTP() inject prices without full re-render
      return `<tr data-sym="${t.symbol}" data-entry="${t.entry_price||0}" data-qty="${t.quantity||0}" data-open="${isOpen?1:0}">
        <td>
          <span class="sym">${t.symbol}</span><br>
          <span style="${outcomeStyle};border-radius:4px;padding:2px 7px;font-size:9.5px;font-weight:800;display:inline-block;margin-top:2px">
            ${t.outcome}
          </span>
        </td>
        <td style="color:var(--text-muted);font-family:var(--font-mono)">${t.signal_date}</td>
        <td><span style="background:#172554;color:#60a5fa;border:1px solid #1d4ed8;border-radius:4px;padding:2px 7px;font-size:10px;font-weight:800">${t.conf_grade}</span></td>
        <td>${renderScoreBar(t.raw_score, t.conf_grade)}</td>
        <td style="color:var(--pro-electric);font-weight:700;font-family:var(--font-mono)">₹${(t.entry_price||0).toLocaleString('en-IN')}</td>
        <td style="font-family:var(--font-mono);font-weight:700">${(()=>{
          if (!isOpen) return '<span style="color:var(--text-muted)">—</span>';
          const liveStock = stocks.find(s => s.symbol === t.symbol);
          const ltp = (liveStock ? liveStock.price : null) || t.current_ltp;
          if (!ltp) return '<span style="color:var(--text-muted)">—</span>';
          const diff = ltp - (t.entry_price||0);
          const col = diff >= 0 ? 'var(--pro-buy)' : 'var(--pro-sell)';
          return `<span style="color:${col}">₹${ltp.toLocaleString('en-IN',{minimumFractionDigits:2})}</span><br><span style="font-size:8.5px;color:${col}">${diff>=0?'+':''}${diff.toFixed(2)}</span>`;
        })()}</td>
        <td style="color:var(--pro-sell);font-family:var(--font-mono)">₹${(t.stop_loss||0).toLocaleString('en-IN')}</td>
        <td style="color:var(--pro-buy);font-family:var(--font-mono)">₹${(t.target_t1||0).toLocaleString('en-IN')}</td>
        <td style="font-family:var(--font-mono)">${t.quantity || '—'}</td>
        <td style="font-weight:800;font-family:var(--font-mono)">${(()=>{
          if (!isOpen) return `<span style="color:${pnlColor}">${t.pnl_amount ? (t.pnl_amount > 0 ? '+' : '') + '₹'+t.pnl_amount.toLocaleString('en-IN') : '—'}</span>`;
          const liveStock = stocks.find(s => s.symbol === t.symbol);
          const ltp = (liveStock ? liveStock.price : null) || t.current_ltp;
          if (!ltp || !t.quantity) return '<span style="color:var(--text-muted)">—</span>';
          const livePnl = (ltp - (t.entry_price||0)) * (t.quantity||0);
          const liveCol = livePnl >= 0 ? 'var(--pro-buy)' : 'var(--pro-sell)';
          return `<span style="color:${liveCol};animation: ${Math.abs(livePnl)>500?'pulse 1.5s ease-in-out infinite':''}">` +
                 `${livePnl >= 0 ? '+' : ''}₹${Math.abs(livePnl).toLocaleString('en-IN',{maximumFractionDigits:0})}</span>` +
                 `<br><span style="font-size:8.5px;color:${liveCol}">LIVE</span>`;
        })()}</td>
        <td style="color:${pnlColor};font-weight:700;font-family:var(--font-mono)">${t.rr_achieved ? t.rr_achieved.toFixed(2)+'R' : '—'}</td>
        <td style="color:var(--text-muted);font-size:11px;font-weight:600">${t.regime}</td>
        <td style="color:var(--text-muted);font-size:11px;max-width:180px;overflow:hidden;text-overflow:ellipsis">${t.notes || '—'}</td>
        <td>
          ${isOpen ? `<button onclick="openCloseModal(${t.id}, ${t.entry_price}, ${t.stop_loss}, '${t.symbol}')"
            style="background:#14532d;color:#4ade80;border:none;border-radius:4px;padding:3.5px 8px;font-size:10px;cursor:pointer;font-weight:800;margin-right:4px">
            Close
          </button>` : ''}
          <button onclick="deleteTrade(${t.id})" style="background:#450a0a;color:#f87171;border:none;border-radius:4px;padding:3.5px 8px;font-size:10px;cursor:pointer;font-weight:800">
            Del
          </button>
        </td>
      </tr>`;
    }).join('');
  });
}

loadScorecard();
loadPrefs();
loadWatchlist();
detectColdStart();
checkMarketStatus();
marketTimer = setInterval(checkMarketStatus, 30000);
setTab(activeTab);

// On page load: check real server state to recover from stale UI state
fetch('/status').then(r=>r.json()).then(d=>{
  document.getElementById('coldBanner').classList.remove('show');
  if(d.scanning){
    setScanningUI(true);
    if(!pollTimer) pollTimer=setInterval(checkStatus,1500);
  } else {
    setScanningUI(false);
    if(pollTimer){ clearInterval(pollTimer); pollTimer=null; }
    if(d.signals && d.signals.length > 0){
      // Real signals from server — always exclude demo stocks
      const demoEl = document.getElementById('demoDataSelect');
      if(demoEl) demoEl.value = 'hide';
      stocks = preprocess(d.signals);
      document.getElementById('lastScan').textContent = d.last_scan ? 'Last scan: ' + d.last_scan : '';
      updateSidebarWidgets();
      updateCounts();
      render();
    } else {
      // No scan ever run — show empty state, no mock stocks
      const demoEl = document.getElementById('demoDataSelect');
      if(demoEl) demoEl.value = 'hide';
      stocks = [];
      render(); // renders empty-state message
    }
    if(d.error && d.error !== 'Scan cancelled' && d.error !== 'Scan stopped by user'){
      showToast('Last scan error: ' + d.error, true);
    }
  }
}).catch(()=>{
  document.getElementById('coldBanner').classList.add('show');
});

// Fetch real sector index data on page load, refresh every 5 min
fetchSectorRotation();
if (activeSectors_fetchTimer) clearInterval(activeSectors_fetchTimer);
activeSectors_fetchTimer = setInterval(fetchSectorRotation, 5 * 60 * 1000);

// ── Fix 1: Live LTP injector for Trade Ledger (every 60s, independent) ──
let _ltpRefreshTimer = null;
function fetchLiveLTP() {
  fetch('/ltp_live')
    .then(r => r.json())
    .then(ltpMap => {
      const rows = document.querySelectorAll('#tblBody tr[data-open="1"]');
      rows.forEach(row => {
        const sym   = row.getAttribute('data-sym');
        const entry = parseFloat(row.getAttribute('data-entry') || 0);
        const qty   = parseFloat(row.getAttribute('data-qty') || 0);
        const ltp   = ltpMap[sym] || ltpMap[(sym||'').toUpperCase()];
        if (!ltp) return;
        const diff = ltp - entry;
        const col  = diff >= 0 ? 'var(--pro-buy)' : 'var(--pro-sell)';
        const cells = row.querySelectorAll('td');
        // 6th column = LTP Now (index 5)
        if (cells[5]) {
          cells[5].innerHTML =
            `<span style="color:${col};font-weight:700;font-family:var(--font-mono)">\u20b9${ltp.toLocaleString('en-IN',{minimumFractionDigits:2})}</span>` +
            `<br><span style="font-size:8.5px;color:${col}">${diff>=0?'+':''}${diff.toFixed(2)}</span>`;
        }
        // 10th column = Live P&L (index 9)
        if (cells[9] && qty > 0) {
          const livePnl = (ltp - entry) * qty;
          const pCol = livePnl >= 0 ? 'var(--pro-buy)' : 'var(--pro-sell)';
          cells[9].innerHTML =
            `<span style="color:${pCol};font-weight:800;font-family:var(--font-mono)">${livePnl>=0?'+':''}\u20b9${Math.abs(livePnl).toLocaleString('en-IN',{maximumFractionDigits:0})}</span>` +
            `<br><span style="font-size:8.5px;color:${pCol}">\u26a1 LIVE</span>`;
        }
      });
    })
    .catch(() => {});
}

// LTP timer is managed inside setTab() above
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug, threaded=True)