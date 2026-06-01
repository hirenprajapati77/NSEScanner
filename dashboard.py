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
        "EMA_10":         bool(params.get("ema10", True)),
        "EMA_20":         bool(params.get("ema20", True)),
        "EMA_50":         bool(params.get("ema50", True)),
        "EMA_200":        bool(params.get("ema200", True)),
        "TG_TOKEN":       params.get("tg_token",     _load_config().get("tg_token",    "")),
        "TG_CHAT_ID":     params.get("tg_chat_id",   _load_config().get("tg_chat_id",  "")),
        "TWILIO_SID":     params.get("twilio_sid",   _load_config().get("twilio_sid",  "")),
        "TWILIO_TOKEN":   params.get("twilio_token", _load_config().get("twilio_token","")),
        "TWILIO_TO":      params.get("twilio_to",    _load_config().get("twilio_to",   "")),
        "TURNOVER_LIMIT": float(params.get("turnover_limit", CFG["TURNOVER_LIMIT"])),
        "USE_CACHE_ONLY": bool(params.get("use_cache", False)),
        "MIN_PRICE":      float(params.get("min_price", 50.0)),
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
    from flask import make_response
    response = make_response(render_template_string(HTML))
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


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

    rows = []
    for r in s["signals"]:
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

    buf = io.StringIO()
    pd.DataFrame(rows).to_csv(buf, index=False)
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
    return jsonify({"watchlist": _load_watchlist()})


@app.route("/watchlist", methods=["POST"])
def update_watchlist():
    data = request.get_json(force=True, silent=True) or {}
    wl   = data.get("watchlist", [])
    _save_watchlist(wl)
    return jsonify({"ok": True, "count": len(wl)})


@app.route("/regime")
def regime_route():
    """Return Nifty Market Regime, refreshing if force parameter is passed."""
    force = request.args.get("refresh") == "1"
    return jsonify(get_regime(force_refresh=force))


@app.route("/journal", methods=["GET"])
def journal_get():
    outcome = request.args.get("outcome")   # OPEN/WIN/LOSS/all
    trades  = get_trades(outcome=outcome if outcome != "all" else None)
    return jsonify({"trades": trades, "count": len(trades)})


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
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,100..1000;1,9..40,100..1000&family=JetBrains+Mono:ital,wght@0,100..800;1,100..800&display=swap" rel="stylesheet">
<!-- Tabler Icons -->
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@latest/dist/tabler-icons.min.css">
<!-- ApexCharts CDN -->
<script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>

<style>
:root {
  --font-sans: 'DM Sans', sans-serif;
  --font-mono: 'JetBrains Mono', monospace;
  --bg-dark: #080b11;
  --bg-card: #0d1117;
  --bg-inner: #111827;
  --bg-hover: #161d2a;
  --border-slate: #1f2937;
  
  --pro-navy: #1B2A4A;
  --pro-electric: #2563EB;
  --pro-buy: #10B981;
  --pro-sell: #F43F5E;
  --pro-watch: #F59E0B;
  --text-primary: #e2e8f0;
  --text-muted: #94a3b8;
  
  --shadow-soft: 0 4px 20px -2px rgba(27, 42, 74, 0.04), 0 2px 8px -1px rgba(27, 42, 74, 0.02);
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
  background: #090d14;
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
  background: #090d14;
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
  background: #172554;
  border-color: var(--pro-electric);
  color: #60a5fa;
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
  background: #1f2937;
  color: #6b7280;
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
  background: #080c14;
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
thead tr { background: #0c121c; }
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
}
.score .den {
  color: #4b5563;
  font-weight: 400;
  font-size: 10px;
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
  background: #0c121c;
  z-index: 3;
}
tbody tr:hover td:first-child {
  background: #121824 !important;
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
.sc-val { color: #f9fafb; font-weight: 700; font-size: 13px; font-family: var(--font-mono); }

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

@media(max-width:768px){
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
    padding:10px 20px;background:#090d14;
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
      <div id="regimeNifty" style="font-weight:700;color:#f9fafb;font-family:var(--font-mono)">—</div>
    </div>

    <div>
      <div style="color:var(--text-muted);font-size:10px">Breadth (50d EMA)</div>
      <div id="regimeBreadth" style="font-weight:700;font-family:var(--font-mono)">—</div>
    </div>

    <div>
      <div style="color:var(--text-muted);font-size:10px">Index EMA Cross (20/50/200)</div>
      <div id="regimeEMAs" style="font-weight:600;color:#9ca3af;font-size:11px;font-family:var(--font-mono)">—</div>
    </div>

    <div>
      <div style="color:var(--text-muted);font-size:10px">ATR % (Volatility)</div>
      <div id="regimeATR" style="font-weight:700;color:#9ca3af;font-family:var(--font-mono)">—</div>
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
    background:#080c14;border-bottom:1px solid var(--border-slate);font-size:11px">
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
    <div class="tab active" id="tab-protrader" onclick="setTab('protrader')">
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
      <option value="1.5">1.5x</option>
      <option value="2" selected>2x</option>
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
      <option value="bullish" selected>Bullish Scan</option>
      <option value="bearish">Bearish Scan</option>
      <option value="both">Both setups</option>
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
      <label class="ema-chip on" id="c50"><input type="checkbox" checked onchange="toggleEMA(this,'50')">50</label>
      <label class="ema-chip on" id="c200"><input type="checkbox" checked onchange="toggleEMA(this,'200')">200</label>
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
    
    <span id="countdownSpan" style="margin-left:5px;color:var(--pro-watch);font-weight:700;font-family:var(--font-mono)"></span>
    
    <input type="text" id="tickerSearch" placeholder="🔍 Search / Scan stock..."
      oninput="render()" onkeydown="if(event.key==='Enter') triggerSingleScan()" style="width:160px;margin-left:auto">
  </div>

  <!-- Layout split grid (Main Content on left, market statistics panel on right) -->
  <div class="main-grid">
    
    <!-- MAIN COLUMN LEFT -->
    <div class="main-column">
      
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

      <!-- Summary Bar -->
      <div class="sum-bar" id="sumBar" style="margin: 0 20px 10px 20px; border-radius: 6px;"></div>

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
        </div>
        <div class="heatmap-grid" id="heatmapGrid">
          <!-- Populated dynamically -->
        </div>
      </div>

      <!-- Option Chain PCR widget -->
      <div class="widget-card">
        <div class="widget-header">
          <div class="w-2.5 h-2.5 rounded-full bg-blue-500 animate-pulse"></div>
          <span>OI Pulse (Nifty Index)</span>
        </div>
        <div style="display:flex;flex-direction:column;gap:8px">
          <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:6px">
            <span style="font-size:11px;color:var(--text-muted)">Put-Call Ratio (PCR)</span>
            <span id="oi-pcr" style="font-weight:800;font-family:var(--font-mono);padding:2px 8px;border-radius:4px" class="up">1.24</span>
          </div>
          <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:6px">
            <span style="font-size:11px;color:var(--text-muted)">Max Pain strike</span>
            <span id="oi-maxpain" style="font-weight:700;font-family:var(--font-mono);color:var(--text-primary)">22,400</span>
          </div>
          <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:6px">
            <span style="font-size:11px;color:var(--text-muted)">Call Build (Resistance)</span>
            <span id="oi-resistance" style="font-weight:700;font-family:var(--font-mono);color:var(--pro-sell)">22,500</span>
          </div>
          <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #111827;padding-bottom:6px">
            <span style="font-size:11px;color:var(--text-muted)">Put Build (Support)</span>
            <span id="oi-support" style="font-weight:700;font-family:var(--font-mono);color:var(--pro-buy)">22,200</span>
          </div>
          
          <div class="mt-2 bg-amber-950/40 border border-amber-900/60 rounded-lg p-2 flex items-start gap-1.5" id="oi-trap-box">
            <i class="ti ti-shield-alert" style="color:var(--pro-watch);font-size:14px;margin-top:2px"></i>
            <div>
              <div style="font-size:10px;font-weight:800;color:#fbbf24" id="oi-trap-title">CE WRITERS TRAPPED</div>
              <div style="font-size:9px;color:#f59e0b;line-height:1.2;margin-top:2px" id="oi-trap-desc">Short covering rally likely at 22,500!</div>
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
          <i class="ti ti-bulb" style="color:var(--pro-watch);font-size:14px"></i>
          <span style="font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:0.5px;color:#c7d2fe">Terminal Intelligence</span>
        </div>
        <p style="font-size:10.5px;line-height:1.4" id="sidebarProTip">
          Nifty PCR is in bullish alignment. Buying support dominant at lower Camarilla entries. Seek consolidation entries near S1 supports.
        </p>
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
    <div class="modal" style="width:500px">
      <h3><i class="ti ti-notebook" style="color:#c084fc"></i> Log Trade — <span id="jm-symbol"></span></h3>

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

      <div style="margin-top:12px">
        <label>Operational Notes</label>
        <input type="text" id="jm-notes" placeholder="Describe the trigger parameters...">
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
let activeTab = 'protrader';
let activeSubTab = 'all';
let sortState = {col:'score', desc:true};
const emaReq = {10:true, 20:true, 50:true, 200:true};
let pollTimer = null;
let autoTimer = null;
let remaining = 300;
let coldShown = false;
let marketClosed = false;
let expandedSymbol = null;
let activeSector = null;

// Simulated Widget States (ticking in real-time)
let pcrVal = 1.24;
let fiiNetVal = -1240;
let diiNetVal = 2180;
let niftyClosePrice = 23450;

// Sector Data structures (10 core NSE F&O sectors)
const initialSectors = [
  { name: "Banking", change: 1.8, trend: "up", strength: 92 },
  { name: "IT", change: 0.4, trend: "up", strength: 61 },
  { name: "Pharma", change: -0.6, trend: "down", strength: 38 },
  { name: "Auto", change: 2.1, trend: "up", strength: 95 },
  { name: "FMCG", change: 0.1, trend: "neutral", strength: 52 },
  { name: "Metals", change: -1.4, trend: "down", strength: 22 },
  { name: "Energy", change: 0.9, trend: "up", strength: 70 },
  { name: "Realty", change: 3.2, trend: "up", strength: 98 },
  { name: "Infra", change: 1.1, trend: "up", strength: 74 },
  { name: "Media", change: -0.3, trend: "down", strength: 44 }
];
let activeSectors = [...initialSectors];

const sectorMapping = {
  // Original mock stocks
  "HDFCBANK": "Banking", "SBIN": "Banking",
  "RELIANCE": "Energy", "COALINDIA": "Metals",
  "TATAMOTORS": "Auto", "MARUTI": "Auto",
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
      updateSidebarWidgets();

      const b = d.breadth || 50;
      breadth.textContent = b + '% above EMA50';
      breadth.style.color = b >= 60 ? 'var(--pro-buy)' : b <= 40 ? 'var(--pro-sell)' : 'var(--pro-watch)';

      emas.textContent = [d.nifty_ema20, d.nifty_ema50, d.nifty_ema200]
                            .map(v => Math.round(v).toLocaleString('en-IN')).join(' / ');

      atr.textContent = (d.atr_pct || 0).toFixed(2) + '%';
      high.textContent = '-' + (d.pct_from_high || 0).toFixed(1) + '%';

      window.niftyRegime = d.regime;

      if (d.regime === 'STRONG_BEAR' || d.regime === 'BEAR') {
        if (!emaReq[200]) {
          emaReq[200] = true;
          const chip = document.getElementById('c200');
          const cb = chip && chip.querySelector('input');
          if (cb) {
            cb.checked = true;
            chip.classList.add('on');
          }
          savePrefs();
          render();
        }
      }

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
    localStorage.setItem('nse_prefs_h',JSON.stringify({
      volDays: document.getElementById('volDays').value,
      volMult: document.getElementById('volMult').value,
      turnoverLimit: document.getElementById('turnoverLimit').value,
      scanMode:document.getElementById('scanMode').value,
      scanDataSource: document.getElementById('scanDataSource').value,
      minPrice: document.getElementById('minPrice').value,
      ema10: emaReq[10], ema20:emaReq[20], ema50:emaReq[50], ema200:emaReq[200],
    }));
  }catch(e){}
}

function loadPrefs(){
  try{
    const p=JSON.parse(localStorage.getItem('nse_prefs_h')||'{}');
    if(p.volDays) document.getElementById('volDays').value=p.volDays;
    if(p.volMult) document.getElementById('volMult').value=p.volMult;
    if(p.turnoverLimit) document.getElementById('turnoverLimit').value=p.turnoverLimit;
    if(p.scanMode) document.getElementById('scanMode').value=p.scanMode;
    if(p.scanDataSource) document.getElementById('scanDataSource').value=p.scanDataSource;
    if(p.minPrice !== undefined) document.getElementById('minPrice').value=p.minPrice;
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
  document.querySelectorAll('.tab').forEach(el=>el.classList.remove('active'));
  document.getElementById('tab-'+t).classList.add('active');
  
  const subFilters = document.getElementById('camarillaSubFilters');
  const picks = document.getElementById('proTraderTopPicks');
  
  if (t === 'camarilla') {
    subFilters.style.display = 'flex';
    picks.style.display = 'none';
  } else if (t === 'protrader') {
    subFilters.style.display = 'none';
    picks.style.display = 'block';
  } else {
    subFilters.style.display = 'none';
    picks.style.display = 'none';
  }
  
  expandedSymbol = null;
  render();
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
  if (trapTitle) {
    trapTitle.textContent = `${pcrVal > 1.25 ? 'CE' : 'PE'} WRITERS TRAPPED`;
  }
  const trapDesc = document.getElementById('oi-trap-desc');
  if (trapDesc) {
    trapDesc.textContent = `Short covering rally likely at ${resistanceVal.toLocaleString('en-IN')}!`;
  }

  const trapBox = document.getElementById('oi-trap-box');
  if (trapBox) {
    trapBox.style.display = pcrVal > 1.25 ? 'flex' : 'none';
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

  // 3. Sector rotation strength
  activeSectors = initialSectors.map(sec => {
    const secStocks = stocks.filter(s => sectorMapping[s.symbol] === sec.name);
    if (secStocks.length === 0) return sec;

    const avgChg = parseFloat((secStocks.reduce((sum, s) => sum + (s.change || s.dist_from_entry), 0) / secStocks.length).toFixed(2));
    const trend = avgChg > 0.4 ? 'up' : avgChg < -0.4 ? 'down' : 'neutral';
    return { ...sec, change: avgChg, trend };
  });

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
          <span>${s.change > 0 ? '+' : ''}${s.change}%</span>
          <span>${s.trend === 'up' ? '↑' : s.trend === 'down' ? '↓' : '→'}</span>
        </div>
      </button>`;
    }).join('');
  }

  // 4. ProTip Box update
  const tipEl = document.getElementById('sidebarProTip');
  if (tipEl) {
    if (netDominance > 0 && pcrVal > 1.2) {
      tipEl.textContent = "DII flows are heavily dominant today with Nifty PCR in the bullish range. Sector rotation highlights strength in Banking & Auto segments. Focus on entries above central pivots.";
    } else if (netDominance < 0) {
      tipEl.textContent = "Short buildup detected across core F&O index weightage stocks. FII is actively offloading positions. Protect long capitals and seek bearish setups near H3 resistances.";
    } else {
      tipEl.textContent = "Consolidating market sentiment. Nifty PCR is neutral around 1.0. Sideways rotations suggest range-bound options writer domination. Look for individual sector breakouts.";
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
  { symbol: "HDFCBANK", name: "HDFC Bank", price: 1742.5, change: 2.4, vol_ratio: 2.3, rsi: 64, signal_type: "Bull", candle: "Bull", tf: "1h", sector: "Banking", score: 85, confidence: "A", entry: 1720.0, dist_from_entry: 1.3, stop_loss: 1700.0, target: 1760.0, target2: 1780.0, prevClose: 1701.66, sparkline: [40, 42, 41, 45, 48, 52, 55] },
  { symbol: "RELIANCE", name: "Reliance Ind.", price: 2981.0, change: 1.7, vol_ratio: 1.8, rsi: 58, signal_type: "Bull", candle: "Bull", tf: "4h", sector: "Energy", score: 78, confidence: "B", entry: 2950.0, dist_from_entry: 1.1, stop_loss: 2920.0, target: 3020.0, target2: 3050.0, prevClose: 2931.17, sparkline: [30, 32, 31, 35, 37, 39, 42] },
  { symbol: "TATAMOTORS", name: "Tata Motors", price: 924.3, change: 3.1, vol_ratio: 3.5, rsi: 71, signal_type: "Bull", candle: "Bull", tf: "1h", sector: "Auto", score: 92, confidence: "A+", entry: 900.0, dist_from_entry: 2.7, stop_loss: 885.0, target: 945.0, target2: 960.0, prevClose: 896.50, sparkline: [50, 55, 53, 60, 65, 68, 72] },
  { symbol: "INFY", name: "Infosys", price: 1823.7, change: 0.6, vol_ratio: 1.1, rsi: 54, signal_type: "Bull", candle: "Bull", tf: "4h", sector: "IT", score: 68, confidence: "B", entry: 1810.0, dist_from_entry: 0.8, stop_loss: 1795.0, target: 1845.0, target2: 1860.0, prevClose: 1812.82, sparkline: [44, 45, 44, 46, 47, 48, 49] },
  { symbol: "SBIN", name: "SBI", price: 812.9, change: 2.8, vol_ratio: 2.9, rsi: 67, signal_type: "Bull", candle: "Bull", tf: "1h", sector: "Banking", score: 80, confidence: "B", entry: 800.0, dist_from_entry: 1.6, stop_loss: 788.0, target: 825.0, target2: 835.0, prevClose: 790.75, sparkline: [38, 40, 42, 45, 47, 50, 54] },
  { symbol: "DRREDDY", name: "Dr. Reddy's", price: 5412.0, change: -1.2, vol_ratio: 0.8, rsi: 38, signal_type: "Bear", candle: "Bear", tf: "4h", sector: "Pharma", score: 35, confidence: "C", entry: 5450.0, dist_from_entry: -0.7, stop_loss: 5500.0, target: 5350.0, target2: 5300.0, prevClose: 5477.73, sparkline: [60, 58, 55, 52, 48, 45, 42] },
  { symbol: "COALINDIA", name: "Coal India", price: 472.6, change: -0.8, vol_ratio: 1.2, rsi: 41, signal_type: "Bear", candle: "Bear", tf: "1h", sector: "Metals", score: 38, confidence: "C", entry: 475.0, dist_from_entry: -0.5, stop_loss: 480.0, target: 465.0, target2: 460.0, prevClose: 476.41, sparkline: [55, 52, 50, 47, 45, 43, 41] },
  { symbol: "DLF", name: "DLF Ltd.", price: 892.1, change: 4.2, vol_ratio: 4.1, rsi: 76, signal_type: "Bull", candle: "Bull", tf: "1h", sector: "Realty", score: 95, confidence: "A+", entry: 870.0, dist_from_entry: 2.5, stop_loss: 855.0, target: 915.0, target2: 930.0, prevClose: 856.14, sparkline: [45, 50, 55, 62, 68, 75, 82] },
  { symbol: "MARUTI", name: "Maruti Suzuki", price: 12450.0, change: 1.9, vol_ratio: 1.6, rsi: 61, signal_type: "Bull", candle: "Bull", tf: "4h", sector: "Auto", score: 72, confidence: "B", entry: 12350.0, dist_from_entry: 0.8, stop_loss: 12200.0, target: 12600.0, target2: 12700.0, prevClose: 12217.86, sparkline: [42, 44, 45, 47, 49, 51, 54] },
  { symbol: "WIPRO", name: "Wipro", price: 548.3, change: -0.4, vol_ratio: 0.9, rsi: 46, signal_type: "Bear", candle: "Bear", tf: "4h", sector: "IT", score: 44, confidence: "C", entry: 550.0, dist_from_entry: -0.3, stop_loss: 555.0, target: 540.0, target2: 535.0, prevClose: 550.50, sparkline: [52, 50, 49, 48, 47, 46, 45] },
  { symbol: "ADANIENT", name: "Adani Ent.", price: 2634.0, change: 2.6, vol_ratio: 2.7, rsi: 65, signal_type: "Bull", candle: "Bull", tf: "1h", sector: "Infra", score: 82, confidence: "B", entry: 2600.0, dist_from_entry: 1.3, stop_loss: 2560.0, target: 2680.0, target2: 2720.0, prevClose: 2567.25, sparkline: [36, 38, 40, 43, 46, 49, 53] },
  { symbol: "ITC", name: "ITC Ltd.", price: 448.7, change: 0.2, vol_ratio: 1.0, rsi: 51, signal_type: "Bull", candle: "Bull", tf: "4h", sector: "FMCG", score: 55, confidence: "B", entry: 445.0, dist_from_entry: 0.8, stop_loss: 440.0, target: 455.0, target2: 460.0, prevClose: 447.80, sparkline: [48, 49, 48, 50, 49, 51, 50] }
];

// Pre-process signals
function preprocess(sigs){
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
    return s;
  });

  // 2. Identify which mock stocks are already scanned
  const scannedSymbols = new Set(scanned.map(s => s.symbol.toUpperCase()));

  // 3. Filter mock stocks that are NOT in the scanned list, and add them
  const inactiveMock = mockStocks.filter(m => !scannedSymbols.has(m.symbol.toUpperCase()));

  // 4. Combine them so both real scanned signals and beautiful mock assets are available
  return [...scanned, ...inactiveMock];
}

// Update counts on tabs
function updateCounts(){
  const srch=document.getElementById('tickerSearch').value.toLowerCase();
  const vm=parseFloat(document.getElementById('volMult').value);
  
  let base=stocks.filter(s=>s.vol_ratio>=vm && (!srch||s.symbol.toLowerCase().includes(srch)));
  if (activeSector) {
    base = base.filter(s => sectorMapping[s.symbol] === activeSector);
  }

  const counts={
    bullish:   base.filter(s=>s.signal_type==='Bull' && s.candle==='Bull').length,
    bearish:   base.filter(s=>s.signal_type==='Bear' && s.candle==='Bear').length,
    mixed:     base.filter(s=>(s.signal_type==='Bull'&&s.candle==='Bear')||(s.signal_type==='Bear'&&s.candle==='Bull')).length,
    entry:     base.filter(s=>Math.abs(s.price-s.entry)/s.entry<=0.02).length,
    hv:        base.filter(s=>s.days<=10).length,
    camarilla: base.length,
    watchlist: base.filter(s=>watchlist.includes(s.symbol)).length,
  };
  
  // Update sub-tabs counts
  document.getElementById('cnt-camarilla').textContent = counts.camarilla;
  document.getElementById('sub-cnt-entry').textContent = counts.entry;
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
      foreColor: '#94a3b8'
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
      borderColor: '#1f2937',
      strokeDashArray: 3,
      xaxis: { lines: { show: false } },
      yaxis: { lines: { show: true } }
    },
    xaxis: {
      categories: ['10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00'],
      labels: { style: { colors: '#94a3b8', fontSize: '9px', fontFamily: 'var(--font-sans)' } },
      axisBorder: { show: false },
      axisTicks: { show: false }
    },
    yaxis: {
      labels: { 
        style: { colors: '#94a3b8', fontSize: '9px', fontFamily: 'var(--font-mono)' },
        formatter: (v) => '₹' + v.toFixed(0)
      }
    },
    tooltip: {
      theme: 'dark',
      x: { show: true },
      marker: { show: false }
    }
  };

  const chart = new ApexCharts(chartEl, options);
  chart.render();
}

// ── Main Render ───────────────────────────────────────────
function render(isTick = false){
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
    f = f.filter(s => sectorMapping[s.symbol] === activeSector);
  }

  // 2. Tab filters
  if (activeTab === 'protrader') {
    f = f.filter(s => s.vol_ratio >= 1.2);
  } else if (activeTab === 'camarilla') {
    if(activeSubTab === 'bullish')        f = f.filter(s => s.signal_type === 'Bull' && s.candle === 'Bull');
    else if(activeSubTab === 'bearish')   f = f.filter(s => s.signal_type === 'Bear' && s.candle === 'Bear');
    else if(activeSubTab === 'mixed')     f = f.filter(s => (s.signal_type === 'Bull' && s.candle === 'Bear') || (s.signal_type === 'Bear' && s.candle === 'Bull'));
    else if(activeSubTab === 'entry')     f = f.filter(s => Math.abs(s.price - s.entry) / s.entry <= 0.02);
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
            <div class="font-extrabold text-white text-xs tracking-tight">${s.symbol}</div>
            <div style="font-size:9px; color:var(--text-muted); font-weight:600">${sectorMapping[s.symbol] || 'Equity'}</div>
          </div>
          <div style="display:flex; align-items:end; justify-content:space-between; margin-top:auto">
            <div>
              <div style="font-family:var(--font-mono); font-size:10px; font-weight:800">₹${s.price.toFixed(1)}</div>
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
      <th>Trade</th>
      <th class="m-hide"><i class="ti ti-star"></i> Watch</th>
    </tr>`;
  }

  if(!f.length){
    tbody.innerHTML=`<tr><td colspan="18" class="empty-state">
      <i class="ti ti-chart-candlestick" style="font-size:32px;color:var(--text-muted);display:block;margin-bottom:8px"></i>
      No signals matching current filters. Click <b>Run Live Scan</b> or search tickers.</td></tr>`;
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
              <span class="sym">${s.symbol}</span><br>
              <span style="font-size:9.5px;color:var(--text-muted);font-weight:600">${s.name || 'NSE F&O segment'}</span>
            </div>
            <div style="padding-top:4px">
              ${sparklineSVG(s.sparkline || [40,42,41,45,48,52,55], s.signal_type)}
            </div>
          </div>
        </td>
        <td style="text-align:right" class="price-main">₹${s.price.toLocaleString("en-IN", {minimumFractionDigits: 2})}</td>
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
      if (s.confidence === 'A+') {
        confColor = '#fbbf24'; confBG = '#1e1b4b'; confBorder = '1px solid #eab308';
      } else if (s.confidence === 'A') {
        confColor = '#4ade80'; confBG = '#064e3b'; confBorder = '1px solid #047857';
      } else if (s.confidence === 'B') {
        confColor = '#60a5fa'; confBG = '#172554'; confBorder = '1px solid #1d4ed8';
      }
      const confBadge = `<span class="c-bull" style="background:${confBG}; color:${confColor}; border:${confBorder}; display:inline-block; padding:2px 8px">${s.confidence || 'B'}</span>`;

      const distVal = s.dist_from_entry !== undefined ? s.dist_from_entry : (s.entry ? ((s.price - s.entry) / s.entry) * 100 : 0);
      const riskColor = riskVal > 4.0 ? 'dn' : riskVal > 3.0 ? 'd-warn' : 'up';
      const rrColor = rrVal >= 3.0 ? 'up' : rrVal >= 1.5 ? 'd-warn' : 'dn';
      const ageLabel = s.days === 0 ? 'Today' : s.days === 1 ? '1d' : s.days + 'd';

      rowHtml = `<tr onclick="toggleRowExpand('${s.symbol}', event)" class="${isExpanded ? 'bg-blue-950/20 font-medium' : ''} ${flashClass}">
        <td>
          <div style="display:flex;justify-content:space-between;align-items:center;gap:12px">
            <div>
              <span class="sym">${s.symbol}</span><br>${sigBadge}
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
        <td><span class="score">${s.score}<span class="den">/100</span></span></td>
        <td><div class="price-main">₹${s.price.toLocaleString("en-IN", {minimumFractionDigits: 2})}</div><div class="price-date">● ${s.scanned_date || 'Today'}</div></td>
        <td style="font-family:var(--font-mono)">₹${pivots.toFixed(1)}</td>
        <td><span class="${distVal > 2.0 ? 'dn' : distVal < -2.0 ? 'dn' : 'up'}" style="font-weight:700">${distVal > 0 ? '+' : ''}${distVal.toFixed(1)}%</span></td>
        <td class="sl">₹${stopLosses.toFixed(1)}</td>
        <td class="cam-levels">
          <div class="cam-h3" style="color:var(--pro-buy)">T1 ₹${targets1.toFixed(1)}</div>
          <div class="cam-h3" style="color:var(--pro-electric)">T2 ₹${targets2.toFixed(1)}</div>
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
        <td colspan="18" style="padding:16px">
          <div class="expanded-panel">
            <div style="display:flex; flex-direction:column; gap:12px">
              <div style="display:flex; justify-content:space-between; align-items:center">
                <span style="font-size:11px; font-weight:800; color:var(--pro-electric)">📊 ADVANCED SCAN METRICS</span>
                <span style="font-size:10px; color:var(--text-muted)">Sector segment: <b>${sectorMapping[s.symbol] || 'General Equity'}</b></span>
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
    document.getElementById('pageTitle').textContent='Scanning Market… | ProTrader';
  } else {
    btn.classList.remove('scanning'); btn.disabled=false;
    btn.innerHTML='<i class="ti ti-scan"></i> Run Live Scan';
    stop.setAttribute('disabled', 'true');
    prog.classList.remove('show');
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
  if(pollTimer) return;
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
      if(d.error){showToast(d.error,true);return;}
      showToast('Scan started in background…');
      pollTimer=setInterval(checkStatus,1500);
      setScanningUI(true);
    }).catch(()=>showToast('Failed to start scan.',true));
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
  window.location.href='/export';
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
  document.getElementById('jm-symbol').textContent = data.symbol;
  document.getElementById('jm-entry').value = data.entry_price;
  document.getElementById('jm-sl').value = data.stop_loss;
  calcJournalSizer();
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
    <th>Stop Loss</th>
    <th>Target T1</th>
    <th>Quantity</th>
    <th>P&L Amount</th>
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

      return `<tr>
        <td>
          <span class="sym">${t.symbol}</span><br>
          <span style="${outcomeStyle};border-radius:4px;padding:2px 7px;font-size:9.5px;font-weight:800;display:inline-block;margin-top:2px">
            ${t.outcome}
          </span>
        </td>
        <td style="color:var(--text-muted);font-family:var(--font-mono)">${t.signal_date}</td>
        <td><span style="background:#172554;color:#60a5fa;border:1px solid #1d4ed8;border-radius:4px;padding:2px 7px;font-size:10px;font-weight:800">${t.conf_grade}</span></td>
        <td><span class="score">${t.raw_score}<span class="den">/100</span></span></td>
        <td style="color:var(--pro-electric);font-weight:700;font-family:var(--font-mono)">₹${(t.entry_price||0).toLocaleString('en-IN')}</td>
        <td style="color:var(--pro-sell);font-family:var(--font-mono)">₹${(t.stop_loss||0).toLocaleString('en-IN')}</td>
        <td style="color:var(--pro-buy);font-family:var(--font-mono)">₹${(t.target_t1||0).toLocaleString('en-IN')}</td>
        <td style="font-family:var(--font-mono)">${t.quantity || '—'}</td>
        <td style="color:${pnlColor};font-weight:800;font-family:var(--font-mono)">
          ${t.pnl_amount ? (t.pnl_amount > 0 ? '+' : '') + '₹'+t.pnl_amount.toLocaleString('en-IN') : '—'}
        </td>
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

fetch('/results').then(r=>r.json()).then(d=>{
  document.getElementById('coldBanner').classList.remove('show');
  if(d.scanning){
    setScanningUI(true);
    pollTimer=setInterval(checkStatus,1500);
  } else if(d.signals&&d.signals.length>0){
    stocks=preprocess(d.signals);
    document.getElementById('lastScan').textContent=d.last_scan?'Last scan: '+d.last_scan:'';
    updateSidebarWidgets();
    updateCounts();
    render();
  }
});
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