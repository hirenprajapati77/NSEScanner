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
import copy
from datetime import datetime

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
                "confidence":      _safe(r, "Confidence",     str,   "B"),
                "signal_strength": _safe(r, "Strength",       str,   "Moderate"),
                "signal_type":     _safe(r, "Signal",         str,   "Bull"),
                "price":           price,
                "entry":           _safe(r, "Entry",          float, 0.0),
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
                                     "ticker": "Starting bullish scan…", "eta": 0})
            bull = run_scan(
                tickers,
                bearish=False,
                progress_cb=_progress,
                cfg_override=cfg_override,
                stop_event=_stop_event,
            )
            all_signals.extend(bull)

        if scan_mode in ("bearish", "both") and not _stop_event.is_set():
            _update_state(progress={"current": 0, "total": len(tickers),
                                     "ticker": "Starting bearish scan…", "eta": 0})
            bear = run_scan(
                tickers,
                bearish=True,
                progress_cb=_progress,
                cfg_override=cfg_override,
                stop_event=_stop_event,
            )
            all_signals.extend(bear)

        all_signals.sort(key=lambda x: x["score"], reverse=True)

        if not _stop_event.is_set():
            save_csv(all_signals)
            now   = datetime.now().strftime("%d %b %Y %H:%M")
            _update_state(
                signals=all_signals,
                last_scan=now,
                scanning=False,
                scan_id=None,
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
    return render_template_string(HTML)


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
    
    return jsonify({
        "is_open": is_open,
        "time": now_ist.strftime("%d-%b-%Y %H:%M:%S IST"),
        "day": now_ist.strftime("%A")
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
            "Confidence":      r.get("confidence", "B"),
            "Strength":        r.get("signal_strength", "Moderate"),
            "Signal":          r.get("signal_type", "Bull"),
            "Price":           r.get("price"),
            "Entry":           r.get("entry"),
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


# ─────────────────────────────────────────────────────────────
# HTML DASHBOARD (inline template with Dynamic Upgrades)
# ─────────────────────────────────────────────────────────────
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title id="pageTitle">NSE Camarilla Volume Scanner</title>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@latest/dist/tabler-icons.min.css">
<style>
:root{--font:\'Outfit\',-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;--r:12px;--bg0:#080b11;--bg1:#0d1117;--bg2:#111827;--bg3:#1f2937;--border:#1f2937;--text:#e5e7eb;--muted:#6b7280;--green:#4ade80;--red:#f87171;--blue:#60a5fa;--yellow:#fbbf24;--teal:#34d399;--orange:#fb923c;}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:var(--font);background:var(--bg0);color:var(--text);padding:16px;font-size:13px;min-height:100vh}
.wrap{background:var(--bg1);border-radius:var(--r);overflow:hidden;border:1px solid var(--border);box-shadow:0 20px 40px rgba(0,0,0,.5)}

/* Top bar */
.top-bar{background:var(--bg2);padding:12px 20px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);flex-wrap:wrap;gap:8px}
.brand{font-size:16px;font-weight:600;display:flex;align-items:center;gap:6px}
.brand .nse{color:var(--blue)}.brand .vol{color:var(--yellow)}.brand .ver{color:var(--muted);font-size:11px;font-weight:400}
.live-badge{font-size:11px;color:var(--teal);background:#064e3b;padding:4px 12px;border-radius:99px;display:flex;align-items:center;gap:5px}

/* Banner alerts */
.cold-banner{display:none;background:#1c1200;border-left:3px solid var(--yellow);padding:10px 20px;font-size:12px;color:var(--yellow);align-items:center;gap:8px}
.cold-banner.show{display:flex}
.market-banner{display:none;background:#270808;border-left:3px solid var(--red);padding:10px 20px;font-size:12px;color:var(--red);align-items:center;gap:8px}

/* Mobile Scroll hint */
.mobile-scroll-hint{display:none;font-size:11px;color:var(--muted);padding:8px 20px;background:#090d14;border-bottom:1px solid var(--border);align-items:center;gap:6px;}

/* Tabs */
.tabs{background:var(--bg2);padding:0 16px;display:flex;gap:4px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.tab{padding:12px 16px;font-size:12px;cursor:pointer;color:var(--muted);border-bottom:2px solid transparent;display:flex;align-items:center;gap:6px;white-space:nowrap;transition:all .2s;user-select:none}
.tab:hover{color:var(--text);background:#161d2a}
.tab.active{color:var(--teal);border-color:var(--teal);font-weight:500}
.tab .cnt{background:#1d4ed8;color:#bfdbfe;border-radius:99px;padding:1px 7px;font-size:10px;font-weight:600}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block}
.dot.g{background:var(--green)}.dot.r{background:var(--red)}.dot.o{background:#fb923c}.dot.p{background:#c084fc}.dot.y{background:#fbbf24}

/* Controls */
.controls{background:var(--bg2);padding:12px 20px;display:flex;gap:10px;align-items:center;flex-wrap:wrap;border-bottom:1px solid var(--border)}
.controls label{font-size:12px;color:var(--muted);font-weight:500}
.controls select,.controls input[type=text]{background:var(--bg3);border:.5px solid #374151;color:var(--text);padding:5px 10px;border-radius:6px;font-size:12px;outline:none;transition:border-color .2s}
.controls select:focus,.controls input[type=text]:focus{border-color:var(--blue)}
.ema-filters{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.ema-chip{display:flex;align-items:center;gap:4px;font-size:11px;background:var(--bg3);border:.5px solid #374151;border-radius:99px;padding:4px 12px;color:var(--muted);cursor:pointer;user-select:none;transition:all .2s}
.ema-chip.on{background:#1e3a5f;border-color:var(--blue);color:var(--blue);font-weight:500}
.ema-chip input{display:none}

/* Buttons */
.btn{background:#1d4ed8;color:#fff;border:none;padding:6px 16px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:500;display:inline-flex;align-items:center;gap:6px;transition:background .2s}
.btn:hover{background:#2563eb}
.btn.scanning{background:#374151;cursor:not-allowed;opacity:.8}
.btn-stop{background:#7f1d1d;color:#fca5a5;border:none;padding:6px 16px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:500;display:none;align-items:center;gap:6px;transition:background .2s}
.btn-stop.show{display:inline-flex}
.btn-stop:hover{background:#991b1b}

/* Progress */
.prog-wrap{display:none;padding:14px 20px;background:var(--bg2);border-bottom:1px solid var(--border);align-items:center;gap:14px;flex-wrap:wrap}
.prog-wrap.show{display:flex}
.prog-bar-bg{flex:1;min-width:200px;height:8px;background:var(--bg3);border-radius:99px;overflow:hidden}
.prog-bar-fill{width:0%;height:100%;background:linear-gradient(90deg,#3b82f6,#60a5fa);border-radius:99px;transition:width .3s}
.prog-txt{font-size:12px;color:var(--muted);font-weight:500}

/* Summary bar */
.sum-bar{display:flex;gap:20px;padding:12px 20px;background:#080c14;border-bottom:1px solid var(--border);flex-wrap:wrap}
.si{font-size:12px;display:flex;align-items:center;gap:5px}
.si .lbl{color:var(--muted)}.si .val{color:var(--text);font-weight:600}
.si .g{color:var(--green)}.si .r{color:var(--red)}.si .y{color:var(--yellow)}

/* Table */
.tbl-wrap{overflow-x:auto;width:100%}
table{width:100%;border-collapse:collapse;font-size:12px;text-align:left}
thead tr{background:#161d2a}
th{padding:10px 14px;color:var(--muted);font-weight:500;font-size:11px;letter-spacing:.04em;white-space:nowrap;border-bottom:1px solid var(--border);cursor:pointer;user-select:none}
th:hover{color:var(--text);background:#1c2637}
.sort-icon{margin-left:3px;font-size:10px;color:#4b5563}
tbody tr{border-bottom:.5px solid var(--border);transition:background .15s}
tbody tr:hover{background:#121824}
td{padding:10px 14px;color:var(--text);white-space:nowrap;vertical-align:middle}
.sym{font-weight:700;font-size:13px;color:#f9fafb}
.sig-buy{background:#14532d;color:var(--green);border-radius:4px;padding:2px 7px;font-size:10px;font-weight:600;display:inline-block;margin-top:2px}
.sig-sell{background:#450a0a;color:var(--red);border-radius:4px;padding:2px 7px;font-size:10px;font-weight:600;display:inline-block;margin-top:2px}
.score{color:var(--blue);font-weight:600;font-size:13px}
.score .den{color:#4b5563;font-weight:400;font-size:11px}
.price-main{color:var(--blue);font-weight:600;font-size:13px}
.price-date{color:#4b5563;font-size:10px;margin-top:2px}
.up{color:var(--green);font-weight:500}.dn{color:var(--red);font-weight:500}.neutral{color:var(--muted)}
.sl{color:var(--red);font-weight:600}
.c-bull{border-radius:4px;padding:2px 8px;font-size:10px;font-weight:600}
.c-bear{border-radius:4px;padding:2px 8px;font-size:10px;font-weight:600}
.d-warn{color:var(--yellow);font-weight:500}
.ema-dots{display:flex;gap:4px}
.edot{width:10px;height:10px;border-radius:50%;background:#374151;border:1px solid var(--border)}
.edot.pass{background:var(--green);box-shadow:0 0 4px var(--green)}
.cam-levels{font-size:11px;line-height:1.6}
.cam-h3{color:var(--red);font-weight:500}.cam-l3{color:var(--teal);font-weight:500}.cam-pivot{color:var(--yellow);font-weight:500}
.vol-bar{height:5px;background:var(--bg3);border-radius:2px;width:60px;overflow:hidden;display:inline-block;vertical-align:middle}
.vol-fill{height:100%;background:var(--green);border-radius:2px}
.vol-fill.high{background:var(--yellow)}.vol-fill.spike{background:var(--red)}

/* Action bar */
.action-bar{background:var(--bg2);padding:12px 20px;display:flex;gap:10px;align-items:center;flex-wrap:wrap;border-top:1px solid var(--border)}
.tg-btn{background:#0369a1;color:#e0f2fe;border:none;border-radius:6px;padding:7px 14px;font-size:11px;font-weight:500;cursor:pointer;display:inline-flex;align-items:center;gap:6px;transition:background .2s}
.tg-btn:hover{background:#0284c7}
.wa-btn{background:#14532d;color:#86efac;border:none;border-radius:6px;padding:7px 14px;font-size:11px;font-weight:500;cursor:pointer;display:inline-flex;align-items:center;gap:6px;transition:background .2s}
.wa-btn:hover{background:#166534}
.csv-btn{background:#374151;color:var(--text);border:none;border-radius:6px;padding:7px 14px;font-size:11px;font-weight:500;cursor:pointer;display:inline-flex;align-items:center;gap:6px;transition:background .2s}
.csv-btn:hover{background:#4b5563}

/* Modal */
.modal-bg{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.85);z-index:100;align-items:center;justify-content:center;padding:20px;backdrop-filter:blur(4px)}
.modal-bg.show{display:flex}
.modal{background:#1f2937;border-radius:10px;padding:24px;width:420px;max-width:100%;border:1px solid #374151;box-shadow:0 25px 50px -12px rgba(0,0,0,.5);animation:mIn .2s ease-out}
@keyframes mIn{from{transform:scale(.95);opacity:0}to{transform:scale(1);opacity:1}}
.modal h3{color:#f9fafb;font-size:16px;margin-bottom:12px;display:flex;align-items:center;gap:8px}
.modal label{color:var(--muted);font-size:12px;display:block;margin-bottom:4px;margin-top:12px;font-weight:500}
.modal input{width:100%;background:#111827;border:.5px solid #374151;color:var(--text);padding:8px 12px;border-radius:6px;font-size:13px;outline:none;transition:border-color .2s}
.modal input:focus{border-color:var(--blue)}
.modal .mrow{display:flex;gap:10px;margin-top:20px}
.modal .btn-save{flex:1;background:#1d4ed8;color:#fff;border:none;border-radius:6px;padding:10px;font-size:13px;font-weight:500;cursor:pointer;transition:background .2s}
.modal .btn-save:hover{background:#2563eb}
.modal .btn-cancel{background:#374151;color:var(--muted);border:none;border-radius:6px;padding:10px 16px;font-size:13px;cursor:pointer;transition:background .2s}
.modal .btn-cancel:hover{color:var(--text)}

/* Empty state */
.empty-state{text-align:center;padding:60px 20px;color:#4b5563}

/* Toast */
.toast{position:fixed;bottom:24px;right:24px;background:#1e293b;border-left:4px solid var(--teal);color:#f1f5f9;padding:14px 24px;border-radius:6px;box-shadow:0 20px 25px -5px rgba(0,0,0,.5);z-index:1000;opacity:0;transform:translateY(20px);transition:all .3s;display:flex;align-items:center;gap:8px;pointer-events:none}
.toast.show{opacity:1;transform:translateY(0)}
.toast.err{border-color:var(--red)}

/* Mobile layout adjustments */
@media(max-width:768px){
  body{padding:8px}
  .top-bar{padding:10px 14px}
  .brand{font-size:14px}
  .tabs{flex-wrap:nowrap;overflow-x:auto;padding:0 8px;scrollbar-width:none}
  .tabs::-webkit-scrollbar{display:none}
  .tab{padding:10px 12px;font-size:11px}
  .controls{padding:10px 14px;gap:8px}
  #scanBtn,#tickerSearch{width:100%;margin-top:4px;margin-left:0 !important}
  .mobile-scroll-hint{display:flex !important;}
  .action-bar{flex-direction:column;align-items:stretch}
  .action-bar button{width:100%;justify-content:center}
  .modal{width:95% !important}
}
</style>
</head>
<body>
<div class="wrap">

  <!-- Top Bar -->
  <div class="top-bar">
    <div class="brand">
      <i class="ti ti-chart-candlestick" style="color:var(--blue);font-size:18px"></i>
      <span class="nse">NSE</span>&nbsp;<span class="vol">Volume</span>&nbsp;Scanner
      <span class="ver">v3.0 [Phase 3 Polish]</span>
    </div>
    <div class="live-badge" id="liveBadge">
      <span style="animation:pulse 1.5s infinite">●</span>
      <span id="liveBadgeText">Idle — Ready to Scan</span>
      <span id="liveStatsSpan" style="display:none">
        Live — <span id="bull-count">0</span> bullish &nbsp;|&nbsp; <span id="bear-count">0</span> bearish
      </span>
    </div>
  </div>

  <!-- Cold-start banner -->
  <div class="cold-banner" id="coldBanner">
    <i class="ti ti-clock-pause"></i>
    <span>Server is waking up from sleep (Render free tier) — first scan may take 30-60 s longer than usual.</span>
    <button onclick="document.getElementById('coldBanner').classList.remove('show')"
      style="margin-left:auto;background:none;border:none;color:var(--yellow);cursor:pointer;font-size:16px">✕</button>
  </div>

  <!-- Market closed banner -->
  <div class="market-banner" id="marketBanner">
    <i class="ti ti-alert-triangle"></i>
    <span>NSE Market is currently Closed — showing data from the last active session. Pausing live timers.</span>
  </div>

  <!-- Mobile Scroll Hint -->
  <div class="mobile-scroll-hint">
    <i class="ti ti-arrow-autofit-width" style="color:var(--teal)"></i>
    <span>Swipe left/right to view all details (Score, Entry, Risk, R:R, Sparklines, Watch)</span>
  </div>

  <!-- Tabs -->
  <div class="tabs">
    <div class="tab active" id="tab-entry"     onclick="setTab('entry')">
      <span class="dot y"></span> Entry Ready <span class="cnt" id="cnt-entry">0</span>
    </div>
    <div class="tab" id="tab-bullish"   onclick="setTab('bullish')">
      <span class="dot g"></span> Bullish <span class="cnt" id="cnt-bullish">0</span>
    </div>
    <div class="tab" id="tab-bearish"   onclick="setTab('bearish')">
      <span class="dot r"></span> Bearish <span class="cnt" id="cnt-bearish">0</span>
    </div>
    <div class="tab" id="tab-mixed"     onclick="setTab('mixed')">
      <span class="dot o"></span> Mixed Setup <span class="cnt" id="cnt-mixed">0</span>
    </div>
    <div class="tab" id="tab-hv"        onclick="setTab('hv')">
      <span class="dot o"></span> New HV Today <span class="cnt" id="cnt-hv">0</span>
    </div>
    <div class="tab" id="tab-camarilla" onclick="setTab('camarilla')">
      <span class="dot p"></span> All Signals <span class="cnt" id="cnt-camarilla">0</span>
    </div>
    <div class="tab" id="tab-watchlist" onclick="setTab('watchlist')">
      <i class="ti ti-star" style="font-size:12px;color:var(--yellow)"></i>
      Watchlist <span class="cnt" id="cnt-watchlist" style="background:#374151;color:var(--muted)">0</span>
    </div>
  </div>

  <!-- Controls -->
  <div class="controls">
    <label>Vol ></label>
    <select id="volDays" onchange="savePrefs();render()">
      <option value="5">5 Day Avg</option>
      <option value="10" selected>10 Day Avg</option>
      <option value="20">20 Day Avg</option>
      <option value="30">30 Day Avg</option>
      <option value="50">50 Day Avg</option>
      <option value="100">100 Day Avg</option>
      <option value="200">200 Day Avg</option>
    </select>
    <label style="margin-left:8px">≥</label>
    <select id="volMult" onchange="savePrefs();render()">
      <option value="1.5">1.5x</option>
      <option value="2" selected>2x</option>
      <option value="2.5">2.5x</option>
      <option value="3">3x</option>
    </select>
    <label style="margin-left:8px">Liquidity:</label>
    <select id="turnoverLimit" onchange="savePrefs()">
      <option value="50000000">₹5 Cr Turnover</option>
      <option value="100000000" selected>₹10 Cr Turnover</option>
      <option value="200000000">₹20 Cr Turnover</option>
      <option value="500000000">₹50 Cr Turnover</option>
    </select>
    <label style="margin-left:8px">Scan:</label>
    <select id="scanMode" onchange="savePrefs()">
      <option value="bullish" selected>Bullish only</option>
      <option value="bearish">Bearish only</option>
      <option value="both">Both</option>
    </select>
    <label style="margin-left:8px">EMA:</label>
    <div class="ema-filters">
      <label class="ema-chip on" id="c10"><input type="checkbox" checked onchange="toggleEMA(this,'10')">10</label>
      <label class="ema-chip on" id="c20"><input type="checkbox" checked onchange="toggleEMA(this,'20')">20</label>
      <label class="ema-chip on" id="c50"><input type="checkbox" checked onchange="toggleEMA(this,'50')">50</label>
      <label class="ema-chip on" id="c200"><input type="checkbox" checked onchange="toggleEMA(this,'200')">200</label>
    </div>
    <button class="btn" id="scanBtn" onclick="startScan()">
      <i class="ti ti-scan-eye"></i> Run Live Scan
    </button>
    <button class="btn-stop" id="stopBtn" onclick="stopScan()">
      <i class="ti ti-player-stop"></i> Stop Scan
    </button>
    <label style="margin-left:8px"><i class="ti ti-refresh"></i> Auto:</label>
    <select id="autoScanSel" onchange="toggleAutoScan()">
      <option value="off" selected>Off</option>
      <option value="180000">3 min</option>
      <option value="300000">5 min</option>
      <option value="600000">10 min</option>
    </select>
    <label style="margin-left:8px"><i class="ti ti-database-off"></i> Mode:</label>
    <select id="scanDataSource" onchange="savePrefs()">
      <option value="live" selected>Live yfinance</option>
      <option value="offline">Cache (Offline)</option>
    </select>
    <span id="countdownSpan" style="margin-left:5px;color:var(--yellow);font-weight:600;font-size:11px"></span>
    <input type="text" id="tickerSearch" placeholder="🔍 Search/Scan ticker…"
      oninput="render()" onkeydown="if(event.key==='Enter') triggerSingleScan()" style="width:170px;margin-left:auto" title="Type ticker (e.g. RELIANCE) and press Enter to trigger a dedicated backend scan.">
  </div>

  <!-- Progress Bar -->
  <div class="prog-wrap" id="progWrap">
    <div class="prog-txt" id="progStatus">Scanning…</div>
    <div class="prog-bar-bg">
      <div class="prog-bar-fill" id="progFill"></div>
    </div>
    <div class="prog-txt" id="progPct">0%</div>
    <div class="prog-txt" id="progETA" style="color:var(--yellow)"></div>
  </div>

  <!-- Summary Bar -->
  <div class="sum-bar" id="sumBar"></div>

  <!-- Table -->
  <div class="tbl-wrap">
    <table>
      <thead><tr>
        <th onclick="sortBy('symbol')">SYMBOL &amp; 5d SPARKLINE <span class="sort-icon" id="si-symbol"><i class="ti ti-selector"></i></span></th>
        <th onclick="sortBy('confidence')">CONF GRADE <span class="sort-icon" id="si-confidence"><i class="ti ti-selector"></i></span></th>
        <th onclick="sortBy('score')">SCORE <span class="sort-icon" id="si-score"><i class="ti ti-selector"></i></span></th>
        <th onclick="sortBy('price')">PRICE <span class="sort-icon" id="si-price"><i class="ti ti-selector"></i></span></th>
        <th onclick="sortBy('entry')">ENTRY (PIVOT) <span class="sort-icon" id="si-entry"><i class="ti ti-selector"></i></span></th>
        <th onclick="sortBy('stop_loss')">STOP LOSS <span class="sort-icon" id="si-stop_loss"><i class="ti ti-selector"></i></span></th>
        <th>TARGETS (T1/T2)</th>
        <th onclick="sortBy('risk_percentage')">RISK % <span class="sort-icon" id="si-risk_percentage"><i class="ti ti-selector"></i></span></th>
        <th onclick="sortBy('rr')">R:R <span class="sort-icon" id="si-rr"><i class="ti ti-selector"></i></span></th>
        <th onclick="sortBy('rs_pct')">RS VS NIFTY <span class="sort-icon" id="si-rs_pct"><i class="ti ti-selector"></i></span></th>
        <th onclick="sortBy('days')">52W AGE <span class="sort-icon" id="si-days"><i class="ti ti-selector"></i></span></th>
        <th onclick="sortBy('turnover_score')">TURNOVER <span class="sort-icon" id="si-turnover_score"><i class="ti ti-selector"></i></span></th>
        <th>EMA ✓</th>
        <th onclick="sortBy('vol_ratio')">VOL <span class="sort-icon" id="si-vol_ratio"><i class="ti ti-selector"></i></span></th>
        <th onclick="sortBy('candle')">CANDLE <span class="sort-icon" id="si-candle"><i class="ti ti-selector"></i></span></th>
        <th>WATCH</th>
      </tr></thead>
      <tbody id="tblBody">
        <tr>
          <td colspan="16" class="empty-state">
            <i class="ti ti-chart-candlestick" style="font-size:32px;color:#374151;display:block;margin-bottom:8px"></i>
            No signals loaded. Click <b>Run Live Scan</b> or search tickers above.
          </td>
        </tr>
      </tbody>
    </table>
  </div>

  <!-- Action Bar -->
  <div class="action-bar">
    <button class="tg-btn" onclick="showModal('tg')"><i class="ti ti-brand-telegram"></i> Telegram</button>
    <button class="wa-btn" onclick="showModal('wa')"><i class="ti ti-brand-whatsapp"></i> WhatsApp</button>
    <button class="tg-btn" onclick="triggerAlerts()" style="background:#4b5563;color:var(--text)"><i class="ti ti-bell-ringing"></i> Trigger Alerts</button>
    <button class="csv-btn" onclick="exportCSV()"><i class="ti ti-download"></i> Export CSV</button>
    <span id="lastScan" style="font-size:11px;color:#4b5563;margin-left:auto"></span>
  </div>

  <!-- Modal -->
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
</div><!-- /.wrap -->

<!-- Toast -->
<div class="toast" id="toast">
  <i class="ti ti-circle-check" style="color:var(--teal);font-size:16px"></i>
  <span id="toastMsg"></span>
</div>

<script>
// ── State ──────────────────────────────────────────────────
let stocks     = [];
let watchlist  = [];
let activeTab  = 'entry';
let sortState  = {col:'score', desc:true};
const emaReq   = {10:true,20:true,50:true,200:true};
let pollTimer  = null;
let autoTimer  = null;
let remaining  = 300;
let coldShown  = false;
let marketClosed = false;

// ── Helpers ───────────────────────────────────────────────
const fmt = n => n >= 1000 ? '₹'+n.toLocaleString('en-IN',{maximumFractionDigits:2}) : '₹'+n.toFixed(2);
const pct = n => (n>0?'+':'')+Number(n).toFixed(2)+'%';

function showToast(msg, err=false){
  const t=document.getElementById('toast');
  t.classList.toggle('err',err);
  const icon = t.querySelector('i');
  if (err) {
    icon.className = 'ti ti-circle-x';
    icon.style.color = 'var(--red)';
  } else {
    icon.className = 'ti ti-circle-check';
    icon.style.color = 'var(--teal)';
  }
  document.getElementById('toastMsg').textContent=msg;
  t.classList.add('show');
  setTimeout(()=>t.classList.remove('show'), 3500);
}

// ── Price Action SVG Sparkline Generator ────────────────
function sparklineSVG(prices) {
  if (!prices || prices.length < 2) return '';
  const min = Math.min(...prices);
  const max = Math.max(...prices);
  const rng = max - min || 1;
  const width = 65;
  const height = 20;
  const pts = prices.map((p, i) =>
    `${(i/(prices.length-1))*width},${height - 2 - ((p-min)/rng)*16}`
  ).join(' ');
  const color = prices[prices.length - 1] >= prices[0] ? '#4ade80' : '#f87171';
  return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" style="vertical-align:middle;overflow:visible;">
    <polyline points="${pts}" fill="none" stroke="${color}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>`;
}

// ── Preferences (localStorage) ───────────────────────────
function savePrefs(){
  try{
    localStorage.setItem('nse_prefs_h',JSON.stringify({
      volDays: document.getElementById('volDays').value,
      volMult: document.getElementById('volMult').value,
      turnoverLimit: document.getElementById('turnoverLimit').value,
      scanMode:document.getElementById('scanMode').value,
      scanDataSource: document.getElementById('scanDataSource').value,
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

// ── Watchlist ─────────────────────────────────────────────
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
  if(idx===-1){watchlist.push(sym);el.style.color='var(--yellow)';showToast(sym+' added to watchlist');}
  else{watchlist.splice(idx,1);el.style.color='#4b5563';showToast(sym+' removed from watchlist');}
  saveWatchlistRemote();
  updateCounts();
  if(activeTab==='watchlist') render();
}

// ── Tabs ──────────────────────────────────────────────────
function setTab(t){
  activeTab=t;
  document.querySelectorAll('.tab').forEach(el=>el.classList.remove('active'));
  document.getElementById('tab-'+t).classList.add('active');
  render();
}

// ── EMA toggle ────────────────────────────────────────────
function toggleEMA(cb,k){
  emaReq[k]=cb.checked;
  document.getElementById('c'+k).classList.toggle('on',cb.checked);
  savePrefs(); render();
}

// ── Sort ──────────────────────────────────────────────────
function sortBy(col){
  sortState={col, desc: sortState.col===col ? !sortState.desc : true};
  document.querySelectorAll('.sort-icon').forEach(el=>el.innerHTML='<i class="ti ti-selector"></i>');
  const el=document.getElementById('si-'+col);
  if(el) el.innerHTML=sortState.desc?'<i class="ti ti-chevron-down"></i>':'<i class="ti ti-chevron-up"></i>';
  render();
}

// ── Pre-process signals ───────────────────────────────────
function preprocess(sigs){
  return (sigs||[]).map(s=>{
    return s;
  });
}

// ── Count tabs ────────────────────────────────────────────
function updateCounts(){
  const srch=document.getElementById('tickerSearch').value.toLowerCase();
  const vm=parseFloat(document.getElementById('volMult').value);
  const base=stocks.filter(s=>s.vol_ratio>=vm && (!srch||s.symbol.toLowerCase().includes(srch)));
  const counts={
    bullish:   base.filter(s=>s.signal_type==='Bull' && s.candle==='Bull').length,
    bearish:   base.filter(s=>s.signal_type==='Bear' && s.candle==='Bear').length,
    mixed:     base.filter(s=>(s.signal_type==='Bull'&&s.candle==='Bear')||(s.signal_type==='Bear'&&s.candle==='Bull')).length,
    entry:     base.filter(s=>Math.abs(s.price-s.entry)/s.entry<=0.005 && s.vol_ratio>=vm && s.ema10_pass && s.ema20_pass && s.ema50_pass && s.ema200_pass).length,
    hv:        base.filter(s=>s.days<=10).length,
    camarilla: base.length,
    watchlist: base.filter(s=>watchlist.includes(s.symbol)).length,
  };
  for(const[k,v] of Object.entries(counts)){
    const el=document.getElementById('cnt-'+k);
    if(el) el.textContent=v;
  }
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

// ── Main render ───────────────────────────────────────────
function render(){
  const tbody=document.getElementById('tblBody');
  const srch=document.getElementById('tickerSearch').value.toLowerCase();
  const vm=parseFloat(document.getElementById('volMult').value);

  let f=stocks.filter(s=>s.vol_ratio>=vm && (!srch||s.symbol.toLowerCase().includes(srch)));

  if(activeTab==='bullish')        f=f.filter(s=>s.signal_type==='Bull' && s.candle==='Bull');
  else if(activeTab==='bearish')   f=f.filter(s=>s.signal_type==='Bear' && s.candle==='Bear');
  else if(activeTab==='mixed')     f=f.filter(s=>(s.signal_type==='Bull'&&s.candle==='Bear')||(s.signal_type==='Bear'&&s.candle==='Bull'));
  else if(activeTab==='entry')     f=f.filter(s=>Math.abs(s.price-s.entry)/s.entry<=0.005 && s.vol_ratio>=vm && s.ema10_pass && s.ema20_pass && s.ema50_pass && s.ema200_pass);
  else if(activeTab==='hv')        f=f.filter(s=>s.days<=10);
  else if(activeTab==='watchlist') f=f.filter(s=>watchlist.includes(s.symbol));

  // Sort
  f.sort((a,b)=>{
    let va=a[sortState.col]??0, vb=b[sortState.col]??0;
    if(typeof va==='string') va=va.toLowerCase(), vb=vb.toLowerCase();
    if(va<vb) return sortState.desc?1:-1;
    if(va>vb) return sortState.desc?-1:1;
    return 0;
  });

  // Summary
  let bulls=0,bears=0,hi3=0,scoreSum=0;
  f.forEach(s=>{if(s.signal_type==='Bear')bears++;else bulls++;if(s.vol_ratio>=3)hi3++;scoreSum+=s.score;});
  const avg=f.length?Math.round(scoreSum/f.length):0;
  document.getElementById('sumBar').innerHTML=`
    <div class="si"><span class="lbl">Total:</span><span class="val">${f.length}</span></div>
    <div class="si"><span class="lbl">Bullish:</span><span class="val g">${bulls}</span></div>
    <div class="si"><span class="lbl">Bearish:</span><span class="val r">${bears}</span></div>
    <div class="si"><span class="lbl">Vol ≥3x:</span><span class="val y">${hi3}</span></div>
    <div class="si"><span class="lbl">Avg score:</span><span class="val">${avg}/100</span></div>`;

  // CSV Button State
  const csvBtn = document.querySelector('.csv-btn');
  if (csvBtn) {
    if (stocks.length === 0) {
      csvBtn.style.opacity = '0.5';
      csvBtn.style.cursor = 'not-allowed';
      csvBtn.setAttribute('title', 'No signals to export');
    } else {
      csvBtn.style.opacity = '1';
      csvBtn.style.cursor = 'pointer';
      csvBtn.removeAttribute('title');
    }
  }

  if(!f.length){
    tbody.innerHTML=`<tr><td colspan="16" class="empty-state">
      <i class="ti ti-chart-candlestick" style="font-size:32px;color:#374151;display:block;margin-bottom:8px"></i>
      No signals. Click <b>Run Live Scan</b> or search tickers above.</td></tr>`;
    return;
  }

  tbody.innerHTML=f.map(s=>{
    const isBull=s.signal_type==='Bull';
    const volPct=Math.min(100,(s.vol_ratio/4)*100);
    const vClass=s.vol_ratio>=3?'spike':s.vol_ratio>=2?'high':'';
    const eDots=[10,20,50,200].map(n=>{
      const pass=s['ema'+n+'_pass'] && emaReq[n];
      return `<div class="edot ${pass?'pass':''}" title="EMA${n} ${pass?'✓':'✗'}"></div>`;
    }).join('');
    const rPct=s.hv_high!==s.hv_low?Math.min(100,Math.max(0,((s.price-s.hv_low)/(s.hv_high-s.hv_low))*100)):50;
    const isFav=watchlist.includes(s.symbol);
    const sigBadge=isBull?`<span class="sig-buy">BUY</span>`:`<span class="sig-sell">SELL</span>`;
    
    // Score breakdown tooltip calculation
    const isBearish = s.signal_type === 'Bear';
    let emaBonus = 0;
    [10, 20, 50, 200].forEach(n => {
      if (s['ema' + n + '_pass']) emaBonus += 5;
    });
    
    let slopeBonus = 0;
    const slope = s.ema20_slope || 0.0;
    if (!isBearish && slope > 0.3) slopeBonus = 5;
    else if (isBearish && slope < -0.3) slopeBonus = 5;
    
    let volBonus = 0;
    if (s.vol_ratio >= 3.0) volBonus = 15;
    else if (s.vol_ratio >= 2.0) volBonus = 10;
    else if (s.vol_ratio >= 1.5) volBonus = 5;
    
    let volPctBonus = (s.vol_percentile || 0) >= 90 ? 5 : 0;
    let rangeBonus = (s.range_expansion || 0) >= 1.5 ? 5 : 0;
    
    let candleBonus = 0;
    if (!isBearish && s.candle === 'Bull') candleBonus = 5;
    else if (isBearish && s.candle === 'Bear') candleBonus = 5;
    
    let entryBonus = 0;
    const atrDist = s.atr_dist || 0.0;
    if (!isBearish && atrDist <= 1.2) entryBonus = 5;
    else if (isBearish && atrDist >= -1.2) entryBonus = 5;

    let rsBonus = s.rs_score || 0;
    
    let extendPenalty = 0;
    if (!isBearish) {
      if (atrDist > 3.0) extendPenalty = -30;
      else if (atrDist > 2.0) extendPenalty = -15;
    } else {
      if (atrDist < -3.0) extendPenalty = -30;
      else if (atrDist < -2.0) extendPenalty = -15;
    }
    
    let chasePenalty = 0;
    const pctVal = !isBearish ? s.pct_above : s.pct_below;
    if (pctVal > 40) chasePenalty = -20;
    else if (pctVal > 25) chasePenalty = -10;
    
    let tooltip = `QUANT MULTI-FACTOR MOMENTUM SCORE&#10;`;
    tooltip += `• Base Score: 50&#10;`;
    if (emaBonus > 0) tooltip += `• EMA Trend Alignment: +${emaBonus}&#10;`;
    if (slopeBonus > 0) tooltip += `• Trend Slope Acceleration: +${slopeBonus}&#10;`;
    if (volBonus > 0) tooltip += `• Relative Volume Spike: +${volBonus} (${s.vol_ratio.toFixed(1)}x)&#10;`;
    if (volPctBonus > 0) tooltip += `• Volume Percentile Peak: +${volPctBonus} (${(s.vol_percentile || 0).toFixed(0)}%)&#10;`;
    if (rangeBonus > 0) tooltip += `• Volatility Range Expansion: +${rangeBonus} (${(s.range_expansion || 0).toFixed(1)}x ATR)&#10;`;
    if (candleBonus > 0) tooltip += `• Candle Pattern Direction: +${candleBonus}&#10;`;
    if (entryBonus > 0) tooltip += `• Low-Risk Support Entry: +${entryBonus}&#10;`;
    if (rsBonus !== 0) tooltip += `• Relative Strength vs NIFTY: ${rsBonus > 0 ? '+' : ''}${rsBonus} (${(s.rs_pct || 0).toFixed(1)}%)&#10;`;
    if (extendPenalty < 0) tooltip += `• Parabolic Overextension Risk: ${extendPenalty} (${atrDist.toFixed(1)} ATRs from EMA)&#10;`;
    if (chasePenalty < 0) tooltip += `• High-Low Chasing Penalty: ${chasePenalty} (${pctVal.toFixed(1)}% above Low)&#10;`;
    tooltip = tooltip.trim();

    // Confidence badge styling with custom colors per grade
    let confColor = '#6b7280';
    let confBG = '#1f2937';
    let confBorder = '1px solid #374151';
    
    if (s.confidence === 'A+') {
      confColor = '#fbbf24'; confBG = '#1e1b4b'; confBorder = '1px solid #eab308';
    } else if (s.confidence === 'A') {
      confColor = '#4ade80'; confBG = '#064e3b'; confBorder = '1px solid #047857';
    } else if (s.confidence === 'B') {
      confColor = '#60a5fa'; confBG = '#172554'; confBorder = '1px solid #1d4ed8';
    } else if (s.confidence === 'C') {
      confColor = '#fb923c'; confBG = '#2c1505'; confBorder = '1px solid #ea580c';
    } else if (s.confidence === 'D') {
      confColor = '#f87171'; confBG = '#450a0a'; confBorder = '1px solid #dc2626';
    }
    
    const confBadge = `<span class="c-bull" style="background:${confBG};color:${confColor};border:${confBorder};display:inline-block;padding:3px 10px">${s.confidence}</span>`;

    // 52W age (DAYS) column styling
    let ageColor = '#f87171'; // stale (>90d)
    if (s.days <= 7) ageColor = '#4ade80';       // fresh
    else if (s.days <= 30) ageColor = '#fbbf24';  // yellow
    else if (s.days <= 90) ageColor = '#fb923c';  // orange
    
    const ageLabel = s.days === 0 ? 'Today' : s.days === 1 ? '1d' : s.days + 'd';

    // Risk and reward styling
    const riskColor = s.risk_percentage > 4.0 ? 'dn' : s.risk_percentage > 3.0 ? 'd-warn' : 'up';
    const rrColor = s.rr >= 3.0 ? 'up' : s.rr >= 1.5 ? 'd-warn' : 'dn';
    const rsColor = s.rs_pct > 0 ? 'up' : 'dn';
    const volColor = s.vol_ratio >= 3.0 ? '#f87171' : s.vol_ratio >= 2.0 ? '#fbbf24' : '#4ade80';

    return `<tr>
      <td>
        <div style="display:flex;justify-content:space-between;align-items:center;gap:12px">
          <div>
            <span class="sym">${s.symbol}</span><br>${sigBadge}
          </div>
          <div style="padding-top:4px" title="5-day Price Action Sparkline">
            ${sparklineSVG(s.sparkline)}
          </div>
        </div>
        <div style="font-size:9px;color:#6b7280;margin-top:5px;display:flex;align-items:center;gap:4px"
          title="Regime: ${s.regime || 'Consolidation'} | 52W Low ${fmt(s.hv_low)} → High ${fmt(s.hv_high)} — at ${rPct.toFixed(1)}%">
          <span>L</span>
          <div style="width:105px;height:3px;background:#374151;border-radius:2px;position:relative">
            <div style="position:absolute;left:${rPct}%;top:-2px;width:6px;height:6px;border-radius:50%;background:var(--blue);box-shadow:0 0 3px var(--blue)"></div>
          </div>
          <span>H</span>
        </div>
      </td>
      <td style="cursor:help" title="${tooltip}"><div style="text-align:center">${confBadge}</div><div style="font-size:9px;color:var(--muted);margin-top:4px;text-align:center">${s.signal_strength || ''}</div></td>
      <td><span class="score" style="cursor:help;" title="${tooltip}">${s.score}<span class="den">/100</span></span></td>
      <td><div class="price-main">${fmt(s.price)}</div><div class="price-date">● ${s.scanned_date||'Today'}</div></td>
      <td>${fmt(s.entry)}</td>
      <td class="sl">${fmt(s.stop_loss)}</td>
      <td class="cam-levels">
        <div class="cam-h3" style="color:var(--teal)">T1 ${fmt(s.target)}</div>
        <div class="cam-h3" style="color:var(--blue)">T2 ${fmt(s.target2 || 0)}</div>
      </td>
      <td class="${riskColor}">${s.risk_percentage ? s.risk_percentage.toFixed(1) + '%' : '—'}</td>
      <td class="${rrColor}" style="font-weight:600">${s.rr ? s.rr.toFixed(1) + 'x' : '—'}</td>
      <td class="${rsColor}">${s.rs_pct ? (s.rs_pct > 0 ? '+' : '') + s.rs_pct.toFixed(1) + '%' : '—'}</td>
      <td style="color:${ageColor};font-weight:600">${ageLabel}</td>
      <td class="neutral" style="font-weight:600">${s.turnover_score ? '₹' + s.turnover_score.toFixed(2) + ' Cr' : '—'}</td>
      <td><div class="ema-dots">${eDots}</div></td>
      <td>
        <div class="vol-bar"><div class="vol-fill" style="width:${volPct}%;background:${volColor}"></div></div>
        <br><span style="font-size:10px;color:var(--muted)">${s.vol_ratio.toFixed(2)}x</span>
      </td>
      <td><span class="${s.candle==='Bull'?'c-bull':'c-bear'}">${s.candle}</span></td>
      <td><i class="ti ti-star" style="font-size:16px;color:${isFav?'var(--yellow)':'#4b5563'};cursor:pointer;transition:color .2s"
        onclick="toggleWatch('${s.symbol}',this)"></i></td>
    </tr>`;
  }).join('');
  updateCounts();
}

// ── Scan flow ─────────────────────────────────────────────
function setScanningUI(on){
  const btn=document.getElementById('scanBtn');
  const stop=document.getElementById('stopBtn');
  const prog=document.getElementById('progWrap');
  if(on){
    btn.classList.add('scanning'); btn.disabled=true;
    btn.innerHTML='<span class="ti ti-loader-quarter" style="animation:spin 1s linear infinite;display:inline-block"></span> Scanning…';
    stop.classList.add('show'); prog.classList.add('show');
    document.getElementById('pageTitle').textContent='Scanning… | NSE Scanner';
  } else {
    btn.classList.remove('scanning'); btn.disabled=false;
    btn.innerHTML='<i class="ti ti-scan-eye"></i> Run Live Scan';
    stop.classList.remove('show'); prog.classList.remove('show');
    document.getElementById('pageTitle').textContent='NSE Camarilla Volume Scanner';
  }
}

function updateProgress(p){
  const tot=p.total||1, cur=p.current||0;
  const pctV=Math.round((cur/tot)*100);
  document.getElementById('progFill').style.width=pctV+'%';
  document.getElementById('progPct').textContent=pctV+'%';
  document.getElementById('progStatus').textContent=`[${cur}/${tot}] ${p.ticker||''}`;
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
      // Auto-dismiss cold-start banners once scanning finishes
      document.getElementById('coldBanner').classList.remove('show');
      
      if(pollTimer){
        clearInterval(pollTimer);
        pollTimer=null;
        showToast('Scan completed! '+(d.signals?d.signals.length:0)+' signals found.');
      }
      if(d.signals){
        stocks=preprocess(d.signals);
        document.getElementById('lastScan').textContent=d.last_scan?'Last scan: '+d.last_scan:'';
        updateCounts(); 
        
        // Auto-switch from 'entry' if empty to prevent a confusing blank list
        if (activeTab === 'entry' && parseInt(document.getElementById('cnt-entry').textContent) === 0) {
          const tabPriority = ['bullish', 'mixed', 'bearish', 'hv', 'camarilla'];
          for (const tab of tabPriority) {
            const cnt = parseInt(document.getElementById('cnt-' + tab).textContent || '0');
            if (cnt > 0) {
              setTab(tab);
              break;
            }
          }
        } else {
          render();
        }
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

// ── Single stock search scan ──────────────────────────────
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

// ── Auto-scan Countdown Timer ─────────────────────────────
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
    // Pause timer client-side if market hours are closed
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

// ── Market status checks ──────────────────────────────
let marketTimer = null;
function checkMarketStatus(){
  fetch('/market_status')
    .then(r=>r.json())
    .then(d=>{
      marketClosed = !d.is_open;
      const mb = document.getElementById('marketBanner');
      if (marketClosed) {
        mb.style.display = 'flex';
        mb.querySelector('span').textContent = `NSE Market is Closed (${d.time}) — showing last session data. Live scanners paused.`;
        if (autoTimer) {
          document.getElementById('countdownSpan').textContent = '(Market Closed)';
        }
      } else {
        mb.style.display = 'none';
      }
    }).catch(()=>{});
}

// ── Alerts Modal ──────────────────────────────────────────
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
    document.getElementById('modalTitle').innerHTML='<i class="ti ti-brand-whatsapp" style="color:#14532d"></i> WhatsApp (Twilio) Alerts';
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

// ── Cold-start detection ──────────────────────────────────
function detectColdStart(){
  const key='nse_last_visit';
  const now=Date.now();
  const last=parseInt(localStorage.getItem(key)||'0');
  if(!last || (now-last)>30*60*1000){
    document.getElementById('coldBanner').classList.add('show');
  }
  localStorage.setItem(key,now);
}

// ── Initialise ────────────────────────────────────────────
loadPrefs();
loadWatchlist();
detectColdStart();
checkMarketStatus();
marketTimer = setInterval(checkMarketStatus, 30000); // Check market operational state every 30s

fetch('/results').then(r=>r.json()).then(d=>{
  if(d.scanning){
    setScanningUI(true);
    pollTimer=setInterval(checkStatus,1500);
  } else if(d.signals&&d.signals.length>0){
    stocks=preprocess(d.signals);
    document.getElementById('lastScan').textContent=d.last_scan?'Last scan: '+d.last_scan:'';
    updateCounts();
    
    // Auto-switch from 'entry' if empty to prevent a confusing blank list on page load
    if (activeTab === 'entry' && parseInt(document.getElementById('cnt-entry').textContent) === 0) {
      const tabPriority = ['bullish', 'mixed', 'bearish', 'hv', 'camarilla'];
      for (const tab of tabPriority) {
        const cnt = parseInt(document.getElementById('cnt-' + tab).textContent || '0');
        if (cnt > 0) {
          setTab(tab);
          break;
        }
      }
    } else {
      render();
    }
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
