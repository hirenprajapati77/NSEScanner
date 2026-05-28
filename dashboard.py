"""
NSE Scanner Web Dashboard  (Flask)
Run:  python dashboard.py
Open: http://localhost:5000
"""

import os
import json
import threading
from datetime import datetime
from flask import Flask, render_template_string, jsonify, request
from scanner import run_scan, CFG, NIFTY500_SAMPLE

app = Flask(__name__)

STATUS_FILE = "scan_status.json"

# ── DOCKER/GUNICORN MULTI-PROCESS PERSISTENT STATE ────────────
# Reads the state from a local JSON file to synchronize progress 
# between multiple Gunicorn worker processes.
def read_status():
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "signals": [],
        "last_scan": "Never",
        "scanning": False,
        "progress": {"current": 0, "total": 0, "ticker": "Idle"}
    }

def write_status(status_data):
    try:
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(status_data, f, indent=2)
    except Exception as e:
        print(f"Error writing status file: {e}")

# Attempt to load the latest scan CSV and import it into state if empty
def load_latest_cache_if_empty():
    status_data = read_status()
    if status_data.get("signals"):
        return
        
    try:
        csv_files = [f for f in os.listdir(".") if f.startswith("scan_") and f.endswith(".csv")]
        if csv_files:
            latest_csv = sorted(csv_files)[-1]
            import pandas as pd
            df = pd.read_csv(latest_csv)
            signals = []
            for _, r in df.iterrows():
                signals.append({
                    "symbol":      str(r["Symbol"]),
                    "score":       int(r["Score"]),
                    "price":       float(r["Price"]),
                    "entry":       float(r["Entry"]),
                    "target":      float(r["Target_H3"]),
                    "stop_loss":   float(r["StopLoss_L3"]),
                    "upside":      float(r["Upside_%"]),
                    "vol_ratio":   float(r["Vol_Ratio"]),
                    "candle":      str(r["Candle"]),
                    "ema10":       float(r["EMA10"]),
                    "ema20":       float(r["EMA20"]),
                    "ema50":       float(r["EMA50"]),
                    "ema200":      float(r["EMA200"]),
                    "hv_high":     float(r["HV_High"]),
                    "hv_low":      float(r["HV_Low"]),
                    "hv_date":     str(r.get("HV_Date", "Unknown")),
                    "days":        int(r.get("Days", 30)),
                    "ema10_pass":  bool(float(r["Price"]) > float(r["EMA10"])),
                    "ema20_pass":  bool(float(r["Price"]) > float(r["EMA20"])),
                    "ema50_pass":  bool(float(r["Price"]) > float(r["EMA50"])),
                    "ema200_pass": bool(float(r["Price"]) > float(r["EMA200"])),
                    "scanned_date": str(r.get("Scanned_At", datetime.now().strftime("%d-%b-%Y")))
                })
            status_data["signals"] = signals
            t_str = latest_csv.replace("scan_", "").replace(".csv", "")
            dt = datetime.strptime(t_str, "%Y%m%d_%H%M")
            status_data["last_scan"] = dt.strftime("%d %b %Y %H:%M")
            write_status(status_data)
    except Exception as e:
        print(f"No existing CSV cache loaded: {e}")

load_latest_cache_if_empty()

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>NSE Camarilla Volume Scanner</title>
  <!-- Outfit Google Font -->
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
  <!-- Tabler Icons CSS -->
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@latest/dist/tabler-icons.min.css">
  <style>
    :root {
      --font-sans: 'Outfit', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      --border-radius-lg: 12px;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: var(--font-sans); background: #080b11; color: #e5e7eb; padding: 20px; font-size: 13px; min-height: 100vh; }
    
    .scanner-wrap { background: #0d1117; border-radius: var(--border-radius-lg); padding: 0; overflow: hidden; border: 1px solid #1f2937; box-shadow: 0 20px 25px -5px rgba(0,0,0,0.5); }
    .top-bar { background: #111827; padding: 12px 20px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid #1f2937; flex-wrap: wrap; gap: 8px; }
    .brand { font-size: 16px; font-weight: 600; color: #f9fafb; display: flex; align-items: center; gap: 6px; }
    .brand span.nse { color: #60a5fa; }
    .brand span.vol { color: #fbbf24; }
    .brand span.scan { color: #f9fafb; }
    .live-badge { font-size: 11px; color: #34d399; background: #064e3b; padding: 4px 12px; border-radius: 99px; font-weight: 500; display: flex; align-items: center; gap: 5px; }
    
    .tabs { background: #111827; padding: 0 16px; display: flex; gap: 4px; border-bottom: 1px solid #1f2937; flex-wrap: wrap; }
    .tab { padding: 12px 16px; font-size: 12px; cursor: pointer; color: #9ca3af; border-bottom: 2px solid transparent; display: flex; align-items: center; gap: 6px; white-space: nowrap; transition: all 0.2s ease; user-select: none; }
    .tab:hover { color: #e5e7eb; background: #161d2a; }
    .tab.active { color: #34d399; border-color: #34d399; font-weight: 500; }
    .tab .count { background: #1d4ed8; color: #bfdbfe; border-radius: 99px; padding: 1px 7px; font-size: 10px; font-weight: 600; }
    .tab .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
    .dot.green { background: #34d399; }
    .dot.red { background: #f87171; }
    .dot.orange { background: #fb923c; }
    .dot.purple { background: #c084fc; }
    
    .controls { background: #111827; padding: 12px 20px; display: flex; gap: 12px; align-items: center; flex-wrap: wrap; border-bottom: 1px solid #1f2937; }
    .controls label { font-size: 12px; color: #9ca3af; font-weight: 500; }
    .controls select { background: #1f2937; border: 0.5px solid #374151; color: #e5e7eb; padding: 5px 10px; border-radius: 6px; font-size: 12px; cursor: pointer; transition: border-color 0.2s; outline: none; }
    .controls select:focus { border-color: #3b82f6; }
    
    .ema-filters { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .ema-chip { display: flex; align-items: center; gap: 4px; font-size: 11px; background: #1f2937; border: 0.5px solid #374151; border-radius: 99px; padding: 4px 12px; color: #9ca3af; cursor: pointer; user-select: none; transition: all 0.2s ease; }
    .ema-chip.on { background: #1e3a5f; border-color: #3b82f6; color: #60a5fa; font-weight: 500; }
    .ema-chip input { display: none; }
    
    .btn { background: #1d4ed8; color: #fff; border: none; padding: 6px 16px; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 500; display: flex; align-items: center; gap: 6px; transition: background 0.2s; }
    .btn:hover { background: #2563eb; }
    .btn.scanning { background: #374151; cursor: not-allowed; opacity: 0.8; }
    
    .tbl-wrap { overflow-x: auto; width: 100%; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; text-align: left; }
    thead tr { background: #161d2a; }
    th { padding: 10px 14px; text-align: left; color: #6b7280; font-weight: 500; font-size: 11px; letter-spacing: 0.04em; white-space: nowrap; border-bottom: 1px solid #1f2937; cursor: pointer; user-select: none; }
    th:hover { color: #f9fafb; background: #1c2637; }
    th .sort-icon { margin-left: 4px; font-size: 10px; color: #4b5563; }
    tbody tr { border-bottom: 0.5px solid #1f2937; transition: background 0.15s; }
    tbody tr:hover { background: #121824; }
    td { padding: 10px 14px; color: #e5e7eb; white-space: nowrap; vertical-align: middle; }
    
    .sym { font-weight: 700; font-size: 13px; color: #f9fafb; }
    .buy-badge { background: #14532d; color: #4ade80; border-radius: 4px; padding: 2px 7px; font-size: 10px; font-weight: 600; display: inline-block; margin-top: 2px; }
    .score { color: #60a5fa; font-weight: 600; font-size: 13px; }
    .score .denom { color: #4b5563; font-weight: 400; font-size: 11px; }
    .price-main { color: #60a5fa; font-weight: 600; font-size: 13px; }
    .price-date { color: #4b5563; font-size: 10px; margin-top: 2px; }
    
    .up { color: #4ade80; font-weight: 500; }
    .dn { color: #f87171; font-weight: 500; }
    .neutral { color: #9ca3af; }
    .sl { color: #f87171; font-weight: 500; }
    
    .candle-bull { background: #14532d; color: #4ade80; border-radius: 4px; padding: 2px 8px; font-size: 10px; font-weight: 600; }
    .candle-bear { background: #450a0a; color: #f87171; border-radius: 4px; padding: 2px 8px; font-size: 10px; border: 1px solid #7f1d1d; font-weight: 600; }
    
    .days-warn { color: #fbbf24; font-weight: 500; }
    
    .tg-btn { background: #0369a1; color: #e0f2fe; border: none; border-radius: 6px; padding: 7px 14px; font-size: 11px; font-weight: 500; cursor: pointer; display: flex; align-items: center; gap: 6px; white-space: nowrap; transition: background 0.2s; }
    .tg-btn:hover { background: #0284c7; }
    .wa-btn { background: #14532d; color: #86efac; border: none; border-radius: 6px; padding: 7px 14px; font-size: 11px; font-weight: 500; cursor: pointer; display: flex; align-items: center; gap: 6px; white-space: nowrap; transition: background 0.2s; }
    .wa-btn:hover { background: #166534; }
    
    .action-bar { background: #111827; padding: 12px 20px; display: flex; gap: 12px; align-items: center; flex-wrap: wrap; border-top: 1px solid #1f2937; }
    
    .modal-bg { display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.85); z-index: 100; align-items: center; justify-content: center; padding: 20px; backdrop-filter: blur(4px); }
    .modal-bg.show { display: flex; }
    .modal { background: #1f2937; border-radius: 10px; padding: 24px; width: 420px; max-width: 100%; border: 1px solid #374151; box-shadow: 0 25px 50px -12px rgba(0,0,0,0.5); position: relative; animation: modalEnter 0.2s ease-out; }
    @keyframes modalEnter { from { transform: scale(0.95); opacity: 0; } to { transform: scale(1); opacity: 1; } }
    .modal h3 { color: #f9fafb; font-size: 16px; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
    .modal label { color: #9ca3af; font-size: 12px; display: block; margin-bottom: 4px; margin-top: 12px; font-weight: 500; }
    .modal input[type=text], .modal input[type=password] { width: 100%; background: #111827; border: 0.5px solid #374151; color: #e5e7eb; padding: 8px 12px; border-radius: 6px; font-size: 13px; outline: none; transition: border-color 0.2s; }
    .modal input[type=text]:focus, .modal input[type=password]:focus { border-color: #3b82f6; }
    .modal .row { display: flex; gap: 10px; margin-top: 20px; }
    .modal .btn-save { flex: 1; background: #1d4ed8; color: #fff; border: none; border-radius: 6px; padding: 10px; font-size: 13px; font-weight: 500; cursor: pointer; transition: background 0.2s; }
    .modal .btn-save:hover { background: #2563eb; }
    .modal .btn-cancel { background: #374151; color: #9ca3af; border: none; border-radius: 6px; padding: 10px 16px; font-size: 13px; cursor: pointer; transition: background 0.2s; }
    .modal .btn-cancel:hover { color: #e5e7eb; background: #4b5563; }
    
    .alert-row { display: flex; align-items: center; gap: 8px; padding: 8px 12px; background: #111827; border-radius: 6px; margin-top: 10px; border-left: 3px solid #34d399; }
    .alert-row span { font-size: 11px; color: #34d399; font-weight: 500; }
    
    .vol-bar { height: 5px; background: #1f2937; border-radius: 2px; width: 60px; overflow: hidden; display: inline-block; vertical-align: middle; }
    .vol-fill { height: 100%; background: #4ade80; border-radius: 2px; }
    .vol-fill.high { background: #fbbf24; }
    .vol-fill.spike { background: #f87171; }
    
    .ema-dots { display: flex; gap: 4px; }
    .edot { width: 10px; height: 10px; border-radius: 50%; background: #374151; border: 1px solid #1f2937; position: relative; }
    .edot.pass { background: #4ade80; box-shadow: 0 0 4px #4ade80; }
    
    .cam-levels { font-size: 11px; line-height: 1.4; }
    .cam-h3 { color: #f87171; font-weight: 500; }
    .cam-l3 { color: #34d399; font-weight: 500; }
    .cam-pivot { color: #fbbf24; font-weight: 500; }
    
    .summary-bar { display: flex; gap: 20px; padding: 12px 20px; background: #080c14; border-bottom: 1px solid #1f2937; flex-wrap: wrap; }
    .sum-item { font-size: 12px; display: flex; align-items: center; gap: 5px; }
    .sum-item .lbl { color: #6b7280; }
    .sum-item .val { color: #e5e7eb; font-weight: 600; }
    .sum-item .val.g { color: #4ade80; }
    .sum-item .val.r { color: #f87171; }
    .sum-item .val.y { color: #fbbf24; }
    
    .progress-wrap { display: none; padding: 16px 20px; background: #111827; border-bottom: 1px solid #1f2937; align-items: center; gap: 16px; flex-wrap: wrap; }
    .progress-wrap.show { display: flex; }
    .progress-bar-container { flex: 1; min-width: 200px; height: 8px; background: #1f2937; border-radius: 99px; overflow: hidden; position: relative; }
    .progress-bar-fill { width: 0%; height: 100%; background: linear-gradient(90deg, #3b82f6, #60a5fa); border-radius: 99px; transition: width 0.2s ease; }
    .progress-text { font-size: 12px; color: #9ca3af; font-weight: 500; }
    
    .empty-state { text-align: center; padding: 60px 20px; color: #4b5563; }
    .empty-state i { font-size: 32px; margin-bottom: 8px; display: block; color: #374151; }
    
    .toast { position: fixed; bottom: 24px; right: 24px; background: #1e293b; border-left: 4px solid #34d399; color: #f1f5f9; padding: 14px 24px; border-radius: 6px; box-shadow: 0 20px 25px -5px rgba(0,0,0,0.5); z-index: 1000; opacity: 0; transform: translateY(20px); transition: all 0.3s ease; display: flex; align-items: center; gap: 8px; pointer-events: none; }
    .toast.show { opacity: 1; transform: translateY(0); }
    
    .outer { position: relative; min-height: 400px; }
  </style>
</head>
<body>

<div class="scanner-wrap outer" id="scanWrap">
  <!-- Top Bar -->
  <div class="top-bar">
    <div class="brand">
      <i class="ti ti-chart-candlestick" style="color:#60a5fa;font-size:18px;"></i>
      <span class="nse">NSE</span> <span class="vol">Volume</span> <span class="scan">Scanner</span>
      <span style="color:#4b5563;font-size:11px;font-weight:400;margin-left:4px;">Ver 2.0</span>
    </div>
    <div class="live-badge">
      <span style="animation: pulse 1.5s infinite; color:#34d399;">●</span>
      Live — <span id="bull-count">0</span> bullish stocks
    </div>
  </div>

  <!-- Tabs Navigation -->
  <div class="tabs">
    <div class="tab active" id="tab-bullish" onclick="setTab('bullish')">
      <i class="ti ti-trending-up" aria-hidden="true" style="font-size:13px;color:#4ade80;"></i>
      Bullish Scanner <span class="count" id="tab-count-bullish">0</span>
    </div>
    <div class="tab" id="tab-bearish" onclick="setTab('bearish')">
      <span class="dot red"></span> Bearish Scanner <span class="count" id="tab-count-bearish">0</span>
    </div>
    <div class="tab" id="tab-hv" onclick="setTab('hv')">
      <span class="dot orange"></span> New HV Today <span class="count" id="tab-count-hv">0</span>
    </div>
    <div class="tab" id="tab-entry" onclick="setTab('entry')">
      <span class="dot green"></span> Entry Ready <span class="count" id="tab-count-entry">0</span>
    </div>
    <div class="tab" id="tab-camarilla" onclick="setTab('camarilla')">
      <span class="dot purple"></span> Camarilla <span class="count" id="tab-count-camarilla">0</span>
    </div>
    <div class="tab" id="tab-watchlist" onclick="setTab('watchlist')">
      <i class="ti ti-star" aria-hidden="true" style="font-size:12px;color:#fbbf24;"></i>
      My Watchlist <span class="count" id="tab-count-watchlist" style="background:#374151;color:#9ca3af;">0</span>
    </div>
  </div>

  <!-- Filters & Controls Bar -->
  <div class="controls">
    <label>Vol ></label>
    <select id="volDays" onchange="triggerFilterChange()">
      <option value="5">5 Day Avg</option>
      <option value="10" selected>10 Day Avg</option>
      <option value="20">20 Day Avg</option>
      <option value="30">30 Day Avg</option>
      <option value="50">50 Day Avg</option>
      <option value="100">100 Day Avg</option>
      <option value="200">200 Day Avg</option>
      <option value="250">1 Year Avg (250d)</option>
      <option value="500">2 Year Avg (500d)</option>
      <option value="750">3 Year Avg (750d)</option>
      <option value="1250">5 Year Avg (1250d)</option>
    </select>

    <label style="margin-left:8px;">Vol multiplier ≥</label>
    <select id="volMult" onchange="triggerFilterChange()">
      <option value="1.5">1.5x</option>
      <option value="2" selected>2x</option>
      <option value="2.5">2.5x</option>
      <option value="3">3x</option>
    </select>

    <label style="margin-left:8px;">EMA filters:</label>
    <div class="ema-filters">
      <label class="ema-chip on" id="c10">
        <input type="checkbox" checked onchange="toggleEMA(this,'10')">10
      </label>
      <label class="ema-chip on" id="c20">
        <input type="checkbox" checked onchange="toggleEMA(this,'20')">20
      </label>
      <label class="ema-chip on" id="c50">
        <input type="checkbox" checked onchange="toggleEMA(this,'50')">50
      </label>
      <label class="ema-chip on" id="c200">
        <input type="checkbox" checked onchange="toggleEMA(this,'200')">200
      </label>
    </div>

    <!-- Glowing Run Scan Button -->
    <button class="btn" id="scanBtn" onclick="startScan()">
      <i class="ti ti-scan-eye"></i> Run Live Scan
    </button>

    <!-- Search Box -->
    <input type="text" id="tickerSearch" placeholder="🔍 Search ticker..." oninput="render()" style="background:#1f2937;border:0.5px solid #374151;color:#e5e7eb;padding:5px 10px;border-radius:6px;font-size:12px;width:150px;margin-left:auto;outline:none;">
  </div>

  <!-- Real-time Progress Bar -->
  <div class="progress-wrap" id="progressContainer">
    <div class="progress-text" id="progressStatus">Scanning...</div>
    <div class="progress-bar-container">
      <div class="progress-bar-fill" id="progressBar"></div>
    </div>
    <div class="progress-text" id="progressPercent">0%</div>
  </div>

  <!-- Summary Statistics Bar -->
  <div class="summary-bar" id="summaryBar"></div>

  <!-- Main Signals Data Table -->
  <div class="tbl-wrap">
    <table>
      <thead>
        <tr>
          <th onclick="handleSort('symbol')">SYMBOL <span class="sort-icon" id="sort-symbol"><i class="ti ti-selector"></i></span></th>
          <th onclick="handleSort('score')">SCORE <span class="sort-icon" id="sort-score"><i class="ti ti-selector"></i></span></th>
          <th onclick="handleSort('hv_date')">HV DATE <span class="sort-icon" id="sort-hv_date"><i class="ti ti-selector"></i></span></th>
          <th onclick="handleSort('days')">DAYS <span class="sort-icon" id="sort-days"><i class="ti ti-selector"></i></span></th>
          <th onclick="handleSort('hv_low')">HV LOW <span class="sort-icon" id="sort-hv_low"><i class="ti ti-selector"></i></span></th>
          <th onclick="handleSort('hv_high')">HV HIGH <span class="sort-icon" id="sort-hv_high"><i class="ti ti-selector"></i></span></th>
          <th onclick="handleSort('price')">PRICE <span class="sort-icon" id="sort-price"><i class="ti ti-selector"></i></span></th>
          <th onclick="handleSort('pct_above')">% ABOVE <span class="sort-icon" id="sort-pct_above"><i class="ti ti-selector"></i></span></th>
          <th onclick="handleSort('upside')">UPSIDE <span class="sort-icon" id="sort-upside"><i class="ti ti-selector"></i></span></th>
          <th onclick="handleSort('rr')">R:R RATIO <span class="sort-icon" id="sort-rr"><i class="ti ti-selector"></i></span></th>
          <th>CAM LEVELS</th>
          <th>EMA ✓</th>
          <th onclick="handleSort('vol_ratio')">VOL <span class="sort-icon" id="sort-vol_ratio"><i class="ti ti-selector"></i></span></th>
          <th onclick="handleSort('candle')">CANDLE <span class="sort-icon" id="sort-candle"><i class="ti ti-selector"></i></span></th>
          <th>WATCH</th>
        </tr>
      </thead>
      <tbody id="tblBody"></tbody>
    </table>
  </div>

  <!-- Bottom Action Bar -->
  <div class="action-bar">
    <button class="tg-btn" onclick="showModal('tg')"><i class="ti ti-brand-telegram" aria-hidden="true"></i> Alert on Telegram</button>
    <button class="wa-btn" onclick="showModal('wa')"><i class="ti ti-brand-whatsapp" aria-hidden="true"></i> Alert on WhatsApp</button>
    <button class="tg-btn" onclick="sendDirectAlerts()" style="background:#4b5563;color:#e5e7eb;"><i class="ti ti-bell-ringing"></i> Trigger Alerts Manually</button>
    <span id="lastScanTime" style="font-size:11px;color:#4b5563;margin-left:auto;">Alerts auto-send when new signals detected</span>
  </div>

  <!-- Beautiful Configuration Modal -->
  <div class="modal-bg" id="modalBg">
    <div class="modal" style="width: 420px;">
      <h3 id="modalTitle">Configure Alerts</h3>
      
      <!-- Telegram Fields -->
      <div id="tgFields" style="display: none;">
        <label>Telegram Bot Token</label>
        <input type="text" id="tgToken" placeholder="e.g. 7123456789:AAF..." />
        
        <label>Chat ID or Group ID</label>
        <input type="text" id="tgChatId" placeholder="e.g. -1001234567890" />
      </div>

      <!-- WhatsApp Fields -->
      <div id="waFields" style="display: none;">
        <label>Twilio Account SID</label>
        <input type="text" id="waSid" placeholder="e.g. ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" />
        
        <label>Twilio Auth Token</label>
        <input type="password" id="waToken" placeholder="your Twilio Auth Token" />
        
        <label>WhatsApp Number (To)</label>
        <input type="text" id="waTo" placeholder="e.g. +919876543210" />
      </div>

      <div id="savedAlerts" style="margin-top:12px;"></div>
      
      <div class="row">
        <button class="btn-save" onclick="saveAlert()">Save & Activate</button>
        <button class="btn-cancel" onclick="closeModal()">Cancel</button>
      </div>
    </div>
  </div>
</div>

<!-- Slide Toast Notification -->
<div class="toast" id="toastBox">
  <i class="ti ti-circle-check" style="color:#34d399;font-size:16px;"></i>
  <span id="toastMsg">Alerts saved successfully!</span>
</div>

<script>
let stocks = [];
let watchlist = JSON.parse(localStorage.getItem('scanner_watchlist') || '[]');
let activeTab = 'bullish';
let currentSort = { col: 'score', desc: true };

const emaReq = { '10': true, '20': true, '50': true, '200': true };

// Formatters
function pct(n) { return (n > 0 ? '+' : '') + Number(n).toFixed(2) + '%'; }
function fmt(n) { return n >= 1000 ? '₹' + n.toLocaleString('en-IN', {maximumFractionDigits:2}) : '₹' + n.toFixed(2); }

function showToast(msg) {
  const t = document.getElementById('toastBox');
  document.getElementById('toastMsg').textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 3000);
}

function setTab(tabName) {
  activeTab = tabName;
  document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + tabName).classList.add('active');
  render();
}

function toggleEMA(cb, key) {
  emaReq[key] = cb.checked;
  document.getElementById('c' + key).classList.toggle('on', cb.checked);
  render();
}

function toggleWatchlist(sym, el) {
  const idx = watchlist.indexOf(sym);
  if (idx === -1) {
    watchlist.push(sym);
    el.style.color = '#fbbf24';
    showToast(`${sym} added to Watchlist`);
  } else {
    watchlist.splice(idx, 1);
    el.style.color = '#4b5563';
    showToast(`${sym} removed from Watchlist`);
  }
  localStorage.setItem('scanner_watchlist', JSON.stringify(watchlist));
  updateTabCounts();
  if (activeTab === 'watchlist') {
    render();
  }
}

function handleSort(column) {
  if (currentSort.col === column) {
    currentSort.desc = !currentSort.desc;
  } else {
    currentSort.col = column;
    currentSort.desc = true;
  }
  
  // Update header sort icons
  document.querySelectorAll('th .sort-icon').forEach(icon => {
    icon.innerHTML = '<i class="ti ti-selector"></i>';
  });
  
  const iconId = 'sort-' + column;
  const iconEl = document.getElementById(iconId);
  if (iconEl) {
    iconEl.innerHTML = currentSort.desc ? '<i class="ti ti-chevron-down"></i>' : '<i class="ti ti-chevron-up"></i>';
  }
  
  render();
}

function triggerFilterChange() {
  render();
}

function preprocessSignals(signals) {
  if (!signals) return [];
  return signals.map(s => {
    const risk = s.entry - s.stop_loss;
    const reward = s.target - s.entry;
    s.rr = risk > 0 ? parseFloat((reward / risk).toFixed(2)) : 0;
    return s;
  });
}

function updateTabCounts() {
  const searchVal = document.getElementById('tickerSearch').value.toLowerCase().trim();
  const volMult = parseFloat(document.getElementById('volMult').value);
  
  // Base filtered dataset matching options
  const baseData = stocks.filter(s => {
    if (s.vol_ratio < volMult) return false;
    if (searchVal && !s.symbol.toLowerCase().includes(searchVal)) return false;
    return true;
  });
  
  const counts = {
    bullish: baseData.filter(s => s.candle === 'Bull').length,
    bearish: baseData.filter(s => s.candle === 'Bear').length,
    hv: baseData.filter(s => s.days <= 10).length,
    entry: baseData.filter(s => Math.abs(s.price - s.entry) / s.entry <= 0.02).length,
    camarilla: baseData.length,
    watchlist: baseData.filter(s => watchlist.includes(s.symbol)).length
  };
  
  for (const [tab, count] of Object.entries(counts)) {
    const el = document.getElementById('tab-count-' + tab);
    if (el) el.textContent = count;
  }
  
  document.getElementById('bull-count').textContent = counts.bullish;
}

function render() {
  const tbody = document.getElementById('tblBody');
  const searchVal = document.getElementById('tickerSearch').value.toLowerCase().trim();
  const volMult = parseFloat(document.getElementById('volMult').value);
  
  // 1. Filter Stocks based on multipliers and search
  let filtered = stocks.filter(s => {
    if (s.vol_ratio < volMult) return false;
    if (searchVal && !s.symbol.toLowerCase().includes(searchVal)) return false;
    return true;
  });
  
  // 2. Filter based on active tab
  if (activeTab === 'bullish') {
    filtered = filtered.filter(s => s.candle === 'Bull');
  } else if (activeTab === 'bearish') {
    filtered = filtered.filter(s => s.candle === 'Bear');
  } else if (activeTab === 'hv') {
    filtered = filtered.filter(s => s.days <= 10);
  } else if (activeTab === 'entry') {
    filtered = filtered.filter(s => Math.abs(s.price - s.entry) / s.entry <= 0.02);
  } else if (activeTab === 'watchlist') {
    filtered = filtered.filter(s => watchlist.includes(s.symbol));
  }
  
  // 3. Sort stocks
  filtered.sort((a, b) => {
    let valA = a[currentSort.col];
    let valB = b[currentSort.col];
    
    // Sort strings case insensitively
    if (typeof valA === 'string') valA = valA.toLowerCase();
    if (typeof valB === 'string') valB = valB.toLowerCase();
    
    if (valA < valB) return currentSort.desc ? 1 : -1;
    if (valA > valB) return currentSort.desc ? -1 : 1;
    return 0;
  });
  
  // 4. Render Summary Metrics
  let bulls = 0, bears = 0, highVol = 0, scoreSum = 0;
  filtered.forEach(s => {
    if (s.candle === 'Bull') bulls++; else bears++;
    if (s.vol_ratio >= 3) highVol++;
    scoreSum += s.score;
  });
  const avgScore = filtered.length ? Math.round(scoreSum / filtered.length) : 0;
  
  document.getElementById('summaryBar').innerHTML = `
    <div class="sum-item"><span class="lbl">Total signals:</span><span class="val">${filtered.length}</span></div>
    <div class="sum-item"><span class="lbl">Bullish candles:</span><span class="val g">${bulls}</span></div>
    <div class="sum-item"><span class="lbl">Bearish candles:</span><span class="val r">${bears}</span></div>
    <div class="sum-item"><span class="lbl">Vol spike ≥3x:</span><span class="val y">${highVol}</span></div>
    <div class="sum-item"><span class="lbl">Avg score:</span><span class="val">${avgScore}/100</span></div>
  `;
  
  // 5. Generate Table Body HTML
  if (filtered.length === 0) {
    tbody.innerHTML = `<tr><td colspan="15" class="empty-state"><i class="ti ti-chart-candlestick" style="font-size:32px;color:#374151;display:block;margin-bottom:8px;"></i>No signals loaded. Click "Run Live Scan" to analyze Nifty 500 stocks!</td></tr>`;
    return;
  }
  
  tbody.innerHTML = filtered.map(s => {
    const isBull = s.candle === 'Bull';
    const volPct = Math.min(100, (s.vol_ratio / 4) * 100);
    const volClass = s.vol_ratio >= 3 ? 'spike' : s.vol_ratio >= 2 ? 'high' : '';
    
    const emaDots = [
      { pass: s.ema10_pass && emaReq['10'], label: '10' },
      { pass: s.ema20_pass && emaReq['20'], label: '20' },
      { pass: s.ema50_pass && emaReq['50'], label: '50' },
      { pass: s.ema200_pass && emaReq['200'], label: '200' }
    ].map(ema => `<div class="edot ${ema.pass ? 'pass' : ''}" title="EMA${ema.label}${ema.pass ? ' ✓' : ' ✗'}"></div>`).join('');
    
    // 52W High/Low Range Slider calculations
    const rangePct = Math.min(100, Math.max(0, ((s.price - s.hv_low) / (s.hv_high - s.hv_low)) * 100));
    
    // DAYS color-coding class: green 0-10d, yellow 11-30d, red >30d
    let daysColorClass = 'dn';
    if (s.days <= 10) daysColorClass = 'up';
    else if (s.days <= 30) daysColorClass = 'days-warn';
    
    // % ABOVE color-coding class: green <10%, yellow 10-25%, red >25%
    let pctColorClass = 'dn';
    if (s.pct_above < 10) pctColorClass = 'up';
    else if (s.pct_above <= 25) pctColorClass = 'days-warn';
    
    const isFav = watchlist.includes(s.symbol);
    const starColor = isFav ? '#fbbf24' : '#4b5563';
    
    // Score breakdown tooltip calculation
    let base = 50;
    let emaBonus = 0;
    if (s.ema10_pass) emaBonus += 5;
    if (s.ema20_pass) emaBonus += 5;
    if (s.ema50_pass) emaBonus += 5;
    if (s.ema200_pass) emaBonus += 5;
    
    let volBonus = 0;
    if (s.vol_ratio >= 3) volBonus = 15;
    else if (s.vol_ratio >= 2) volBonus = 10;
    else if (s.vol_ratio >= 1.5) volBonus = 5;
    
    let candleBonus = s.candle === 'Bull' ? 5 : 0;
    
    let lowPenalty = 0;
    if (s.pct_above < 5) lowPenalty = 5;
    else if (s.pct_above > 50) lowPenalty = -40;
    else if (s.pct_above > 30) lowPenalty = -30;
    else if (s.pct_above > 25) lowPenalty = -20;
    else if (s.pct_above > 15) lowPenalty = -10;
    else if (s.pct_above < 10 && s.pct_above >= 5) lowPenalty = 2;
    
    let tooltip = `Base Score: 50\n`;
    if (emaBonus > 0) tooltip += `+${emaBonus} for EMAs passed\n`;
    if (volBonus > 0) tooltip += `+${volBonus} for Volume spike (${s.vol_ratio.toFixed(1)}x)\n`;
    if (candleBonus > 0) tooltip += `+${candleBonus} for Bullish candle\n`;
    if (lowPenalty > 0) tooltip += `+${lowPenalty} for low % above 52W low\n`;
    if (lowPenalty < 0) tooltip += `${lowPenalty} Penalty: Chasing (${s.pct_above.toFixed(1)}% above 52W low)`;
    tooltip = tooltip.trim();
    
    return `
      <tr>
        <td>
          <span class="sym">${s.symbol}</span><br>
          <span class="buy-badge">BUY</span>
          <div style="font-size:9px;color:#6b7280;margin-top:5px;display:flex;align-items:center;gap:4px;" title="Price position between 52W Low (${fmt(s.hv_low)}) and 52W High (${fmt(s.hv_high)}) is at ${rangePct.toFixed(1)}%">
            <span>52W L</span>
            <div style="width:55px;height:3px;background:#374151;border-radius:1.5px;position:relative;">
              <div style="position:absolute;left:${rangePct}%;top:-1.5px;width:6px;height:6px;border-radius:50%;background:#3b82f6;box-shadow:0 0 3px #3b82f6;"></div>
            </div>
            <span>52W H</span>
          </div>
        </td>
        <td><span class="score" style="cursor:help;" title="${tooltip}">${s.score}<span class="denom">/100</span></span></td>
        <td style="color:#9ca3af">${s.hv_date}</td>
        <td class="${daysColorClass}">${s.days}d</td>
        <td>${fmt(s.hv_low)}</td>
        <td>${fmt(s.hv_high)}</td>
        <td><div class="price-main">${fmt(s.price)}</div><div class="price-date">● ${s.scanned_date || 'Today'}</div></td>
        <td class="${pctColorClass}">${pct(s.pct_above)}</td>
        <td class="${s.upside < 0 ? 'dn' : 'up'}">${pct(s.upside)}</td>
        <td class="neutral">1 : ${s.rr ? s.rr.toFixed(1) : '0.0'}</td>
        <td class="cam-levels">
          <div class="cam-h3">H3 ${fmt(s.target)}</div>
          <div class="cam-pivot">P ${fmt(s.entry)}</div>
          <div class="cam-l3">L3 ${fmt(s.stop_loss)}</div>
        </td>
        <td><div class="ema-dots">${emaDots}</div></td>
        <td>
          <div class="vol-bar"><div class="vol-fill ${volClass}" style="width:${volPct}%"></div></div>
          <br><span style="font-size:10px;color:#9ca3af">${s.vol_ratio.toFixed(2)}x</span>
        </td>
        <td><span class="${isBull ? 'candle-bull' : 'candle-bear'}">${s.candle}</span></td>
        <td><i class="ti ti-star" aria-hidden="true" style="font-size:16px;color:${starColor};cursor:pointer;transition:color 0.2s;" onclick="toggleWatchlist('${s.symbol}', this)"></i></td>
      </tr>
    `;
  }).join('');
}

// ── SCANS AND BACKGROUND CHECKS ──────────────────────────────
let pollInterval = null;

function checkScanStatus() {
  fetch('/status')
    .then(r => r.json())
    .then(data => {
      const btn = document.getElementById('scanBtn');
      const pWrap = document.getElementById('progressContainer');
      
      if (data.scanning) {
        btn.classList.add('scanning');
        btn.disabled = true;
        btn.innerHTML = '<span class="ti ti-loader-quarter" style="animation:spin 1s linear infinite;display:inline-block;"></span> Scanning...';
        
        pWrap.classList.add('show');
        const curr = data.progress.current;
        const tot = data.progress.total;
        const pctVal = tot > 0 ? Math.round((curr / tot) * 100) : 0;
        
        document.getElementById('progressBar').style.width = pctVal + '%';
        document.getElementById('progressStatus').textContent = `Scanning: [${curr}/${tot}] downloading data for ${data.progress.ticker}`;
        document.getElementById('progressPercent').textContent = pctVal + '%';
      } else {
        btn.classList.remove('scanning');
        btn.disabled = false;
        btn.innerHTML = '<i class="ti ti-scan-eye"></i> Run Live Scan';
        pWrap.classList.remove('show');
        
        if (pollInterval) {
          clearInterval(pollInterval);
          pollInterval = null;
          showToast("Live Scan Completed Successfully!");
        }
        
        if (data.signals) {
          stocks = preprocessSignals(data.signals);
          document.getElementById('lastScanTime').textContent = data.last_scan ? 'Last scan: ' + data.last_scan : 'Never Scanned';
          updateTabCounts();
          render();
        }
      }
    })
    .catch(() => {
      if (pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
      }
    });
}

function startScan() {
  if (pollInterval) return; // already active
  
  const params = {
    vol_days: document.getElementById('volDays').value,
    vol_mult: document.getElementById('volMult').value,
    ema10:  document.getElementById('c10').classList.contains('on'),
    ema20:  document.getElementById('c20').classList.contains('on'),
    ema50:  document.getElementById('c50').classList.contains('on'),
    ema200: document.getElementById('c200').classList.contains('on'),
    
    // Pass alert credentials dynamically from localStorage
    tg_token: localStorage.getItem('cfg_tg_token') || '',
    tg_chat_id: localStorage.getItem('cfg_tg_chat_id') || '',
    twilio_sid: localStorage.getItem('cfg_wa_sid') || '',
    twilio_token: localStorage.getItem('cfg_wa_token') || '',
    twilio_to: localStorage.getItem('cfg_wa_to') || ''
  };
  
  fetch('/scan', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params)
  })
  .then(r => r.json())
  .then(data => {
    if (data.error) {
      showToast("Error: " + data.error);
    } else {
      showToast("Live scanning initialized in background!");
      pollInterval = setInterval(checkScanStatus, 1500);
    }
  })
  .catch(() => showToast("Failed to initialize scan."));
}

function sendDirectAlerts() {
  const params = {
    tg_token: localStorage.getItem('cfg_tg_token') || '',
    tg_chat_id: localStorage.getItem('cfg_tg_chat_id') || '',
    twilio_sid: localStorage.getItem('cfg_wa_sid') || '',
    twilio_token: localStorage.getItem('cfg_wa_token') || '',
    twilio_to: localStorage.getItem('cfg_wa_to') || ''
  };
  
  fetch('/alert', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params)
  })
  .then(r => r.json())
  .then(data => showToast(data.message))
  .catch(() => showToast("Failed to send alerts."));
}

// ── ALERTS MODAL CONFIGURATION ──────────────────────────────
let modalType = '';

function showModal(type) {
  modalType = type;
  document.getElementById('modalBg').classList.add('show');
  
  const tgF = document.getElementById('tgFields');
  const waF = document.getElementById('waFields');
  
  if (type === 'tg') {
    document.getElementById('modalTitle').innerHTML = '<i class="ti ti-brand-telegram" style="color:#0369a1;"></i> Configure Telegram Alerts';
    tgF.style.display = 'block';
    waF.style.display = 'none';
    
    // Load from localStorage or fall back to backend config
    const savedToken = localStorage.getItem('cfg_tg_token');
    const savedChatId = localStorage.getItem('cfg_tg_chat_id');
    
    if (savedToken) document.getElementById('tgToken').value = savedToken;
    if (savedChatId) document.getElementById('tgChatId').value = savedChatId;
    
    if (!savedToken || !savedChatId) {
      fetch('/get_config')
        .then(r => r.json())
        .then(cfg => {
          if (!document.getElementById('tgToken').value) document.getElementById('tgToken').value = cfg.tg_token || '';
          if (!document.getElementById('tgChatId').value) document.getElementById('tgChatId').value = cfg.tg_chat_id || '';
        });
    }
  } else {
    document.getElementById('modalTitle').innerHTML = '<i class="ti ti-brand-whatsapp" style="color:#14532d;"></i> Configure WhatsApp (Twilio) Alerts';
    tgF.style.display = 'none';
    waF.style.display = 'block';
    
    const savedSid = localStorage.getItem('cfg_wa_sid');
    const savedToken = localStorage.getItem('cfg_wa_token');
    const savedTo = localStorage.getItem('cfg_wa_to');
    
    if (savedSid) document.getElementById('waSid').value = savedSid;
    if (savedToken) document.getElementById('waToken').value = savedToken;
    if (savedTo) document.getElementById('waTo').value = savedTo;
    
    if (!savedSid || !savedToken || !savedTo) {
      fetch('/get_config')
        .then(r => r.json())
        .then(cfg => {
          if (!document.getElementById('waSid').value) document.getElementById('waSid').value = cfg.twilio_sid || '';
          if (!document.getElementById('waToken').value) document.getElementById('waToken').value = cfg.twilio_token || '';
          if (!document.getElementById('waTo').value) document.getElementById('waTo').value = cfg.twilio_to || '';
        });
    }
  }
}

function closeModal() {
  document.getElementById('modalBg').classList.remove('show');
}

function saveAlert() {
  let params = { type: modalType };
  
  if (modalType === 'tg') {
    const token = document.getElementById('tgToken').value.trim();
    const chatId = document.getElementById('tgChatId').value.trim();
    if (!token || !chatId) {
      showToast('Please fill in all Telegram fields.');
      return;
    }
    localStorage.setItem('cfg_tg_token', token);
    localStorage.setItem('cfg_tg_chat_id', chatId);
    params.f1 = token;
    params.f2 = chatId;
  } else {
    const sid = document.getElementById('waSid').value.trim();
    const token = document.getElementById('waToken').value.trim();
    const to = document.getElementById('waTo').value.trim();
    if (!sid || !token || !to) {
      showToast('Please fill in all Twilio WhatsApp fields.');
      return;
    }
    localStorage.setItem('cfg_wa_sid', sid);
    localStorage.setItem('cfg_wa_token', token);
    localStorage.setItem('cfg_wa_to', to);
    params.f1 = sid;
    params.f2 = to;
    params.f3 = token;
  }
  
  fetch('/save_config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params)
  })
  .then(r => r.json())
  .then(data => {
    showToast(data.message);
    closeModal();
  })
  .catch(() => showToast('Failed to save configuration.'));
}

// ── INITIALIZATION ──────────────────────────────────────────
fetch('/results')
  .then(r => r.json())
  .then(data => {
    if (data.scanning) {
      pollInterval = setInterval(checkScanStatus, 1500);
    } else if (data.signals) {
      stocks = preprocessSignals(data.signals);
      document.getElementById('lastScanTime').textContent = data.last_scan ? 'Last scan: ' + data.last_scan : 'Never Scanned';
      updateTabCounts();
      render();
    }
  });
</script>

<style>
  /* Animations helper */
  @keyframes spin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
  }
  @keyframes pulse {
    0% { opacity: 0.3; }
    50% { opacity: 1; }
    100% { opacity: 0.3; }
  }
</style>
</body>
</html>"""

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/results")
def results():
    status_data = read_status()
    return jsonify({
        "signals": status_data["signals"],
        "last_scan": status_data["last_scan"],
        "scanning": status_data["scanning"]
    })

@app.route("/status")
def status():
    status_data = read_status()
    return jsonify({
        "scanning": status_data["scanning"],
        "progress": status_data["progress"],
        "last_scan": status_data["last_scan"],
        "signals": status_data["signals"] if not status_data["scanning"] else []
    })

@app.route("/get_config")
def get_config():
    return jsonify({
        "tg_token": CFG["TG_TOKEN"],
        "tg_chat_id": CFG["TG_CHAT_ID"],
        "twilio_sid": CFG["TWILIO_SID"],
        "twilio_token": CFG["TWILIO_TOKEN"],
        "twilio_from": CFG["TWILIO_FROM"],
        "twilio_to": CFG["TWILIO_TO"].replace("whatsapp:", "") if CFG["TWILIO_TO"] else ""
    })

@app.route("/save_config", methods=["POST"])
def save_config():
    body = request.get_json() or {}
    t = body.get("type")
    f1 = body.get("f1", "") # Token or SID
    f2 = body.get("f2", "") # Chat ID or WA number
    f3 = body.get("f3", "") # Twilio Token
    
    env_path = ".env"
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as file:
            lines = file.readlines()
            
    config_map = {}
    for line in lines:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            config_map[k.strip()] = v.strip()
            
    if t == "tg":
        config_map["TG_TOKEN"] = f1
        config_map["TG_CHAT_ID"] = f2
        CFG["TG_TOKEN"] = f1
        CFG["TG_CHAT_ID"] = f2
    elif t == "wa":
        config_map["TWILIO_SID"] = f1
        config_map["TWILIO_TOKEN"] = f3
        wa_to = f"whatsapp:{f2}" if not f2.startswith("whatsapp:") else f2
        config_map["TWILIO_TO"] = wa_to
        CFG["TWILIO_SID"] = f1
        CFG["TWILIO_TOKEN"] = f3
        CFG["TWILIO_TO"] = wa_to
        
    with open(env_path, "w", encoding="utf-8") as file:
        for k, v in config_map.items():
            file.write(f"{k}={v}\n")
            
    return jsonify({"message": f"Successfully configured and activated {t.upper()} alerts!"})

@app.route("/scan", methods=["POST"])
def scan():
    status_data = read_status()
    if status_data.get("scanning", False):
        return jsonify({"error": "Scan already running"}), 429
        
    body = request.get_json() or {}
    CFG["VOL_DAYS"] = int(body.get("vol_days", CFG["VOL_DAYS"]))
    CFG["VOL_MULT"] = float(body.get("vol_mult", CFG["VOL_MULT"]))
    CFG["EMA_10"]   = str(body.get("ema10",  True)).lower() != "false"
    CFG["EMA_20"]   = str(body.get("ema20",  True)).lower() != "false"
    CFG["EMA_50"]   = str(body.get("ema50",  True)).lower() != "false"
    CFG["EMA_200"]  = str(body.get("ema200", True)).lower() != "false"
    
    # Capture dynamic credentials from localStorage payload
    tg_token = body.get("tg_token", "")
    tg_chat_id = body.get("tg_chat_id", "")
    twilio_sid = body.get("twilio_sid", "")
    twilio_token = body.get("twilio_token", "")
    twilio_to = body.get("twilio_to", "")
    
    if tg_token: CFG["TG_TOKEN"] = tg_token
    if tg_chat_id: CFG["TG_CHAT_ID"] = tg_chat_id
    if twilio_sid: CFG["TWILIO_SID"] = twilio_sid
    if twilio_token: CFG["TWILIO_TOKEN"] = twilio_token
    if twilio_to:
        wa_to = f"whatsapp:{twilio_to}" if not twilio_to.startswith("whatsapp:") else twilio_to
        CFG["TWILIO_TO"] = wa_to
        
    # Write starting scan status
    status_data["scanning"] = True
    status_data["progress"] = {"current": 0, "total": len(NIFTY500_SAMPLE), "ticker": "Initializing..."}
    write_status(status_data)
    
    def progress_callback(current, total, ticker):
        s_data = read_status()
        s_data["progress"] = {"current": current, "total": total, "ticker": ticker}
        write_status(s_data)
        
    def bg_scan():
        try:
            signals = run_scan(NIFTY500_SAMPLE, progress_cb=progress_callback)
            
            # Save automatically to CSV
            from scanner import save_csv, send_telegram, send_whatsapp
            try:
                save_csv(signals)
            except Exception as e:
                print(f"Error saving CSV: {e}")
                
            # Dispatches alerts automatically
            if signals:
                try:
                    send_telegram(signals)
                except Exception as e:
                    print(f"Error sending TG alert: {e}")
                try:
                    send_whatsapp(signals)
                except Exception as e:
                    print(f"Error sending WA alert: {e}")
                    
            # Complete scan status file update
            s_data = read_status()
            s_data["signals"] = signals
            s_data["last_scan"] = datetime.now().strftime("%d %b %Y %H:%M")
            s_data["scanning"] = False
            s_data["progress"] = {"current": len(NIFTY500_SAMPLE), "total": len(NIFTY500_SAMPLE), "ticker": "Completed"}
            write_status(s_data)
            
        except Exception as e:
            print(f"Background Scan Error: {e}")
            s_data = read_status()
            s_data["scanning"] = False
            write_status(s_data)
            
    threading.Thread(target=bg_scan, daemon=True).start()
    return jsonify({"status": "Scan initialized"})

@app.route("/alert", methods=["POST"])
def alert():
    body = request.get_json() or {}
    
    tg_token = body.get("tg_token", "")
    tg_chat_id = body.get("tg_chat_id", "")
    twilio_sid = body.get("twilio_sid", "")
    twilio_token = body.get("twilio_token", "")
    twilio_to = body.get("twilio_to", "")
    
    if tg_token: CFG["TG_TOKEN"] = tg_token
    if tg_chat_id: CFG["TG_CHAT_ID"] = tg_chat_id
    if twilio_sid: CFG["TWILIO_SID"] = twilio_sid
    if twilio_token: CFG["TWILIO_TOKEN"] = twilio_token
    if twilio_to:
        wa_to = f"whatsapp:{twilio_to}" if not twilio_to.startswith("whatsapp:") else twilio_to
        CFG["TWILIO_TO"] = wa_to

    from scanner import send_telegram, send_whatsapp
    status_data = read_status()
    signals = status_data.get("signals", [])
    if not signals:
        return jsonify({"message": "No active signals in cache to alert on."})
        
    send_telegram(signals)
    send_whatsapp(signals)
    return jsonify({"message": f"Manual alerts triggered successfully for {len(signals)} active signal(s)!"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
