# admin_routes.py — ADD to existing routes

from flask import Blueprint, render_template_string, jsonify, request
import psutil
import sqlite3
import os
import time
from datetime import datetime

# Import cache states from other modules
from data_fetcher import _price_cache
import scanner_core
from journal import DB_PATH

admin_bp = Blueprint('admin', __name__)

# Premium Admin Interface
ADMIN_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ProTrader Terminal - System Administration</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;900&family=Outfit:wght@400;600;800&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@latest/tabler-icons.min.css">
    <style>
        :root {
            --bg-dark: #090d16;
            --bg-panel: #111827;
            --bg-card: #1f2937;
            --text-main: #f3f4f6;
            --text-muted: #9ca3af;
            --pro-blue: #3b82f6;
            --pro-green: #10b981;
            --pro-orange: #f59e0b;
            --pro-red: #ef4444;
            --border: #374151;
            --font-mono: 'JetBrains Mono', monospace;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            background-color: var(--bg-dark);
            color: var(--text-main);
            font-family: 'Inter', sans-serif;
            padding: 24px;
            min-height: 100vh;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 24px;
            border-bottom: 1px solid var(--border);
            padding-bottom: 16px;
        }

        h1 {
            font-family: 'Outfit', sans-serif;
            font-size: 24px;
            font-weight: 800;
            display: flex;
            align-items: center;
            gap: 10px;
            background: linear-gradient(to right, #60a5fa, #3b82f6);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .btn {
            background: linear-gradient(135deg, var(--pro-blue), #1d4ed8);
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 8px;
            font-weight: 600;
            font-family: inherit;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 8px;
            transition: all 0.2s;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.2);
        }

        .btn:hover {
            opacity: 0.95;
            transform: translateY(-1px);
        }

        .btn-red {
            background: linear-gradient(135deg, var(--pro-red), #b91c1c);
            box-shadow: 0 4px 12px rgba(239, 68, 68, 0.2);
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            margin-bottom: 24px;
        }

        .card {
            background-color: var(--bg-panel);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.3);
        }

        .card-title {
            font-size: 14px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-muted);
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .card-title i {
            font-size: 18px;
            color: var(--pro-blue);
        }

        .metric-row {
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }

        .metric-row:last-child {
            border-bottom: none;
        }

        .metric-label {
            color: var(--text-muted);
            font-size: 14px;
        }

        .metric-value {
            font-family: var(--font-mono);
            font-weight: bold;
            font-size: 14px;
        }

        .progress-bar-container {
            width: 100%;
            height: 6px;
            background-color: var(--border);
            border-radius: 3px;
            margin-top: 6px;
            overflow: hidden;
        }

        .progress-bar {
            height: 100%;
            background-color: var(--pro-blue);
            border-radius: 3px;
        }

        .table-card {
            grid-column: 1 / -1;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }

        th, td {
            text-align: left;
            padding: 12px;
            border-bottom: 1px solid var(--border);
            font-size: 14px;
        }

        th {
            color: var(--text-muted);
            font-weight: 600;
        }

        td {
            font-family: var(--font-mono);
        }

        tr:hover {
            background-color: rgba(255,255,255,0.02);
        }

        .badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
        }

        .badge-green {
            background-color: rgba(16, 185, 129, 0.1);
            color: var(--pro-green);
            border: 1px solid rgba(16, 185, 129, 0.2);
        }

        .badge-red {
            background-color: rgba(239, 68, 68, 0.1);
            color: var(--pro-red);
            border: 1px solid rgba(239, 68, 68, 0.2);
        }

        .badge-blue {
            background-color: rgba(59, 130, 246, 0.1);
            color: var(--pro-blue);
            border: 1px solid rgba(59, 130, 246, 0.2);
        }
    </style>
</head>
<body>

    <header>
        <h1><i class="ti ti-settings"></i> ProTrader Terminal System Admin</h1>
        <button class="btn btn-red" onclick="clearCache()"><i class="ti ti-trash"></i> Flush Cache Cache</button>
    </header>

    <div class="grid">
        <!-- System Resources -->
        <div class="card">
            <div class="card-title"><i class="ti ti-cpu"></i> System Status</div>
            <div class="metric-row">
                <span class="metric-label">CPU Utilization</span>
                <span class="metric-value" id="cpu-val">--%</span>
            </div>
            <div class="progress-bar-container"><div class="progress-bar" id="cpu-bar" style="width: 0%"></div></div>

            <div class="metric-row" style="margin-top: 16px;">
                <span class="metric-label">RAM Memory Usage</span>
                <span class="metric-value" id="ram-val">--%</span>
            </div>
            <div class="progress-bar-container"><div class="progress-bar" id="ram-bar" style="width: 0%; background-color: var(--pro-orange)"></div></div>

            <div class="metric-row" style="margin-top: 16px;">
                <span class="metric-label">Disk Storage space</span>
                <span class="metric-value" id="disk-val">--%</span>
            </div>
            <div class="progress-bar-container"><div class="progress-bar" id="disk-bar" style="width: 0%; background-color: var(--pro-green)"></div></div>
        </div>

        <!-- Scanner Metrics -->
        <div class="card">
            <div class="card-title"><i class="ti ti-clock"></i> Scanner Core</div>
            <div class="metric-row">
                <span class="metric-label">Last scan time</span>
                <span class="metric-value" id="scan-time">--</span>
            </div>
            <div class="metric-row">
                <span class="metric-label">Stocks scanned</span>
                <span class="metric-value" id="scan-count">--</span>
            </div>
            <div class="metric-row">
                <span class="metric-label">Signals generated</span>
                <span class="metric-value" id="signals-count">--</span>
            </div>
            <div class="metric-row">
                <span class="metric-label">Duration</span>
                <span class="metric-value" id="scan-duration">--s</span>
            </div>
        </div>

        <!-- Database & Cache -->
        <div class="card">
            <div class="card-title"><i class="ti ti-database"></i> Database & Cache</div>
            <div class="metric-row">
                <span class="metric-label">DB File Size</span>
                <span class="metric-value" id="db-size">-- MB</span>
            </div>
            <div class="metric-row">
                <span class="metric-label">Total Trades Logged</span>
                <span class="metric-value" id="total-trades">--</span>
            </div>
            <div class="metric-row">
                <span class="metric-label">Price Cache Size</span>
                <span class="metric-value" id="cache-size">--</span>
            </div>
            <div class="metric-row">
                <span class="metric-label">Sector Cache Age</span>
                <span class="metric-value" id="sector-cache-age">-- mins</span>
            </div>
            <div class="metric-row">
                <span class="metric-label">LTP Fetch Status (15m)</span>
                <span class="metric-value" id="ltp-fetch-stats">--</span>
            </div>
        </div>

        <!-- Recent Alerts Audit Trail -->
        <div class="card table-card">
            <div class="card-title"><i class="ti ti-bell"></i> Telegram Alert History Audit Trail</div>
            <div style="overflow-x: auto;">
                <table>
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>Symbol</th>
                            <th>Score</th>
                            <th>Regime</th>
                            <th>Entry</th>
                            <th>Channel</th>
                        </tr>
                    </thead>
                    <tbody id="alerts-body">
                        <tr>
                            <td colspan="6" style="text-align: center; color: var(--text-muted);">Loading alert audit logs...</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        function loadStatus() {
            fetch('/admin/api/status')
                .then(res => res.json())
                .then(data => {
                    // Resources
                    document.getElementById('cpu-val').textContent = data.system.cpu_pct + '%';
                    document.getElementById('cpu-bar').style.width = data.system.cpu_pct + '%';
                    document.getElementById('ram-val').textContent = data.system.ram_pct + '%';
                    document.getElementById('ram-bar').style.width = data.system.ram_pct + '%';
                    document.getElementById('disk-val').textContent = data.system.disk_pct + '%';
                    document.getElementById('disk-bar').style.width = data.system.disk_pct + '%';

                    // Scanner
                    document.getElementById('scan-time').textContent = data.scanner.last_scan || 'Never';
                    document.getElementById('scan-count').textContent = data.scanner.symbols_scanned || '0';
                    document.getElementById('signals-count').textContent = data.scanner.signals_found || '0';
                    document.getElementById('scan-duration').textContent = (data.scanner.scan_duration_secs || 0).toFixed(1) + 's';

                    // Cache & DB
                    document.getElementById('db-size').textContent = (data.database.db_size_mb || 0).toFixed(2) + ' MB';
                    document.getElementById('total-trades').textContent = data.database.total_trades || '0';
                    document.getElementById('cache-size').textContent = data.cache.price_cache_size || '0';
                    document.getElementById('sector-cache-age').textContent = (data.cache.sector_cache_age_mins || 0).toFixed(1) + 'm';
                    
                    const succ = data.cache.live_fetch_success_15m || 0;
                    const fail = data.cache.live_fetch_failure_15m || 0;
                    document.getElementById('ltp-fetch-stats').innerHTML = 
                        `<span style="color:var(--pro-green)">${succ} Ok</span> / <span style="color:var(--pro-red)">${fail} Fail</span>`;

                    // Table
                    const rows = data.recent_alerts.map(r => `
                        <tr>
                            <td>${r[4]}</td>
                            <td><strong style="color:var(--pro-blue)">${r[0]}</strong></td>
                            <td>${r[1]}</td>
                            <td><span class="badge ${r[3] === 'BULL' ? 'badge-green' : r[3] === 'BEAR' ? 'badge-red' : 'badge-blue'}">${r[3]}</span></td>
                            <td>₹${r[2]}</td>
                            <td>telegram</td>
                        </tr>
                    `).join('');
                    document.getElementById('alerts-body').innerHTML = rows || '<tr><td colspan="6" style="text-align:center;color:var(--text-muted)">No alerts logged yet.</td></tr>';
                });
        }

        function clearCache() {
            if (confirm('Are you sure you want to flush all price/sector caches?')) {
                fetch('/admin/api/clear-cache', { method: 'POST' })
                    .then(res => res.json())
                    .then(data => {
                        alert('Cache flushed successfully!');
                        loadStatus();
                    });
            }
        }

        loadStatus();
        setInterval(loadStatus, 10000);
    </script>
</body>
</html>
"""

@admin_bp.route('/admin')
def admin_dashboard():
    return render_template_string(ADMIN_HTML)

@admin_bp.route('/admin/api/status')
def admin_status():
    """System health for admin panel"""
    # Try fetching stats from the scan_metrics table for historical scans
    last_scan_time = "Never"
    symbols_scanned = 0
    signals_found = 0
    scan_duration_secs = 0.0
    
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("""
            SELECT timestamp, duration_seconds, tickers_scanned, signals_found 
            FROM scan_metrics 
            ORDER BY id DESC LIMIT 1
        """).fetchone()
        if row:
            last_scan_time = row[0]
            scan_duration_secs = float(row[1])
            symbols_scanned = int(row[2])
            signals_found = int(row[3])
        conn.close()
    except Exception:
        pass

    # Scanner status
    scanner_status = {
        'last_scan': last_scan_time,
        'symbols_scanned': symbols_scanned,
        'signals_found': signals_found,
        'scan_duration_secs': scan_duration_secs,
        'auto_run': True
    }
    
    # Cache status
    sector_age = (time.time() - scanner_core._sector_cache_time) / 60 if scanner_core._sector_cache_time > 0 else 0.0
    
    from data_fetcher import get_fetch_stats
    succ, fail = get_fetch_stats()
    
    cache_status = {
        'price_cache_size': len(_price_cache),
        'sector_cache_age_mins': sector_age,
        'oldest_cache_entry': min(
            (time.time() - v['time'])/60 
            for v in _price_cache.values()
        ) if _price_cache else 0.0,
        'live_fetch_success_15m': succ,
        'live_fetch_failure_15m': fail
    }
    
    # DB status
    total_trades = 0
    open_trades = 0
    total_alerts = 0
    db_size_mb = 0.0
    
    try:
        conn = sqlite3.connect(DB_PATH)
        total_trades = conn.execute("SELECT COUNT(*) FROM trade_journal").fetchone()[0]
        open_trades = conn.execute("SELECT COUNT(*) FROM trade_journal WHERE outcome='OPEN'").fetchone()[0]
        total_alerts = conn.execute("SELECT COUNT(*) FROM alert_history").fetchone()[0]
        conn.close()
    except Exception:
        pass
        
    if os.path.exists(DB_PATH):
        db_size_mb = os.path.getsize(DB_PATH) / 1024 / 1024
        
    db_status = {
        'total_trades': total_trades,
        'open_trades': open_trades,
        'total_alerts': total_alerts,
        'db_size_mb': db_size_mb
    }
    
    # System resources
    system_status = {
        'cpu_pct': psutil.cpu_percent(),
        'ram_pct': psutil.virtual_memory().percent,
        'disk_pct': psutil.disk_usage('/').percent
    }
    
    # Recent alerts
    recent_alerts = []
    try:
        conn = sqlite3.connect(DB_PATH)
        recent_alerts = conn.execute("""
            SELECT symbol, score, entry, regime, alert_time, channel 
            FROM alert_history 
            ORDER BY alert_time DESC LIMIT 20
        """).fetchall()
        conn.close()
    except Exception:
        pass
        
    return jsonify({
        'scanner': scanner_status,
        'cache': cache_status,
        'database': db_status,
        'system': system_status,
        'recent_alerts': recent_alerts
    })

@admin_bp.route('/admin/api/clear-cache', methods=['POST'])
def clear_cache():
    global _price_cache
    _price_cache.clear()
    scanner_core._sector_cache.clear()
    scanner_core._sector_cache_time = 0
    return jsonify({'status': 'cache cleared'})
