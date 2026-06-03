# alert_engine.py — UPGRADE existing send_alert()

import time
from datetime import datetime

alert_cooldown = {}  # symbol -> last_alert_timestamp

def should_send_alert(symbol, score, cooldown_minutes=30):
    """Rate limiting + score gate"""
    if score < 75:
        return False, f"Score {score} < 75 threshold"
    
    now = time.time()
    last_sent = alert_cooldown.get(symbol, 0)
    
    if (now - last_sent) < cooldown_minutes * 60:
        remaining = int((cooldown_minutes * 60 - (now - last_sent)) / 60)
        return False, f"Cooldown active — {remaining} mins remaining"
    
    return True, "OK"


def format_telegram_alert(stock, score_breakdown, signal_explanation):
    """Professional alert format"""
    entry = stock.get('entry', 0.0)
    sl = stock.get('sl', 0.0)
    t1 = stock.get('t1', 0.0)
    rr = stock.get('rr', 0.0)
    
    # Safely format passed items
    passed_list = signal_explanation.get('passed', [])
    conditions_met_str = '\n'.join('• ' + r for r in passed_list) if passed_list else '• None'
    
    msg = f"""
🎯 *BUY SIGNAL — ProTrader Terminal*

*{stock.get('symbol', 'UNKNOWN')}* | {stock.get('sector', 'General Equity')}
━━━━━━━━━━━━━━━━━━━━

📈 *Price:* ₹{stock.get('ltp', 0.0):.2f}
🎯 *Entry:* ₹{entry:.2f}
🛡 *Stop Loss:* ₹{sl:.2f}
🏆 *Target T1:* ₹{t1:.2f}
📊 *RR:* {rr:.1f}:1

⚡ *RVOL:* {stock.get('rvol', 0.0):.1f}x
🏅 *Score:* {score_breakdown.get('total', 0.0)}/100 ({score_breakdown.get('grade', 'C')})
⏱ *TF:* {stock.get('timeframe', '1d')}

✅ *Conditions Met:*
{conditions_met_str}

📌 *Regime:* {stock.get('market_regime', 'NEUTRAL')} ({stock.get('regime_confidence', 0)}%)

⚠️ _Risk 1% of capital. Use bracket orders._
#NSE #Intraday #{stock.get('symbol', 'UNKNOWN')}
"""
    return msg


def store_alert_history(symbol, alert_data, db_conn):
    """Store every alert for audit trail"""
    # Use datetime.now().strftime for standard SQLite format
    db_conn.execute("""
        INSERT INTO alert_history 
        (symbol, score, entry, sl, target, rr, rvol, 
         regime, alert_time, channel)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (symbol, alert_data.get('score'), alert_data.get('entry'),
          alert_data.get('sl'), alert_data.get('target'), alert_data.get('rr'),
          alert_data.get('rvol'), alert_data.get('regime'), 
          datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'telegram'))
