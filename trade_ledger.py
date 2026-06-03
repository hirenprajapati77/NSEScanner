# trade_ledger.py — UPGRADE existing open_trades()

from datetime import datetime

def get_trade_metrics(trade, current_ltp):
    entry = trade['entry_price']
    sl = trade['stop_loss']
    t1 = trade['target_t1']
    qty = trade['quantity'] or 1
    
    # Parse entry_date safely
    entry_date = trade.get('created_at') # Or signal_date
    if isinstance(entry_date, str):
        try:
            entry_date = datetime.strptime(entry_date, "%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                entry_date = datetime.strptime(entry_date, "%Y-%m-%d")
            except Exception:
                entry_date = datetime.now()
    elif not isinstance(entry_date, datetime):
        entry_date = datetime.now()
        
    # MTM (Mark to Market)
    mtm = (current_ltp - entry) * qty if trade.get('signal_type', 'Bull').lower() == 'bull' else (entry - current_ltp) * qty
    
    # Risk & Reward
    risk_per_share = abs(entry - sl)
    reward_per_share = abs(t1 - entry)
    risk_total = risk_per_share * qty
    reward_total = reward_per_share * qty
    
    # Current RR
    if trade.get('signal_type', 'Bull').lower() == 'bull':
        if current_ltp > entry:
            current_rr = (current_ltp - entry) / risk_per_share if risk_per_share > 0 else 0.0
        else:
            current_rr = -(entry - current_ltp) / risk_per_share if risk_per_share > 0 else 0.0
    else:
        # Bear / short position
        if current_ltp < entry:
            current_rr = (entry - current_ltp) / risk_per_share if risk_per_share > 0 else 0.0
        else:
            current_rr = -(current_ltp - entry) / risk_per_share if risk_per_share > 0 else 0.0
            
    # Days in trade
    days = (datetime.now() - entry_date).days
    
    # Status
    is_bull = trade.get('signal_type', 'Bull').lower() == 'bull'
    if (is_bull and current_ltp >= t1) or (not is_bull and current_ltp <= t1):
        status = 'TARGET HIT'
        status_color = '#10B981'
    elif (is_bull and current_ltp <= sl) or (not is_bull and current_ltp >= sl):
        status = 'SL HIT'
        status_color = '#EF4444'
    elif trade.get('outcome') != 'OPEN':
        status = 'EXIT'
        status_color = '#6B7280'
    else:
        status = 'OPEN'
        status_color = '#F59E0B'
    
    return {
        'ltp': current_ltp,
        'mtm': round(mtm, 2),
        'risk_total': round(risk_total, 2),
        'reward_total': round(reward_total, 2),
        'current_rr': round(current_rr, 2),
        'days': days,
        'status': status,
        'status_color': status_color
    }
