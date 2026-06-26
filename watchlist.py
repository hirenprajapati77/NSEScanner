# watchlist.py — add status logic

from datetime import datetime

def get_watchlist_status(stock, entry_price, sl, target, current_ltp, signal_type="Bull"):
    """
    TRIGGERED: LTP crossed entry level
    WAITING:   LTP not yet at entry
    EXPIRED:   Signal > 5 days old without trigger
    CLOSED:    Trade manually closed
    """
    # Parse added_date safely
    added_date = stock.get('added_date')
    if isinstance(added_date, str):
        try:
            # Try full timestamp format first
            added_date = datetime.strptime(added_date, "%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                # Try short date format
                added_date = datetime.strptime(added_date, "%Y-%m-%d")
            except Exception:
                added_date = datetime.now()
    elif not isinstance(added_date, datetime):
        added_date = datetime.now()
        
    signal_age_days = (datetime.now() - added_date).days
    
    if stock.get('manually_closed') or stock.get('status') == 'CLOSED':
        status = 'CLOSED'
        color = '#6B7280'
    elif signal_age_days > 5:
        status = 'EXPIRED'
        color = '#EF4444'
    elif (signal_type == "Bull" and current_ltp >= entry_price) or (signal_type == "Bear" and current_ltp <= entry_price and current_ltp > 0):
        status = 'TRIGGERED'
        color = '#10B981'
    else:
        status = 'WAITING'
        color = '#F59E0B'
    
    if signal_type == "Bull":
        risk = entry_price - sl
        reward = target - entry_price
        current_rr = (current_ltp - entry_price) / risk if risk > 0 else 0.0
        dist = round((entry_price - current_ltp) / current_ltp * 100, 2) if current_ltp > 0 else 0.0
    else:
        risk = sl - entry_price
        reward = entry_price - target
        current_rr = (entry_price - current_ltp) / risk if risk > 0 else 0.0
        dist = round((current_ltp - entry_price) / entry_price * 100, 2) if entry_price > 0 else 0.0
    
    return {
        'status': status,
        'color': color,
        'entry': entry_price,
        'sl': sl,
        'target': target,
        'current_ltp': current_ltp,
        'current_rr': round(current_rr, 2),
        'dist_to_entry': dist
    }
