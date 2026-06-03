# score_engine.py — UPGRADE existing score()

SCORE_WEIGHTS = {
    'volume':     30,   # RVOL ratio vs 10-day avg
    'ema':        25,   # EMA alignment quality
    'camarilla':  20,   # Distance from H4/L4
    'sector':     10,   # Sector momentum strength
    'market':     10,   # Market regime score
    'atr':         5,   # Volatility risk factor
}

def calculate_score(stock_data, sector_data, market_data):
    breakdown = {}
    
    # VOLUME (30 pts)
    rvol = stock_data.get('rvol', 1.0)
    vol_score = min(rvol / 3.0 * 30, 30)  # 3x RVOL = full 30 pts
    breakdown['volume'] = {
        'score': round(vol_score, 1),
        'max': 30,
        'detail': f"RVOL {rvol:.1f}x (need 3x for max)"
    }
    
    # EMA (25 pts)
    ema_score = 0
    if stock_data.get('ema20', 0) > stock_data.get('ema50', 0):   ema_score += 8
    if stock_data.get('ema50', 0) > stock_data.get('ema200', 0):  ema_score += 9
    if stock_data.get('close', 0) > stock_data.get('ema20', 0):   ema_score += 8
    # Close > ema200 bonus handled above
    breakdown['ema'] = {
        'score': round(ema_score, 1),
        'max': 25,
        'detail': f"EMA20:{stock_data.get('ema20', 0):.0f} EMA50:{stock_data.get('ema50', 0):.0f} EMA200:{stock_data.get('ema200', 0):.0f}"
    }
    
    # CAMARILLA (20 pts)
    dist = stock_data.get('dist_pct', 5.0)
    if dist <= 0.5:   cam_score = 20
    elif dist <= 1.0: cam_score = 16
    elif dist <= 2.0: cam_score = 10
    elif dist <= 5.0: cam_score = 4
    else:             cam_score = 0
    breakdown['camarilla'] = {
        'score': cam_score,
        'max': 20,
        'detail': f"{dist:.1f}% from trigger (0.5% = max score)"
    }
    
    # SECTOR (10 pts)
    sector_name = stock_data.get('sector', '')
    # Safe lookup for sector performance
    sector_chg = 0.0
    if isinstance(sector_data, dict):
        # Could be inside '1d' or flat dictionary
        if '1d' in sector_data and isinstance(sector_data['1d'], dict):
            sector_chg = sector_data['1d'].get(sector_name, 0.0)
        else:
            sector_chg = sector_data.get(sector_name, 0.0)
    
    sec_score = min(max((sector_chg + 3) / 6 * 10, 0), 10)
    breakdown['sector'] = {
        'score': round(sec_score, 1),
        'max': 10,
        'detail': f"{sector_name} {sector_chg:+.2f}%"
    }
    
    # MARKET (10 pts)
    mkt_score_raw = market_data.get('score', 50)
    mkt_score = mkt_score_raw / 100 * 10
    breakdown['market'] = {
        'score': round(mkt_score, 1),
        'max': 10,
        'detail': f"Regime: {market_data.get('regime')} ({mkt_score_raw:.0f}/100)"
    }
    
    # ATR (5 pts)
    atr_pct = stock_data.get('atr_pct', 2.0)
    atr_score = max(5 - atr_pct, 0)  # lower ATR = better score
    breakdown['atr'] = {
        'score': round(atr_score, 1),
        'max': 5,
        'detail': f"ATR {atr_pct:.2f}% (lower = less risky)"
    }
    
    total = sum(v['score'] for v in breakdown.values())
    
    return {
        'total': round(total, 1),
        'grade': 'A+' if total >= 90 else 'A' if total >= 80 else 'B' if total >= 70 else 'C',
        'breakdown': breakdown
    }
