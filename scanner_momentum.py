# scanner_momentum.py — UPGRADE existing buy_condition()

def check_buy_signal(stock_data, market_regime):
    """
    Strict BUY conditions — all must pass
    """
    d = stock_data
    reasons_pass = []
    reasons_fail = []
    
    # CONDITION 1: Market regime gate
    regime = market_regime.get('regime', 'NEUTRAL') if isinstance(market_regime, dict) else market_regime
    if regime == 'BEAR':
        return False, {
            "passed": [],
            "failed": ["BLOCKED: Bear market regime active"],
            "entry": 0.0,
            "sl": 0.0,
            "t1": 0.0,
            "rr": 0.0
        }
    
    # CONDITION 2: Camarilla H4 breakout
    h4 = d.get('camarilla_h4', 0)
    close = d.get('close', 0)
    if h4 > 0 and close > h4 * 1.001:  # 0.1% buffer above H4
        reasons_pass.append(f"H4 breakout ✓ ({close:.1f} > {h4:.1f})")
    else:
        reasons_fail.append(f"No H4 breakout ({close:.1f} vs {h4:.1f})")
    
    # CONDITION 3: EMA alignment
    ema20 = d.get('ema20', 0)
    ema50 = d.get('ema50', 0)
    ema200 = d.get('ema200', 0)
    ema_aligned = ema20 > ema50 > ema200
    close_above_ema20 = close > ema20
    
    if ema_aligned and close_above_ema20:
        reasons_pass.append("EMA alignment ✓ (20>50>200, close>EMA20)")
    else:
        if not ema_aligned:
            reasons_fail.append(f"EMA misaligned ({ema20:.1f}/{ema50:.1f}/{ema200:.1f})")
        if not close_above_ema20:
            reasons_fail.append(f"Close below EMA20 ({close:.1f} < {ema20:.1f})")
    
    # CONDITION 4: Relative Volume
    rvol = d.get('rvol', 0)
    if rvol >= 1.8:
        reasons_pass.append(f"RVOL ✓ ({rvol:.1f}x)")
    else:
        reasons_fail.append(f"Low RVOL ({rvol:.1f}x < 1.8x required)")
    
    # CONDITION 5: Risk/Reward
    entry = h4
    sl = d.get('camarilla_l3', close * 0.98)
    t1 = d.get('camarilla_h6', close * 1.03)
    risk = entry - sl
    reward = t1 - entry
    rr = reward / risk if risk > 0 else 0.0
    
    if rr >= 1.5:
        reasons_pass.append(f"RR ✓ ({rr:.1f}:1)")
    else:
        reasons_fail.append(f"Poor RR ({rr:.1f}:1 < 1.5 required)")
    
    # CONDITION 6: Distance from entry
    dist_pct = abs((close - entry) / entry * 100) if entry > 0 else 999.0
    if dist_pct <= 2.0:
        reasons_pass.append(f"Entry fresh ✓ ({dist_pct:.1f}% from trigger)")
    else:
        reasons_fail.append(f"Too extended ({dist_pct:.1f}% > 2% from trigger)")
    
    # ALL conditions must pass
    all_pass = len(reasons_fail) == 0
    
    explanation = {
        "passed": reasons_pass,
        "failed": reasons_fail,
        "entry": round(entry, 2),
        "sl": round(sl, 2),
        "t1": round(t1, 2),
        "rr": round(rr, 2)
    }
    
    return all_pass, explanation


def check_sell_signal(stock_data):
    """SELL: Close breaks below L4"""
    close = stock_data.get('close', 0)
    l4 = stock_data.get('camarilla_l4', 0)
    
    if l4 > 0 and close < l4:
        return True, f"L4 breakdown ({close:.1f} < L4 {l4:.1f})"
    return False, None
