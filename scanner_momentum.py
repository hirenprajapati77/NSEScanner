# scanner_momentum.py — UPGRADE existing buy_condition()

def check_buy_signal(stock_data, market_regime):
    """
    Strict BUY conditions — all must pass
    """
    d = stock_data
    reasons_pass = []
    reasons_fail = []
    
    # CONDITION 1: Market regime — NOTE: hard BEAR block removed (FIX E)
    # Individual gates in scanner.py (RSI floor, vol gate, risk cap) already handle
    # bear-market signal quality. A blanket block here prevents all after-hours setups.
    regime = market_regime.get('regime', 'NEUTRAL') if isinstance(market_regime, dict) else market_regime
    # Log regime for transparency but do not block outright
    if regime in ('BEAR', 'STRONG_BEAR'):
        reasons_pass.append(f"Bear regime noted ({regime}) — individual filters apply")
    
    # CONDITION 2: Camarilla H4 breakout
    h4 = d.get('camarilla_h4', 0)
    close = d.get('close', 0)
    if h4 > 0 and close > h4 * 1.001:  # 0.1% buffer above H4
        reasons_pass.append(f"H4 breakout ✓ ({close:.1f} > {h4:.1f})")
    else:
        reasons_fail.append(f"No H4 breakout ({close:.1f} vs {h4:.1f})")
    
    # CONDITION 3: EMA alignment (dynamic based on enabled EMAs)
    enabled_emas = []
    if d.get("ema10_on", True):
        enabled_emas.append((10, d.get("ema10", 0)))
    if d.get("ema20_on", True):
        enabled_emas.append((20, d.get("ema20", 0)))
    if d.get("ema50_on", True):
        enabled_emas.append((50, d.get("ema50", 0)))
    if d.get("ema200_on", True):
        enabled_emas.append((200, d.get("ema200", 0)))

    ema_aligned = True
    for i in range(len(enabled_emas) - 1):
        if enabled_emas[i][1] <= enabled_emas[i+1][1]:
            ema_aligned = False
            break

    close_above_enabled = True
    for span, val in enabled_emas:
        if close <= val:
            close_above_enabled = False
            break

    if ema_aligned and close_above_enabled:
        label = ">".join(str(span) for span, val in enabled_emas)
        reasons_pass.append(f"EMA alignment ✓ ({label if label else 'None'}, close>enabled EMAs)")
    else:
        if not ema_aligned:
            vals_str = "/".join(f"{val:.1f}" for span, val in enabled_emas)
            reasons_fail.append(f"EMA misaligned ({vals_str})")
        if not close_above_enabled:
            failed_spans = [str(span) for span, val in enabled_emas if close <= val]
            reasons_fail.append(f"Close below enabled EMA ({', '.join(failed_spans)})")
    
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
    rr = min(99.0, rr)
    
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
