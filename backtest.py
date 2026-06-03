# backtest.py — NEW module

import pandas as pd
import numpy as np
import yfinance as yf

def run_backtest(symbol, years=1, strategy='camarilla_h4'):
    """
    Backtest Camarilla H4 breakout strategy
    """
    period_map = {1: '1y', 3: '3y', 5: '5y'}
    clean_symbol = symbol.replace(".NS", "")
    
    try:
        ticker = yf.Ticker(clean_symbol + ".NS")
        hist = ticker.history(
            period=period_map.get(years, '1y'),
            interval='1d',
            auto_adjust=True
        )
    except Exception:
        return None
    
    if len(hist) < 20:
        return None
    
    trades = []
    
    for i in range(5, len(hist)):
        today = hist.iloc[i]
        
        # Calculate Camarilla for today using yesterday's OHLC
        prev = hist.iloc[i-1]
        h4 = prev['Close'] + 1.1/12 * (prev['High'] - prev['Low'])
        l3 = prev['Close'] - 1.1/12 * (prev['High'] - prev['Low'])
        h6 = (prev['High'] / prev['Low']) * prev['Close']
        
        # Entry: today's open near H4
        entry = today['Open']
        
        if entry > h4 * 0.998 and entry < h4 * 1.02:
            sl = l3
            t1 = h6
            
            if sl >= entry or t1 <= entry:
                continue
            
            rr = (t1 - entry) / (entry - sl)
            if rr < 1.5:
                continue
            
            # Simulate trade
            hit_target = today['High'] >= t1
            hit_sl = today['Low'] <= sl
            
            if hit_target and not hit_sl:
                result = 'WIN'
                pnl_pct = (t1 - entry) / entry * 100
            elif hit_sl:
                result = 'LOSS'
                pnl_pct = (sl - entry) / entry * 100
            else:
                result = 'OPEN'
                pnl_pct = (today['Close'] - entry) / entry * 100
            
            trades.append({
                'date': today.name.strftime('%Y-%m-%d') if hasattr(today.name, 'strftime') else str(today.name),
                'symbol': clean_symbol,
                'entry': round(entry, 2),
                'sl': round(sl, 2),
                't1': round(t1, 2),
                'rr': round(rr, 2),
                'result': result,
                'pnl_pct': round(pnl_pct, 2)
            })
    
    if not trades:
        return None
    
    df = pd.DataFrame(trades)
    wins = df[df['result'] == 'WIN']
    
    win_rate = len(wins) / len(df) * 100 if len(df) > 0 else 0.0
    total_pnl = df['pnl_pct'].sum()
    
    # Sharpe ratio (simplified)
    returns = df['pnl_pct'].values
    sharpe = (np.mean(returns) / np.std(returns) * np.sqrt(252)) if np.std(returns) > 0 else 0.0
    
    # Max drawdown
    cumulative = (1 + df['pnl_pct'] / 100).cumprod()
    rolling_max = cumulative.cummax()
    drawdown = ((cumulative - rolling_max) / rolling_max * 100).min() if len(cumulative) > 0 else 0.0
    if pd.isna(drawdown):
        drawdown = 0.0
        
    # CAGR
    cagr = 0.0
    if years > 0 and total_pnl > -100:
        cagr = ((1 + total_pnl/100) ** (1/years) - 1) * 100
    
    return {
        'symbol': clean_symbol,
        'period': f'{years}Y',
        'total_trades': len(df),
        'win_rate': round(win_rate, 1),
        'total_pnl_pct': round(total_pnl, 2),
        'max_drawdown_pct': round(drawdown, 2),
        'sharpe_ratio': round(sharpe, 2),
        'cagr_pct': round(cagr, 2),
        'avg_rr': round(df['rr'].mean(), 2) if len(df) > 0 else 0.0,
        'trades': df.to_dict('records')
    }
