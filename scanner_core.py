# scanner_core.py — UPGRADE existing scan_stocks()

import concurrent.futures
import yfinance as yf
import time
import numpy as np
import pandas as pd
from datetime import datetime
from sector_rotation import get_sector_performance_multi, STOCK_SECTOR_MAP

# Cache sector data separately (refreshes every 5 min)
_sector_cache = {}
_sector_cache_time = 0

def get_sector_data_cached():
    global _sector_cache, _sector_cache_time
    if time.time() - _sector_cache_time > 300:  # 5 min cache
        try:
            _sector_cache = get_sector_performance_multi(['1d'])
            _sector_cache_time = time.time()
        except Exception:
            _sector_cache = {'1d': {}}
    return _sector_cache

def fetch_single_stock(symbol):
    """Fetch + process one stock — called in parallel"""
    try:
        clean_symbol = symbol.replace(".NS", "")
        ticker = yf.Ticker(clean_symbol + ".NS")
        
        # Use fast_info for current price (no cache)
        fast = ticker.fast_info
        ltp = fast.get('last_price', 0)
        prev_close = fast.get('previous_close', 0)
        year_high = fast.get('year_high', 0)
        year_low = fast.get('year_low', 0)
        
        if not ltp or ltp == 0:
            return None
        
        # History for indicators (15-min cache OK)
        hist = ticker.history(
            period="60d", 
            interval="1h",
            auto_adjust=True
        )
        
        if len(hist) < 20:
            return None
        
        # Calculate indicators
        close_series = hist['Close']
        volume_series = hist['Volume']
        
        ema20 = close_series.ewm(span=20).mean().iloc[-1]
        ema50 = close_series.ewm(span=50).mean().iloc[-1]
        ema200 = close_series.ewm(span=200).mean().iloc[-1] if len(close_series) >= 200 else close_series.mean()
        
        avg_vol = volume_series.iloc[-20:-1].mean()
        curr_vol = volume_series.iloc[-1]
        rvol = curr_vol / avg_vol if avg_vol > 0 else 1.0
        
        change_pct = ((ltp - prev_close) / prev_close * 100) if prev_close > 0 else 0
        
        # Calculate 14-day daily ATR using the hourly history as fallback/estimate or download daily data for ATR
        highs = hist['High']
        lows = hist['Low']
        closes_prev = hist['Close'].shift(1)
        tr1 = highs - lows
        tr2 = (highs - closes_prev).abs()
        tr3 = (lows - closes_prev).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=14).mean().iloc[-1]
        if pd.isna(atr) or atr == 0:
            atr = ltp * 0.02
        atr_pct = (atr / ltp) * 100
        
        # 52-week High/Low Range Camarilla pivots
        hv_high = year_high if year_high and year_high > 0 else highs.max()
        hv_low = year_low if year_low and year_low > 0 else lows.min()
        range_52w = hv_high - hv_low
        if range_52w <= 0:
            range_52w = ltp * 0.1
            
        camarilla_h4 = ltp + range_52w * 1.1 / 2
        camarilla_l3 = ltp - range_52w * 1.1 / 4
        camarilla_l4 = ltp - range_52w * 1.1 / 2
        camarilla_h5 = (hv_high / hv_low) * ltp if hv_low > 0 else ltp * 1.05
        camarilla_h6 = camarilla_h5 + 1.168 * (camarilla_h5 - camarilla_h4)
        
        # Resolve sector mapping
        sector = STOCK_SECTOR_MAP.get(clean_symbol, "General Equity")
        
        # Distance from entry
        trigger = camarilla_h4
        dist_pct = abs((ltp - trigger) / trigger * 100) if trigger > 0 else 0.0
        
        return {
            'symbol': clean_symbol,
            'ltp': round(ltp, 2),
            'price': round(ltp, 2),
            'close': round(ltp, 2),
            'change_pct': round(change_pct, 2),
            'change': round(change_pct, 2),
            'ema20': round(ema20, 2),
            'ema50': round(ema50, 2),
            'ema200': round(ema200, 2),
            'rvol': round(rvol, 2),
            'volume': int(curr_vol),
            'atr': round(atr, 2),
            'atr_pct': round(atr_pct, 2),
            'camarilla_h4': round(camarilla_h4, 2),
            'camarilla_l3': round(camarilla_l3, 2),
            'camarilla_l4': round(camarilla_l4, 2),
            'camarilla_h5': round(camarilla_h5, 2),
            'camarilla_h6': round(camarilla_h6, 2),
            'hv_high': round(hv_high, 2),
            'hv_low': round(hv_low, 2),
            'sector': sector,
            'dist_pct': round(dist_pct, 2),
            'scan_time': datetime.now()
        }
    except Exception as e:
        return None

def scan_nifty500_parallel(symbol_list, max_workers=50):
    """
    Scan 500 stocks in parallel
    Target: < 60 seconds for full Nifty500
    """
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {
            executor.submit(fetch_single_stock, sym): sym 
            for sym in symbol_list
        }
        for future in concurrent.futures.as_completed(future_to_symbol, timeout=55):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception:
                pass
    return results
