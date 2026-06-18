# data_fetcher.py — Primary + Fallback logic

import yfinance as yf
import requests
import json
import time

# In-memory cache for last known prices
_price_cache = {}

# In-memory log of recent fetch attempts (timestamp, success: bool) to track stats over last 15 minutes
_fetch_history = []

def _prune_fetch_history():
    now = time.time()
    global _fetch_history
    _fetch_history = [item for item in _fetch_history if now - item[0] <= 900.0]

def get_fetch_stats():
    _prune_fetch_history()
    successes = sum(1 for item in _fetch_history if item[1])
    failures = sum(1 for item in _fetch_history if not item[1])
    return successes, failures

def get_stock_price(symbol):
    """
    Try: Cache (short TTL) → Yahoo (with retry) → NSE API → Cache (Fallback)
    Returns a 3-tuple: (price, source, status)
    status is one of: 'live', 'cached', 'unavailable'
    """
    clean_symbol = symbol.replace(".NS", "").strip().upper()
    
    # 1. First-pass check: Cache hit within short TTL (30s)
    if clean_symbol in _price_cache:
        cached = _price_cache[clean_symbol]
        age = time.time() - cached['time']
        if age <= 30.0:
            return cached['price'], cached['source'], 'live'
            
    # 2. PRIMARY: Yahoo Finance fast_info with retry/backoff
    price = None
    yahoo_success = False
    for attempt in range(2):
        try:
            ticker = yf.Ticker(clean_symbol + ".NS")
            price = ticker.fast_info.get('lastPrice') or ticker.fast_info.get('last_price')
            if price and price > 0:
                yahoo_success = True
                break
        except Exception:
            pass
        if attempt < 1:
            time.sleep(0.5)
            
    if yahoo_success and price:
        _price_cache[clean_symbol] = {'price': price, 'time': time.time(), 'source': 'yahoo'}
        _fetch_history.append((time.time(), True))
        return price, 'yahoo', 'live'
        
    # 3. FALLBACK 1: NSE unofficial API
    nse_success = False
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.nseindia.com'
        }
        # Warm up session cookies for nseindia.com to prevent blocking
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=5)
        url = f"https://www.nseindia.com/api/quote-equity?symbol={clean_symbol}"
        resp = session.get(url, headers=headers, timeout=5)
        data = resp.json()
        price = data['priceInfo']['lastPrice']
        if price and price > 0:
            nse_success = True
    except Exception:
        pass
        
    if nse_success and price:
        _price_cache[clean_symbol] = {'price': price, 'time': time.time(), 'source': 'nse'}
        _fetch_history.append((time.time(), True))
        return price, 'nse', 'live'
        
    # Record a fetch failure since both Yahoo and NSE network calls failed
    _fetch_history.append((time.time(), False))
    
    # 4. FALLBACK 2: Last known cache (stale)
    if clean_symbol in _price_cache:
        cached = _price_cache[clean_symbol]
        age_mins = (time.time() - cached['time']) / 60
        return cached['price'], f"cache ({age_mins:.0f}m old)", 'cached'
        
    return None, 'unavailable', 'unavailable'
