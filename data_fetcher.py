# data_fetcher.py — Primary + Fallback logic

import yfinance as yf
import requests
import json
import time

# In-memory cache for last known prices
_price_cache = {}

def get_stock_price(symbol):
    """
    Try: Yahoo → NSE API → Cache
    """
    # Remove any trailing .NS or leading indices formatting for clean queries if needed,
    # but the caller is expected to pass a clean symbol (e.g. "INFY"). We append ".NS" for Yahoo.
    clean_symbol = symbol.replace(".NS", "")
    
    # PRIMARY: Yahoo Finance fast_info
    try:
        ticker = yf.Ticker(clean_symbol + ".NS")
        price = ticker.fast_info['last_price']
        if price and price > 0:
            _price_cache[clean_symbol] = {'price': price, 'time': time.time(), 'source': 'yahoo'}
            return price, 'yahoo'
    except Exception:
        pass
    
    # FALLBACK 1: NSE unofficial API
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
        if price > 0:
            _price_cache[clean_symbol] = {'price': price, 'time': time.time(), 'source': 'nse'}
            return price, 'nse'
    except Exception:
        pass
    
    # FALLBACK 2: Last known cache
    if clean_symbol in _price_cache:
        cached = _price_cache[clean_symbol]
        age_mins = (time.time() - cached['time']) / 60
        return cached['price'], f'cache ({age_mins:.0f}m old)'
    
    return None, 'unavailable'
