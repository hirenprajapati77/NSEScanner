# sector_rotation.py — ADD multi-timeframe to existing

import yfinance as yf

SECTOR_TICKERS = {
    "Banking": "^NSEBANK",
    "IT": "^CNXIT",
    "Pharma": "^CNXPHARMA",
    "Auto": "^CNXAUTO",
    "FMCG": "^CNXFMCG",
    "Metals": "^CNXMETAL",
    "Energy": "^CNXENERGY",
    "Realty": "^CNXREALTY",
    "Infra": "^CNXINFRA",
    "Media": "^CNXMEDIA"
}

STOCK_SECTOR_MAP = {
    # Original mock stocks
    "HDFCBANK": "Banking", "SBIN": "Banking", "ICICIBANK": "Banking", "AXISBANK": "Banking", "KOTAKBANK": "Banking",
    "RELIANCE": "Energy", "COALINDIA": "Metals", "TATASTEEL": "Metals",
    "TMCV": "Auto", "MARUTI": "Auto",
    "INFY": "IT", "WIPRO": "IT", "TCS": "IT",
    "DRREDDY": "Pharma", "DLF": "Realty",
    "ADANIENT": "Infra", "ITC": "FMCG",
    
    # Real database scanned stocks
    "CHENNPETRO": "Energy", "ATGL": "Energy",
    "CUB": "Banking", "FEDERALBNK": "Banking", "RBLBANK": "Banking", "BANDHANBNK": "Banking",
    "BHARATFORG": "Auto", "MOTHERSON": "Auto",
    "OFSS": "IT", "ALKEM": "Pharma", "ZYDUSLIFE": "Pharma",
    "SUNTV": "Media", "PIDILITIND": "FMCG", "TATACHEM": "FMCG",
    "SOLARINDS": "Infra", "SIEMENS": "Infra", "LT": "Infra", "INDUSTOWER": "Infra"
}

def get_sector_performance_multi(periods=['1d', '5d', '20d']):
    """Fetch sector data for multiple periods"""
    results = {p: {} for p in periods}
    
    period_map = {'1d': '5d', '5d': '1mo', '20d': '3mo'}
    interval_map = {'1d': '5m', '5d': '1d', '20d': '1d'}
    
    for sector_name, ticker_sym in SECTOR_TICKERS.items():
        try:
            ticker = yf.Ticker(ticker_sym)
            
            for period_label in periods:
                yf_period = period_map[period_label]
                yf_interval = interval_map[period_label]
                
                hist = ticker.history(
                    period=yf_period, 
                    interval=yf_interval,
                    auto_adjust=True
                )
                
                if len(hist) >= 2:
                    if period_label == '1d':
                        # Use fast close difference or last two closing values
                        # yfinance close column might be a series or dataframe
                        close_col = hist['Close']
                        start_price = float(close_col.iloc[-2])
                        end_price = float(close_col.iloc[-1])
                    else:
                        close_col = hist['Close']
                        days = int(period_label.replace('d', ''))
                        start_price = float(close_col.iloc[-days]) if len(close_col) >= days else float(close_col.iloc[0])
                        end_price = float(close_col.iloc[-1])
                    
                    change_pct = ((end_price - start_price) / start_price) * 100
                    results[period_label][sector_name] = round(change_pct, 2)
                else:
                    results[period_label][sector_name] = 0.0
                    
        except Exception:
            for p in periods:
                if sector_name not in results[p]:
                    results[p][sector_name] = 0.0
    
    # Add ranking
    for period in periods:
        sorted_sectors = sorted(results[period].items(), 
                                key=lambda x: x[1], reverse=True)
        results[f'{period}_ranked'] = [s[0] for s in sorted_sectors]
        results[f'{period}_top3'] = [s[0] for s in sorted_sectors[:3]]
    
    return results
