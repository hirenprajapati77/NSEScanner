# ai_terminal.py — UPGRADE existing generate_intelligence()

def build_ai_prompt(scan_results, market_data, sector_data, fii_data):
    """Structured prompt for consistent output"""
    
    top_stock = scan_results[0] if scan_results else None
    
    prompt = f"""
You are the AI engine of ProTrader Terminal, an NSE intraday scanner.
Respond ONLY in this exact JSON format. No other text.

{{
  "market_status": "one of: Strongly Bearish | Bearish | Neutral | Bullish | Strongly Bullish",
  "market_reason": "one sentence max",
  "top_signal": {{
    "symbol": "stock symbol or null",
    "entry": price_number_or_null,
    "sl": price_number_or_null,
    "target": price_number_or_null,
    "confidence": percentage_0_to_100,
    "why": "one sentence why this stock",
    "risk_note": "one sentence risk warning"
  }},
  "next_action": "one specific actionable sentence for trader",
  "regime_note": "one sentence about current market regime"
}}

Market Data:
- Regime: {market_data.get('regime', 'NEUTRAL')} (Score: {market_data.get('score', 50)}/100)
- Nifty: {market_data.get('nifty_price', 0.0)}
- Breadth: {market_data.get('pct_above_ema50', 'N/A')}% above EMA50
- FII Net: ₹{fii_data.get('fii_net', 0)/100:.0f}Cr
- DII Net: ₹{fii_data.get('dii_net', 0)/100:.0f}Cr
- Top Sector: {max(sector_data, key=sector_data.get) if sector_data else 'N/A'}

Signals Found: {len(scan_results)} total
Top Stock: {top_stock['symbol'] if top_stock else 'None'}
Score: {top_stock['score'] if top_stock else 'N/A'}
Entry: {top_stock['entry'] if top_stock else 'N/A'}
SL: {top_stock['sl'] if top_stock else 'N/A'}
Target: {top_stock['t1'] if top_stock else 'N/A'}
RVOL: {top_stock['rvol'] if top_stock else 'N/A'}x
"""
    return prompt


def parse_ai_response(response_text):
    """Parse structured JSON response"""
    import json
    try:
        clean = response_text.strip()
        if '```' in clean:
            # Handle markdown code blocks
            parts = clean.split('```')
            for part in parts:
                if part.strip().startswith('{') or part.strip().startswith('json'):
                    clean = part.strip()
                    if clean.startswith('json'):
                        clean = clean[4:].strip()
                    break
        return json.loads(clean)
    except Exception:
        return {
            "market_status": "Unknown",
            "market_reason": "AI parsing error",
            "top_signal": None,
            "next_action": "Run live scan for fresh signals",
            "regime_note": "Check market manually"
        }
