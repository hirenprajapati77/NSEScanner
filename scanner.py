"""
NSE Camarilla Volume Scanner
=============================
Uses Yahoo Finance (5-year data) to scan NSE stocks for:
  - Volume spike above N-day average
  - Price above EMA 10, 20, 50, 200
  - Camarilla pivot levels → Entry, Stop Loss, Target

Alerts via Telegram Bot and/or WhatsApp (Twilio)
"""

import os
import time
import logging
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("NSEScanner")

# ─────────────────────────────────────────────
# CONFIG  (override via .env or edit here)
# ─────────────────────────────────────────────
CFG = {
    # Volume: today's vol must be >= VOL_MULT × VOL_DAYS avg
    "VOL_DAYS":  int(os.getenv("VOL_DAYS",  "10")),
    "VOL_MULT":  float(os.getenv("VOL_MULT", "2.0")),

    # EMA filters — set to False to disable a particular EMA check
    "EMA_10":  os.getenv("EMA_10",  "true").lower() == "true",
    "EMA_20":  os.getenv("EMA_20",  "true").lower() == "true",
    "EMA_50":  os.getenv("EMA_50",  "true").lower() == "true",
    "EMA_200": os.getenv("EMA_200", "true").lower() == "true",

    # Minimum Camarilla score  (0–100, based on price position vs pivots)
    "MIN_SCORE": int(os.getenv("MIN_SCORE", "55")),

    # Telegram
    "TG_TOKEN":   os.getenv("TG_TOKEN",   ""),    # Bot token from @BotFather
    "TG_CHAT_ID": os.getenv("TG_CHAT_ID", ""),    # Chat/group/channel ID

    # WhatsApp via Twilio
    "TWILIO_SID":   os.getenv("TWILIO_SID",   ""),
    "TWILIO_TOKEN": os.getenv("TWILIO_TOKEN", ""),
    "TWILIO_FROM":  os.getenv("TWILIO_FROM",  "whatsapp:+14155238886"),  # Twilio sandbox
    "TWILIO_TO":    os.getenv("TWILIO_TO",    ""),   # your number e.g. whatsapp:+919876543210

    # Scan interval in seconds (for continuous mode)
    "INTERVAL": int(os.getenv("SCAN_INTERVAL", "300")),  # 5 min
}

# ─────────────────────────────────────────────
# NSE STOCK UNIVERSE  (add/remove as needed)
# ─────────────────────────────────────────────
NIFTY500_SAMPLE = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "HINDUNILVR.NS", "ITC.NS", "SBIN.NS", "BHARTIARTL.NS", "KOTAKBANK.NS",
    "LT.NS", "AXISBANK.NS", "ASIANPAINT.NS", "MARUTI.NS", "SUNPHARMA.NS",
    "TITAN.NS", "BAJFINANCE.NS", "WIPRO.NS", "NESTLEIND.NS", "ULTRACEMCO.NS",
    "APOLLOHOSP.NS", "TECHM.NS", "HCLTECH.NS", "POWERGRID.NS", "NTPC.NS",
    "TATAMOTORS.NS", "ONGC.NS", "JSWSTEEL.NS", "TATASTEEL.NS", "BAJAJFINSV.NS",
    "DIVISLAB.NS", "DRREDDY.NS", "CIPLA.NS", "EICHERMOT.NS", "HEROMOTOCO.NS",
    "GRASIM.NS", "BPCL.NS", "COALINDIA.NS", "INDUSINDBK.NS", "ADANIPORTS.NS",
    "DABUR.NS", "MARICO.NS", "PIDILITIND.NS", "BERGEPAINT.NS", "HAVELLS.NS",
    "TATACONSUM.NS", "GODREJCP.NS", "MUTHOOTFIN.NS", "CHOLAFIN.NS", "SRF.NS",
    "AARTIIND.NS", "ABCAPITAL.NS", "ACC.NS", "AIAENG.NS", "ALKEM.NS",
    "AMBUJACEM.NS", "APLLTD.NS", "AUBANK.NS", "BALKRISIND.NS", "BANDHANBNK.NS",
    "BEL.NS", "BHARATFORG.NS", "BIOCON.NS", "CANBK.NS", "CESC.NS",
    "CROMPTON.NS", "CUB.NS", "DEEPAKNTR.NS", "DELTACORP.NS", "ESCORTS.NS",
    "FEDERALBNK.NS", "GAIL.NS", "GMRINFRA.NS", "GNFC.NS", "GODREJPROP.NS",
    "GRANULES.NS", "GSPL.NS", "HAPPSTMNDS.NS", "HINDPETRO.NS", "HINDCOPPER.NS",
    "IBULHSGFIN.NS", "IDFCFIRSTB.NS", "IEX.NS", "IPCALAB.NS", "IRCTC.NS",
    "JINDALSTEL.NS", "JUBLFOOD.NS", "KANSAINER.NS", "LALPATHLAB.NS", "LTIM.NS",
    "LUPIN.NS", "M&M.NS", "MANAPPURAM.NS", "MFSL.NS", "MINDTREE.NS",
    "MOTHERSON.NS", "MPHASIS.NS", "MRF.NS", "NMDC.NS", "OBEROIRLTY.NS",
    "OFSS.NS", "PAGEIND.NS", "PEL.NS", "PERSISTENT.NS", "PETRONET.NS",
    "PFC.NS", "POLYMED.NS", "PVR.NS", "RAMCOCEM.NS", "RBLBANK.NS",
    "RECLTD.NS", "SAIL.NS", "SAREGAMA.NS", "SHREECEM.NS", "SIEMENS.NS",
    "STAR.NS", "SUPREMEIND.NS", "SYNGENE.NS", "TEJASNET.NS", "TORNTPHARM.NS",
    "TRENT.NS", "TTKPRESTIG.NS", "UBL.NS", "VEDL.NS", "VOLTAS.NS",
    "ZOMATO.NS", "ZYDUSLIFE.NS",
]


# ─────────────────────────────────────────────
# CAMARILLA PIVOT CALCULATION
# ─────────────────────────────────────────────
def camarilla_levels(high: float, low: float, close: float) -> dict:
    """
    Camarilla Pivots (8-level)
    Key levels:
        H3 / L3 → standard intraday support/resistance
        H4 / L4 → breakout levels (strong signals)
    """
    rng = high - low
    pivot = (high + low + close) / 3
    return {
        "pivot": round(pivot, 2),
        "H1":    round(close + rng * 1.1 / 12, 2),
        "H2":    round(close + rng * 1.1 / 6,  2),
        "H3":    round(close + rng * 1.1 / 4,  2),  # Target
        "H4":    round(close + rng * 1.1 / 2,  2),  # Breakout target
        "L1":    round(close - rng * 1.1 / 12, 2),
        "L2":    round(close - rng * 1.1 / 6,  2),
        "L3":    round(close - rng * 1.1 / 4,  2),  # Stop loss
        "L4":    round(close - rng * 1.1 / 2,  2),  # Hard stop
    }


# ─────────────────────────────────────────────
# SCORE CALCULATION  (0–100)
# ─────────────────────────────────────────────
def compute_score(row: dict) -> int:
    score = 50  # base

    # +10 for each EMA passed
    for ema in ["ema10", "ema20", "ema50", "ema200"]:
        if row.get(ema + "_pass"):
            score += 5

    # Volume bonus
    vol_ratio = row.get("vol_ratio", 1)
    if vol_ratio >= 3:
        score += 15
    elif vol_ratio >= 2:
        score += 10
    elif vol_ratio >= 1.5:
        score += 5

    # Candle bonus
    if row.get("candle") == "Bull":
        score += 5

    # Distance from HV Low (lower = better entry)
    pct_above = row.get("pct_above", 100)
    if pct_above < 5:
        score += 5
    elif pct_above > 20:
        score -= 10

    return min(100, max(0, score))


# ─────────────────────────────────────────────
# FETCH & ANALYSE ONE STOCK
# ─────────────────────────────────────────────
def analyse(ticker: str) -> dict | None:
    try:
        vol_days = CFG["VOL_DAYS"]
        min_required_len = max(210, vol_days + 10)
        
        # Configure a browser-spoofed requests session to prevent yfinance cloud blocks on Render
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9"
        })
        
        df = yf.download(ticker, period="5y", interval="1d", progress=False, auto_adjust=True, session=session)
        if df is None or df.empty or len(df) < min_required_len:
            return None

        df.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in df.columns]
        df.dropna(inplace=True)

        # ── EMAs ──────────────────────────────
        df["ema10"]  = df["close"].ewm(span=10, adjust=False).mean()
        df["ema20"]  = df["close"].ewm(span=20, adjust=False).mean()
        df["ema50"]  = df["close"].ewm(span=50, adjust=False).mean()
        df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()

        # ── Volume N-day average ───────────────
        vol_days = CFG["VOL_DAYS"]
        df["vol_avg"] = df["volume"].rolling(vol_days).mean().shift(1)

        last = df.iloc[-1]
        prev = df.iloc[-2]

        close  = float(last["close"])
        high   = float(last["high"])
        low    = float(last["low"])
        volume = float(last["volume"])
        vol_avg = float(last["vol_avg"]) if not pd.isna(last["vol_avg"]) else 0

        if vol_avg == 0:
            return None

        vol_ratio = volume / vol_avg

        # ── Volume filter ──────────────────────
        if vol_ratio < CFG["VOL_MULT"]:
            return None

        # ── EMA filters ───────────────────────
        ema_checks = {
            "ema10_pass":  (not CFG["EMA_10"])  or (close > float(last["ema10"])),
            "ema20_pass":  (not CFG["EMA_20"])  or (close > float(last["ema20"])),
            "ema50_pass":  (not CFG["EMA_50"])  or (close > float(last["ema50"])),
            "ema200_pass": (not CFG["EMA_200"]) or (close > float(last["ema200"])),
        }
        if not all(ema_checks.values()):
            return None

        # ── Camarilla ─────────────────────────
        # Optimized 52-week High/Low and Date lookup
        last_252 = df.iloc[-252:]
        hv_high = float(last_252["high"].max())
        hv_low  = float(last_252["low"].min())
        hv_high_idx = last_252["high"].idxmax()
        hv_date = hv_high_idx.strftime("%d-%b-%Y")
        days_since_high = int((df.index[-1] - hv_high_idx).days)

        cam = camarilla_levels(hv_high, hv_low, close)

        # ── Candle pattern (simple) ────────────
        body = close - float(last["open"])
        candle = "Bull" if body > 0 else "Bear"

        # ── % above HV low ────────────────────
        pct_above = round((close - hv_low) / hv_low * 100, 2) if hv_low else 0
        upside    = round((cam["H3"] - close) / close * 100, 2)

        result = {
            "symbol":    ticker.replace(".NS", ""),
            "price":     round(close, 2),
            "hv_high":   round(hv_high, 2),
            "hv_low":    round(hv_low, 2),
            "hv_date":   hv_date,
            "days":      days_since_high,
            "pct_above": pct_above,
            "upside":    upside,
            "stop_loss": cam["L3"],
            "target":    cam["H3"],
            "entry":     cam["pivot"],
            "cam":       cam,
            "candle":    candle,
            "vol_ratio": round(vol_ratio, 2),
            "volume":    int(volume),
            "vol_avg":   int(vol_avg),
            "ema10":     round(float(last["ema10"]),  2),
            "ema20":     round(float(last["ema20"]),  2),
            "ema50":     round(float(last["ema50"]),  2),
            "ema200":    round(float(last["ema200"]), 2),
            "scanned_date": df.index[-1].strftime("%d-%b-%Y"),
            **ema_checks,
        }
        result["score"] = compute_score(result)
        if result["score"] < CFG["MIN_SCORE"]:
            return None
        return result

    except Exception as e:
        log.debug(f"{ticker}: {e}")
        return None


# ─────────────────────────────────────────────
# SCAN ALL STOCKS
# ─────────────────────────────────────────────
def run_scan(tickers: list[str], progress_cb=None) -> list[dict]:
    results = []
    total = len(tickers)
    for i, ticker in enumerate(tickers, 1):
        log.info(f"[{i}/{total}] Scanning {ticker}")
        if progress_cb:
            try:
                progress_cb(i, total, ticker)
            except Exception:
                pass
        r = analyse(ticker)
        if r:
            results.append(r)
            log.info(f"  ✅ SIGNAL  {r['symbol']}  price={r['price']}  "
                     f"score={r['score']}  vol={r['vol_ratio']}x  candle={r['candle']}")
        time.sleep(0.3)   # be polite to Yahoo Finance
    results.sort(key=lambda x: x["score"], reverse=True)
    return results


# ─────────────────────────────────────────────
# DISPLAY TABLE
# ─────────────────────────────────────────────
def print_table(signals: list[dict]):
    if not signals:
        print("\n  No signals found with current filters.\n")
        return

    h = f"{'SYMBOL':<14}{'SCORE':>6}  {'PRICE':>10}  {'ENTRY':>10}  {'TARGET':>10}  {'STOPLOSS':>10}  {'UPSIDE':>7}  {'VOL':>5}  {'CANDLE'}"
    print("\n" + "=" * 95)
    print("  NSE CAMARILLA VOLUME SCANNER  —  " + datetime.now().strftime("%d %b %Y %H:%M"))
    print("  Filters: EMA " +
          " | ".join([f"{'✓' if CFG[k] else '✗'}{k[4:]}" for k in ['EMA_10','EMA_20','EMA_50','EMA_200']]) +
          f"  |  Vol > {CFG['VOL_MULT']}x {CFG['VOL_DAYS']}-day avg")
    print("=" * 95)
    print(h)
    print("-" * 95)
    for r in signals:
        print(f"  {r['symbol']:<12}  {r['score']:>3}/100  "
              f"₹{r['price']:>9,.2f}  ₹{r['entry']:>9,.2f}  "
              f"₹{r['target']:>9,.2f}  ₹{r['stop_loss']:>9,.2f}  "
              f"{r['upside']:>+6.1f}%  {r['vol_ratio']:>4.1f}x  {r['candle']}")
    print("=" * 95)
    print(f"  {len(signals)} signal(s) found\n")


# ─────────────────────────────────────────────
# TELEGRAM ALERT
# ─────────────────────────────────────────────
def send_telegram(signals: list[dict]):
    token   = CFG["TG_TOKEN"]
    chat_id = CFG["TG_CHAT_ID"]
    if not token or not chat_id:
        log.warning("Telegram not configured — skipping.")
        return

    now = datetime.now().strftime("%d %b %Y %H:%M")
    lines = [f"📊 *NSE Camarilla Scanner* — {now}",
             f"🔍 Vol > {CFG['VOL_MULT']}x | EMA filters ON\n"]

    for r in signals[:15]:   # Telegram has message length limits
        ema_ok = "✅" if all([r["ema10_pass"], r["ema20_pass"], r["ema50_pass"], r["ema200_pass"]]) else "⚠️"
        candle = "🟢" if r["candle"] == "Bull" else "🔴"
        lines.append(
            f"{candle} *{r['symbol']}*  Score: {r['score']}/100\n"
            f"  Price: ₹{r['price']:,.2f}  {ema_ok} EMA\n"
            f"  Entry: ₹{r['entry']:,.2f}  |  Target: ₹{r['target']:,.2f}  |  SL: ₹{r['stop_loss']:,.2f}\n"
            f"  Upside: {r['upside']:+.1f}%  |  Vol: {r['vol_ratio']:.1f}x avg\n"
        )

    if len(signals) > 15:
        lines.append(f"_... and {len(signals)-15} more signals_")

    msg = "\n".join(lines)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}, timeout=10)
        if resp.ok:
            log.info("✅ Telegram alert sent")
        else:
            log.error(f"Telegram error: {resp.text}")
    except Exception as e:
        log.error(f"Telegram exception: {e}")


# ─────────────────────────────────────────────
# WHATSAPP ALERT  (via Twilio)
# ─────────────────────────────────────────────
def send_whatsapp(signals: list[dict]):
    sid   = CFG["TWILIO_SID"]
    token = CFG["TWILIO_TOKEN"]
    from_ = CFG["TWILIO_FROM"]
    to    = CFG["TWILIO_TO"]
    if not all([sid, token, to]):
        log.warning("WhatsApp (Twilio) not configured — skipping.")
        return

    now = datetime.now().strftime("%d %b %Y %H:%M")
    lines = [f"📊 NSE Scanner — {now}", f"Vol>{CFG['VOL_MULT']}x | EMA ON\n"]
    for r in signals[:10]:
        lines.append(
            f"{'🟢' if r['candle']=='Bull' else '🔴'} {r['symbol']} ({r['score']}/100)\n"
            f"  Entry ₹{r['entry']:,.2f} | Target ₹{r['target']:,.2f} | SL ₹{r['stop_loss']:,.2f}\n"
            f"  Vol: {r['vol_ratio']:.1f}x | {r['candle']}\n"
        )
    msg = "\n".join(lines)

    url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
    try:
        resp = requests.post(url, data={"From": from_, "To": to, "Body": msg},
                             auth=(sid, token), timeout=10)
        if resp.status_code in (200, 201):
            log.info("✅ WhatsApp alert sent")
        else:
            log.error(f"WhatsApp error: {resp.text}")
    except Exception as e:
        log.error(f"WhatsApp exception: {e}")


# ─────────────────────────────────────────────
# SAVE TO CSV
# ─────────────────────────────────────────────
def save_csv(signals: list[dict]):
    if not signals:
        return
    rows = []
    for r in signals:
        rows.append({
            "Symbol":    r["symbol"],
            "Score":     r["score"],
            "Price":     r["price"],
            "Entry":     r["entry"],
            "Target_H3": r["target"],
            "StopLoss_L3": r["stop_loss"],
            "Upside_%":  r["upside"],
            "Vol_Ratio": r["vol_ratio"],
            "Candle":    r["candle"],
            "EMA10":     r["ema10"],
            "EMA20":     r["ema20"],
            "EMA50":     r["ema50"],
            "EMA200":    r["ema200"],
            "HV_High":   r["hv_high"],
            "HV_Low":    r["hv_low"],
            "Scanned_At": datetime.now().strftime("%Y-%m-%d %H:%M"),
        })
    fname = f"scan_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    pd.DataFrame(rows).to_csv(fname, index=False)
    log.info(f"📁 Results saved to {fname}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main(continuous: bool = False, custom_tickers: list[str] | None = None):
    tickers = custom_tickers or NIFTY500_SAMPLE
    while True:
        log.info(f"🔎 Starting scan — {len(tickers)} stocks")
        signals = run_scan(tickers)
        print_table(signals)
        save_csv(signals)

        if signals:
            send_telegram(signals)
            send_whatsapp(signals)

        if not continuous:
            break

        log.info(f"⏳ Next scan in {CFG['INTERVAL']}s — press Ctrl+C to stop")
        time.sleep(CFG["INTERVAL"])


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="NSE Camarilla Volume Scanner")
    parser.add_argument("--continuous", action="store_true", help="Run continuously on interval")
    parser.add_argument("--tickers",    nargs="*",           help="Custom ticker list e.g. RELIANCE.NS TCS.NS")
    parser.add_argument("--vol-days",   type=int,            help="Volume average days (default 10)")
    parser.add_argument("--vol-mult",   type=float,          help="Volume multiplier (default 2.0)")
    args = parser.parse_args()

    if args.vol_days: CFG["VOL_DAYS"] = args.vol_days
    if args.vol_mult: CFG["VOL_MULT"] = args.vol_mult

    main(continuous=args.continuous, custom_tickers=args.tickers)
