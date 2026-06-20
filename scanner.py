"""
NSE Camarilla Volume Scanner — Robust v3.0 [Phase 2 Hardened]
============================================================
Improvements in Phase 2:
  • Critical Fix #1: Alert deduplication & cooldown engine using alert_history.json & SQLite tracker.
  • Critical Fix #2: Strict DataFrame validation layer (validate_dataframe) rejecting corrupt pricing, negative ranges, and NaNs.
  • Critical Fix #3: Dynamic ATR-based stop-loss cap (minimum 1.5 * ATR stop, maximum 5% entry risk threshold).
  • Critical Fix #4: Thread-safe deepcopy architecture across Flask state handlers.
  • High Priority #1: Average 20-day turnover liquidity filter (10 Crore INR threshold).
  • High Priority #2: 90-day stock return relative strength versus NIFTY index (^NSEI) return.
  • High Priority #3: Dynamic signal confidence grades (A+ to D) with strength levels.
  • High Priority #4: Backtesting SQLite database architecture logging metrics, alerts, and signal histories.
  • Performance system monitoring utilizing psutil logging metrics.
"""

import os
import io
import time
import json
import logging
import threading
import sqlite3
from datetime import datetime
from typing import Callable, Dict, List, Optional
import pandas as pd
import requests
import redis
import yfinance as yf
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from regime import get_regime, adjust_score_for_regime
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import psutil
except ImportError:
    psutil = None

load_dotenv()

# Initialize Redis
try:
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    redis_client.ping()  # test connection
except Exception as e:
    logging.getLogger("NSEScanner").warning(f"Redis not available: {e}")
    redis_client = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("NSEScanner")


# ─────────────────────────────────────────────────────────────
# CONFIG  (all values override-able via .env or cfg_override)
# ─────────────────────────────────────────────────────────────
CFG: Dict = {
    # Volume: today's vol must be >= VOL_MULT × VOL_DAYS-day average
    "VOL_DAYS":       int(os.getenv("VOL_DAYS",       "10")),
    "VOL_MULT":       float(os.getenv("VOL_MULT",     "1.5")),
    # EMA filters — set env to "false" to skip that particular EMA
    "EMA_10":         os.getenv("EMA_10",  "true").lower() == "true",
    "EMA_20":         os.getenv("EMA_20",  "true").lower() == "true",
    "EMA_50":         os.getenv("EMA_50",  "false").lower() == "true",
    "EMA_200":        os.getenv("EMA_200", "false").lower() == "true",
    # Minimum score to surface a signal
    "MIN_SCORE":      int(os.getenv("MIN_SCORE",      "0")),
    # 52-week lookback window (trading days)
    "HV_DAYS":        int(os.getenv("HV_DAYS",        "252")),
    # Concurrency
    "MAX_WORKERS":    int(os.getenv("MAX_WORKERS",    "25")),
    # Telegram
    "TG_TOKEN":       os.getenv("TG_TOKEN",       ""),
    "TG_CHAT_ID":     os.getenv("TG_CHAT_ID",     ""),
    # WhatsApp / Twilio
    "TWILIO_SID":     os.getenv("TWILIO_SID",     ""),
    "TWILIO_TOKEN":   os.getenv("TWILIO_TOKEN",   ""),
    "TWILIO_FROM":    os.getenv("TWILIO_FROM",    "whatsapp:+14155238886"),
    "TWILIO_TO":      os.getenv("TWILIO_TO",      ""),
    # Continuous scan interval (seconds)
    "INTERVAL":       int(os.getenv("SCAN_INTERVAL",  "300")),
    # Liquidity Filter (10 Crore INR default: 1 Crore = 10,000,000)
    "TURNOVER_LIMIT": float(os.getenv("TURNOVER_LIMIT", "100000000")),
    "USE_CACHE_ONLY": False,
    # default MIN_PRICE set to 50.0 INR and MAX_52W_AGE set to 180 days
    "MIN_PRICE":      float(os.getenv("MIN_PRICE",      "50.0")),
    "MAX_52W_AGE":    int(os.getenv("MAX_52W_AGE",    "180")),
    "STOCK_TIMEOUT":  int(os.getenv("STOCK_TIMEOUT",  "30")),
    "SCAN_DEADLINE":  int(os.getenv("SCAN_DEADLINE",  "600")),
    "MIN_TURNOVER_CR": float(os.getenv("MIN_TURNOVER_CR", "10.0")),
    "SCAN_MODE":      os.getenv("SCAN_MODE",          "both"),
}

def is_post_market_invalidation_window() -> bool:
    """Check if current time is in the post-market cache invalidation window (15:30 to 16:05 IST)."""
    try:
        from datetime import datetime, timezone, timedelta
        utc_now = datetime.now(timezone.utc)
        ist_now = utc_now + timedelta(hours=5, minutes=30)
        # Invalidation window is between 15:30 and 16:05 IST (3:30 PM to 4:05 PM)
        if (ist_now.hour == 15 and ist_now.minute >= 30) or (ist_now.hour == 16 and ist_now.minute <= 5):
            return True
    except Exception as e:
        log = logging.getLogger("NSEScanner")
        log.warning(f"Error checking invalidation window: {e}")
    return False

SKIP_TICKERS = {
    "VMART.NS",        # V-Mart acquired by Reliance — delisted
    "TATAMOTORS.NS",   # Demerged into TMCV + TMPV
    "MINDTREE.NS",     # Merged into LTIMindtree (now LTM.NS)
    "PVR.NS",          # Merged with INOX — now PVRINOX.NS
    "DELTACORP.NS",    # Suspended
    "TEJASNET.NS",     # Often illiquid/suspended
    "VEEFIN.NS",       # Delisted
    "KALPATPOWR.NS",   # Timeout/Delisted
    "L&TFH.NS",        # Timeout/Delisted
    "UJAAS.NS",        # No data
    # ── Confirmed dead/renamed as of Jun 2026 (saves prefetch retry time) ──
    "LTIM.NS",         # Renamed → LTM.NS (Feb 2026)
    "ZOMATO.NS",       # Rebranded → ETERNAL.NS (Apr 2025)
    "GMRINFRA.NS",     # Renamed → GMRP.NS
    "IBULHSGFIN.NS",   # Merged/Delisted
    "DALMIACEMB.NS",   # Merged into DALMIABHA
    "MAHINDCIE.NS",    # Delisted
    "LAXMIMACH.NS",    # Delisted
    "INOXLEISUR.NS",   # Merged → PVRINOX.NS
    "ISEC.NS",         # Renamed → ICICIKSEC.NS
    "MTAR.NS",         # Renamed/Delisted
    "MINDA.NS",        # Merged → UNOMINDA.NS
    "SEQUENT.NS",      # Delisted
    "PVCL.NS",         # Delisted
    "AMARAJABAT.NS",   # Renamed → AMARA.NS
    "WELSPUNIND.NS",   # Renamed
    "TVSMOTORS.NS",    # Wrong symbol — use TVSMOTOR.NS
    "ASIANHOTEL.NS",   # Renamed
    "ADANITRANS.NS",   # Delisted (merged into ADANIGREEN)
    "ANDHRBANK.NS",    # Merged into Union Bank
    "TCNSBRANDS.NS",   # Delisted (acquired)
    "ARBL.NS",         # Renamed → AMARA.NS
    "AMINES.NS",       # Delisted
    "DELHIBANK.NS",    # Merged into Bank of Baroda
    "DHANI.NS",        # Renamed/Delisted
    "INDSWFTMED.NS",   # Delisted
    "GDL.NS",          # Delisted
    "EQUITAS.NS",      # Merged → EQUITASBNK.NS
    "JCHAC.NS",        # Delisted
    "INFOEDGE.NS",     # Renamed → NAUKRI.NS
    "OCCL.NS",         # Delisted
    "PRAJ.NS",         # Data issues
    "MEGH.NS",         # Delisted
    "KKALPATARUPROJ.NS", # Delisted
    "JUNIPERHOTEL.NS", # Delisted
    "NLC.NS",          # Renamed → NLCINDIA.NS
    "SAILESH.NS",      # Delisted
    "SPICEJET.NS",     # Suspended
    "SALPG.NS",        # Delisted
    "SHRIRAMCIT.NS",   # Merged → SHRIRAMFIN.NS
    "SHRIRAMC.NS",     # Merged → SHRIRAMFIN.NS
    "SRTRANSFIN.NS",   # Merged → SHRIRAMFIN.NS
    "SGBSEBI.NS",      # Sovereign Gold Bond — no equity data
    "PUREIT.NS",       # Delisted
    "TINPLATE.NS",     # Data issues (use TINPLATE.BO)
    "SUNCLAYLTD.NS",   # Delisted
    "SUVENPHAR.NS",    # Renamed → SUVEN.NS
    "TRANSRAIL.NS",    # IPO 2024, insufficient 2y history
    "SWANENERGY.NS",   # Delisted
    "TATAMETALI.NS",   # Merged into TATASTEEL
    "ULTRATECH.NS",    # Wrong symbol — use ULTRACEMCO.NS
}


# ─────────────────────────────────────────────────────────────
# SHARED HTTP SESSION  (one session, thread-safe reads)
# ─────────────────────────────────────────────────────────────
def _make_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://",  adapter)
    sess.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })
    return sess


_SESSION = _make_session()
_YF_SEMAPHORE = threading.Semaphore(1)  # max 1 concurrent YF request (prevents yfinance state corruption)


# ─────────────────────────────────────────────────────────────
# BACKTESTING & SQLITE HISTORY ENGINE
# ─────────────────────────────────────────────────────────────
DB_FILE = "scanner.db"

def init_db() -> None:
    """
    Initializes the SQLite database and creates tables if they do not exist.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # 1. signal_history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signal_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                ticker TEXT,
                signal_type TEXT,
                entry REAL,
                stoploss REAL,
                target1 REAL,
                target2 REAL,
                score INTEGER,
                confidence TEXT,
                rs_pct REAL,
                turnover_crore REAL,
                scanned_at TEXT
            )
        """)
        
        # 2. alert_history table (v5.0 upgrade)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT NOT NULL,
                score       REAL,
                entry       REAL,
                sl          REAL,
                target      REAL,
                rr          REAL,
                rvol        REAL,
                regime      TEXT,
                alert_time  TEXT DEFAULT (datetime('now','localtime')),
                channel     TEXT DEFAULT 'telegram'
            )
        """)
        
        # 3. scan_metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scan_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                duration_seconds REAL,
                tickers_scanned INTEGER,
                signals_found INTEGER,
                cache_hits INTEGER,
                cpu_usage REAL,
                memory_usage REAL
            )
        """)
        
        conn.commit()
        conn.close()
        log.info("💾 SQLite database initialized successfully.")
    except Exception as exc:
        log.error(f"Error initializing SQLite database: {exc}")


init_db()


# ─────────────────────────────────────────────────────────────
# NSE STOCK UNIVERSE  (~120 Nifty-500 stocks)
# ─────────────────────────────────────────────────────────────
# Full hardcoded Nifty 500 list (494 stocks) — used as fallback
NIFTY500_HARDCODED: List[str] = [
    # Nifty 50
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS",
    "HINDUNILVR.NS","ITC.NS","SBIN.NS","BHARTIARTL.NS","KOTAKBANK.NS",
    "LT.NS","AXISBANK.NS","ASIANPAINT.NS","MARUTI.NS","SUNPHARMA.NS",
    "TITAN.NS","BAJFINANCE.NS","WIPRO.NS","NESTLEIND.NS","ULTRACEMCO.NS",
    "APOLLOHOSP.NS","TECHM.NS","HCLTECH.NS","POWERGRID.NS","NTPC.NS",
    "TATAMOTORS.NS","ONGC.NS","JSWSTEEL.NS","TATASTEEL.NS","BAJAJFINSV.NS",
    "DIVISLAB.NS","DRREDDY.NS","CIPLA.NS","EICHERMOT.NS","HEROMOTOCO.NS",
    "GRASIM.NS","BPCL.NS","COALINDIA.NS","INDUSINDBK.NS","ADANIPORTS.NS",
    "ADANIENT.NS","HINDALCO.NS","SBILIFE.NS","HDFCLIFE.NS","M&M.NS",
    "TATACONSUM.NS","BRITANNIA.NS","BAJAJ-AUTO.NS","LTM.NS","SHRIRAMFIN.NS",
    # Nifty Next 50
    "DABUR.NS","MARICO.NS","PIDILITIND.NS","BERGEPAINT.NS","HAVELLS.NS",
    "GODREJCP.NS","MUTHOOTFIN.NS","CHOLAFIN.NS","SRF.NS","AARTIIND.NS",
    "ABCAPITAL.NS","ACC.NS","AIAENG.NS","ALKEM.NS","AMBUJACEM.NS",
    "AUBANK.NS","BALKRISIND.NS","BANDHANBNK.NS","BEL.NS","BHARATFORG.NS",
    "BIOCON.NS","CANBK.NS","CESC.NS","CROMPTON.NS","CUB.NS",
    "DEEPAKNTR.NS","ESCORTS.NS","FEDERALBNK.NS","GAIL.NS","GMRINFRA.NS",
    "GNFC.NS","GODREJPROP.NS","GRANULES.NS","GSPL.NS","HINDPETRO.NS",
    "HINDCOPPER.NS","IDFCFIRSTB.NS","IEX.NS","IPCALAB.NS","IRCTC.NS",
    "JINDALSTEL.NS","JUBLFOOD.NS","KANSAINER.NS","LALPATHLAB.NS","LUPIN.NS",
    "MANAPPURAM.NS","MFSL.NS","MOTHERSON.NS","MPHASIS.NS","MRF.NS",
    # Nifty Midcap 150
    "NMDC.NS","OBEROIRLTY.NS","OFSS.NS","PAGEIND.NS","PEL.NS",
    "PERSISTENT.NS","PETRONET.NS","PFC.NS","PVR.NS","RAMCOCEM.NS",
    "RBLBANK.NS","RECLTD.NS","SAIL.NS","SHREECEM.NS","SIEMENS.NS",
    "SUPREMEIND.NS","SYNGENE.NS","TRENT.NS","UBL.NS","VEDL.NS",
    "VOLTAS.NS","ETERNAL.NS","ZYDUSLIFE.NS","ABFRL.NS","ABSLAMC.NS",
    "AFFLE.NS","AJANTPHARM.NS","ALEMBICLTD.NS","ANGELONE.NS","APLAPOLLO.NS",
    "APLLTD.NS","ARVINDFASN.NS","ASTRAL.NS","ATUL.NS","AUROPHARMA.NS",
    "AVANTIFEED.NS","BAJAJHLDNG.NS","BATAINDIA.NS","BAYERCROP.NS","BIKAJI.NS",
    "BLUESTARCO.NS","BRIGADE.NS","CAMS.NS","CANFINHOME.NS","CASTROLIND.NS",
    "CDSL.NS","CENTURYPLY.NS","CERA.NS","CHALET.NS","CHAMBLFERT.NS",
    "CLEAN.NS","COFORGE.NS","COLPAL.NS","CONCORDBIO.NS","COROMANDEL.NS",
    "CREDITACC.NS","CYIENT.NS","DALMIACEMB.NS","DCMSHRIRAM.NS","DEEPAKFERT.NS",
    "DELTACORP.NS","DEVYANI.NS","DIXON.NS","DMART.NS","EIHOTEL.NS",
    "ELECON.NS","ELGIEQUIP.NS","EMAMILTD.NS","ENDURANCE.NS","ENGINERSIN.NS",
    "EPL.NS","EQUITASBNK.NS","EXIDEIND.NS","FINEORG.NS","FINPIPE.NS",
    "FSL.NS","GICRE.NS","GILLETTE.NS","GLAXO.NS","GLENMARK.NS",
    "GLOBAL.NS","GODFRYPHLP.NS","GRAPHITE.NS","GREAVESCOT.NS","GREENPANEL.NS",
    "GRINDWELL.NS","GSFC.NS","GUJGASLTD.NS","HAPPSTMNDS.NS","HATSUN.NS",
    "HEG.NS","HFCL.NS","HIKAL.NS","HLEGLAS.NS","HONAUT.NS",
    "IBULHSGFIN.NS","ICICIPRULI.NS","ICICIGI.NS","IDBI.NS","IGARASHI.NS",
    "IIFL.NS","INDHOTEL.NS","INDIACEM.NS","INDIANB.NS","INDIGO.NS",
    "INDUSINDBK.NS","INOXLEISUR.NS","IOB.NS","IOLCP.NS","IRB.NS",
    "IRFC.NS","ISEC.NS","ITDC.NS","J&KBANK.NS","JBCHEPHARM.NS",
    "JINDALSAW.NS","JKLAKSHMI.NS","JKPAPER.NS","JMFINANCIL.NS","JSWENERGY.NS",
    "JUBLINGREA.NS","JUSTDIAL.NS","KAJARIACER.NS","KALPATPOWR.NS","KEC.NS",
    "KFINTECH.NS","KNRCON.NS","KRBL.NS","KSCL.NS","L&TFH.NS",
    "LAXMIMACH.NS","LICHSGFIN.NS","LINDEINDIA.NS","LODHA.NS","LICI.NS",
    "M&MFIN.NS","MAHINDCIE.NS","MAHLIFE.NS","MANINFRA.NS","MASFIN.NS",
    "MAXHEALTH.NS","MCX.NS","METROPOLIS.NS","MIDHANI.NS","MINDACORP.NS",
    "MINDA.NS","MMTC.NS","MOIL.NS","MRPL.NS","MTAR.NS",
    "NAUKRI.NS","NAVINFLUOR.NS","NBCC.NS","NCC.NS","NIACL.NS",
    "NLCINDIA.NS","NOCIL.NS","NUVAMA.NS","OLECTRA.NS","ORIENTELEC.NS",
    "PGHH.NS","PHOENIXLTD.NS","POLYCAB.NS","POLYMED.NS","POONAWALLA.NS",
    "POWERMECH.NS","PRAJIND.NS","PRINCEPIPE.NS","PRIVISCL.NS","PSB.NS",
    "PTC.NS","PVCL.NS","RADICO.NS","RAILTEL.NS","RAJESHEXPO.NS",
    "RAJRATAN.NS","RALLIS.NS","RITES.NS","RVNL.NS","SAFARI.NS",
    "SANOFI.NS","SAPPHIRE.NS","SCHAEFFLER.NS","SEQUENT.NS","SHYAMMETL.NS",
    "SJVN.NS","SKFINDIA.NS","SOBHA.NS","SOLARA.NS","SOMANYCERA.NS",
    "SPANDANA.NS","SPARC.NS","STAR.NS","STARCEMENT.NS","STLTECH.NS",
    "SUMICHEM.NS","SUNTV.NS","SUPRAJIT.NS","SURYAROSNI.NS","SUZLON.NS",
    "SYMPHONY.NS","TATACHEM.NS","TATACOMM.NS","TATAELXSI.NS","TATAINVEST.NS",
    "TATAPOWER.NS","TCNSBRANDS.NS","TEAMLEASE.NS","TEJASNET.NS","THERMAX.NS",
    "TIINDIA.NS","TIMKEN.NS","TORNTPHARM.NS","TORNTPOWER.NS","TTKPRESTIG.NS",
    "TVSMOTORS.NS","UCOBANK.NS","UJJIVANSFB.NS","UNIONBANK.NS","UNITDSPR.NS",
    "UNIPARTS.NS","USHAMART.NS","VBL.NS","VIJAYA.NS","VINATIORGA.NS",
    "VTL.NS","WELCORP.NS","WELSPUNIND.NS","WHIRLPOOL.NS","WOCKPHARMA.NS",
    "YESBANK.NS","ZEEL.NS","ZENITHEXPO.NS","ZENSARTECH.NS","INDUSTOWER.NS",
    # Extra Nifty 500 additions
    "AARTIDRUGS.NS","ADANIGREEN.NS","ADANIPOWER.NS","ADANITRANS.NS",
    "AEGISLOG.NS","AGROPHOS.NS","AMARAJABAT.NS","AMBER.NS","AMINES.NS",
    "ANANTRAJ.NS","ANDHRBANK.NS","APCOTEXIND.NS","APOLLOTYRE.NS","ARBL.NS",
    "ARCHIES.NS","ARVIND.NS","ASHOKLEY.NS","ASIANHOTEL.NS","ASTRAZEN.NS",
    "ATGL.NS","BALRAMCHIN.NS","BALAMINES.NS","BAYERCROP.NS","BBTC.NS",
    "BEML.NS","BFINVEST.NS","BGRENERGY.NS","BHEL.NS","BIRLACORPN.NS",
    "BORORENEW.NS","BOSCHLTD.NS","BSL.NS","CEATLTD.NS","CMSINFO.NS",
    "CONCOR.NS","CRISIL.NS","CROMPTON.NS","DELHIBANK.NS","DHANI.NS",
    "DLF.NS","DLINKINDIA.NS","DREDGECORP.NS","EIHOTEL.NS","EQUITAS.NS",
    "ESTER.NS","FLAIR.NS","FRETAIL.NS","GAEL.NS","GARFIBRES.NS",
    "GAYAPROJ.NS","GDL.NS","GPIL.NS","GREENPLY.NS","GULFOILLUB.NS",
    "GVKPIL.NS","HCC.NS","HDFCAMC.NS","HERITGFOOD.NS","HNDFDS.NS",
    "HPL.NS","HUHTAMAKI.NS","IBREALEST.NS","ICIL.NS","IGPL.NS",
    "IMFA.NS","INDIGOPNTS.NS","INDNIPPON.NS","INDORAMA.NS","INDSWFTMED.NS",
    "INFOEDGE.NS","INTELLECT.NS","IONEXCHANG.NS","ITC.NS","ITDC.NS",
    "JCHAC.NS","JISLJALEQS.NS","JKCEMENT.NS","JKLAKSHMI.NS","JMFINANCIL.NS",
    "JPPOWER.NS","JSWHL.NS","JUNIPERHOTEL.NS","KALYANKJIL.NS","KAMDHENU.NS",
    "KDDL.NS","KIRIINDUS.NS","KITEX.NS","KKALPATARUPROJ.NS","KOLTEPATIL.NS",
    "KOPRAN.NS","KPIL.NS","KRSNAA.NS","KTKBANK.NS","KPITTECH.NS",
    "LATENTVIEW.NS","LAURUSLABS.NS","LAXMIMACH.NS","LEMONTREE.NS","LGBBROSLTD.NS",
    "MANGLMCEM.NS","MARKSANS.NS","MATRIMONY.NS","MAYURUNIQ.NS","MEDPLUS.NS",
    "MEGH.NS","MMTC.NS","MOREPENLAB.NS","MOTILALOFS.NS","MPHASIS.NS",
    "NAUKRI.NS","NESCO.NS","NEWGEN.NS","NFL.NS","NHPC.NS",
    "NIITLTD.NS","NIITMTS.NS","NLC.NS","NSLNISP.NS","NUCLEUS.NS",
    "OCCL.NS","ONGC.NS","ORIENTCEM.NS","PATELENG.NS","PAYTM.NS",
    "PCJEWELLER.NS","PDMJEPAPER.NS","PENIND.NS","PENINLAND.NS","PFIZER.NS",
    "PIIND.NS","POCL.NS","PRAJ.NS","PRAKASH.NS","PREMEXPLN.NS",
    "PRESTIGE.NS","PRICOLLTD.NS","PRITIKAUTO.NS","PUNJABCHEM.NS","PUREIT.NS",
    "PVRINOX.NS","QUESS.NS","RATNAMANI.NS","RAYMOND.NS","REDINGTON.NS",
    "REPCOHOME.NS","RESPONIND.NS","RITES.NS","ROHLTD.NS","ROSSARI.NS",
    "RPOWER.NS","RTNPOWER.NS","RVNL.NS","SADBHAV.NS","SAILESH.NS",
    "SAKSOFT.NS","SALPG.NS","SANDHAR.NS","SANGHIIND.NS","SARDAEN.NS",
    "SAREGAMA.NS","SARLAPOLY.NS","SBFC.NS","SBICARD.NS","SBILIFE.NS",
    "SCHNEIDER.NS","SFL.NS","SGBSEBI.NS","SHALBY.NS","SHAREINDIA.NS",
    "SHILPAMED.NS","SHOPERSTOP.NS","SHRIRAMCIT.NS","SHRIRAMC.NS","SKMEGGPROD.NS",
    "SMSPHARMA.NS","SNOWMAN.NS","SOLARINDS.NS","SONATSOFTW.NS","SOUTHBANK.NS",
    "SPICEJET.NS","SPTL.NS","SRHHYPOLTD.NS","SRTRANSFIN.NS","STARHEALTH.NS",
    "STEELCAS.NS","STLTECH.NS","STYLAMIND.NS","SUDARSCHEM.NS","SUKHJITS.NS",
    "SUNPHARMA.NS","SUNCLAYLTD.NS","SUPRAJIT.NS","SUPRIYA.NS","SUVENPHAR.NS",
    "SWANENERGY.NS","TANLA.NS","TASTYBITE.NS","TATAMETALI.NS","TATVA.NS",
    "TIMETECHNO.NS","TINPLATE.NS","TITAGARH.NS","TNPETRO.NS","TPLPLASTEH.NS",
    "TRANSRAIL.NS","TRIDENT.NS","TRITURBINE.NS","TRIVENI.NS","TTKHLTCARE.NS",
    "TVSMOTOR.NS","UGROCAP.NS","UJAAS.NS","ULTRATECH.NS","UNOMINDA.NS",
    "UTIAMC.NS","VGUARD.NS","VAIBHAVGBL.NS","VARDHACRLC.NS","VARROC.NS",
    "VEEFIN.NS","VENKEYS.NS","VESUVIUS.NS","VMART.NS","VOLTAMP.NS",
    "VSTIND.NS","WABAG.NS","WINDMACHIN.NS","WONDERLA.NS","WSI.NS",
    "XCHANGING.NS","YESBANK.NS","ZENTEC.NS","ZOMATO.NS","ZUARI.NS"
]

def get_nse_universe() -> List[str]:
    """
    Try to fetch the live NSE equity list.
    Falls back to the hardcoded Nifty 500 sample if fetch fails.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.nseindia.com"
        }
        session = requests.Session()
        session.headers.update(headers)
        # Warm-up cookie
        session.get("https://www.nseindia.com", timeout=10)
        resp = session.get(
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",
            timeout=15
        )
        if resp.ok:
            data = resp.json()
            symbols = [d["symbol"] + ".NS" for d in data.get("data", []) if "symbol" in d]
            if len(symbols) > 100:
                log.info(f"✅ Fetched {len(symbols)} stocks from NSE API")
                return symbols
    except Exception as e:
        log.warning(f"NSE API fetch failed ({e}), using hardcoded list")

    return NIFTY500_HARDCODED

NIFTY500_SAMPLE = get_nse_universe()

# Deduplicate while preserving order
_seen: set = set()
NIFTY500_SAMPLE = [
    t for t in NIFTY500_SAMPLE
    if not (t in _seen or _seen.add(t))  # type: ignore[func-returns-value]
]


# ─────────────────────────────────────────────────────────────
# PERFORMANCE & SYSTEM METRICS LOGGING
# ─────────────────────────────────────────────────────────────
def get_sys_metrics() -> tuple:
    """
    Returns (cpu_usage, memory_mb) if psutil is available, otherwise (0.0, 0.0).
    """
    if psutil is not None:
        try:
            proc = psutil.Process(os.getpid())
            cpu = proc.cpu_percent(interval=None)
            mem = proc.memory_info().rss / (1024 * 1024)
            return round(cpu, 2), round(mem, 2)
        except Exception:
            pass
    return 0.0, 0.0


# ─────────────────────────────────────────────────────────────
# DATA QUALITY INTEGRITY VALIDATION
# ─────────────────────────────────────────────────────────────
def validate_dataframe(df: pd.DataFrame, ticker: str) -> bool:
    """
    Validates the structural and logical integrity of daily OHLCV bars.
    Returns True if valid, False if corrupt or contains structural anomalies.
    """
    if df is None or df.empty:
        log.warning(f"❌ {ticker}: Empty or null DataFrame.")
        return False

    required = ["open", "high", "low", "close", "volume"]
    for col in required:
        if col not in df.columns:
            log.warning(f"❌ {ticker}: Missing critical column '{col}'")
            return False

    # Check for NaN values in critical features
    if df[required].isna().any().any():
        log.warning(f"❌ {ticker}: Contains NaN value in critical data rows.")
        return False

    # Check price values > 0
    if (df["open"] <= 0).any() or (df["high"] <= 0).any() or (df["low"] <= 0).any() or (df["close"] <= 0).any():
        log.warning(f"❌ {ticker}: Invalid prices detected (<= 0).")
        return False

    # Check negative volume
    if (df["volume"] < 0).any():
        log.warning(f"❌ {ticker}: Negative volume detected.")
        return False

    # Check structural boundaries
    if not (df["high"] >= df["low"]).all():
        log.warning(f"❌ {ticker}: Structural violation: high < low.")
        return False
    if not (df["high"] >= df["open"]).all():
        log.warning(f"❌ {ticker}: Structural violation: high < open.")
        return False
    if not (df["high"] >= df["close"]).all():
        log.warning(f"❌ {ticker}: Structural violation: high < close.")
        return False
    if not (df["low"] <= df["open"]).all():
        log.warning(f"❌ {ticker}: Structural violation: low > open.")
        return False
    if not (df["low"] <= df["close"]).all():
        log.warning(f"❌ {ticker}: Structural violation: low > close.")
        return False

    return True


# ─────────────────────────────────────────────────────────────
# MARKET HOURS & NIFTY RELATIVE STRENGTH ENGINE
# ─────────────────────────────────────────────────────────────
from datetime import time as dt_time, timedelta, timezone

def is_nse_market_open() -> bool:
    """
    Checks if the NSE market is currently open (9:15 AM - 3:30 PM IST, Monday - Friday).
    Uses standard library UTC offset calculation to remain timezone-accurate and dependency-free.
    """
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist_tz)
    if now_ist.weekday() >= 5:  # Saturday, Sunday
        return False
    market_start = dt_time(9, 15)
    market_end = dt_time(15, 30)
    return market_start <= now_ist.time() <= market_end


_NIFTY_CACHE: Dict[str, Dict] = {}

def get_nifty_returns() -> tuple:
    """
    Downloads and caches both the 20-day and 50-day returns of the NIFTY 50 index (^NSEI).
    Returns (nifty_20d_return, nifty_50d_return).
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if "returns" in _NIFTY_CACHE and _NIFTY_CACHE.get("date") == today_str:
        cached = _NIFTY_CACHE["returns"]
        return cached["20d"], cached["50d"], cached.get("63d", 0.0)
    
    try:
        with _YF_SEMAPHORE:
            df = yf.download(
                "^NSEI",
                period="6mo",
                interval="1d",
                progress=False,
                auto_adjust=True,
                timeout=30,
            )
        if df is not None and not df.empty and len(df) >= 50:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [str(c).lower() for c in df.columns]
            df.dropna(inplace=True)
            close_today = float(df["close"].iloc[-1])
            close_20d = float(df["close"].iloc[-20])
            close_50d = float(df["close"].iloc[-50])
            close_63d = float(df["close"].iloc[-63]) if len(df) >= 63 else close_50d
            
            nifty_20d = (close_today - close_20d) / close_20d * 100
            nifty_50d = (close_today - close_50d) / close_50d * 100
            nifty_63d = (close_today - close_63d) / close_63d * 100
            
            _NIFTY_CACHE["returns"] = {"20d": nifty_20d, "50d": nifty_50d, "63d": nifty_63d}
            _NIFTY_CACHE["date"] = today_str
            log.info(f"📈 Nifty returns cached: 20d={nifty_20d:.2f}%, 50d={nifty_50d:.2f}%, 63d={nifty_63d:.2f}%")
            return nifty_20d, nifty_50d, nifty_63d
    except Exception as exc:
        log.error(f"Error fetching Nifty Index returns: {exc}")
    return 0.0, 0.0, 0.0


# ─────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────
# YFINANCE DOWNLOAD  (with fault-tolerant persistent caching)
# ─────────────────────────────────────────────────────────────
CACHE_DIR = "data_cache"

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes the DataFrame structure:
    - Flattens MultiIndex columns to flat lowercase strings
    - Converts Open, High, Low, Close, Volume to numeric float types
    - Cleans up and discards string metadata rows (such as header rows loaded from corrupted cache)
    """
    if df is None or df.empty:
        return df

    # 1. Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0].lower() for c in df.columns]
    else:
        df.columns = [str(c).lower() for c in df.columns]

    # 2. Convert OHLCV columns to numeric, coercing strings/errors to NaN
    required = ["open", "high", "low", "close", "volume"]
    for col in required:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 3. Drop rows that have NaN in critical columns (e.g. metadata text rows)
    df.dropna(subset=required, inplace=True)
    return df


def _download(ticker: str, retries: int = 3, use_cache_only: bool = False) -> Optional[pd.DataFrame]:
    # Ensure cache directory exists
    if not os.path.exists(CACHE_DIR):
        try:
            os.makedirs(CACHE_DIR)
        except Exception:
            pass

    cache_path = os.path.join(CACHE_DIR, f"{ticker}.csv")
    
    # If use_cache_only is True, strictly load from cache (offline scan mode)
    if use_cache_only:
        if os.path.exists(cache_path):
            try:
                df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                if df is not None and not df.empty:
                    df = normalize_dataframe(df)
                    if df is not None and not df.empty:
                        return df
            except Exception:
                pass
        return None

    # Check cache validity — dynamic TTL (120s during market hours, 12hr otherwise)
    if not is_post_market_invalidation_window() and os.path.exists(cache_path):
        try:
            mtime = os.path.getmtime(cache_path)
            _ttl = 120 if is_nse_market_open() else 43200   # 2 mins dynamic market TTL, 12 hours EOD cache TTL
            if time.time() - mtime < _ttl:
                df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                if df is not None and not df.empty:
                    df = normalize_dataframe(df)
                    if df is not None and not df.empty:
                        log.debug(f"{ticker}: Loaded from local cache (TTL={_ttl}s)")
                        return df
        except Exception:
            pass

    for attempt in range(retries):
        try:
            with _YF_SEMAPHORE:
                df = yf.download(
                    ticker,
                    period="2y",
                    interval="1d",
                    progress=False,
                    auto_adjust=True,
                    timeout=30,
                )
            if df is not None and not df.empty:
                df = normalize_dataframe(df)
                # Save to cache
                try:
                    df.to_csv(cache_path)
                except Exception:
                    pass
                return df
        except Exception as exc:
            if attempt < retries - 1:
                wait = 1.5 ** attempt
                log.debug(f"{ticker}: attempt {attempt + 1} failed ({exc}), retrying in {wait:.1f}s")
                time.sleep(wait)
            else:
                # Fallback to older cache if download completely failed (extreme resilience)
                if os.path.exists(cache_path):
                    try:
                        df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                        if df is not None and not df.empty:
                            df = normalize_dataframe(df)
                            if df is not None and not df.empty:
                                log.warning(f"{ticker}: Download failed; falling back to stale local cache")
                                return df
                    except Exception:
                        pass
                log.debug(f"{ticker}: all {retries} download attempts failed — {exc}")
    return None


# ─────────────────────────────────────────────────────────────
# ANALYSE ONE STOCK
# ─────────────────────────────────────────────────────────────
def _get_cache_hit(ticker: str) -> int:
    cache_path = os.path.join(CACHE_DIR, f"{ticker}.csv")
    if os.path.exists(cache_path):
        try:
            mtime = os.path.getmtime(cache_path)
            if time.time() - mtime < 3600:
                return 1
        except Exception:
            pass
    return 0



def fetch_tail(ticker: str) -> Optional[pd.DataFrame]:
    """Download the last 5 days of data for tail merging."""
    try:
        with _YF_SEMAPHORE:
            data = yf.download(
                ticker, period="5d", interval="1d",
                progress=False, auto_adjust=True,
                timeout=15
            )
        if data is not None and not data.empty:
            df = normalize_dataframe(data.copy())
            if df is not None and not df.empty:
                return df
    except Exception as e:
        log.debug(f"Failed to fetch tail for {ticker}: {e}")
    return None

def fetch_with_timeout(
    ticker: str,
    bearish: bool = False,
    cfg_override: Optional[Dict] = None,
    explain_skip: bool = False,
    timeout: int = 30,
    market_context: Optional[Dict] = None,
) -> Optional[Dict]:
    """
    Wraps analyse() with a hard per-stock timeout.
    If yfinance hangs, this returns None after timeout seconds.
    """
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(analyse, ticker, bearish=bearish, cfg_override=cfg_override, explain_skip=explain_skip, market_context=market_context)
        try:
            return future.result(timeout=timeout)
        except FuturesTimeoutError:
            log.warning(f"⏱️ TIMEOUT: {ticker} skipped after {timeout}s")
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Analysis timed out after {timeout} seconds"
                }
            return None
        except Exception as e:
            log.warning(f"Error during analysis of {ticker}: {e}")
            if explain_skip:
                return {
                    "symbol": ticker.replace(".NS", ""),
                    "skipped": True,
                    "reason": f"Analysis failed: {str(e)}"
                }
            return None


def prefetch_batch(tickers: list, period="2y", progress_cb: Optional[Callable] = None) -> dict:
    """Download tickers in batches of 50 concurrently."""
    cache = {}
    batch_size = 50
    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    
    cache_lock = threading.Lock()
    total_batches = len(batches)
    completed_batches = 0
    completed_lock = threading.Lock()
    
    def _download_batch(batch_idx_and_batch):
        nonlocal completed_batches
        idx, batch = batch_idx_and_batch
        try:
            local_cache = {}
            missing_batch = []
            tail_refresh_batch = []
            
            if redis_client and not is_post_market_invalidation_window():
                for ticker in batch:
                    try:
                        cached = redis_client.get(f"stock:{ticker}")
                        if cached:
                            try:
                                cache_data = json.loads(cached)
                            except Exception:
                                cache_data = None
                            
                            if isinstance(cache_data, dict) and "timestamp" in cache_data and "data" in cache_data:
                                last_full = cache_data.get("last_full_refresh", 0)
                                age = time.time() - cache_data["timestamp"]
                                _ttl = 120 if is_nse_market_open() else 43200
                                
                                if time.time() - last_full >= 86400:
                                    missing_batch.append(ticker)
                                    continue
                                    
                                data_val = cache_data["data"]
                                if age < _ttl:
                                    if data_val == "EMPTY":
                                        df = pd.DataFrame()
                                        local_cache[ticker] = (df, last_full)
                                    else:
                                        df = pd.read_json(io.StringIO(data_val), orient='split')
                                        if df is not None and (df.empty or len(df) >= 260):
                                            local_cache[ticker] = (df, last_full)
                                        else:
                                            missing_batch.append(ticker)
                                else:
                                    if data_val == "EMPTY":
                                        missing_batch.append(ticker)
                                    else:
                                        try:
                                            df = pd.read_json(io.StringIO(data_val), orient='split')
                                            if df is not None and not df.empty and len(df) >= 260:
                                                tail_refresh_batch.append((ticker, df, last_full))
                                            else:
                                                missing_batch.append(ticker)
                                        except Exception:
                                            missing_batch.append(ticker)
                            else:
                                missing_batch.append(ticker)
                        else:
                            missing_batch.append(ticker)
                    except Exception:
                        missing_batch.append(ticker)
            else:
                missing_batch = batch
                
            if missing_batch:
                with _YF_SEMAPHORE:
                    data = yf.download(
                        missing_batch, period=period, interval="1d",
                        progress=False, auto_adjust=True,
                        group_by="ticker", threads=False
                    )
                for ticker in missing_batch:
                    try:
                        if isinstance(data.columns, pd.MultiIndex):
                            all_tickers = list(data.columns.get_level_values(0).unique())
                            if ticker in all_tickers:
                                df = data[ticker].copy()
                                df.columns = [str(c).lower() for c in df.columns]
                                df = df.dropna()
                                local_cache[ticker] = (df if not df.empty else None, time.time())
                            else:
                                local_cache[ticker] = (None, time.time())
                        else:
                            if len(missing_batch) == 1 and ticker == missing_batch[0]:
                                df = data.copy()
                                df.columns = [str(c).lower() for c in df.columns]
                                df = df.dropna()
                                local_cache[ticker] = (df if not df.empty else None, time.time())
                            else:
                                local_cache[ticker] = (None, time.time())
                    except Exception:
                        local_cache[ticker] = (None, time.time())

            if tail_refresh_batch:
                tail_tickers = [t[0] for t in tail_refresh_batch]
                with _YF_SEMAPHORE:
                    tail_data = yf.download(
                        tail_tickers, period="5d", interval="1d",
                        progress=False, auto_adjust=True,
                        group_by="ticker", threads=False
                    )
                for ticker, old_df, last_full in tail_refresh_batch:
                    new_df = None
                    try:
                        if isinstance(tail_data.columns, pd.MultiIndex):
                            all_tickers = list(tail_data.columns.get_level_values(0).unique())
                            if ticker in all_tickers:
                                df = tail_data[ticker].copy()
                                df.columns = [str(c).lower() for c in df.columns]
                                new_df = df.dropna()
                        else:
                            if len(tail_refresh_batch) == 1 and ticker == tail_tickers[0]:
                                df = tail_data.copy()
                                df.columns = [str(c).lower() for c in df.columns]
                                new_df = df.dropna()
                    except Exception:
                        pass
                    
                    if new_df is not None and not new_df.empty:
                        merged_df = pd.concat([old_df[~old_df.index.isin(new_df.index)], new_df]).sort_index()
                        merged_df = merged_df.iloc[-505:]
                        local_cache[ticker] = (merged_df, last_full)
                    else:
                        local_cache[ticker] = (old_df, last_full)

            if redis_client:
                for ticker, (df, last_full) in local_cache.items():
                    try:
                        if df is not None and not df.empty:
                            cache_payload = {
                                "timestamp": time.time(),
                                "last_full_refresh": last_full,
                                "data": df.to_json(date_format='iso', orient='split')
                            }
                            redis_client.setex(f"stock:{ticker}", 86400, json.dumps(cache_payload))
                        else:
                            cache_payload = {
                                "timestamp": time.time(),
                                "last_full_refresh": last_full,
                                "data": "EMPTY"
                            }
                            redis_client.setex(f"stock:{ticker}", 86400, json.dumps(cache_payload))
                    except Exception as e:
                        log.debug(f"Failed to cache {ticker} to Redis: {e}")
                            
            with cache_lock:
                for ticker, (df, last_full) in local_cache.items():
                    cache[ticker] = df
                
            with completed_lock:
                completed_batches += 1
                curr_comp = completed_batches
                
            log.info(f"Batch {idx+1}: {len(batch)} stocks prefetched")
            if progress_cb:
                progress_cb(0, len(tickers), f"⚡ Prefetching batch {curr_comp}/{total_batches}...")
        except Exception as e:
            log.warning(f"Batch {idx+1} prefetch failed: {e}")
            with cache_lock:
                for ticker in batch:
                    cache[ticker] = None
            with completed_lock:
                completed_batches += 1
                curr_comp = completed_batches
            if progress_cb:
                progress_cb(0, len(tickers), f"⚡ Prefetching batch {curr_comp}/{total_batches}...")

    with ThreadPoolExecutor(max_workers=5) as ex:
        ex.map(_download_batch, list(enumerate(batches)))
        
    return cache



def analyse(
    ticker: str,
    bearish: bool = False,
    cfg_override: Optional[Dict] = None,
    explain_skip: bool = False,
    df: Optional[pd.DataFrame] = None,
    market_context: Optional[Dict] = None,
) -> Optional[Dict]:
    import agent_engine
    return agent_engine.analyse(
        ticker=ticker,
        bearish=bearish,
        cfg_override=cfg_override,
        explain_skip=explain_skip,
        df=df,
        market_context=market_context,
        SKIP_TICKERS=SKIP_TICKERS,
        CFG=CFG,
        _download=_download,
        get_nifty_returns=get_nifty_returns,
        is_nse_market_open=is_nse_market_open,
    )

# ─────────────────────────────────────────────────────────────
# SCAN ALL STOCKS  (concurrent via ThreadPoolExecutor)
# ─────────────────────────────────────────────────────────────
def run_scan(
    tickers: Optional[List[str]] = None,
    bearish: bool = False,
    progress_cb: Optional[Callable] = None,
    cfg_override: Optional[Dict] = None,
    stop_event: Optional[threading.Event] = None,
    market_context: Optional[Dict] = None,
) -> List[Dict]:
    """
    Scan tickers concurrently.

    progress_cb(current, total, ticker, eta_seconds, current_signals) is called after each ticker.
    stop_event: set it to cancel an in-progress scan gracefully.
    """
    scan_start = time.time()
    original_cfg = cfg_override
    cfg_override = dict(cfg_override or {})
    raw_tickers = list(tickers or NIFTY500_SAMPLE)
    # Fix 1: Warn if market is closed but proceed using last session data
    if not is_nse_market_open():
        log.warning("⚠️ NSE Market is closed — proceeding with last session data...")
        orig_vol_mult = float(cfg_override.get("VOL_MULT", CFG["VOL_MULT"]))
        effective_vol_mult = min(orig_vol_mult, 0.5)
        log.warning(f"Market closed — lowering VOL_MULT from "
                    f"{orig_vol_mult} to {effective_vol_mult} "
                    f"for after-hours scan")
        cfg_override["VOL_MULT"] = effective_vol_mult

    # Dynamic VOL_MULT adjustments for market hours based on session timing
    if is_nse_market_open():
        IST = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(IST)
        market_open = now.replace(hour=9, minute=15)
        minutes_elapsed = (now - market_open).seconds // 60
        
        orig_mult = float(cfg_override.get("VOL_MULT", CFG["VOL_MULT"]))
        
        if minutes_elapsed < 30:      # 9:15–9:45
            effective = min(orig_mult, 0.2)
            reason = "First 30 min"
        elif minutes_elapsed < 75:    # 9:45–10:30
            effective = min(orig_mult, 0.4)
            reason = "Early session"
        elif minutes_elapsed < 150:   # 10:30–12:00
            effective = min(orig_mult, 0.7)
            reason = "Mid morning"
        elif minutes_elapsed < 240:   # 12:00–13:30
            effective = min(orig_mult, 1.0)
            reason = "Midday"
        else:                          # 13:30–15:30
            effective = orig_mult      # full filter applies
            reason = "Afternoon"
        
        if effective < orig_mult:
            log.info(f"⏰ {reason} — VOL_MULT adjusted "
                     f"{orig_mult}x → {effective}x")
            cfg_override["VOL_MULT"] = effective
    tickers = [t for t in raw_tickers if t not in SKIP_TICKERS]
    skipped_count = len(raw_tickers) - len(tickers)
    if skipped_count > 0:
        log.info(f"⏭️ Skipped {skipped_count} known bad/delisted tickers before scanning.")

    if "_data_cache" in cfg_override and cfg_override["_data_cache"]:
        log.info("⚡ Using pre-cached data for scan.")
        data_cache = cfg_override["_data_cache"]
    else:
        log.info("⚡ Prefetching batch data...")
        prefetch_start = time.time()
        data_cache = prefetch_batch(tickers, progress_cb=progress_cb)
        log.info(f"Prefetch done in {time.time()-prefetch_start:.1f}s")
        cfg_override["_data_cache"] = data_cache
        if isinstance(original_cfg, dict):
            original_cfg["_data_cache"] = data_cache

    total   = len(tickers)
    results: List[Dict] = []
    rejection_reasons = {
        "turnover": 0, "price_floor": 0, "freshness": 0,
        "data_sanity": 0, "rr_ratio": 0, "rs_gate": 0,
        "rsi_gate": 0, "rsi_extreme": 0, "live_dump": 0, "ema_alignment": 0,
        "volume_floor": 0, "extension": 0, "sector_lagging": 0,
        "regime": 0, "score_floor": 0, "delisted": 0, "error": 0,
        "stop_too_tight": 0, "gap": 0, "stop_loss_inversion": 0,
    }
    counter  = {"n": 0, "start": time.time(), "cache_hits": 0, "rejected": 0}
    lock     = threading.Lock()
    workers  = (cfg_override or {}).get("MAX_WORKERS", CFG["MAX_WORKERS"])
    stock_timeout = (cfg_override or {}).get("STOCK_TIMEOUT", CFG["STOCK_TIMEOUT"])
    scan_deadline = (cfg_override or {}).get("SCAN_DEADLINE", CFG["SCAN_DEADLINE"])

    # Clear requests_cache before every scan to prevent stale data
    try:
        import requests_cache
        requests_cache.clear()
        log.info("🧹 requests_cache cleared successfully before starting scan.")
    except Exception as e:
        log.warning(f"Could not clear requests_cache: {e}")

    # Ensure yfinance timezone cache directory exists and do not delete it to prevent concurrent SQLite write lock failures
    yf_cache_temp = os.path.join("data_cache", "yf_cache_temp")
    try:
        os.makedirs(yf_cache_temp, exist_ok=True)
        yf.set_tz_cache_location(yf_cache_temp)
    except Exception as e:
        log.warning(f"Could not set timezone cache location: {e}")

    # Download Nifty index first for RS ranking
    get_nifty_returns()

    regime_data  = get_regime()
    regime_name  = regime_data.get("regime", "NEUTRAL")
    log.info(f"📊 Market Regime: {regime_name} | "
             f"Breadth: {regime_data.get('breadth', 50)}% | "
             f"NIFTY: ₹{regime_data.get('nifty_close', 0):,.2f}")

    # FIX C: In bear regimes, automatically loosen the RS 50d floor to -10.0
    # so that stocks only slightly underperforming Nifty still show up
    if regime_name in ("BEAR", "STRONG_BEAR"):
        cfg_override = dict(cfg_override or {})
        cfg_override.setdefault("rs_50d_floor", -10.0)
        log.info(f"🐻 Bear regime detected — loosening RS 50d floor to {cfg_override['rs_50d_floor']:.1f}%")

    data_cache = cfg_override.get("_data_cache", {})

    def _process_ticker(ticker: str) -> None:
        if stop_event and stop_event.is_set():
            return
        if time.time() - counter["start"] > scan_deadline:
            return

        df = data_cache.get(ticker)
        is_hit = 0
        if df is None and redis_client and not is_post_market_invalidation_window():
            try:
                cached = redis_client.get(f"stock:{ticker}")
                if cached:
                    try:
                        cache_data = json.loads(cached)
                    except Exception:
                        cache_data = None
                    
                    if isinstance(cache_data, dict) and "timestamp" in cache_data and "data" in cache_data:
                        last_full = cache_data.get("last_full_refresh", 0)
                        age = time.time() - cache_data["timestamp"]
                        _ttl = 120 if is_nse_market_open() else 43200
                        
                        if time.time() - last_full >= 86400:
                            pass # Force full fetch
                        elif age < _ttl:
                            data_val = cache_data["data"]
                            if data_val == "EMPTY":
                                df = pd.DataFrame()
                                is_hit = 1
                            else:
                                df = pd.read_json(io.StringIO(data_val), orient='split')
                                if df is not None and (df.empty or len(df) >= 260):
                                    is_hit = 1
                                else:
                                    df = None
                        else:
                            data_val = cache_data["data"]
                            if data_val != "EMPTY":
                                try:
                                    old_df = pd.read_json(io.StringIO(data_val), orient='split')
                                    if old_df is not None and not old_df.empty and len(old_df) >= 260:
                                        new_df = fetch_tail(ticker)
                                        if new_df is not None and not new_df.empty:
                                            merged_df = pd.concat([old_df[~old_df.index.isin(new_df.index)], new_df]).sort_index()
                                            df = merged_df.iloc[-505:]
                                            try:
                                                cache_payload = {
                                                    "timestamp": time.time(),
                                                    "last_full_refresh": last_full,
                                                    "data": df.to_json(date_format='iso', orient='split')
                                                }
                                                redis_client.setex(f"stock:{ticker}", 86400, json.dumps(cache_payload))
                                            except Exception:
                                                pass
                                            is_hit = 1
                                except Exception:
                                    pass
            except Exception as e:
                log.debug(f"Redis cache miss/error for {ticker}: {e}")
        
        if df is None or (not df.empty and len(df) < 260):
            result = fetch_with_timeout(ticker, bearish=bearish, cfg_override=cfg_override, timeout=stock_timeout, market_context=market_context)
        elif df.empty:
            result = None
            is_hit = 1
        else:
            is_hit = 1
            result = analyse(ticker, bearish=bearish, cfg_override=cfg_override, df=df, market_context=market_context)

        # Detect structured skip dicts from agent_engine
        is_skip = isinstance(result, dict) and result.get("skipped") is True
        if is_skip:
            gate = result.get("skip_gate", "unknown")
            reason = result.get("reason", "")
            if gate == "error":
                log.warning(f"[ENGINE ERROR] {ticker}: {reason}")
            with lock:
                if gate in rejection_reasons:
                    rejection_reasons[gate] += 1
                else:
                    rejection_reasons[gate] = rejection_reasons.get(gate, 0) + 1
                counter["rejected"] += 1
            result = None

        if result:
            result["raw_score"] = result["score"]  # keep original raw score
            result["regime_score"] = adjust_score_for_regime(
                result["score"],
                regime=regime_name,
                is_bearish=(result.get("signal_type") == "Bear"),
            )
            result["regime"]       = regime_name
            result["regime_emoji"] = regime_data.get("emoji", "⚖️")

            # Filter on RAW score, not regime-adjusted
            min_score = (cfg_override or {}).get("MIN_SCORE", CFG["MIN_SCORE"])
            if regime_name in ("BEAR", "STRONG_BEAR"):
                min_score = max(0, min_score - 10)
            if result["raw_score"] < min_score:
                with lock:
                    rejection_reasons["score_floor"] = rejection_reasons.get("score_floor", 0) + 1
                    counter["rejected"] += 1
                result = None

        with lock:
            if result:
                results.append(result)

            counter["n"] += 1
            counter["cache_hits"] += is_hit
            n       = counter["n"]
            elapsed = time.time() - counter["start"]
            eta     = int((elapsed / n) * (total - n)) if n > 0 else 0

        mode = "BEAR" if bearish else "BULL"
        log.info(f"[{n}/{total}] {mode} {ticker} → {'✅ SIGNAL (' + str(result.get('confidence', '')) + ')' if result else 'skip'}")

        if progress_cb:
            try:
                # Pass a copy to avoid race condition during incremental rendering
                progress_cb(n, total, ticker, eta, list(results))
            except Exception:
                pass

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_process_ticker, t) for t in tickers]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                log.warning(f"Error in processing ticker thread: {e}")

    # Force progress to 100% if deadline or timeout cut the scan short
    if progress_cb and counter["n"] < total:
        try:
            progress_cb(total, total, "Complete (deadline reached)", 0, results)
        except Exception:
            pass

    results.sort(key=lambda x: x["score"], reverse=True)
    
    # Save scan metrics and signal history to SQLite database
    duration = time.time() - counter["start"]
    cpu_usg, mem_mb = get_sys_metrics()
    
    # Write Scan metrics record to DB
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT INTO scan_metrics (
                timestamp, duration_seconds, tickers_scanned, signals_found, cache_hits, cpu_usage, memory_usage
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (now_str, duration, total, len(results), counter["cache_hits"], cpu_usg, mem_mb))
        conn.commit()
        conn.close()
    except Exception as exc:
        log.error(f"Error saving scan metrics: {exc}")

    # Write signal details to SQLite database
    if results:
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            for r in results:
                cursor.execute("""
                    INSERT INTO signal_history (
                        date, ticker, signal_type, entry, stoploss, target1, target2, score, confidence, rs_pct, turnover_crore, scanned_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    r.get("scanned_date", ""),
                    r["symbol"],
                    r["signal_type"],
                    r["entry"],
                    r["stop_loss"],
                    r["target"],
                    r["target2"],
                    r["score"],
                    r["confidence"],
                    r.get("rs_pct", 0.0),
                    r.get("turnover_score", 0.0),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ))
            conn.commit()
            conn.close()
        except Exception as exc:
            log.error(f"Error logging signal history to SQLite: {exc}")

    elapsed = time.time() - scan_start
    log.info(f"⚡ Scan completed: {len(results)} signals "
             f"from {len(tickers)} stocks in "
             f"{elapsed/60:.1f} min ({elapsed:.0f}s)")

    total_scanned = counter["n"]
    total_rejected = counter["rejected"]
    rejection_rate = round((total_rejected / total_scanned * 100), 1) if total_scanned > 0 else 0.0

    scan_meta = {
        "total_scanned"      : total_scanned,
        "signals_found"      : len(results),
        "rejected_count"     : total_rejected,
        "rejection_rate_pct" : rejection_rate,
        "rejection_reasons"  : {k: v for k, v in rejection_reasons.items() if v > 0},
        "regime"             : regime_name,
        "duration_s"         : round(elapsed, 1),
    }

    log.info(f"📋 scan_meta: {scan_meta}")

    # Auto-log signals to trade journal
    try:
        from journal import add_trade, is_trade_logged
        for s in results:
            sym = s.get("symbol")
            sig_date = datetime.now().strftime("%Y-%m-%d")
            sig_type = s.get("signal_type", "Bull")
            entry_price = float(s.get("entry_trigger", 0.0))
            stop_loss = float(s.get("stop_loss", 0.0))
            
            # Check for duplicate entry on the same symbol with identical levels
            if not is_trade_logged(sym, entry_price, stop_loss):
                ltp = float(s.get("ltp", 0.0))
                try:
                    from agent_engine import validate_journal_entry
                    validate_journal_entry(entry_price, ltp, sym)
                except AssertionError as ae:
                    log.warning(f"⚠️ {ae}")
                    continue

                breakdown = s.get("score_breakdown", {})
                notes = f"Auto-logged. Breakdown: Vol={breakdown.get('volume', 0)}, Mom={breakdown.get('momentum', 0)}, Fresh={breakdown.get('freshness', 0)}, RR={breakdown.get('rr', 0)}, Ext={breakdown.get('extension', 0)}, Sector={breakdown.get('sector_bonus', 0)}, HardCap={breakdown.get('hard_cap', False)}"
                
                trade_data = {
                    "symbol": sym,
                    "signal_date": sig_date,
                    "signal_type": sig_type,
                    "conf_grade": s.get("conf_grade", "A"),
                    "raw_score": int(s.get("raw_score", 0)),
                    "regime_score": int(s.get("regime_score", 0)),
                    "regime": s.get("regime", "NEUTRAL"),
                    "entry_price": entry_price,
                    "stop_loss": float(s.get("stop_loss", 0.0)),
                    "target_t1": float(s.get("target", 0.0)),
                    "target_t2": float(s.get("target2")) if s.get("target2") is not None else None,
                    "risk_pct": round(abs(entry_price - float(s.get("stop_loss", 0.0))) / entry_price * 100, 2) if entry_price else 0.0,
                    "rr_ratio": float(s.get("rr_ratio", 0.0)),
                    "notes": notes,
                    "capital": 100000.0,
                    "quantity": 0,
                    "trade_value": 0.0,
                    "risk_amount": 0.0,
                    "actual_entry": entry_price,
                }
                add_trade(trade_data)
                log.info(f"💾 Auto-logged signal {sym} ({sig_type}) to Trade Ledger.")
    except Exception as journal_err:
        log.warning(f"Failed to auto-log signals to trade journal: {journal_err}")

    # Auto-update open trades based on live prices
    try:
        from journal import update_open_trades
        log.info("🔄 Running auto-close checks on OPEN trades...")
        update_open_trades()
    except Exception as journal_update_err:
        log.warning(f"Failed to run auto-close check on open trades: {journal_update_err}")

    return {
        "signals":     results,
        "regime":      regime_data,
        "duration":    elapsed,
        "scan_meta":   scan_meta,
    }


# ─────────────────────────────────────────────────────────────
# CLI TABLE DISPLAY
# ─────────────────────────────────────────────────────────────
def print_table(signals: List[Dict]) -> None:
    if not signals:
        log.info("\n  No signals found with current filters.\n")
        return

    mode = signals[0].get("signal_type", "Bull") if signals else "Bull"
    print("\n" + "=" * 115)
    print(f"  NSE CAMARILLA VOLUME SCANNER v3.0 [{mode.upper()}] — "
          + datetime.now().strftime("%d %b %Y %H:%M"))
    print("=" * 115)
    hdr = (f"  {'SYMBOL':<14}{'SCORE':>6} {'CONF':>4} {'PRICE':>10} {'ENTRY':>10} "
           f"{'TARGET1':>10} {'STOPLOSS':>10} {'RISK%':>6} {'R:R':>5} {'TURNOVER':>9} CANDLE")
    print(hdr)
    print("-" * 115)
    for r in signals:
        print(
            f"  {r['symbol']:<12} {r['score']:>3}/100 "
            f"{r['confidence']:>4} "
            f"Rs.{r['price']:>9,.2f} Rs.{r['entry']:>9,.2f} "
            f"Rs.{r['target']:>9,.2f} Rs.{r['stop_loss']:>9,.2f} "
            f"{r['risk_percentage']:>5.1f}% {r['rr']:>4.1f} {r['turnover_score']:>7.1f}Cr {r['candle']}"
        )
    print("=" * 115)
    print(f"  {len(signals)} signal(s) found\n")


# ─────────────────────────────────────────────────────────────
# ALERT DEDUPLICATION & COOLDOWN ENGINE
# ─────────────────────────────────────────────────────────────
ALERT_HISTORY_FILE = "alert_history.json"

def filter_cooldown_signals(signals: List[Dict], channel: str, cooldown_hours: float = 4.0) -> List[Dict]:
    """
    Advanced cooldown deduplication.
    Prevents repeated alerts for same symbol, same signal, and same entry price zone (within 0.5% buffer)
    during the cooldown period. Expired records are automatically cleaned up.
    Survives application restarts by using alert_history.json.
    """
    now = time.time()
    history: List[Dict] = []
    
    # Load alert history from file
    if os.path.exists(ALERT_HISTORY_FILE):
        try:
            with open(ALERT_HISTORY_FILE, encoding="utf-8") as fh:
                history = json.load(fh)
        except Exception:
            pass

    # Clean old history (older than cooldown_hours)
    cutoff = now - (cooldown_hours * 3600)
    cleaned_history = [entry for entry in history if entry.get("timestamp", 0) > cutoff]

    filtered = []
    duplicate_count = 0
    
    for s in signals:
        symbol = s["symbol"]
        sig_type = s["signal_type"]
        entry_price = s["entry"]
        
        # Check if identical alert exists in cooldown window
        is_duplicate = False
        for entry in cleaned_history:
            if (
                entry.get("symbol") == symbol
                and entry.get("signal_type") == sig_type
                and entry.get("channel") == channel
            ):
                # Entry price buffer check (0.5% tolerance)
                prev_price = entry.get("entry_price", 0.0)
                if prev_price > 0 and abs(entry_price - prev_price) / prev_price <= 0.005:
                    is_duplicate = True
                    break
        
        if is_duplicate:
            duplicate_count += 1
            log.info(f"⏳ [Duplicate Suppressed] {symbol} ({sig_type}) in cooldown zone (Price: ₹{entry_price:.2f})")
            continue
            
        filtered.append(s)
        # Add new alert entry to history
        cleaned_history.append({
            "symbol": symbol,
            "signal_type": sig_type,
            "entry_price": entry_price,
            "channel": channel,
            "timestamp": now
        })
        
        # Save to SQLite table alert_history
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO alert_history 
                (symbol, score, entry, sl, target, rr, rvol, regime, alert_time, channel) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'), ?)
                """,
                (symbol, s.get("score", 70.0), entry_price, s.get("stop_loss", entry_price * 0.985),
                 s.get("target", entry_price * 1.03), s.get("rr", 2.0), s.get("rvol", 2.0),
                 s.get("regime", "NEUTRAL"), channel)
            )
            conn.commit()
            conn.close()
        except Exception as exc:
            log.error(f"Error saving alert to SQLite: {exc}")

    # Save updated history back to file
    try:
        with open(ALERT_HISTORY_FILE, "w", encoding="utf-8") as fh:
            json.dump(cleaned_history, fh, indent=2)
    except Exception:
        pass

    log.info(f"📢 Alerts processed for {channel}: {len(filtered)} dispatched, {duplicate_count} suppressed.")
    return filtered


# ─────────────────────────────────────────────────────────────
# TELEGRAM ALERT
# ─────────────────────────────────────────────────────────────
def send_telegram(
    signals: List[Dict],
    cfg_override: Optional[Dict] = None,
) -> None:
    cfg     = {**CFG, **(cfg_override or {})}
    token   = cfg.get("TG_TOKEN", "")
    chat_id = cfg.get("TG_CHAT_ID", "")
    if not token or not chat_id:
        log.warning("Telegram not configured — skipping.")
        return

    # Apply channel-aware cooldown deduplication (4 hours)
    signals = filter_cooldown_signals(signals, "tg")
    if not signals:
        log.info("No new signals to alert on Telegram after cooldown filtering.")
        return

    now   = datetime.now().strftime("%d %b %Y %H:%M")
    lines = [
        f"📊 *NSE Camarilla Scanner v3.0* — {now}",
        f"🔍 Vol > {cfg['VOL_MULT']}x | EMA filters ON\n",
    ]
    for r in signals[:15]:
        emoji = "🟢" if r.get("signal_type") == "Bull" else "🔴"
        lines.append(
            f"{emoji} *{r['symbol']}* — Confidence: *{r['confidence']}* ({r['signal_strength']})\n"
            f"  Score: {r['score']}/100 | Price: Rs.{r['price']:,.2f} | Entry (Pivot): Rs.{r['entry']:,.2f}\n"
            f"  T1 (H3): Rs.{r['target']:,.2f} | T2 (H4): Rs.{r['target2']:,.2f} | SL (L3): Rs.{r['stop_loss']:,.2f}\n"
            f"  R:R: {r['rr']:.1f}x | Risk: {r['risk_percentage']}% | RS (20d): {r['rs_pct']:+.1f}%\n"
        )
    if len(signals) > 15:
        lines.append(f"_... and {len(signals) - 15} more signals_")

    msg = "\n".join(lines)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
        if resp.ok:
            log.info("✅ Telegram alert sent")
        else:
            log.error(f"Telegram error: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        log.error(f"Telegram exception: {exc}")


# ─────────────────────────────────────────────────────────────
# WHATSAPP ALERT  (via Twilio)
# ─────────────────────────────────────────────────────────────
def send_whatsapp(
    signals: List[Dict],
    cfg_override: Optional[Dict] = None,
) -> None:
    cfg   = {**CFG, **(cfg_override or {})}
    sid   = cfg.get("TWILIO_SID",   "")
    token = cfg.get("TWILIO_TOKEN", "")
    from_ = cfg.get("TWILIO_FROM",  "whatsapp:+14155238886")
    to    = cfg.get("TWILIO_TO",    "")
    if not all([sid, token, to]):
        log.warning("WhatsApp (Twilio) not configured — skipping.")
        return

    # Apply channel-aware cooldown deduplication (4 hours)
    signals = filter_cooldown_signals(signals, "wa")
    if not signals:
        log.info("No new signals to alert on WhatsApp after cooldown filtering.")
        return

    now   = datetime.now().strftime("%d %b %Y %H:%M")
    lines = [f"📊 NSE Scanner v3.0 — {now}", f"Vol>{cfg['VOL_MULT']}x\n"]
    for r in signals[:10]:
        emoji = "🟢" if r.get("signal_type") == "Bull" else "🔴"
        lines.append(
            f"{emoji} {r['symbol']} ({r['score']}/100 - {r['confidence']})\n"
            f"  Price Rs.{r['price']:,.2f} | Entry (Pivot) Rs.{r['entry']:,.2f}\n"
            f"  T1 Rs.{r['target']:,.2f} | T2 Rs.{r['target2']:,.2f} | SL Rs.{r['stop_loss']:,.2f}\n"
            f"  R:R: {r['rr']:.1f}x | Vol: {r['vol_ratio']:.1f}x | {r['candle']}\n"
        )
    msg = "\n".join(lines)
    url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
    try:
        resp = requests.post(
            url,
            data={"From": from_, "To": to, "Body": msg},
            auth=(sid, token),
            timeout=10,
        )
        if resp.status_code in (200, 201):
            log.info("✅ WhatsApp alert sent")
        else:
            log.error(f"WhatsApp error: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        log.error(f"WhatsApp exception: {exc}")


# ─────────────────────────────────────────────────────────────
# SAVE TO CSV  (returns filename)
# ─────────────────────────────────────────────────────────────
def save_csv(signals: List[Dict]) -> str:
    if not signals:
        return ""
    rows = []
    for r in signals:
        rows.append({
            "Symbol":          r["symbol"],
            "Score":           r["score"],
            "Regime_Score":    r.get("regime_score", r["score"]),
            "Confidence":      r["confidence"],
            "Strength":        r["signal_strength"],
            "Signal":          r.get("signal_type", "Bull"),
            "Price":           r["price"],
            "Entry":           r["entry"],
            "Target_H3":       r["target"],
            "Target_H4":       r["target2"],
            "StopLoss_L3":     r["stop_loss"],
            "Risk_%":          r["risk_percentage"],
            "R:R":             r["rr"],
            "Upside_%":        r["upside"],
            "Vol_Ratio":       r["vol_ratio"],
            "Candle":          r["candle"],
            "EMA10":           r["ema10"],
            "EMA20":           r["ema20"],
            "EMA50":           r["ema50"],
            "EMA200":          r["ema200"],
            "HV_High":         r["hv_high"],
            "HV_Low":          r["hv_low"],
            "HV_Date":         r["hv_date"],
            "Days":            r["days"],
            "EMA20_Slope":     r.get("ema20_slope", 0.0),
            "ATR_Dist":        r.get("atr_dist", 0.0),
            "Vol_Percentile":  r.get("vol_percentile", 0.0),
            "Range_Expansion": r.get("range_expansion", 1.0),
            "ATR":             r.get("atr", 0.0),
            "RS_Pct":          r.get("rs_pct", 0.0),
            "Turnover_Cr":     r.get("turnover_score", 0.0),
            "Sparkline":       json.dumps(r.get("sparkline", [])),  # Persist sparkline list as JSON string
            "Scanned_At":      datetime.now().strftime("%Y-%m-%d %H:%M"),
        })
    fname = f"scan_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    pd.DataFrame(rows).to_csv(fname, index=False)
    log.info(f"📁 Results saved to {fname}")
    return fname


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main(
    continuous: bool = False,
    custom_tickers: Optional[List[str]] = None,
    bearish: bool = False,
) -> None:
    tickers = custom_tickers or NIFTY500_SAMPLE
    mode    = "BEARISH" if bearish else "BULLISH"

    while True:
        log.info(f"🔎 Starting {mode} scan — {len(tickers)} stocks")
        scan_res = run_scan(tickers, bearish=bearish)
        signals = scan_res["signals"] if isinstance(scan_res, dict) else scan_res
        print_table(signals)
        save_csv(signals)
        if signals:
            send_telegram(signals)
            send_whatsapp(signals)
        if not continuous:
            break
        log.info(f"⏳ Next scan in {CFG['INTERVAL']}s — Ctrl+C to stop")
        time.sleep(CFG["INTERVAL"])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NSE Camarilla Volume Scanner v3.0 [Phase 2]")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously on interval")
    parser.add_argument("--bearish",    action="store_true",
                        help="Run bearish scan (price below EMAs)")
    parser.add_argument("--tickers",    nargs="*",
                        help="Custom ticker list  e.g. RELIANCE.NS TCS.NS")
    parser.add_argument("--vol-days",   type=int,
                        help="Volume average window in days (default 10)")
    parser.add_argument("--vol-mult",   type=float,
                        help="Volume multiplier threshold (default 2.0)")
    parser.add_argument("--workers",    type=int,
                        help="Concurrent download threads (default 6)")
    args = parser.parse_args()

    if args.vol_days: CFG["VOL_DAYS"]    = args.vol_days
    if args.vol_mult: CFG["VOL_MULT"]    = args.vol_mult
    if args.workers:  CFG["MAX_WORKERS"] = args.workers

    main(
        continuous=args.continuous,
        custom_tickers=args.tickers,
        bearish=args.bearish,
    )
