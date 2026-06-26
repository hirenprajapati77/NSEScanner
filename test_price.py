import json, sys, io, os, time, glob
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Read current scan_status.json
d = json.load(open('scan_status.json', encoding='utf-8'))
sigs = d.get('signals', [])
print(f"scan_status.json: {len(sigs)} signals, last_scan={d.get('last_scan')}, scanning={d.get('scanning')}")
for s in sigs:
    print(f"  {s['symbol']}: price={s['price']}, change={s['change']}, time={s['scanned_time']}")

# Check all cache files
print("\nCache files:")
caches = sorted(glob.glob('data_cache/*.csv'))
import pandas as pd
for f in caches[:10]:
    sym = os.path.basename(f)
    age_min = (time.time() - os.path.getmtime(f)) / 60
    try:
        df = pd.read_csv(f, index_col=0, parse_dates=True)
        if isinstance(df.columns[0], tuple):
            df.columns = [c[0].lower() for c in df.columns]
        else:
            df.columns = [str(c).lower() for c in df.columns]
        last_close = float(df['close'].iloc[-1])
        last_date = df.index[-1].date()
        print(f"  {sym}: age={age_min:.0f}min, last_date={last_date}, close={last_close:.2f}")
    except Exception as e:
        print(f"  {sym}: ERROR {e}")

# Check scan CSV exports for recent scan
scan_csvs = sorted(glob.glob('scan_*.csv'), reverse=True)[:3]
print(f"\nRecent scan CSVs: {scan_csvs}")
for f in scan_csvs:
    print(f"\n  {f}:")
    try:
        df = pd.read_csv(f)
        print(df[['Symbol', 'Price', 'Change_Pct', 'Scanned_Time']].to_string() if 'Symbol' in df.columns else df.head(3).to_string())
    except Exception as e:
        print(f"  ERROR: {e}")
