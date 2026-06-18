# NSE Camarilla Volume Scanner

Scans NSE stocks using 5-year Yahoo Finance data, applies:
- **Camarilla pivot levels** → Entry (Pivot), Target (H3), Stop Loss (L3)
- **Volume spike filter** → today's volume > N-day average × multiplier
- **EMA filters** → price must be above EMA 10, 20, 50, 200
- **Telegram & WhatsApp alerts** on every new signal batch

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure alerts
```bash
cp .env.example .env
# Edit .env and fill in your Telegram Bot Token + Chat ID
# and/or Twilio WhatsApp credentials
```

### 3a. Run CLI scanner (one-time)
```bash
python scanner.py
```

### 3b. Run CLI scanner continuously (every 5 min)
```bash
python scanner.py --continuous
```

### 3c. Run Web Dashboard
```bash
python dashboard.py
# Open http://localhost:5000
```

---

## CLI Options
```
python scanner.py --vol-days 20 --vol-mult 2.5 --continuous
python scanner.py --tickers RELIANCE.NS TCS.NS INFY.NS
```

---

## How Camarilla Levels Work

```
H4  ← Breakout target (strong bull)
H3  ← 🎯 TARGET  (Camarilla resistance)
──────────────────────────────────────
PIVOT  ← 📍 ENTRY  (fair value)
──────────────────────────────────────
L3  ← 🛑 STOP LOSS  (Camarilla support)
L4  ← Hard stop (strong bear signal)
```

Formulas:
- Range = High − Low (Prior Session)
- H3 = Close + Range × 1.1 / 4
- L3 = Close − Range × 1.1 / 4
- H4 = Close + Range × 1.1 / 2
- L4 = Close − Range × 1.1 / 2
- Pivot = (High + Low + Close) / 3

---

## Telegram Setup (Free)

1. Open Telegram → search `@BotFather`
2. Send `/newbot` → follow prompts → copy **Bot Token**
3. Add the bot to your group/channel
4. Get your Chat ID: forward any group message to `@userinfobot`
5. Paste both into `.env`

---

## WhatsApp Setup (Twilio Free Tier)

1. Sign up free at [twilio.com](https://twilio.com)
2. Go to **Messaging → Try it out → Send a WhatsApp message**
3. Join the sandbox by sending the join code from your WhatsApp
4. Copy Account SID and Auth Token from your Twilio console
5. Paste into `.env` along with your WhatsApp number (`+91XXXXXXXXXX`)

---

## Score System (0–100)

| Condition                   | Points (Max) |
|-----------------------------|--------------|
| Base score                  | 30           |
| Volume Bonus (≥3x/2x/1.5x/1x)| +25/18/10/5  |
| Momentum Bonus (RSI/EMA/MACD)| +20          |
| Freshness Bonus (≥0.8/0.5/0.2)| +10/7/3      |
| R:R Ratio Bonus (≥3.0/2.0)  | +10/5        |
| Sector Bonus (Rank 1-3/4-7) | +5/3         |

### Grades
- **A**: Score ≥ 80
- **B**: Score ≥ 65
- **C**: Score ≥ 50
- **REJECT**: Score < 50

---

## Stock Universe

`scanner.py` includes ~115 Nifty 500 stocks by default.
To add your own:
```python
NIFTY500_SAMPLE = ["YOURTICKER.NS", "ANOTHER.NS", ...]
```
All NSE tickers on Yahoo Finance use the `.NS` suffix.

---

## Output CSV

Each scan saves a timestamped CSV: `scan_YYYYMMDD_HHMM.csv`
with columns: Symbol, Score, Price, Entry, Target_H3, StopLoss_L3,
Upside_%, Vol_Ratio, Candle, EMA10/20/50/200, HV_High, HV_Low.
