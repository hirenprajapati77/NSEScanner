"""
Trade Journal Backend — journal.py
Handles all DB operations for the trade journal.
"""

import sqlite3
import os
from datetime import datetime
from typing import Optional, List, Dict

# Points directly to the active scanner.db in the project root
DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "scanner.db"
)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables and run v5.0 migrations. Call on app startup."""
    with get_conn() as conn:
        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(alert_history)")
            cols = [r[1] for r in cursor.fetchall()]
            if cols and 'alert_time' not in cols:
                conn.execute("DROP TABLE alert_history")
        except Exception:
            pass

        conn.executescript("""
        CREATE TABLE IF NOT EXISTS trade_journal (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT NOT NULL,
            signal_date     TEXT NOT NULL,
            signal_type     TEXT DEFAULT 'Bull',
            conf_grade      TEXT DEFAULT 'A',
            raw_score       INTEGER DEFAULT 0,
            regime_score    INTEGER DEFAULT 0,
            regime          TEXT DEFAULT 'NEUTRAL',
            entry_price     REAL NOT NULL,
            stop_loss       REAL NOT NULL,
            target_t1       REAL NOT NULL,
            target_t2       REAL,
            risk_pct        REAL,
            rr_ratio        REAL,
            capital         REAL DEFAULT 0,
            quantity        INTEGER DEFAULT 0,
            trade_value     REAL DEFAULT 0,
            risk_amount     REAL DEFAULT 0,
            actual_entry    REAL,
            actual_exit     REAL,
            exit_date       TEXT,
            exit_reason     TEXT,
            pnl_points      REAL DEFAULT 0,
            pnl_pct         REAL DEFAULT 0,
            pnl_amount      REAL DEFAULT 0,
            rr_achieved     REAL DEFAULT 0,
            outcome         TEXT DEFAULT 'OPEN',
            notes           TEXT DEFAULT '',
            screenshot_url  TEXT DEFAULT '',
            created_at      TEXT DEFAULT (datetime('now','localtime')),
            updated_at      TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_journal_symbol
            ON trade_journal(symbol);
        CREATE INDEX IF NOT EXISTS idx_journal_outcome
            ON trade_journal(outcome);
        CREATE INDEX IF NOT EXISTS idx_journal_date
            ON trade_journal(signal_date);

        -- v5.0: Alert history audit table (Section 7)
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
        );
        CREATE INDEX IF NOT EXISTS idx_alerts_symbol ON alert_history(symbol);
        CREATE INDEX IF NOT EXISTS idx_alerts_time   ON alert_history(alert_time);

        -- v5.0: watchlist table (Section 5)
        CREATE TABLE IF NOT EXISTS watchlist (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL UNIQUE,
            entry_price REAL,
            sl          REAL,
            target      REAL,
            sector      TEXT DEFAULT '',
            notes       TEXT DEFAULT '',
            added_date  TEXT DEFAULT (datetime('now','localtime')),
            manually_closed INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_watchlist_symbol ON watchlist(symbol);
        """)

        # v5.0 ALTER TABLE migrations — safe on existing DB
        for migration in [
            "ALTER TABLE trade_journal ADD COLUMN status TEXT DEFAULT 'OPEN'",
            "ALTER TABLE trade_journal ADD COLUMN days_held INTEGER DEFAULT 0",
            "ALTER TABLE trade_journal ADD COLUMN exit_price REAL",
            "ALTER TABLE trade_journal ADD COLUMN mtm REAL DEFAULT 0",
        ]:
            try:
                conn.execute(migration)
            except Exception:
                pass  # column already exists — safe to ignore

        # Indexes on new columns
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trade_journal(status)")
        except Exception:
            pass


def add_trade(data: Dict) -> int:
    """Insert a new trade. Returns new row id."""
    sql = """
    INSERT INTO trade_journal (
        symbol, signal_date, signal_type, conf_grade,
        raw_score, regime_score, regime,
        entry_price, stop_loss, target_t1, target_t2,
        risk_pct, rr_ratio,
        capital, quantity, trade_value, risk_amount,
        actual_entry, notes
    ) VALUES (
        :symbol, :signal_date, :signal_type, :conf_grade,
        :raw_score, :regime_score, :regime,
        :entry_price, :stop_loss, :target_t1, :target_t2,
        :risk_pct, :rr_ratio,
        :capital, :quantity, :trade_value, :risk_amount,
        :actual_entry, :notes
    )"""
    with get_conn() as conn:
        cur = conn.execute(sql, {
            "symbol":       data.get("symbol", ""),
            "signal_date":  data.get("signal_date") or datetime.now().strftime("%Y-%m-%d"),
            "signal_type":  data.get("signal_type", "Bull"),
            "conf_grade":   data.get("conf_grade", "A"),
            "raw_score":    data.get("raw_score", 0),
            "regime_score": data.get("regime_score", 0),
            "regime":       data.get("regime", "NEUTRAL"),
            "entry_price":  data.get("entry_price", 0),
            "stop_loss":    data.get("stop_loss", 0),
            "target_t1":    data.get("target_t1", 0),
            "target_t2":    data.get("target_t2"),
            "risk_pct":     data.get("risk_pct", 0),
            "rr_ratio":     data.get("rr_ratio", 0),
            "capital":      data.get("capital", 0),
            "quantity":     data.get("quantity", 0),
            "trade_value":  data.get("trade_value", 0),
            "risk_amount":  data.get("risk_amount", 0),
            "actual_entry": data.get("actual_entry") or data.get("entry_price"),
            "notes":        data.get("notes", ""),
        })
        return cur.lastrowid


def close_trade(trade_id: int, data: Dict) -> Dict:
    """
    Close a trade with exit price and reason.
    Auto-calculates PnL, outcome, rr_achieved.
    """
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM trade_journal WHERE id=?", (trade_id,)
        ).fetchone()
        if not row:
            return {"error": "Trade not found"}

        actual_entry = data.get("actual_entry") or row["actual_entry"] or row["entry_price"]
        actual_exit  = data.get("actual_exit", 0)
        quantity     = data.get("quantity")     or row["quantity"] or 1
        exit_reason  = data.get("exit_reason", "MANUAL")
        notes        = data.get("notes", row["notes"] or "")

        is_bull = row["signal_type"].lower() == "bull"

        # Calculate PnL
        pnl_points = round(actual_exit - actual_entry, 2)
        pnl_pct    = round((pnl_points / actual_entry) * 100, 2) if actual_entry else 0.0
        pnl_amount = round(pnl_points * quantity, 2)

        # Risk per share (absolute distance to prevent sign inversion on tight/flipped stops)
        risk_per_share = abs(actual_entry - row["stop_loss"])

        # Realized R:R achieved
        if risk_per_share and risk_per_share != 0:
            rr_achieved = round(pnl_points / risk_per_share, 2) if is_bull else round((-pnl_points) / risk_per_share, 2)
        else:
            rr_achieved = 0.0

        # Outcome
        if pnl_points > 0:
            outcome = "WIN"
        elif pnl_points < 0:
            outcome = "LOSS"
        else:
            outcome = "EXPIRED"

        conn.execute("""
            UPDATE trade_journal SET
                actual_entry  = ?,
                actual_exit   = ?,
                exit_date     = ?,
                exit_reason   = ?,
                pnl_points    = ?,
                pnl_pct       = ?,
                pnl_amount    = ?,
                rr_achieved   = ?,
                outcome       = ?,
                notes         = ?,
                updated_at    = datetime('now','localtime')
            WHERE id = ?
        """, (actual_entry, actual_exit,
              data.get("exit_date") or datetime.now().strftime("%Y-%m-%d"),
              exit_reason, pnl_points, pnl_pct, pnl_amount,
              rr_achieved, outcome, notes, trade_id))

        return {
            "id": trade_id,
            "outcome": outcome,
            "pnl_amount": pnl_amount,
            "pnl_pct": pnl_pct,
            "rr_achieved": rr_achieved,
        }


def get_trades(outcome: str = None, limit: int = 200) -> List[Dict]:
    """Fetch trades. Pass outcome='OPEN'/'WIN'/'LOSS' to filter."""
    sql = "SELECT * FROM trade_journal"
    params = []
    if outcome:
        sql += " WHERE outcome = ?"
        params.append(outcome)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def delete_trade(trade_id: int) -> bool:
    with get_conn() as conn:
        conn.execute("DELETE FROM trade_journal WHERE id=?", (trade_id,))
        return True


def get_scorecard() -> Dict:
    """
    Calculate live performance metrics from all closed trades.
    Returns scorecard dict for dashboard header display.
    """
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT outcome, pnl_amount, pnl_pct, rr_achieved, rr_ratio, regime
            FROM trade_journal
            WHERE outcome IN ('WIN','LOSS','EXPIRED')
        """).fetchall()

    # Default empty structure for regime stats
    regimes_data = {
        "STRONG_BULL": {"total": 0, "wins": 0},
        "NEUTRAL": {"total": 0, "wins": 0},
        "BEAR": {"total": 0, "wins": 0}
    }

    if not rows:
        regime_stats = {
            cat: {"total": 0, "wins": 0, "win_rate": 0.0}
            for cat in regimes_data
        }
        return {
            "total": 0, "wins": 0, "losses": 0,
            "win_rate": 0.0, "avg_rr": 0.0,
            "profit_factor": 0.0, "total_pnl": 0.0,
            "expectancy": 0.0, "open": len(get_trades(outcome="OPEN")),
            "regime_stats": regime_stats
        }

    total    = len(rows)
    wins     = sum(1 for r in rows if r["outcome"] == "WIN")
    losses   = sum(1 for r in rows if r["outcome"] == "LOSS")
    win_rate = round((wins / total) * 100, 1) if total else 0.0

    gross_profit = sum(r["rr_achieved"] for r in rows if r["outcome"] == "WIN" and r["rr_achieved"])
    gross_loss   = abs(sum(r["rr_achieved"] for r in rows if r["outcome"] == "LOSS" and r["rr_achieved"]))
    total_pnl    = sum(r["pnl_amount"] for r in rows)

    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else None
    avg_rr = round(
        sum(r["rr_achieved"] for r in rows) / total, 2
    ) if total else 0.0

    # Expectancy = (Win% * Avg Win) - (Loss% * Avg Loss)
    avg_win  = gross_profit / wins   if wins   else 0.0
    avg_loss = - (gross_loss / losses) if losses else 0.0
    expectancy = round(
        ((win_rate / 100.0) * avg_win) - ((1.0 - win_rate / 100.0) * abs(avg_loss)), 2
    )


    # Calculate regime stats
    for r in rows:
        reg = (r["regime"] or "NEUTRAL").upper()
        if "BULL" in reg:
            category = "STRONG_BULL"
        elif "BEAR" in reg:
            category = "BEAR"
        else:
            category = "NEUTRAL"
            
        regimes_data[category]["total"] += 1
        if r["outcome"] == "WIN":
            regimes_data[category]["wins"] += 1

    regime_stats = {}
    for cat, data in regimes_data.items():
        total_cat = data["total"]
        wins_cat = data["wins"]
        win_rate_cat = round((wins_cat / total_cat) * 100, 1) if total_cat else 0.0
        regime_stats[cat] = {
            "total": total_cat,
            "wins": wins_cat,
            "win_rate": win_rate_cat
        }

    return {
        "total":         total,
        "wins":          wins,
        "losses":        losses,
        "open":          len(get_trades(outcome="OPEN")),
        "win_rate":      win_rate,
        "avg_rr":        avg_rr,
        "profit_factor": profit_factor,
        "total_pnl":     round(total_pnl, 2),
        "gross_profit":  round(gross_profit, 2),
        "gross_loss":    round(gross_loss, 2),
        "expectancy":    expectancy,
        "avg_win":       round(avg_win, 2),
        "avg_loss":      round(avg_loss, 2),
        "regime_stats":  regime_stats
    }



def is_trade_logged(symbol: str, entry_price: float, stop_loss: float) -> bool:
    """Check if a trade has already been logged for this symbol with identical entry and stop loss levels."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM trade_journal WHERE symbol = ? AND abs(entry_price - ?) < 0.01 AND abs(stop_loss - ?) < 0.01",
            (symbol, entry_price, stop_loss)
        ).fetchone()
        return row is not None


def update_open_trades():
    """
    Check all OPEN trades against their live LTP.
    Auto-closes trades that have hit target_t1 or stop_loss.
    """
    from data_fetcher import get_stock_price
    
    open_trades = get_trades(outcome="OPEN")
    if not open_trades:
        return
        
    for t in open_trades:
        sym = t["symbol"].strip().upper()
        clean_sym = sym.replace(".NS", "")
        try:
            ltp, source, status = get_stock_price(clean_sym)
            if not ltp or float(ltp) <= 0:
                continue
            ltp = round(float(ltp), 2)
        except Exception:
            continue
            
        sig_type = t.get("signal_type", "Bull")
        entry_price = float(t.get("entry_price", 0.0))
        stop_loss = float(t.get("stop_loss", 0.0))
        target_t1 = float(t.get("target_t1", 0.0))
        quantity = int(t.get("quantity") or 0)
        
        outcome = None
        exit_price = None
        achieved_rr = 0.0
        
        if sig_type == "Bull":
            if ltp >= target_t1:
                outcome = "WIN"
                exit_price = target_t1
                denom = abs(entry_price - stop_loss)
                achieved_rr = round((target_t1 - entry_price) / denom, 2) if denom != 0 else 0.0
            elif ltp <= stop_loss:
                outcome = "LOSS"
                exit_price = stop_loss
                denom = abs(entry_price - stop_loss)
                achieved_rr = round((stop_loss - entry_price) / denom, 2) if denom != 0 else -1.0
        elif sig_type == "Bear":
            if ltp <= target_t1:
                outcome = "WIN"
                exit_price = target_t1
                denom = abs(entry_price - stop_loss)
                achieved_rr = round((entry_price - target_t1) / denom, 2) if denom != 0 else 0.0
            elif ltp >= stop_loss:
                outcome = "LOSS"
                exit_price = stop_loss
                denom = abs(entry_price - stop_loss)
                achieved_rr = round((entry_price - stop_loss) / denom, 2) if denom != 0 else -1.0
                
        if outcome and exit_price is not None:
            pnl_points = round(exit_price - entry_price, 2)
            pnl_pct    = round((pnl_points / entry_price) * 100, 2) if entry_price else 0.0
            pnl_amount = round(pnl_points * quantity, 2)
            
            exit_date = datetime.now().strftime("%Y-%m-%d")
            notes = (t.get("notes") or "") + f" | Auto-closed at {exit_price} ({outcome}) via update_open_trades"
            
            with get_conn() as conn:
                conn.execute("""
                    UPDATE trade_journal SET
                        actual_exit   = ?,
                        exit_date     = ?,
                        exit_reason   = ?,
                        pnl_points    = ?,
                        pnl_pct       = ?,
                        pnl_amount    = ?,
                        rr_achieved   = ?,
                        outcome       = ?,
                        notes         = ?,
                        updated_at    = datetime('now','localtime')
                    WHERE id = ?
                """, (exit_price, exit_date, "AUTO_CLOSE", pnl_points, pnl_pct, pnl_amount,
                      achieved_rr, outcome, notes, t["id"]))
            print(f"Auto-closed trade {sym} ({sig_type}) -> {outcome} at exit_price={exit_price}, achieved_rr={achieved_rr}")


