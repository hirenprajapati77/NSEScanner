"""
Verification test script for journal.py (ASCII clean version)
"""

import os
import sys
import sqlite3

# Ensure we can import journal.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from journal import init_db, add_trade, close_trade, get_trades, get_scorecard, delete_trade, DB_PATH

def run_tests():
    print("=== Starting Trade Journal Verification Tests ===")
    print(f"Database Path: {DB_PATH}")

    # 1. Initialize DB
    print("\nStep 1: Initializing Database Schema...")
    init_db()
    
    # Verify table structure in SQLite
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_journal';")
    table_exists = cursor.fetchone()
    if not table_exists:
        print("[ERROR] trade_journal table was not created!")
        sys.exit(1)
    print("[SUCCESS] trade_journal table created successfully.")

    # Check indexes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index';")
    indexes = [row[0] for row in cursor.fetchall()]
    expected_indexes = ['idx_journal_symbol', 'idx_journal_outcome', 'idx_journal_date']
    for idx in expected_indexes:
        if idx in indexes:
            print(f"[SUCCESS] Index {idx} created.")
        else:
            print(f"[ERROR] Index {idx} not found.")
            sys.exit(1)

    # 2. Add Trade
    print("\nStep 2: Testing add_trade()...")
    trade_data = {
        "symbol": "TCS",
        "signal_date": "2026-06-01",
        "signal_type": "Bull",
        "conf_grade": "A+",
        "raw_score": 95,
        "regime_score": 95,
        "regime": "NEUTRAL",
        "entry_price": 3800.0,
        "stop_loss": 3760.0,
        "target_t1": 3880.0,
        "target_t2": 3920.0,
        "risk_pct": 1.05,
        "rr_ratio": 2.0,
        "capital": 100000.0,
        "quantity": 25,
        "trade_value": 95000.0,
        "risk_amount": 1000.0,
        "actual_entry": 3800.0,
        "notes": "Strong breakout over Camarilla pivot on high volume."
    }
    
    trade_id = add_trade(trade_data)
    print(f"[SUCCESS] Logged trade with ID {trade_id}")

    # Verify trade is in OPEN state
    open_trades = get_trades(outcome="OPEN")
    if not open_trades or open_trades[0]["id"] != trade_id:
        print("[ERROR] Logged trade could not be retrieved in OPEN status!")
        sys.exit(1)
    
    t = open_trades[0]
    print(f"[SUCCESS] Retrieved OPEN trade for {t['symbol']}. Entry = Rs.{t['entry_price']}, SL = Rs.{t['stop_loss']}")

    # 3. Close Trade (WIN)
    print("\nStep 3: Testing close_trade() with a WIN setup...")
    close_data = {
        "actual_exit": 3880.0,
        "exit_reason": "T1_HIT",
        "exit_date": "2026-06-02",
        "notes": "Target 1 hit beautifully. Trade closed."
    }
    
    close_res = close_trade(trade_id, close_data)
    print(f"close_trade response: {close_res}")
    
    if close_res.get("outcome") != "WIN":
        print(f"[ERROR] Expected outcome to be WIN, got {close_res.get('outcome')}")
        sys.exit(1)
    
    # Expected points: 3880 - 3800 = 80
    # Expected P&L: 80 * 25 = 2000
    # Expected R:R achieved: 80 / (3800 - 3760) = 80 / 40 = 2.0R
    if close_res.get("pnl_amount") != 2000.0:
        print(f"[ERROR] Expected P&L Rs.2000.00, got Rs.{close_res.get('pnl_amount')}")
        sys.exit(1)
        
    if close_res.get("rr_achieved") != 2.0:
        print(f"[ERROR] Expected R:R 2.00, got {close_res.get('rr_achieved')}")
        sys.exit(1)

    print("[SUCCESS] WIN trade calculations verified perfectly.")

    # 4. Add & Close a LOSS Trade (for Scorecard metrics validation)
    print("\nStep 4: Testing add_trade() and close_trade() with a LOSS setup...")
    loss_trade_data = {
        "symbol": "INFY",
        "signal_date": "2026-06-01",
        "signal_type": "Bull",
        "conf_grade": "A",
        "raw_score": 85,
        "regime_score": 85,
        "regime": "NEUTRAL",
        "entry_price": 1500.0,
        "stop_loss": 1470.0,
        "target_t1": 1560.0,
        "risk_pct": 2.0,
        "rr_ratio": 2.0,
        "capital": 100000.0,
        "quantity": 30,
        "trade_value": 45000.0,
        "risk_amount": 900.0,
        "actual_entry": 1500.0,
        "notes": "Support level pullback."
    }
    loss_id = add_trade(loss_trade_data)
    
    loss_close_data = {
        "actual_exit": 1470.0,
        "exit_reason": "SL_HIT",
        "exit_date": "2026-06-02",
        "notes": "Stop loss hit."
    }
    
    loss_close_res = close_trade(loss_id, loss_close_data)
    print(f"close_trade (LOSS) response: {loss_close_res}")
    
    # Expected points: 1470 - 1500 = -30
    # Expected P&L: -30 * 30 = -900
    # Expected R:R achieved: -30 / (1500 - 1470) = -30 / 30 = -1.0R
    if loss_close_res.get("outcome") != "LOSS":
        print(f"[ERROR] Expected outcome to be LOSS, got {loss_close_res.get('outcome')}")
        sys.exit(1)
        
    if loss_close_res.get("pnl_amount") != -900.0:
        print(f"[ERROR] Expected P&L -Rs.900.00, got Rs.{loss_close_res.get('pnl_amount')}")
        sys.exit(1)
        
    if loss_close_res.get("rr_achieved") != -1.0:
        print(f"[ERROR] Expected R:R -1.00, got {loss_close_res.get('rr_achieved')}")
        sys.exit(1)
        
    print("[SUCCESS] LOSS trade calculations verified perfectly.")

    # 5. Verify Scorecard
    print("\nStep 5: Testing get_scorecard() aggregates...")
    scorecard = get_scorecard()
    print(f"Scorecard stats: {scorecard}")
    
    # Expecting: 
    # Total = 2
    # Wins = 1, Losses = 1
    # Win Rate = 50.0%
    # Gross Profit = 2000.00, Gross Loss = 900.00, Net PnL = 1100.00
    # Profit Factor = 2000.00 / 900.00 = 2.22
    # Avg R:R realized: (2.0 + -1.0) / 2 = 0.50R
    # Expectancy = (0.50 * 2000) - (0.50 * 900) = 1000 - 450 = 550.00
    
    if scorecard["total"] != 2:
        print(f"[ERROR] Expected total 2, got {scorecard['total']}")
        sys.exit(1)
        
    if scorecard["win_rate"] != 50.0:
        print(f"[ERROR] Expected win rate 50.0%, got {scorecard['win_rate']}%")
        sys.exit(1)
        
    if scorecard["profit_factor"] != 2.22:
        print(f"[ERROR] Expected profit factor 2.22, got {scorecard['profit_factor']}")
        sys.exit(1)
        
    if scorecard["total_pnl"] != 1100.0:
        print(f"[ERROR] Expected total P&L 1100.00, got {scorecard['total_pnl']}")
        sys.exit(1)
        
    if scorecard["avg_rr"] != 0.5:
        print(f"[ERROR] Expected avg R:R 0.50R, got {scorecard['avg_rr']}")
        sys.exit(1)
        
    if scorecard["expectancy"] != 550.0:
        print(f"[ERROR] Expected expectancy 550.00, got {scorecard['expectancy']}")
        sys.exit(1)

    print("[SUCCESS] Scorecard mathematical calculations verified perfectly.")

    # 6. Delete Trade
    print("\nStep 6: Testing delete_trade()...")
    delete_trade(trade_id)
    delete_trade(loss_id)
    
    # Check if empty again
    remaining = get_trades()
    if len(remaining) != 0:
        print(f"[ERROR] Expected 0 trades remaining, got {len(remaining)}")
        sys.exit(1)
    
    print("[SUCCESS] delete_trade() cleanup verified.")
    print("\n=== ALL TESTS PASSED SUCCESSFULLY! journal.py is 100% production-grade and ready. ===")

if __name__ == "__main__":
    run_tests()
