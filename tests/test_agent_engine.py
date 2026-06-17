# -*- coding: utf-8 -*-
"""
Unit tests for MACD divergence penalty and rank-based sector bonus logic.
"""
import os
import sys
import pandas as pd
from datetime import datetime

# Add the project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import agent_engine

def generate_mock_df(close_prices, bullish=True, high_volume=False):
    n = len(close_prices)
    dates = pd.date_range(end=datetime.now().date(), periods=n, freq='D')
    
    volume = [1000000.0] * n
    if high_volume:
        volume[-1] = 3000000.0
        
    opens = [100.0] * n
    highs = [100.0] * n
    lows = [100.0] * n
    
    for i, c in enumerate(close_prices):
        opens[i] = c
        highs[i] = c * 1.01
        lows[i] = c * 0.99
        
    prev_close = close_prices[-2]
    curr_close = close_prices[-1]
    
    if bullish:
        rng = (curr_close - prev_close) * 4.0 / 1.1
        prev_open = prev_close / 1.03
    else:
        rng = (prev_close - curr_close) * 4.0 / 1.1
        prev_open = prev_close * 1.03
        
    if rng < 2.0:
        rng = 2.0
        
    prev_high = prev_close + rng / 2.0
    prev_low = prev_close - rng / 2.0
    
    opens[-2] = prev_open
    highs[-2] = prev_high
    lows[-2] = prev_low
    
    # Ensure 52w high is on last day to pass freshness check
    max_prev_high = max(highs[:-1])
    highs[-1] = max(max_prev_high + 10.0, curr_close * 1.02)
    lows[-1] = curr_close * 0.98
    opens[-1] = prev_close
    
    df = pd.DataFrame({
        "open": opens,
        "high": highs,
        "low": lows,
        "close": close_prices,
        "volume": volume
    }, index=dates)
    return df

def test_macd_bearish_divergence_penalty():
    """
    Inject a bullish MACD crossover (macd crosses above signal)
    but with a declining histogram slope (hist[-1] < hist[-3]).
    Assert that mom_bonus is reduced by 8, not increased by 5.
    Assert score_breakdown["macd_divergence"] == True.
    """
    p_bull_div = [100.0] * 66 + [100.0, 98.2857, 101.2857, 98.2857, 100.8571]
    df = generate_mock_df(p_bull_div, bullish=True, high_volume=True)
    res = agent_engine.analyse(
        ticker="HDFCBANK.NS",
        bearish=False,
        df=df,
        market_context={"REGIME": "STRONG_BULL", "SECTOR_MOMENTUM": {}},
        is_nse_market_open=lambda: False,
        cfg_override={"MIN_TURNOVER_CR": 0.0, "MIN_SCORE": 0}
    )
    
    assert not res.get("skipped", False), f"Analysis skipped: {res.get('reason')}"
    sb = res["score_breakdown"]
    assert sb["macd_divergence"] is True, "Expected macd_divergence to be True"
    # base mom_bonus before crossover penalty: +5 (positive EMA20 slope)
    # With divergence penalty: 5 - 8 = -3
    assert sb["momentum"] == -3, f"Expected mom_bonus -3, got {sb['momentum']}"

def test_macd_confirmed_crossover_bonus():
    """
    Inject a bullish MACD crossover with rising histogram slope.
    Assert mom_bonus increases by 5.
    Assert score_breakdown["macd_divergence"] == False.
    """
    p_bull_conf = [100.0] * 66 + [100.0, 97.0, 96.1429, 99.1429, 102.1429]
    df = generate_mock_df(p_bull_conf, bullish=True, high_volume=True)
    res = agent_engine.analyse(
        ticker="HDFCBANK.NS",
        bearish=False,
        df=df,
        market_context={"REGIME": "STRONG_BULL", "SECTOR_MOMENTUM": {}},
        is_nse_market_open=lambda: False,
        cfg_override={"MIN_TURNOVER_CR": 0.0, "MIN_SCORE": 0}
    )
    
    assert not res.get("skipped", False), f"Analysis skipped: {res.get('reason')}"
    sb = res["score_breakdown"]
    assert sb["macd_divergence"] is False, "Expected macd_divergence to be False"
    # base mom_bonus before crossover bonus: +5 (positive EMA20 slope) + 5 (RSI >= 60)
    # With confirmed crossover bonus: 5 + 5 + 5 = 15
    assert sb["momentum"] == 15, f"Expected mom_bonus 15, got {sb['momentum']}"

def test_sector_bonus_leading_sector():
    """
    Inject a bullish stock in the rank-1 sector.
    Assert score_breakdown["sector_bonus"] == 5.
    """
    # HDFCBANK is in "Banking" sector
    p_bull_conf = [100.0] * 66 + [100.0, 97.0, 96.1429, 99.1429, 102.1429]
    df = generate_mock_df(p_bull_conf, bullish=True, high_volume=True)
    res = agent_engine.analyse(
        ticker="HDFCBANK.NS",
        bearish=False,
        df=df,
        market_context={
            "REGIME": "STRONG_BULL",
            "SECTOR_MOMENTUM": {
                "sectors": [
                    {"name": "Banking", "change": 2.5},
                    {"name": "IT", "change": 1.5},
                    {"name": "Pharma", "change": 0.5}
                ]
            }
        },
        is_nse_market_open=lambda: False,
        cfg_override={"MIN_TURNOVER_CR": 0.0, "MIN_SCORE": 0}
    )
    
    assert not res.get("skipped", False), f"Analysis skipped: {res.get('reason')}"
    sb = res["score_breakdown"]
    assert sb["sector_bonus"] == 5, f"Expected sector_bonus 5, got {sb['sector_bonus']}"

def test_sector_bonus_rank2_sector():
    """
    Inject a bullish stock in the rank-2 sector.
    Assert score_breakdown["sector_bonus"] == 3.
    """
    # INFY is in "IT" sector
    p_bull_conf = [100.0] * 66 + [100.0, 97.0, 96.1429, 99.1429, 102.1429]
    df = generate_mock_df(p_bull_conf, bullish=True, high_volume=True)
    res = agent_engine.analyse(
        ticker="INFY.NS",
        bearish=False,
        df=df,
        market_context={
            "REGIME": "STRONG_BULL",
            "SECTOR_MOMENTUM": {
                "sectors": [
                    {"name": "Banking", "change": 2.5},
                    {"name": "IT", "change": 1.5},
                    {"name": "Pharma", "change": 0.5}
                ]
            }
        },
        is_nse_market_open=lambda: False,
        cfg_override={"MIN_TURNOVER_CR": 0.0, "MIN_SCORE": 0}
    )
    
    assert not res.get("skipped", False), f"Analysis skipped: {res.get('reason')}"
    sb = res["score_breakdown"]
    assert sb["sector_bonus"] == 3, f"Expected sector_bonus 3, got {sb['sector_bonus']}"

def test_stop_loss_inversion():
    """
    Construct a PIVOT_PLAY setup with a small body close to high,
    where L1 ends up above the pivot, creating a stop-loss inversion.
    Assert that the setup is skipped with reason 'stop_loss_inversion'.
    """
    n = 66
    dates = pd.date_range(end=datetime.now().date(), periods=n, freq='D')
    opens = [100.0] * n
    highs = [100.0] * n
    lows = [100.0] * n
    closes = [100.0] * n
    
    # Yesterday: close near high (100), low far below (90)
    # This leads to pivot = 96.67, and L1 = 99.08 (SL > Entry)
    closes[-2] = 100.0
    opens[-2] = 99.9  # small body -> PIVOT_PLAY
    highs[-2] = 100.0
    lows[-2] = 90.0
    
    # Today
    closes[-1] = 97.0
    opens[-1] = 100.0
    highs[-1] = 101.0
    lows[-1] = 96.0
    
    df = pd.DataFrame({
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": [1000000.0] * n
    }, index=dates)
    
    res = agent_engine.analyse(
        ticker="HDFCBANK.NS",
        bearish=False,
        df=df,
        market_context={"REGIME": "NEUTRAL", "SECTOR_MOMENTUM": {}},
        is_nse_market_open=lambda: False,
        cfg_override={"MIN_TURNOVER_CR": 0.0, "MIN_SCORE": 0}
    )
    
    assert res.get("skipped") is True, "Expected analysis to be skipped"
    assert res.get("skip_gate") == "stop_loss_inversion", f"Expected skip_gate stop_loss_inversion, got {res.get('skip_gate')}"
    assert "Stop-loss inversion" in res.get("reason"), f"Expected stop loss inversion reason, got {res.get('reason')}"

if __name__ == "__main__":
    tests = [
        ("test_macd_bearish_divergence_penalty", test_macd_bearish_divergence_penalty),
        ("test_macd_confirmed_crossover_bonus", test_macd_confirmed_crossover_bonus),
        ("test_sector_bonus_leading_sector", test_sector_bonus_leading_sector),
        ("test_sector_bonus_rank2_sector", test_sector_bonus_rank2_sector),
        ("test_stop_loss_inversion", test_stop_loss_inversion),
    ]
    
    print("=== Running Agent Engine Logic Tests ===")
    failed = False
    for name, test_func in tests:
        try:
            test_func()
            print(f"  [PASS] {name}")
        except AssertionError as ae:
            print(f"  [FAIL] {name}: {ae}")
            failed = True
        except Exception as e:
            print(f"  [ERROR] {name}: {e}")
            failed = True
            
    print("\nTest Run Complete.")
    sys.exit(1 if failed else 0)
