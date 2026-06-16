import pandas as pd
from datetime import datetime

def analyse(
    ticker: str,
    bearish: bool = False,
    cfg_override: dict = None,
    explain_skip: bool = False,
    df = None,
    market_context: dict = None,
    # These dependencies should be passed or imported locally
    SKIP_TICKERS=None,
    CFG=None,
    _download=None,
    get_nifty_returns=None,
    is_nse_market_open=None,
):
    """
    ProTrader Camarilla Scanner Agent Logic.
    Returns a signal dict on success, or {"skipped": True, "skip_gate": "<gate>", "reason": "<msg>"} on rejection.
    The caller should check result.get("skipped") to determine if it's a signal or a rejection.
    """
    def _skip(gate: str, reason: str, flags: list = None):
        res = {"symbol": ticker.replace(".NS", ""), "skipped": True, "skip_gate": gate, "reason": reason}
        if flags:
            res["flags"] = flags
        return res

    if SKIP_TICKERS and ticker in SKIP_TICKERS:
        return _skip("delisted", "Ticker is in the dead/delisted skip list")

    cfg = {**(CFG or {}), **(cfg_override or {})}
    
    # Extract injected market context
    ctx = market_context or {}
    nifty_ltp = ctx.get("NIFTY_LTP", 25000.0)
    nifty_ema50 = ctx.get("NIFTY_EMA50", 24000.0)
    nifty_prev_close = ctx.get("NIFTY_PREV_CLOSE", 25000.0)
    sector_dict = ctx.get("SECTOR_MOMENTUM", {})
    regime = ctx.get("REGIME", "SIDEWAYS")
    scan_time = ctx.get("SCAN_TIME_IST", "15:30")
    vol_mode = ctx.get("VOLUME_RATIO_MODE", "1.0x_standard")
    scan_mode = ctx.get("SCAN_MODE", "bullish")
    min_turnover_cr = float(cfg.get("MIN_TURNOVER_CR", 10.0))
    price_cap = cfg.get("PRICE_CAP")

    # 0. REGIME GATE
    if not bearish and regime == "STRONG_BEAR":
        return _skip("regime", "Regime gate: STRONG_BEAR blocks bullish setups")
    if bearish and regime == "STRONG_BULL":
        return _skip("regime", "Regime gate: STRONG_BULL blocks bearish setups")

    try:
        if df is None or df.empty:
            if _download:
                df = _download(ticker, use_cache_only=cfg.get("USE_CACHE_ONLY", False))
            if df is None or df.empty:
                return _skip("data_sanity", "No data available")

        if len(df) < 65:
            return _skip("data_sanity", "Insufficient data history")

        # Normalize column names — prefetch_batch may return capitalized or MultiIndex columns
        try:
            if isinstance(df.columns, pd.MultiIndex):
                # MultiIndex: take first level and lowercase
                df.columns = [str(c[0]).lower() for c in df.columns]
            else:
                df.columns = [str(c).lower() for c in df.columns]
        except Exception as col_err:
            return _skip("data_sanity", f"Column rename failed: {col_err} | cols={list(df.columns)}")

        # Keep only known OHLCV columns
        ohlcv = [c for c in df.columns if c in ("open", "high", "low", "close", "volume")]
        if "close" not in ohlcv:
            return _skip("data_sanity", f"No 'close' column after normalize. Got: {list(df.columns)[:10]}")
        df = df[ohlcv].dropna()

        # Calculate base metrics
        close = float(df["close"].iloc[-1])
        prev_close = float(df["close"].iloc[-2])
        high = float(df["high"].iloc[-1])
        low = float(df["low"].iloc[-1])
        volume = float(df["volume"].iloc[-1])
        today_open = float(df["open"].iloc[-1])
        
        prev_high = float(df["high"].iloc[-2])
        prev_low = float(df["low"].iloc[-2])
        prev_open = float(df["open"].iloc[-2])
        prev_vol = float(df["volume"].iloc[-2])
        
        # 1a. LIQUIDITY
        last_5_vols = df["volume"].iloc[-5:]
        last_5_closes = df["close"].iloc[-5:]
        turnovers = last_5_vols * last_5_closes
        avg_daily_turnover_cr = turnovers.median() / 10000000.0
        
        if avg_daily_turnover_cr < min_turnover_cr:
            return _skip("turnover", f"Low liquidity: {avg_daily_turnover_cr:.1f}Cr < {min_turnover_cr}Cr")

        # 1b. PRICE FLOOR
        min_price = float(cfg.get("MIN_PRICE", 50.0))
        if close < min_price:
            return _skip("price_floor", f"Price below minimum: {close} < {min_price}")
        if price_cap and close > float(price_cap):
            return _skip("price_floor", f"Price above cap: {close} > {price_cap}")

        # 1c. MOMENTUM FRESHNESS
        high_52w = float(df["high"].iloc[-252:].max()) if len(df) >= 252 else float(df["high"].max())
        low_52w = float(df["low"].iloc[-252:].min()) if len(df) >= 252 else float(df["low"].min())
        
        hv_high_idx = df["high"].iloc[-252:].idxmax() if len(df) >= 252 else df["high"].idxmax()
        days_since_52w_high = int((datetime.now().date() - hv_high_idx.date()).days)
        freshness_score = max(0.0, 1.0 - (days_since_52w_high / 180.0))
        if freshness_score == 0:
            return _skip("freshness", f"Stale momentum: {days_since_52w_high} days since 52W High")

        # 1d. DATA SANITY
        if (last_5_vols <= 100).any():
            return _skip("data_sanity", "Data sanity: zero or near-zero volume in last 5 days", flags=["BAD_DATA"])

        prev_range = prev_high - prev_low
        if prev_range < 0.005 * prev_close:
            return _skip("data_sanity", "Data sanity: prev range < 0.5% (possible bad tick)")
        range_52w = high_52w - low_52w
        if range_52w < 0.10 * prev_close:
            return _skip("data_sanity", "Data sanity: 52w range < 10%")

        # 2. CAMARILLA MATH (Nick Scott Variant)
        pivot = round((prev_high + prev_low + prev_close) / 3, 2)
        rng = prev_range
        cam = {
            "pivot": pivot,
            "H1": round(prev_close + rng * 1.1 / 12, 2),
            "H2": round(prev_close + rng * 1.1 / 6, 2),
            "H3": round(prev_close + rng * 1.1 / 4, 2),
            "H4": round(prev_close + rng * 1.1 / 2, 2),
            "L1": round(prev_close - rng * 1.1 / 12, 2),
            "L2": round(prev_close - rng * 1.1 / 6, 2),
            "L3": round(prev_close - rng * 1.1 / 4, 2),
            "L4": round(prev_close - rng * 1.1 / 2, 2),
        }
        if cam["H3"] >= cam["H4"] or cam["L3"] <= cam["L4"]:
            return _skip("data_sanity", "Camarilla math error")

        # 3. DYNAMIC ENTRY SETUP DETECTION
        # 14-period ATR
        highs = df["high"]
        lows = df["low"]
        closes = df["close"].shift(1)
        tr = pd.concat([highs - lows, (highs - closes).abs(), (lows - closes).abs()], axis=1).max(axis=1)
        atr_14_val = tr.rolling(14).mean().iloc[-1]
        atr_14 = (atr_14_val / close) * 100 if close else 2.0
        
        prev_body_pct = (prev_close - prev_open) / prev_open * 100
        body_threshold = max(1.5, 0.5 * atr_14)
        
        gap_pct = (today_open - prev_close) / prev_close * 100

        setup_type = ""
        entry_trigger = 0.0
        target = 0.0
        stop_loss = 0.0
        
        if prev_body_pct > body_threshold:
            setup_type = "H3_BREAKOUT"
            entry_trigger = cam["H3"]
            target = cam["H4"]
            stop_loss = max(cam["H2"], cam["L3"])
            if gap_pct > 2.0 and not bearish:
                return _skip("rr_ratio", f"Gap up > 2.0%, setup ruined")
        elif prev_body_pct < -body_threshold:
            setup_type = "L3_BREAKDOWN"
            entry_trigger = cam["L3"]
            target = cam["L4"]
            stop_loss = min(cam["L2"], cam["H3"])
        else:
            setup_type = "PIVOT_PLAY"
            entry_trigger = pivot # Simplified for pivot play
            target = cam["H3"] if not bearish else cam["L3"]
            stop_loss = cam["L1"] if not bearish else cam["H1"]
            
        # Directional logic overrides
        if bearish and setup_type == "H3_BREAKOUT":
            setup_type = "PIVOT_PLAY"
            entry_trigger = pivot
            target = cam["L3"]
            stop_loss = cam["H1"]
        elif not bearish and setup_type == "L3_BREAKDOWN":
            setup_type = "PIVOT_PLAY"
            entry_trigger = pivot
            target = cam["H3"]
            stop_loss = cam["L1"]

        # R:R Ratio
        risk = abs(entry_trigger - stop_loss)
        reward = abs(target - entry_trigger)
        rr_ratio = reward / risk if risk > 0 else 0
        if rr_ratio < 1.5:
            return _skip("rr_ratio", f"R:R ratio < 1.5 ({rr_ratio:.2f})")

        # Stop Distance Guard
        stop_distance_pct = abs(entry_trigger - stop_loss) / entry_trigger * 100 if entry_trigger else 0.0
        if stop_distance_pct < 0.5:
            return _skip("stop_too_tight", f"Stop distance too tight: {stop_distance_pct:.2f}% < 0.5%")


        # 4. TREND & MOMENTUM GATES
        # Relative Strength
        stock_ret_50d = (close - df["close"].iloc[-50]) / df["close"].iloc[-50] * 100 if len(df) >= 50 else 0
        stock_ret_63d = (close - df["close"].iloc[-63]) / df["close"].iloc[-63] * 100 if len(df) >= 63 else stock_ret_50d
        
        nifty_20d, nifty_50d, nifty_63d = (0.0, 0.0, 0.0)
        if get_nifty_returns:
            nifty_20d, nifty_50d, nifty_63d = get_nifty_returns()
            
        rs_50 = stock_ret_50d - nifty_50d
        rs_63 = stock_ret_63d - nifty_63d
        
        if not bearish and rs_50 < 0 and rs_63 < 0:
            return _skip("rs_gate", "RS_50 and RS_63 both negative")
        if bearish and rs_50 > 0 and rs_63 > 0:
            return _skip("rs_gate", "RS_50 and RS_63 both positive")

        # RSI Gate
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        rsi_series = 100 - (100 / (1 + rs))
        rsi_14 = float(rsi_series.iloc[-1])
        if pd.isna(rsi_14): rsi_14 = 50.0

        rsi_floor_map = {"STRONG_BULL": 40, "WEAK_BULL": 38, "SIDEWAYS": 35, "WEAK_BEAR": 32, "STRONG_BEAR": 28}
        floor = rsi_floor_map.get(regime, 35)
        
        if not bearish and rsi_14 < floor:
            return _skip("rsi_gate", f"RSI < {floor} ({rsi_14:.1f})")
        if bearish and rsi_14 > (100 - floor):
            return _skip("rsi_gate", f"RSI > {100-floor} ({rsi_14:.1f})")
        if bearish and rsi_14 < 20:
            return _skip("rsi_extreme", f"Oversold bear setup: RSI < 20 ({rsi_14:.1f})")

        # Live Dump Protection
        intraday_chg_pct = (close - prev_close) / prev_close * 100
        is_market_open = is_nse_market_open() if is_nse_market_open else True
        if is_market_open:
            if not bearish and intraday_chg_pct < -1.0:
                return _skip("live_dump", f"Live dump protection: < -1.0% intraday ({intraday_chg_pct:.1f}%)")
            if bearish and intraday_chg_pct > 1.0:
                return _skip("live_dump", f"Live pump protection: > 1.0% intraday ({intraday_chg_pct:.1f}%)")

        # EMA Trend Alignment
        df["ema10"] = df["close"].ewm(span=10, adjust=False).mean()
        df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
        df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()
        
        if cfg.get("EMA_50", False):
            if not bearish and close < df["ema50"].iloc[-1]:
                return _skip("ema_alignment", f"LTP < EMA50")
            if bearish and close > df["ema50"].iloc[-1]:
                return _skip("ema_alignment", f"LTP > EMA50")

        # 5. TIME-ADJUSTED VOLUME PROFILING
        vol_10d = df["volume"].iloc[-10:].median()
        vol_20d = df["volume"].iloc[-20:].median()
        baseline_vol = (vol_10d + vol_20d) / 2
        
        time_factor = 1.0
        if is_market_open and ":" in scan_time:
            try:
                hr, mn = map(int, scan_time.split(":"))
                mins = hr * 60 + mn
                if mins < 9*60+30: time_factor = 0.12
                elif mins < 10*60: time_factor = 0.22
                elif mins < 10*60+30: time_factor = 0.32
                elif mins < 11*60+30: time_factor = 0.45
                elif mins < 13*60: time_factor = 0.58
                elif mins < 14*60: time_factor = 0.68
                elif mins < 15*60: time_factor = 0.80
                elif mins < 15*60+20: time_factor = 0.92
                else: time_factor = 1.0
            except:
                pass
            
        expected_vol = baseline_vol * time_factor
        vol_ratio_live = volume / expected_vol if expected_vol > 0 else 1.0
        
        required_floor = 1.0
        if "0.5x" in vol_mode: required_floor = 0.5
        elif "1.2x" in vol_mode: required_floor = 1.2
        
        if vol_ratio_live < required_floor:
            return _skip("volume_floor", f"Low volume ratio: {vol_ratio_live:.2f} < {required_floor}")
            
        vol_spike = bool(vol_ratio_live >= 3.0)

        # 6. QUANT SCORE (0-100)
        score = 30
        flags = []
        if vol_spike: flags.append("VOL_SPIKE")
        
        # Extension Penalty
        extension_pct = (close - entry_trigger) / entry_trigger * 100 if not bearish else (entry_trigger - close) / entry_trigger * 100
        extension_threshold = max(3.0, 1.5 * atr_14)
        ext_penalty = 0
        if extension_pct > extension_threshold * 2:
            return _skip("extension", f"Overextended past 2x threshold ({extension_pct:.1f}%)")
        elif extension_pct > extension_threshold:
            ext_penalty = -20
            flags.append("EXTENDED")
            
        # Volume Bonus (max +25) — cumulative tiers, each replaces previous
        vol_bonus = 0
        if vol_ratio_live >= 3.0:   vol_bonus = 25
        elif vol_ratio_live >= 2.0: vol_bonus = 18
        elif vol_ratio_live >= 1.5: vol_bonus = 10
        elif vol_ratio_live >= 1.0: vol_bonus = 5
        
        # Momentum Bonus (max +20)
        mom_bonus = 0
        if not bearish and rsi_14 >= 60: mom_bonus += 5
        if not bearish and rsi_14 >= 70: mom_bonus += 5
        if bearish and rsi_14 <= 40: mom_bonus += 5
        if bearish and rsi_14 <= 30: mom_bonus += 5
        
        ema20_slope = (df["ema20"].iloc[-1] - df["ema20"].iloc[-2]) / df["ema20"].iloc[-2] * 100 if len(df) >= 2 else 0
        if not bearish and ema20_slope > 0: mom_bonus += 5
        if bearish and ema20_slope < 0: mom_bonus += 5
        
        # MACD
        df["macd_12"] = df["close"].ewm(span=12, adjust=False).mean()
        df["macd_26"] = df["close"].ewm(span=26, adjust=False).mean()
        df["macd"] = df["macd_12"] - df["macd_26"]
        df["macdsignal"] = df["macd"].ewm(span=9, adjust=False).mean()
        df["macdhist"] = df["macd"] - df["macdsignal"]
        
        hist_today = df["macdhist"].iloc[-1]
        hist_yest = df["macdhist"].iloc[-2] if len(df) >= 2 else 0
        hist_3_days_ago = df["macdhist"].iloc[-3] if len(df) >= 3 else hist_yest
        macd_hist_slope = hist_today - hist_3_days_ago
        macd_divergence = False
        
        macd_cross = (hist_yest < 0 and hist_today > 0)
        macd_cross_down = (hist_yest > 0 and hist_today < 0)
        
        if not bearish:
            if macd_cross and macd_hist_slope > 0:
                mom_bonus += 5
            elif macd_cross and macd_hist_slope <= 0:
                mom_bonus -= 8
                macd_divergence = True
        else:
            if macd_cross_down and macd_hist_slope < 0:
                mom_bonus += 5
            elif macd_cross_down and macd_hist_slope >= 0:
                mom_bonus -= 8
                macd_divergence = True
            
        # Freshness Bonus (max +10) — cumulative tiers, no double-count
        fresh_bonus = 0
        if freshness_score >= 0.8:   fresh_bonus = 10
        elif freshness_score >= 0.5: fresh_bonus = 7
        elif freshness_score >= 0.2: fresh_bonus = 3
        
        # R:R Bonus
        rr_bonus = 0
        if rr_ratio >= 3.0: rr_bonus += 10
        elif rr_ratio >= 2.0: rr_bonus += 5
        
        # Sector Bonus (max +5)
        from sector_rotation import STOCK_SECTOR_MAP
        sec_name = STOCK_SECTOR_MAP.get(ticker.replace(".NS", ""), "Unknown")
        sector_bonus = 0
        sector_rank = None
        if sec_name != "Unknown" and sector_dict:
            sec_list = sector_dict.get("sectors", [])
            sec_list_sorted = sorted(sec_list, key=lambda x: x.get("change", 0.0), reverse=True)
            for idx, s in enumerate(sec_list_sorted):
                if s.get("name") == sec_name:
                    sector_rank = idx + 1
                    break
            
            if sector_rank is not None:
                total_sectors = len(sec_list_sorted)
                if not bearish:
                    if sector_rank == 1:   sector_bonus = 5
                    elif sector_rank == 2: sector_bonus = 3
                    else:                  sector_bonus = 0
                else:
                    if sector_rank == total_sectors:     sector_bonus = 5
                    elif sector_rank == total_sectors-1: sector_bonus = 3
                    else:                                sector_bonus = 0
        
        score_breakdown = {
            "volume": vol_bonus,
            "momentum": mom_bonus,
            "freshness": fresh_bonus,
            "rr": rr_bonus,
            "extension": ext_penalty,
            "sector_bonus": sector_bonus,
            "hard_cap": False,
            "macd_divergence": macd_divergence
        }
        
        score = score + vol_bonus + mom_bonus + fresh_bonus + rr_bonus + ext_penalty + sector_bonus
        
        # HARD CAPS
        if (not bearish and rsi_14 < 40) or (bearish and rsi_14 > 60):
            score = min(score, 45)
            score_breakdown["hard_cap"] = True
            
        score = max(0, min(100, score))
        
        conf_grade = "REJECT"
        if score >= 80: conf_grade = "A"
        elif score >= 65: conf_grade = "B"
        elif score >= 50: conf_grade = "C"
        else: 
            return _skip("score_floor", f"Score < 50 ({score})")

        entry_status = "FRESH"
        if "EXTENDED" in flags: entry_status = "EXTENDED"
        
        # H5/H6 mapping
        h5_val = (high_52w / low_52w) * close if low_52w > 0 else close * 1.05
        h6_val = h5_val + 1.168 * (h5_val - cam["H4"])

        return {
            "symbol"         : ticker.replace(".NS", ""),
            "setup_type"     : setup_type,
            "ltp"            : close,
            "entry_trigger"  : entry_trigger,
            "target"         : target,
            "stop_loss"      : stop_loss,
            "rr_ratio"       : round(rr_ratio, 2),
            "dist_pct"       : round(extension_pct, 2),
            "quant_score"    : score,
            "conf_grade"     : conf_grade,
            "entry_status"   : entry_status,
            "vol_ratio_live" : round(vol_ratio_live, 2),
            "vol_spike"      : vol_spike,
            "rs_50"          : round(rs_50, 2),
            "rs_63"          : round(rs_63, 2),
            "rsi_14"         : round(rsi_14, 2),
            "regime"         : regime,
            "sector"         : sec_name, 
            "sector_rank"    : sector_rank,
            "score_breakdown": score_breakdown,
            "camarilla"      : cam,
            "flags"          : flags,
            "scan_meta"      : {
                "scanned_at"   : scan_time,
                "formula"      : "camarilla_nick_scott",
                "regime"       : regime,
            },
            
            # Legacy mapping for dashboard compatibility
            "price": close,
            "change": round(intraday_chg_pct, 2),
            "prevClose": round(prev_close, 2),
            "score": score,
            "confidence": score, # Changed to score for dashboard sorting
            "signal_strength": "Institutional Strong" if score >= 80 else "Moderate",
            "signal_type": "Bull" if not bearish else "Bear",
            "entry": entry_trigger,
            "target2": target,
            "camarilla_h4": cam["H4"],
            "camarilla_l3": cam["L3"],
            "camarilla_l4": cam["L4"],
            "camarilla_h5": round(h5_val, 2),
            "camarilla_h6": round(h6_val, 2),
            "vol_ratio": round(vol_ratio_live, 2),
            "candle": "Bull" if close > today_open else "Bear",
            "ema10": df["ema10"].iloc[-1],
            "ema20": df["ema20"].iloc[-1],
            "ema50": df["ema50"].iloc[-1],
            "ema200": df["ema200"].iloc[-1],
            "days": days_since_52w_high,
            "rs_pct": round(rs_50, 2),
            "turnover_score": round(avg_daily_turnover_cr, 2),
            "sparkline": df["close"].iloc[-7:].tolist() if len(df) >= 7 else [],
            "rvol": round(vol_ratio_live, 2),
            "risk_percentage": round(stop_distance_pct, 2),
            "rr": round(rr_ratio, 2),
        }

    except Exception as exc:
        import traceback
        tb = traceback.format_exc()
        return _skip("error", f"Exception: {exc} | TB: {tb.splitlines()[-3:]}")


def get_market_context(scan_mode: str = "bullish", vol_mode: str = "1.0x_standard") -> dict:
    from regime import get_regime
    from datetime import datetime
    
    r = get_regime()
    regime = r.get("regime", "NEUTRAL")
    nifty_close = r.get("nifty_close", 25000.0)
    nifty_ema50 = r.get("nifty_ema50", 24000.0)
    nifty_prev_close = r.get("nifty_prev_close", nifty_close)
    
    return {
        "NIFTY_LTP": nifty_close,
        "NIFTY_EMA50": nifty_ema50,
        "NIFTY_PREV_CLOSE": nifty_prev_close,
        "SECTOR_MOMENTUM": {},
        "REGIME": regime,
        "SCAN_TIME_IST": datetime.now().strftime("%H:%M"),
        "VOLUME_RATIO_MODE": vol_mode,
        "SCAN_MODE": scan_mode
    }


def validate_journal_entry(entry_price: float, ltp: float, symbol: str) -> None:
    assert abs(entry_price - ltp) / ltp < 0.15, \
        f"Suspicious entry price {entry_price} vs LTP {ltp} for {symbol} — skipping journal write"

