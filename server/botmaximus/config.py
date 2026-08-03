"""BOTMAXIMUS (BTC) data-layer configuration — the §10 parameter surface.

Every value can be overridden via environment variables or a `.env` file
in the server root (e.g. `MONGO_URI=...`).
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # storage
    mongo_uri: str = "mongodb://localhost:27017"
    db_name: str = "botmaximus"

    # instrument & source
    symbol: str = "BTCUSDT"
    binance_ws_url: str = "wss://stream.binance.com:9443/stream"
    binance_futures_ws_url: str = "wss://fstream.binance.com"  # routed: /market, /public
    binance_rest_url: str = "https://api.binance.com"
    binance_futures_rest_url: str = "https://fapi.binance.com"

    # pipeline flow
    queue_maxsize: int = 1000          # bounded queues → backpressure (§2)
    tick_throttle_ms: int = 1000       # store at most one price tick per second
    funding_throttle_ms: int = 30_000  # mark-price stream is 1/s; funding moves slowly
    orderbook_throttle_ms: int = 5_000 # depth snapshots stored at most once per 5s
    oi_poll_s: int = 30                # open interest REST poll cadence

    # OHLCV gap backfill (§11 step 4): heal candles missed during dead sockets
    backfill_enabled: bool = True
    backfill_scan_minutes: int = 180            # periodic cycles look this far back
    backfill_startup_scan_minutes: int = 10_080 # first cycle after boot: 7 days
    backfill_check_s: int = 60

    # staleness budgets, ms (§9) — a dataset over budget is excluded from live use
    budget_price_tick_ms: int = 3_000
    budget_ohlcv_1m_ms: int = 75_000
    budget_funding_ms: int = 90_000
    budget_open_interest_ms: int = 120_000
    budget_orderbook_ms: int = 30_000
    # liquidations are event-driven and legitimately silent for hours → no budget

    # retention / TTL, seconds (§10)
    ttl_price_ticks_s: int = 7 * 24 * 3600        # 7 days of 1/s ticks
    ttl_ohlcv_1m_s: int = 10 * 365 * 24 * 3600    # keep candles ~10 years
    ttl_funding_s: int = 2 * 365 * 24 * 3600
    ttl_open_interest_s: int = 2 * 365 * 24 * 3600
    ttl_liquidations_s: int = 365 * 24 * 3600
    ttl_orderbook_s: int = 30 * 24 * 3600         # heavy — keep 30 days of snapshots

    # quality gate (§8)
    max_minute_move_pct: float = 5.0   # phantom-tick jump threshold vs previous record
    clock_skew_tolerance_ms: int = 2_000
    illiquid_gap_ms: int = 15_000      # no tick for this long → illiquid_window flag

    # declared now, consumed by the RAG/reaction layers later (§10)
    embargo_window_s: int = 60
    reaction_lags: str = "1m,5m,15m,1h,4h,1d"

    # ---- decision layer (Decision & Execution Master Prompt §3) ----
    # Locked by operator 2026-07-23; layers that need a None value must fail loudly.
    holding_period_target_s: int = 300          # ~5min holds, high frequency
    order_style: str = "taker"
    max_drawdown_kill_pct: float = 15.0         # L3 master kill, from equity peak
    risk_per_trade_pct: float = 0.25
    max_open_risk_pct: float = 1.0
    daily_loss_limit_pct: float = 3.0           # L2 portfolio halt
    leverage_cap: float = 3.0
    margin_mode: str = "isolated"
    position_mode: str = "one_way"
    starting_equity_paper: float = 10_000.0
    liq_stop_buffer_pct: float = 20.0           # stop sits ≥ this % of stop-distance inside liq price
    llm_model: str | None = None                # set at Scrutiny Gate pass; consumers raise if unset
    paper_fill_model: str | None = None         # set at execution pass

    # ---- backtest cost model (§5.3) — never frictionless ----
    taker_fee_rate: float = 0.0005              # Binance USDⓈ-M taker, each side
    slippage_bps: float = 1.0                   # conservative constant floor, never zero
    latency_bars: int = 1                       # decision→fill delay, in 1m bars

    # ---- backtest validation gate (§5.5) ----
    bt_walkforward_folds: int = 4
    bt_purge_bars: int = 5                      # ≈ holding period; drop label-overlapping samples
    bt_embargo_bars: int = 5
    bt_min_trades: int = 30                     # statistical power floor
    bt_max_drawdown_pct: float = 25.0
    bt_min_regimes_positive: int = 2
    bt_candidate_trials: int = 1                # N trials for deflated-Sharpe correction

    # ---- strategy DSL & generation (Strategy DSL Master Prompt §2) ----
    # DSL-side params are set. Generator-side params are declared here but left
    # unset on purpose: §2 requires the build to fail loudly rather than invent
    # a value, and Pass C2 is where they get decided.
    max_holding_bars_cap: int = 60          # DSL time_exit ceiling, 1m bars (1h)
    default_time_exit_bars: int = 5         # ≈ holding_period_target_s
    diversity_threshold: float = 0.85       # §5.7 similarity ceiling for dedupe
    regime_vol_lookback: int = 60           # realised-vol window for the vol axis
    regime_vol_ref_lookback: int = 1440     # trailing reference for high/low vol
    # Bars the regime label must stay out-of-scope before an open position is
    # invalidated. On the 1m grid the 6-bucket label flips constantly; at 1 the
    # Gate-3 trend seed exited on `regime` 307 times in 336 trades, paying a
    # taker fee for each flicker. Not a DSL field — it is a friction control the
    # operator owns, not a strategy choice the generator may tune.
    regime_invalidation_confirm_bars: int = 5
    vector_store: str = "chroma"            # §6.2 memory; operator lock 2026-07-23

    generation_llm: str | None = None       # §2 GENERATION_LLM — C2, fail loudly
    candidate_cap_per_cycle: int | None = None   # protects multiple-testing math
    generation_cadence_s: int | None = None      # throttled to backtest throughput
    decay_repair_trigger: str | None = None      # §7.1 sequential-test trigger

    # api
    api_host: str = "127.0.0.1"
    api_port: int = 8300
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"


settings = Settings()
