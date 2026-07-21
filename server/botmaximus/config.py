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

    # pipeline flow
    queue_maxsize: int = 1000          # bounded queues → backpressure (§2)
    tick_throttle_ms: int = 1000       # store at most one price tick per second

    # staleness budgets, ms (§9) — a dataset over budget is excluded from live use
    budget_price_tick_ms: int = 3_000
    budget_ohlcv_1m_ms: int = 75_000

    # retention / TTL, seconds (§10)
    ttl_price_ticks_s: int = 7 * 24 * 3600        # 7 days of 1/s ticks
    ttl_ohlcv_1m_s: int = 10 * 365 * 24 * 3600    # keep candles ~10 years

    # quality gate (§8)
    max_minute_move_pct: float = 5.0   # phantom-tick jump threshold vs previous record
    clock_skew_tolerance_ms: int = 2_000
    illiquid_gap_ms: int = 15_000      # no tick for this long → illiquid_window flag

    # declared now, consumed by the RAG/reaction layers later (§10)
    embargo_window_s: int = 60
    reaction_lags: str = "1m,5m,15m,1h,4h,1d"

    # api
    api_host: str = "127.0.0.1"
    api_port: int = 8300
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"


settings = Settings()
