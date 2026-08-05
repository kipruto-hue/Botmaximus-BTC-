"""Bybit V5 payload -> canonical envelope.

Written against frames captured from the live V5 public stream, not from
documentation: the collectors this feeds already have one venue-migration scar
(Binance's 2026-04-23 routing change connected fine and delivered nothing), and
a parser written from memory fails the same silent way.

**The output payload keys are identical to the Binance parser's.** That is the
whole point of the canonical envelope: features, coverage, the backtester and
the DSL must not be able to tell which venue produced a bar. Anything that
leaks a venue-specific key here becomes a venue-specific bug three layers away.

Shape differences that mattered:

- kline carries `confirm`; only closed candles are stored. `end` is the close
  time (start + 59_999 ms), matching Binance's `k.T`.
- kline has **no trade count**. Binance supplied `trades`; Bybit does not, so
  the field is None rather than 0 — a fabricated zero would be indistinguishable
  from a genuinely tradeless minute.
- `publicTrade.S` is the **taker's** side, the inverse of Binance's
  `buyer_is_maker`.
- `tickers` is a snapshot-then-delta topic and deltas carry only changed fields.
  Merging is the collector's job; by the time a payload reaches here it is a
  complete state.
- **Epoch timestamps arrive as ints on the websocket and as decimal STRINGS on
  the REST endpoints** (`fundingRateTimestamp`, `timestamp`, `nextFundingTime`).
  Every one is coerced with `int()` rather than trusted, because the failure is
  a TypeError deep inside the envelope on the backfill path only — invisible
  until a history sync runs.
"""
from __future__ import annotations

from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.envelope import Envelope, from_epoch_ms


class BybitParser:
    def parse(self, item: RawItem) -> Envelope:
        fn = {
            "btc_price_tick": self._parse_trade,
            "btc_ohlcv_1m": self._parse_kline,
            "btc_funding": self._parse_ticker,
            "btc_liquidation": self._parse_liquidation,
            "btc_orderbook": self._parse_book,
            "btc_open_interest": self._parse_open_interest,
            "btc_funding_8h": self._parse_funding_hist,
            "btc_oi_5m": self._parse_oi_hist,
        }.get(item.dataset_id)
        if fn is None:
            raise ValueError(f"unknown dataset_id {item.dataset_id}")
        env = fn(item)
        env.backfill = item.backfill
        return env

    # ---- websocket ----
    def _parse_trade(self, item: RawItem) -> Envelope:
        """publicTrade row: {T, s, S, v, p, L, i, BT, seq}."""
        d = item.raw
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(int(d["T"])),
            collection_time=item.collection_time,
            payload={
                "price": float(d["p"]),
                "qty": float(d["v"]),
                "trade_id": d.get("i"),
                # Bybit reports the TAKER side; Binance reports whether the
                # buyer was the maker. Inverting here keeps the canonical
                # meaning identical across venues.
                "buyer_is_maker": d.get("S") == "Sell",
            },
        )

    def _parse_kline(self, item: RawItem) -> Envelope:
        """kline row. Only confirmed candles should reach this."""
        k = item.raw
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(int(k["end"])),        # close time, as Binance k.T
            collection_time=item.collection_time,
            payload={
                "open": float(k["open"]),
                "high": float(k["high"]),
                "low": float(k["low"]),
                "close": float(k["close"]),
                "volume": float(k["volume"]),
                "quote_volume": float(k["turnover"]),
                # Bybit does not publish a trade count. None, never 0: a
                # fabricated zero is indistinguishable from a real quiet minute.
                "trades": None,
                "open_time": from_epoch_ms(int(k["start"])),
                "close_time": from_epoch_ms(int(k["end"])),
            },
        )

    def _parse_ticker(self, item: RawItem) -> Envelope:
        """Merged `tickers` state: funding rate + mark/index price.

        Bybit folds into one topic what Binance split across markPrice@1s and a
        REST open-interest poll. The collector merges deltas before enqueueing,
        so every field is present here.
        """
        d = item.raw
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(int(d["_ts"])),
            collection_time=item.collection_time,
            payload={
                "mark_price": float(d["markPrice"]),
                "index_price": float(d["indexPrice"]),
                "funding_rate": float(d["fundingRate"]),
                "next_funding_time": from_epoch_ms(int(d["nextFundingTime"])),
            },
        )

    def _parse_liquidation(self, item: RawItem) -> Envelope:
        """allLiquidation row: {T, s, S, v, p}.

        **The direction convention is not verified.** Binance's forceOrder used
        an order side where SELL meant a long was force-closed. Bybit documents
        `S` as a *position* side, which would invert the meaning. Getting it
        backwards would silently flip the sign of any liquidation-pressure
        feature, so the raw venue value is preserved in `side_raw` and the
        interpreted field is left explicitly unresolved rather than guessed.

        No seed uses liquidation direction today (§8.1 seeds are macro and
        structural), so nothing is blocked. Resolve this by correlating
        liquidation clusters against price direction over real data BEFORE any
        strategy reads it.
        """
        d = item.raw
        qty = float(d["v"])
        price = float(d["p"])
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(int(d["T"])),
            collection_time=item.collection_time,
            payload={
                "side": d.get("S"),
                "side_raw": d.get("S"),
                "side_convention": "unverified_bybit_allLiquidation",
                "price": price,
                "qty": qty,
                "notional_usd": price * qty,
            },
        )

    def _parse_book(self, item: RawItem) -> Envelope:
        """A book snapshot the collector rebuilt from snapshot + deltas.

        Arrives already sorted and trimmed to the top N levels, in the same
        {b, a, E} shape the Binance depth parser consumed, so the summary
        arithmetic below is shared behaviour rather than a second version of it.
        """
        d = item.raw
        bids = [[float(p), float(q)] for p, q in d["b"]]
        asks = [[float(p), float(q)] for p, q in d["a"]]
        bid_vol = sum(q for _, q in bids)
        ask_vol = sum(q for _, q in asks)
        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 0.0
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(int(d["E"])),
            collection_time=item.collection_time,
            payload={
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": round(best_ask - best_bid, 8),
                "bid_volume": bid_vol,
                "ask_volume": ask_vol,
                "imbalance": (bid_vol - ask_vol) / (bid_vol + ask_vol) if bid_vol + ask_vol else 0.0,
                "bids": bids,
                "asks": asks,
            },
        )

    def _parse_open_interest(self, item: RawItem) -> Envelope:
        """Live OI, carried on `tickers` rather than a REST poll."""
        d = item.raw
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(int(d["_ts"])),
            collection_time=item.collection_time,
            payload={"open_interest": float(d["openInterest"])},
        )

    # ---- REST history ----
    def _parse_funding_hist(self, item: RawItem) -> Envelope:
        """/v5/market/funding/history row: {symbol, fundingRate,
        fundingRateTimestamp}. The settled series the cost model charges from."""
        d = item.raw
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(int(d["fundingRateTimestamp"])),
            collection_time=item.collection_time,
            payload={"funding_rate": float(d["fundingRate"])},
        )

    def _parse_oi_hist(self, item: RawItem) -> Envelope:
        """/v5/market/open-interest row: {openInterest, timestamp}.

        Bybit serves this back 2+ years, measured. Binance's equivalent was
        capped at 30 days, which is why OI-based strategies could not be
        evaluated at all once the 90-day holdout was sealed.

        `openInterest` is in base units (BTC); there is no USD field on this
        endpoint, so the value is derived as None rather than invented.
        """
        d = item.raw
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(int(d["timestamp"])),
            collection_time=item.collection_time,
            payload={
                "open_interest": float(d["openInterest"]),
                "open_interest_value_usd": None,
            },
        )
