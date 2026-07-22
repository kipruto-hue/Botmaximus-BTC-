"""Binance payload → canonical envelope. `event_time` always comes from the
exchange timestamp, never the local clock (§1.1).
"""
from __future__ import annotations

from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.envelope import Envelope, from_epoch_ms


class BinanceParser:
    def parse(self, item: RawItem) -> Envelope:
        fn = {
            "btc_price_tick": self._parse_agg_trade,
            "btc_ohlcv_1m": self._parse_kline,
            "btc_funding": self._parse_mark_price,
            "btc_liquidation": self._parse_force_order,
            "btc_orderbook": self._parse_depth,
            "btc_open_interest": self._parse_open_interest,
        }.get(item.dataset_id)
        if fn is None:
            raise ValueError(f"unknown dataset_id {item.dataset_id}")
        env = fn(item)
        env.backfill = item.backfill
        return env

    def _parse_agg_trade(self, item: RawItem) -> Envelope:
        d = item.raw
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(d["T"]),          # trade time
            collection_time=item.collection_time,
            payload={
                "price": float(d["p"]),
                "qty": float(d["q"]),
                "trade_id": d.get("a"),
                "buyer_is_maker": bool(d.get("m")),
            },
        )

    def _parse_kline(self, item: RawItem) -> Envelope:
        k = item.raw["k"]
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(k["T"]),          # candle close time
            collection_time=item.collection_time,
            payload={
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "volume": float(k["v"]),
                "quote_volume": float(k["q"]),
                "trades": int(k["n"]),
                "open_time": from_epoch_ms(k["t"]),
                "close_time": from_epoch_ms(k["T"]),
            },
        )

    def _parse_mark_price(self, item: RawItem) -> Envelope:
        """Futures markPrice@1s: mark/index price + current funding rate."""
        d = item.raw
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(d["E"]),
            collection_time=item.collection_time,
            payload={
                "mark_price": float(d["p"]),
                "index_price": float(d["i"]),
                "funding_rate": float(d["r"]),
                "next_funding_time": from_epoch_ms(d["T"]),
            },
        )

    def _parse_force_order(self, item: RawItem) -> Envelope:
        """Futures forceOrder: one liquidated position."""
        o = item.raw["o"]
        qty = float(o["q"])
        price = float(o.get("ap") or o["p"])
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(o["T"]),
            collection_time=item.collection_time,
            payload={
                "side": o["S"],                 # SELL = long liquidated, BUY = short
                "price": price,
                "qty": qty,
                "notional_usd": price * qty,
                "order_status": o.get("X"),
            },
        )

    def _parse_depth(self, item: RawItem) -> Envelope:
        """Futures depth20 snapshot → book summary + full levels."""
        d = item.raw
        bids = [[float(p), float(q)] for p, q in d["b"]]
        asks = [[float(p), float(q)] for p, q in d["a"]]
        bid_vol = sum(q for _, q in bids)
        ask_vol = sum(q for _, q in asks)
        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 0.0
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(d["E"]),
            collection_time=item.collection_time,
            payload={
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": round(best_ask - best_bid, 8),
                "bid_volume": bid_vol,
                "ask_volume": ask_vol,
                # -1 all asks … +1 all bids: standing imbalance of the top 20 levels
                "imbalance": (bid_vol - ask_vol) / (bid_vol + ask_vol) if bid_vol + ask_vol else 0.0,
                "bids": bids,
                "asks": asks,
            },
        )

    def _parse_open_interest(self, item: RawItem) -> Envelope:
        """Futures REST openInterest: {symbol, openInterest, time}."""
        d = item.raw
        return Envelope(
            dataset_id=item.dataset_id, source=item.source, symbol=item.symbol,
            event_time=from_epoch_ms(d["time"]),
            collection_time=item.collection_time,
            payload={"open_interest": float(d["openInterest"])},
        )
