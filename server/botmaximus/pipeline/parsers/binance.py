"""Binance payload → canonical envelope. `event_time` always comes from the
exchange timestamp, never the local clock (§1.1).
"""
from __future__ import annotations

from botmaximus.pipeline.bus import RawItem
from botmaximus.pipeline.envelope import Envelope, from_epoch_ms


class BinanceParser:
    def parse(self, item: RawItem) -> Envelope:
        if item.dataset_id == "btc_price_tick":
            return self._parse_agg_trade(item)
        if item.dataset_id == "btc_ohlcv_1m":
            return self._parse_kline(item)
        raise ValueError(f"unknown dataset_id {item.dataset_id}")

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
